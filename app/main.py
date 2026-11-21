"""FastAPI 应用工厂与路由。

并发与一致性语义：

- **写**：进程内写锁 + SQLite ``BEGIN IMMEDIATE``（两个 API 实例共用同一
  SQLite 卷时按提交顺序串行）。校验（求解）、关系变更、版号 +1 与回包留存
  （response_log）在同一事务中完成；任一环节失败整体回滚，关系与版号照旧。
- **读**：独立的只读连接 + 只读事务，不进入写锁。写事务未提交时，整站与
  按层位查询直接返回最近一次已提交的同版分组、成员、年代与版号；提交之后
  发起的新查询读到新版。
- **回包**：写事务提交后再读一次最新已提交状态生成回包。刚登记的对接若在
  回包前被另一端撤销，回包会带上撤销后的分组；而当次提交时留存的回包仍可
  按（遗址, version）在任一实例取回。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Callable, Iterator

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from . import db
from .schemas import (
    CorrelationCreate,
    SiteCreate,
    StratumCreate,
    SuperpositionCreate,
)
from .service import load_and_solve, serialize_solution, serialize_stratum
from .solver import Solution

MAX_STRATA_PER_SITE = 40


class ConflictError(HTTPException):
    """语义冲突（409）：约束无解或违反业务规则。"""

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        content: dict[str, Any] = {"detail": message}
        if details:
            content.update(details)
        super().__init__(status_code=409, detail=content)


class _Infeasible(Exception):
    """事务内部使用：新记录使整站约束无解，触发回滚。

    args = (message, contradiction, previous_solution)；contradiction 由
    待录关系与校验时的旧记录生成，previous_solution 对应这份旧记录。
    """


def _default_db_path() -> str:
    configured = os.environ.get("DB_PATH")
    if configured:
        return configured
    # 容器内 /data 由卷挂载；本地裸跑时回退到工作目录下的 data/
    if os.path.isdir("/data") and os.access("/data", os.W_OK):
        return "/data/archaeo.db"
    return os.path.join(os.getcwd(), "data", "archaeo.db")


def create_app(db_path: str | None = None) -> FastAPI:
    db_path = db_path or _default_db_path()
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    write_conn = db.connect(db_path)
    db.init_db(write_conn)
    read_conn = db.connect(db_path)
    write_lock = threading.Lock()  # 进程内写串行化；跨实例由 BEGIN IMMEDIATE 保证
    read_lock = threading.Lock()   # 仅保护 read_conn 对象本身，读不与写互斥

    @asynccontextmanager
    async def _lifespan(_app: FastAPI):
        yield
        write_conn.close()
        read_conn.close()

    app = FastAPI(
        title="考古地层对接 API",
        version="1.0.0",
        description=(
            "录入同一遗址（最多 40 个层位）的探方、层位编号与可能沉积年代段"
            "（整年闭区间，可含空档），登记跨探方层位对接与下层→上层叠压关系；"
            "直接或间接对接的层位视为同一次沉积并共用一个年份，自动完成分组、"
            "成环检测与可行年代段计算。每次成功写入的完整回包随版号一同留存，"
            "可按遗址与版号在任一实例取回。"
        ),
        lifespan=_lifespan,
    )
    app.state.write_conn = write_conn
    app.state.read_conn = read_conn
    app.state.write_lock = write_lock
    app.state.read_lock = read_lock
    app.state.db_path = db_path
    # 测试钩子：写事务提交后、生成回包前调用（用于构造跨实例竞态）
    app.state.post_commit_hook: Callable[[], None] | None = None

    # ---------- 内部辅助 ----------

    def _require_site(conn: sqlite3.Connection, site_id: int) -> sqlite3.Row:
        site = db.get_site(conn, site_id)
        if site is None:
            raise HTTPException(404, f"遗址 {site_id} 不存在")
        return site

    def _require_stratum(
        conn: sqlite3.Connection, stratum_id: int, site_id: int
    ) -> sqlite3.Row:
        row = db.get_stratum(conn, stratum_id)
        if row is None or row["site_id"] != site_id:
            raise HTTPException(
                404, f"层位 {stratum_id} 不属于遗址 {site_id} 或不存在"
            )
        return row

    def _solve_payload(conn: sqlite3.Connection, site_id: int) -> dict[str, Any]:
        solution = load_and_solve(conn, site_id)
        return serialize_solution(solution, db.get_version(conn))

    def _contradiction_of(solution: Solution) -> dict[str, Any]:
        """从一次无解求解中提取矛盾见证。"""
        c = solution.contradiction
        if c is None:  # 理论上不会发生
            return {"reason": "UNKNOWN", "message": "约束不成立", "witness": []}
        return {
            "reason": c.reason,
            "message": c.message,
            "witness": [
                {"kind": w.kind, "record_id": w.record_id, "detail": w.detail}
                for w in c.witness
            ],
        }

    def _dumps(payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False)

    @contextmanager
    def _write_txn() -> Iterator[sqlite3.Connection]:
        try:
            with db.transaction(write_conn) as conn:
                yield conn
        except sqlite3.IntegrityError as exc:
            # 唯一性兜底：并发下防御性映射为 409（正常路径已在事务内预检）
            raise ConflictError(f"写入违反约束：{exc}") from exc

    def _record_and_validate(
        conn: sqlite3.Connection,
        site_id: int,
        message: str,
        insert: Callable[[sqlite3.Connection], Any],
    ) -> dict[str, Any]:
        """事务内：先算旧记录的可行解，再写入新记录并重算、留存回包。

        无解则抛 _Infeasible，由事务回滚保证关系与版号照旧。
        """
        previous = _solve_payload(conn, site_id)  # 校验时的旧记录对应的解
        record = insert(conn)
        new_version = db.bump_version(conn)
        solution = load_and_solve(conn, site_id)  # 待录关系 + 旧记录
        if not solution.feasible:
            raise _Infeasible(message, _contradiction_of(solution), previous)
        payload = {
            "record": record,
            "solution": serialize_solution(solution, new_version),
        }
        db.store_response(conn, site_id, new_version, _dumps(payload))
        return payload

    def _commit_payload(
        conn: sqlite3.Connection, site_id: int, key: str, value: Any
    ) -> dict[str, Any]:
        """事务内：无需再校验的写入（建站/撤销），重算并留存回包。"""
        new_version = db.bump_version(conn)
        payload = {
            key: value,
            "solution": serialize_solution(
                load_and_solve(conn, site_id), new_version
            ),
        }
        db.store_response(conn, site_id, new_version, _dumps(payload))
        return payload

    def _fresh_solution(site_id: int) -> dict[str, Any]:
        """提交后另开只读事务，读取最新已提交状态（可能已含另一端的新提交）。"""
        with read_lock:
            with db.read_transaction(read_conn):
                return _solve_payload(read_conn, site_id)

    def _respond(site_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        """写事务提交后生成回包：record/deleted 取自本次提交，solution 读最新版。

        若刚登记的对接在回包前被另一端撤销，回包会带上撤销后的分组。
        """
        hook = app.state.post_commit_hook
        if hook is not None:
            hook()
        key = "record" if "record" in payload else "deleted"
        return {key: payload[key], "solution": _fresh_solution(site_id)}

    def _as_conflict(exc: _Infeasible) -> ConflictError:
        message, contradiction, previous = exc.args
        return ConflictError(
            message,
            {"contradiction": contradiction, "previous_solution": previous},
        )

    # ---------- 遗址 ----------

    @app.post("/sites", status_code=201, tags=["sites"])
    def create_site(body: SiteCreate):
        with write_lock:
            with _write_txn() as conn:
                if db.get_site_by_name(conn, body.name) is not None:
                    raise ConflictError(f"遗址名称 {body.name!r} 已存在")
                site = db.create_site(conn, body.name)
                payload = _commit_payload(conn, site["id"], "record", dict(site))
        return _respond(payload["record"]["id"], payload)

    @app.get("/sites", tags=["sites"])
    def list_sites():
        with read_lock:
            with db.read_transaction(read_conn):
                return [dict(r) for r in db.list_sites(read_conn)]

    @app.get("/sites/{site_id}", tags=["sites"])
    def get_site(site_id: int):
        with read_lock:
            with db.read_transaction(read_conn):
                return dict(_require_site(read_conn, site_id))

    # ---------- 层位 ----------

    @app.post("/sites/{site_id}/strata", status_code=201, tags=["strata"])
    def create_stratum(site_id: int, body: StratumCreate):
        intervals = [(iv.year_from, iv.year_to) for iv in body.intervals]
        with write_lock:
            try:
                with _write_txn() as conn:
                    _require_site(conn, site_id)
                    if db.count_strata(conn, site_id) >= MAX_STRATA_PER_SITE:
                        raise ConflictError(
                            f"同一遗址最多 {MAX_STRATA_PER_SITE} 个层位，已达上限"
                        )
                    if db.find_stratum(
                        conn, site_id, body.trench, body.layer_no
                    ) is not None:
                        raise ConflictError(
                            f"探方 {body.trench} 的层位 {body.layer_no} 已存在"
                        )
                    payload = _record_and_validate(
                        conn,
                        site_id,
                        "新层位的年代段与现有约束无共同解，已拒绝录入",
                        lambda c: serialize_stratum(
                            c,
                            db.create_stratum(
                                c, site_id, body.trench, body.layer_no,
                                body.note, intervals,
                            ),
                        ),
                    )
            except _Infeasible as exc:
                raise _as_conflict(exc)
        return _respond(site_id, payload)

    @app.get("/sites/{site_id}/strata", tags=["strata"])
    def list_strata(site_id: int):
        with read_lock:
            with db.read_transaction(read_conn):
                _require_site(read_conn, site_id)
                return [
                    serialize_stratum(read_conn, r)
                    for r in db.list_strata(read_conn, site_id)
                ]

    @app.get("/strata/{stratum_id}", tags=["strata"])
    def get_stratum(stratum_id: int):
        with read_lock:
            with db.read_transaction(read_conn):
                row = db.get_stratum(read_conn, stratum_id)
                if row is None:
                    raise HTTPException(404, f"层位 {stratum_id} 不存在")
                return serialize_stratum(read_conn, row)

    # ---------- 对接 ----------

    @app.post("/sites/{site_id}/correlations", status_code=201, tags=["correlations"])
    def create_correlation(site_id: int, body: CorrelationCreate):
        if body.stratum_a_id == body.stratum_b_id:
            raise ConflictError("对接的两个层位不能相同")
        with write_lock:
            try:
                with _write_txn() as conn:
                    _require_site(conn, site_id)
                    row_a = _require_stratum(conn, body.stratum_a_id, site_id)
                    row_b = _require_stratum(conn, body.stratum_b_id, site_id)
                    if row_a["trench"] == row_b["trench"]:
                        raise ConflictError(
                            "对接只能登记不同探方的层位（同一探方层位编号天然分层）"
                        )
                    if db.find_correlation(
                        conn, body.stratum_a_id, body.stratum_b_id
                    ) is not None:
                        raise ConflictError("这两个层位之间的对接已存在")
                    payload = _record_and_validate(
                        conn,
                        site_id,
                        "该对接会使约束无解（无法共用年份或叠压成环），已拒绝",
                        lambda c: dict(
                            db.create_correlation(
                                c, site_id, body.stratum_a_id, body.stratum_b_id
                            )
                        ),
                    )
            except _Infeasible as exc:
                raise _as_conflict(exc)
        return _respond(site_id, payload)

    @app.get("/sites/{site_id}/correlations", tags=["correlations"])
    def list_correlations(site_id: int):
        with read_lock:
            with db.read_transaction(read_conn):
                _require_site(read_conn, site_id)
                return [dict(r) for r in db.list_correlations(read_conn, site_id)]

    @app.delete("/correlations/{correlation_id}", tags=["correlations"])
    def delete_correlation(correlation_id: int):
        with write_lock:
            with _write_txn() as conn:
                row = db.get_correlation(conn, correlation_id)
                if row is None:
                    raise HTTPException(404, f"对接 {correlation_id} 不存在")
                site_id = row["site_id"]
                db.delete_correlation(conn, correlation_id)
                # 撤销只会放松约束，按剩余对接重算必然可行
                payload = _commit_payload(conn, site_id, "deleted", correlation_id)
        return _respond(site_id, payload)

    # ---------- 叠压 ----------

    @app.post("/sites/{site_id}/superpositions", status_code=201, tags=["superpositions"])
    def create_superposition(site_id: int, body: SuperpositionCreate):
        if body.lower_id == body.upper_id:
            raise ConflictError("下层与上层不能是同一个层位")
        with write_lock:
            try:
                with _write_txn() as conn:
                    _require_site(conn, site_id)
                    _require_stratum(conn, body.lower_id, site_id)
                    _require_stratum(conn, body.upper_id, site_id)
                    if db.find_superposition(
                        conn, body.lower_id, body.upper_id
                    ) is not None:
                        raise ConflictError("该叠压关系已存在")
                    payload = _record_and_validate(
                        conn,
                        site_id,
                        "该叠压关系无解（成环或下层沉积年晚于上层），已拒绝",
                        lambda c: dict(
                            db.create_superposition(
                                c, site_id, body.lower_id, body.upper_id
                            )
                        ),
                    )
            except _Infeasible as exc:
                raise _as_conflict(exc)
        return _respond(site_id, payload)

    @app.get("/sites/{site_id}/superpositions", tags=["superpositions"])
    def list_superpositions(site_id: int):
        with read_lock:
            with db.read_transaction(read_conn):
                _require_site(read_conn, site_id)
                return [dict(r) for r in db.list_superpositions(read_conn, site_id)]

    @app.delete("/superpositions/{superposition_id}", tags=["superpositions"])
    def delete_superposition(superposition_id: int):
        with write_lock:
            with _write_txn() as conn:
                row = db.get_superposition(conn, superposition_id)
                if row is None:
                    raise HTTPException(404, f"叠压关系 {superposition_id} 不存在")
                site_id = row["site_id"]
                db.delete_superposition(conn, superposition_id)
                payload = _commit_payload(conn, site_id, "deleted", superposition_id)
        return _respond(site_id, payload)

    # ---------- 查询 ----------

    @app.get("/sites/{site_id}/solution", tags=["query"])
    def get_solution(site_id: int):
        """整站分组、组间叠压边与每组可同时成立的全部年代段。"""
        with read_lock:
            with db.read_transaction(read_conn):
                _require_site(read_conn, site_id)
                return _solve_payload(read_conn, site_id)

    @app.get("/strata/{stratum_id}/correlation-view", tags=["query"])
    def get_stratum_correlation_view(stratum_id: int):
        """按原始层位查询：同组全部成员 + 该组可与其他层位同时成立的全部年代段。"""
        with read_lock:
            with db.read_transaction(read_conn):
                row = db.get_stratum(read_conn, stratum_id)
                if row is None:
                    raise HTTPException(404, f"层位 {stratum_id} 不存在")
                site_id = row["site_id"]
                solution = load_and_solve(read_conn, site_id)
                version = db.get_version(read_conn)
                gid = solution.group_of[stratum_id]
                group = next(g for g in solution.groups if g.group_id == gid)
                members = [
                    serialize_stratum(read_conn, db.get_stratum(read_conn, sid))
                    for sid in group.member_ids
                ]
                return {
                    "stratum_id": stratum_id,
                    "group_id": gid,
                    "version": version,
                    "feasible": solution.feasible,
                    "members": members,
                    "group_domain": [[lo, hi] for lo, hi in group.domain],
                    "feasible_years": [[lo, hi] for lo, hi in group.feasible_years],
                    "earliest_year": group.earliest_year,
                    "latest_year": group.latest_year,
                    "contradiction": serialize_solution(solution, version)[
                        "contradiction"
                    ],
                }

    @app.get("/sites/{site_id}/responses/{version}", tags=["query"])
    def get_committed_response(site_id: int, version: int):
        """按遗址与版号取回该次提交时留存的完整回包（record/deleted + solution）。

        回包与关系变更、版号在同一事务中提交：即使关系后来撤销、原实例重启，
        任一实例都能取回与当次一致的回包。查无回包（该版被拒绝或不存在）返回 404。
        """
        with read_lock:
            with db.read_transaction(read_conn):
                _require_site(read_conn, site_id)
                payload = db.get_response(read_conn, site_id, version)
        if payload is None:
            raise HTTPException(
                404, f"遗址 {site_id} 没有版本 {version} 的留存回包"
            )
        return JSONResponse(content=json.loads(payload))

    @app.exception_handler(ConflictError)
    def _conflict_handler(_request, exc: ConflictError):
        return JSONResponse(status_code=409, content=exc.detail)

    return app


app = create_app()
