"""回包留存、跨实例共享与读写一致性测试。

覆盖：
- 每次成功写入的回包（record/deleted + 完整 solution 含 version）随版号留存，
  可按（遗址, version）取回；查无回包返回 404；
- 关系撤销、原实例重启后，仍可在另一实例取回与当次一致的回包；
- 登记回包生成前若关系被另一端撤销，回包带上撤销后的分组；
- 409 拒绝：contradiction 来自待录关系与校验时旧记录，previous_solution 对应
  这份旧记录，关系与版号照旧、不留存回包；
- 写事务未提交时查询直接返回已提交的同版状态，提交后新查询读新版；
- 两个实例共用同一 SQLite 文件，写按提交顺序串行；
- 旧库（无 response_log 表）数据保留、可平滑升级。
"""

import concurrent.futures
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def two_clients(tmp_path):
    """两个 API 实例共用同一 SQLite 文件（模拟共用同一卷）。"""
    db_file = str(tmp_path / "shared.db")
    app1 = create_app(db_path=db_file)
    app2 = create_app(db_path=db_file)
    with TestClient(app1) as c1, TestClient(app2) as c2:
        yield c1, c2


def _make_site(client, name="仰韶村遗址"):
    r = client.post("/sites", json={"name": name})
    assert r.status_code == 201, r.text
    return r.json()["record"]


def _add_stratum(client, site_id, trench, layer_no, intervals, note=None):
    r = client.post(
        f"/sites/{site_id}/strata",
        json={
            "trench": trench,
            "layer_no": layer_no,
            "intervals": [
                {"year_from": lo, "year_to": hi} for lo, hi in intervals
            ],
            "note": note,
        },
    )
    assert r.status_code == 201, r.text
    return r.json()["record"]


def _add_pair(client, site_id):
    """两个可对接的层位（不同探方、同年代段）。"""
    a = _add_stratum(client, site_id, "T1", "L1", [(100, 200)])
    b = _add_stratum(client, site_id, "T2", "L1", [(100, 200)])
    return a, b


# ---------- 回包留存 ----------

def test_write_response_is_stored_and_retrievable_by_version(client):
    site = _make_site(client)
    sid = site["id"]
    a, b = _add_pair(client, sid)

    r = client.post(
        f"/sites/{sid}/correlations",
        json={"stratum_a_id": a["id"], "stratum_b_id": b["id"]},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert "record" in body and "solution" in body
    version = body["solution"]["version"]

    # 按遗址 + version 取回的回包与当次一致
    snap = client.get(f"/sites/{sid}/responses/{version}")
    assert snap.status_code == 200
    assert snap.json() == body
    assert snap.json()["record"]["id"] == body["record"]["id"]
    assert len(snap.json()["solution"]["groups"]) == 1  # 当次合并为一组


def test_delete_response_is_stored_with_deleted_key(client):
    site = _make_site(client)
    sid = site["id"]
    a, b = _add_pair(client, sid)
    corr = client.post(
        f"/sites/{sid}/correlations",
        json={"stratum_a_id": a["id"], "stratum_b_id": b["id"]},
    ).json()["record"]

    r = client.delete(f"/correlations/{corr['id']}")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["deleted"] == corr["id"]
    version = body["solution"]["version"]

    snap = client.get(f"/sites/{sid}/responses/{version}")
    assert snap.status_code == 200
    assert snap.json() == body
    assert snap.json()["deleted"] == corr["id"]
    assert len(snap.json()["solution"]["groups"]) == 2  # 撤销后拆成两组


def test_response_snapshot_404_when_missing(client):
    site = _make_site(client)
    sid = site["id"]
    _add_pair(client, sid)

    # 从未存在过的版号
    assert client.get(f"/sites/{sid}/responses/999").status_code == 404
    # 遗址不存在
    assert client.get("/sites/999/responses/1").status_code == 404


def test_rejected_write_leaves_no_snapshot_and_same_version(client):
    site = _make_site(client)
    sid = site["id"]
    a = _add_stratum(client, sid, "T1", "L1", [(100, 200)])
    b = _add_stratum(client, sid, "T2", "L1", [(300, 400)])  # 与 a 无共同年份

    version_before = client.get(f"/sites/{sid}/solution").json()["version"]
    r = client.post(
        f"/sites/{sid}/correlations",
        json={"stratum_a_id": a["id"], "stratum_b_id": b["id"]},
    )
    assert r.status_code == 409
    body = r.json()
    # contradiction 用待录关系与校验时的旧记录生成
    assert body["contradiction"]["reason"] == "EMPTY_DOMAIN"
    witness_ids = {w["record_id"] for w in body["contradiction"]["witness"]}
    assert witness_ids == {a["id"], b["id"]}
    # previous_solution 对应该旧记录：两组、可行、旧版号
    previous = body["previous_solution"]
    assert previous["feasible"] is True
    assert len(previous["groups"]) == 2
    assert previous["version"] == version_before

    # 拒绝后关系与版号照旧，且无留存回包
    assert client.get(f"/sites/{sid}/correlations").json() == []
    assert client.get(f"/sites/{sid}/solution").json()["version"] == version_before
    assert client.get(f"/sites/{sid}/responses/{version_before + 1}").status_code == 404


# ---------- 跨实例与重启 ----------

def test_snapshot_survives_revocation_and_restart_cross_instance(tmp_path):
    db_file = str(tmp_path / "persist.db")
    with TestClient(create_app(db_path=db_file)) as c1:
        site = _make_site(c1)
        sid = site["id"]
        a, b = _add_pair(c1, sid)
        reg = c1.post(
            f"/sites/{sid}/correlations",
            json={"stratum_a_id": a["id"], "stratum_b_id": b["id"]},
        ).json()
        reg_version = reg["solution"]["version"]
        # 撤销对接（关系变更 + 版号 + 回包留存同事务提交）
        r = c1.delete(f"/correlations/{reg['record']['id']}")
        assert r.status_code == 200, r.text
        del_version = r.json()["solution"]["version"]

    # 原实例已重启：另一实例（新进程对象）打开同一数据库文件
    with TestClient(create_app(db_path=db_file)) as c2:
        # 关系已撤销，但登记的回包仍与当次一致（合并为一组）
        snap = c2.get(f"/sites/{sid}/responses/{reg_version}")
        assert snap.status_code == 200
        assert snap.json() == reg
        assert len(snap.json()["solution"]["groups"]) == 1
        # 撤销的回包同样可取
        snap_del = c2.get(f"/sites/{sid}/responses/{del_version}")
        assert snap_del.status_code == 200
        assert snap_del.json()["deleted"] == reg["record"]["id"]
        assert len(snap_del.json()["solution"]["groups"]) == 2
        # 当前状态是撤销后的两组
        assert len(c2.get(f"/sites/{sid}/solution").json()["groups"]) == 2


def test_two_instances_share_state_and_serialize_commits(two_clients):
    c1, c2 = two_clients
    site = _make_site(c1)
    sid = site["id"]
    # 另一实例立即可见已提交状态
    assert c2.get(f"/sites/{sid}").status_code == 200

    def add(i):
        client = c1 if i % 2 == 0 else c2
        return client.post(
            f"/sites/{sid}/strata",
            json={
                "trench": f"T{i}",
                "layer_no": "L1",
                "intervals": [{"year_from": 1, "year_to": 1000}],
            },
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(add, range(20)))
    assert all(r.status_code == 201 for r in results), [
        r.text for r in results if r.status_code != 201
    ]
    # 两实例按提交顺序串行核算：1 次建站 + 20 次层位
    sol = c2.get(f"/sites/{sid}/solution").json()
    assert sol["version"] == 21
    assert len(sol["groups"]) == 20
    assert c1.get(f"/sites/{sid}/solution").json()["version"] == 21


def test_registration_response_reflects_revocation_before_reply(two_clients):
    """刚登记的对接在回包前被另一端撤销：回包带上撤销后的分组，
    但当次提交留存的回包仍是登记时的合并分组。"""
    c1, c2 = two_clients
    app1 = c1.app
    site = _make_site(c1)
    sid = site["id"]
    a, b = _add_pair(c1, sid)
    version_before = c1.get(f"/sites/{sid}/solution").json()["version"]

    def revoke_on_other_end():
        corrs = c2.get(f"/sites/{sid}/correlations").json()
        assert len(corrs) == 1
        r = c2.delete(f"/correlations/{corrs[0]['id']}")
        assert r.status_code == 200, r.text

    app1.state.post_commit_hook = revoke_on_other_end
    try:
        r = c1.post(
            f"/sites/{sid}/correlations",
            json={"stratum_a_id": a["id"], "stratum_b_id": b["id"]},
        )
    finally:
        app1.state.post_commit_hook = None
    assert r.status_code == 201, r.text
    body = r.json()
    # 回包带上撤销后的分组：两个层位各自成组
    assert len(body["solution"]["groups"]) == 2
    assert body["record"]["stratum_a_id"] == min(a["id"], b["id"])

    # 当次提交（登记）留存的回包仍是合并后的一组
    reg_version = version_before + 1
    snap = c2.get(f"/sites/{sid}/responses/{reg_version}")
    assert snap.status_code == 200
    assert len(snap.json()["solution"]["groups"]) == 1
    assert snap.json()["record"]["id"] == body["record"]["id"]
    # 撤销提交的回包也已留存
    snap_del = c2.get(f"/sites/{sid}/responses/{reg_version + 1}")
    assert snap_del.status_code == 200
    assert snap_del.json()["deleted"] == body["record"]["id"]


# ---------- 读写一致性 ----------

def test_reads_return_committed_state_during_uncommitted_write(client, tmp_path):
    """写事务未提交时，整站与按层位查询直接返回已提交的同版状态。"""
    app = client.app
    site = _make_site(client)
    sid = site["id"]
    s1 = _add_stratum(client, sid, "T1", "L1", [(100, 200)])
    committed = client.get(f"/sites/{sid}/solution").json()
    version_before = committed["version"]

    # 在写连接上开启未提交的写事务（模拟进行中的写入）
    write_conn = app.state.write_conn
    write_conn.execute("BEGIN IMMEDIATE")
    try:
        cur = write_conn.execute(
            "INSERT INTO strata(site_id, trench, layer_no) VALUES (?, 'T9', 'L9')",
            (sid,),
        )
        write_conn.execute(
            "INSERT INTO stratum_intervals(stratum_id, year_from, year_to) "
            "VALUES (?, 1, 100)",
            (cur.lastrowid,),
        )
        # 整站查询：直接返回已提交的同版分组与版号，看不到未提交的新层位
        sol = client.get(f"/sites/{sid}/solution").json()
        assert sol["version"] == version_before
        assert sol["groups"] == committed["groups"]
        # 按层位查询：同版成员、年代与版号
        view = client.get(f"/strata/{s1['id']}/correlation-view").json()
        assert view["version"] == version_before
        assert [m["id"] for m in view["members"]] == [s1["id"]]
        assert view["feasible_years"] == [[100, 200]]
    finally:
        write_conn.execute("ROLLBACK")

    # 提交后新查询读新版
    _add_stratum(client, sid, "T2", "L1", [(100, 200)])
    sol = client.get(f"/sites/{sid}/solution").json()
    assert sol["version"] == version_before + 1
    assert len(sol["groups"]) == 2


def test_concurrent_writes_keep_response_versions_consistent(client):
    """每个成功回包的 solution 版号不小于本次提交版号，且最终版号连续。"""
    site = _make_site(client)
    sid = site["id"]

    def add(i):
        return client.post(
            f"/sites/{sid}/strata",
            json={
                "trench": f"T{i}",
                "layer_no": "L1",
                "intervals": [{"year_from": 1, "year_to": 1000}],
            },
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(add, range(20)))
    assert all(r.status_code == 201 for r in results)
    final_version = client.get(f"/sites/{sid}/solution").json()["version"]
    assert final_version == 21
    # 每次提交的回包留存都可按版号取回，且回包内 solution 版号与键一致
    for version in range(2, final_version + 1):
        snap = client.get(f"/sites/{sid}/responses/{version}")
        assert snap.status_code == 200
        assert snap.json()["solution"]["version"] == version


# ---------- 旧库兼容 ----------

def test_old_database_without_response_log_is_preserved(tmp_path):
    """旧库（无 response_log 表）数据保留：平滑升级、可继续写入并留存。"""
    db_file = str(tmp_path / "old.db")
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE sites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE strata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL REFERENCES sites(id),
            trench TEXT NOT NULL,
            layer_no TEXT NOT NULL,
            note TEXT,
            created TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (site_id, trench, layer_no)
        );
        CREATE TABLE stratum_intervals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stratum_id INTEGER NOT NULL REFERENCES strata(id) ON DELETE CASCADE,
            year_from INTEGER NOT NULL,
            year_to INTEGER NOT NULL,
            CHECK (year_from <= year_to)
        );
        CREATE TABLE correlations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL REFERENCES sites(id),
            stratum_a_id INTEGER NOT NULL REFERENCES strata(id),
            stratum_b_id INTEGER NOT NULL REFERENCES strata(id),
            created TEXT NOT NULL DEFAULT (datetime('now')),
            CHECK (stratum_a_id < stratum_b_id),
            UNIQUE (site_id, stratum_a_id, stratum_b_id)
        );
        CREATE TABLE superpositions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL REFERENCES sites(id),
            lower_id INTEGER NOT NULL REFERENCES strata(id),
            upper_id INTEGER NOT NULL REFERENCES strata(id),
            created TEXT NOT NULL DEFAULT (datetime('now')),
            CHECK (lower_id <> upper_id),
            UNIQUE (site_id, lower_id, upper_id)
        );
        """
    )
    conn.execute("INSERT INTO meta(key, value) VALUES ('version', '3')")
    conn.execute("INSERT INTO sites(id, name) VALUES (1, '老遗址')")
    conn.execute(
        "INSERT INTO strata(id, site_id, trench, layer_no) VALUES (1, 1, 'T1', 'L1')"
    )
    conn.execute(
        "INSERT INTO stratum_intervals(stratum_id, year_from, year_to) "
        "VALUES (1, 100, 200)"
    )
    conn.commit()
    conn.close()

    with TestClient(create_app(db_path=db_file)) as c:
        # 旧数据原样可读，版号照旧
        sol = c.get("/sites/1/solution").json()
        assert sol["feasible"] is True
        assert sol["version"] == 3
        view = c.get("/strata/1/correlation-view").json()
        assert view["feasible_years"] == [[100, 200]]
        # 旧库没有留存回包：查无回包返回 404
        assert c.get("/sites/1/responses/3").status_code == 404
        # 新写入正常留存，版号在旧版号上递增
        r = c.post(
            "/sites/1/strata",
            json={
                "trench": "T2",
                "layer_no": "L1",
                "intervals": [{"year_from": 150, "year_to": 250}],
            },
        )
        assert r.status_code == 201, r.text
        version = r.json()["solution"]["version"]
        assert version == 4
        snap = c.get(f"/sites/1/responses/{version}")
        assert snap.status_code == 200
        assert snap.json() == r.json()
