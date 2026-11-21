# 考古地层对接 API（FastAPI + SQLite）

管理同一考古遗址最多 40 个层位的探方编号、层位编号与可能沉积年代段（整年闭区间，
可含空档），登记跨探方层位对接与“下层 → 上层”叠压关系，并自动完成：

- **分组**：直接或间接对接的层位视为同一次沉积，合并为连通组，全组共用一个年份；
- **成环检测**：合组后的叠压关系（含同组自环）不得成环；
- **年代约束**：下层沉积年严格早于上层；同组年份必须落在各成员年代段的交集内；
- **可行年代段求解**：对每个组给出可与其他全部层位约束同时成立的所有年份
  （保留空档的闭区间列表）；
- **撤销重算**：删除对接/叠压后按剩余关系重新分组、重新求解。

## 快速开始（docker compose）

```bash
docker compose up --build
# 实例 1:  http://localhost:8000/docs
# 实例 2:  http://localhost:8001/docs   （两个实例共用同一 SQLite 卷）
```

compose 启动 **两个 API 实例**（`api1`、`api2`），挂载同一个命名卷
`archaeo-data`（容器内 `/data/archaeo.db`）。任一端 `/docs` 都可登记、撤销
对接和叠压；写事务按提交顺序串行核算，回包留存在两端都可取回。
`docker compose down` 不会删除数据；如需清空：`docker compose down -v`。

## 本地裸跑

```bash
python -m pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
# 数据库默认在 ./data/archaeo.db，可用环境变量 DB_PATH 覆盖
```

## 年代表示约定

- 年份均为整数；公元后用正整数，公元前用负整数（如 `-221` 表示公元前 221 年）。
- 每个层位给一个或多个闭区间 `[year_from, year_to]`，多段之间允许空档
  （如 `[-5000,-4000]` 与 `[-3000,-2000]`，中间一千年无沉积可能）。
- 同组年代域 = 组内各层位区间的交集；求解结果 `feasible_years` 同样是带空档的区间列表。

## 主要接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sites` | 建遗址 |
| GET | `/sites` / `/sites/{id}` | 遗址列表/详情 |
| POST | `/sites/{id}/strata` | 录入层位（探方、层位号、年代段；每遗址上限 40） |
| GET | `/sites/{id}/strata` / `/strata/{id}` | 层位查询 |
| POST | `/sites/{id}/correlations` | 登记两探方层位对接 |
| GET | `/sites/{id}/correlations` | 对接列表 |
| DELETE | `/correlations/{id}` | 撤销对接，按剩余对接重算 |
| POST | `/sites/{id}/superpositions` | 登记叠压（下层 → 上层） |
| GET | `/sites/{id}/superpositions` | 叠压列表 |
| DELETE | `/superpositions/{id}` | 撤销叠压并重算 |
| GET | `/sites/{id}/solution` | 整站分组、组间边、各组全部可行年代段 |
| GET | `/strata/{id}/correlation-view` | 按原始层位查同组成员与可行年代段 |
| GET | `/sites/{id}/responses/{version}` | 取回该次提交留存的完整回包（查无回包返回 404） |

### 录入示例

```bash
SID=1
# 层位：探方 T1 第 3 层，两个可能年代段（中间有空档）
curl -X POST localhost:8000/sites/$SID/strata -H 'Content-Type: application/json' -d '{
  "trench": "T1", "layer_no": "L3",
  "intervals": [{"year_from": -5000, "year_to": -4000},
                {"year_from": -3000, "year_to": -2000}]
}'
# 叠压：下层 1 -> 上层 2
curl -X POST localhost:8000/sites/$SID/superpositions \
  -H 'Content-Type: application/json' -d '{"lower_id": 1, "upper_id": 2}'
# 跨探方对接：层位 1 与层位 3 属同一次沉积
curl -X POST localhost:8000/sites/$SID/correlations \
  -H 'Content-Type: application/json' -d '{"stratum_a_id": 1, "stratum_b_id": 3}'
```

## 无解时的行为

写入会在**同一事务**内先重算再提交。若新记录导致无解（`EMPTY_DOMAIN` /
`GROUP_CYCLE` / `ORDER_CONFLICT`），事务回滚，接口返回 `409`，响应体包含：

- `contradiction`：由**待录关系与校验时的旧记录**生成的一组本身即矛盾的
  **原始记录见证**（层位年代段、对接、叠压，含可读原因与记录 id，便于定位）；
- `previous_solution`：对应这份旧记录的求解结果（保持可行、保持可用）。

拒绝时关系和版号照旧，也不留存回包。

## 回包留存

每次成功写入的完整回包（`record` 或 `deleted` + 本次提交的完整 `solution`，
含 `version`）与关系变更、版号在**同一事务**中提交（`response_log` 表），
写入失败则全部不生效。之后可在任一实例按遗址与版号取回：

```bash
curl localhost:8000/sites/1/responses/5   # 与当次一致的回包；查无回包返回 404
```

即使关系后来撤销、原实例已经重启，取回的仍是当次提交时的回包。
注意与实时回包的区别：写接口返回的 `solution` 是提交后读取的最新已提交状态
——刚登记的对接若在回包前被另一端撤销，登记回包会带上撤销后的分组；
而留存回包永远对应它那一次提交。

## 一致性与并发

- 写操作在进程内写锁 + SQLite `BEGIN IMMEDIATE` 下串行：两个实例共用同一
  SQLite 卷时，校验（求解）与提交按**提交顺序**串行核算，不会出现
  “校验后、提交前”被另一端改写。每次成功写入使全局 `version` 加 1。
- 读操作走独立的只读连接与只读事务，**不进入写锁**：写事务未提交时，整站与
  按层位查询直接返回最近一次已提交的同版分组、成员、年代与版号；提交之后
  发起的新查询读到新版。
- 分组与年代结论**不物化落库**，每次查询及重启都由原始记录现场求解；
  原始记录与回包留存由 SQLite 事务保证原子性，不存在半写入的中间态。
- SQLite 启用 WAL 与外键约束，连接带 30s busy timeout。

## 求解算法（`app/solver.py`）

1. 并查集按对接关系求连通组；组域为成员年代域交集（支持空档的区间运算）。
2. 叠压边提升为组间边；Kahn 拓扑排序判环，DFS 三色标记提取具体环与见证。
3. DAG 上两次贪心：
   - 最早解 E：拓扑序每组取域内 `> max(E[前驱])` 的最小年份；
   - 最晚解 L：逆拓扑序每组取域内 `< min(L[后继])` 的最大年份。
   任意可行解 F 都满足 E ≤ F ≤ L，故组 g 的全部可同时成立年份恰为
   其年代域 ∩ [E[g], L[g]]（仍可为含空档的多段区间）。

## 测试

```bash
python -m pytest tests/ -q
```

覆盖区间工具、缺口域求解、成环/空域/次序冲突及矛盾见证、40 层上限、
无解拒绝与旧结论保留、撤销重算、并发写入串行化、重启后结论恢复、
回包留存与按版号取回、跨实例共享与竞态、写事务未提交时的读一致性、
旧库（无 response_log 表）平滑升级。
