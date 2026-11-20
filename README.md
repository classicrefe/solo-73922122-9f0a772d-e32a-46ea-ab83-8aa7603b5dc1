# Half-Batch Transaction Lab

给数仓工程师复现"批次主表 + 测值明细装载时被读到半批"问题的试验 API。
FastAPI + PostgreSQL。每轮实验从相同基线重新开始，提供 **A/B 两路独立事务**，
可逐步 begin、执行单条可信 SQL、commit 或 rollback，全程记录输入、结果、
原始错误与事务结局，用于修正装载边界。

## 启动

```bash
docker compose up --build
# API: http://localhost:8000  交互文档: http://localhost:8000/docs
```

## 数据模型与基线

每轮创建独立 schema `r_<round_id>`（轮次之间数据、连接、锁互不串用）：

```sql
batches(id INT PRIMARY KEY, ready BOOLEAN NOT NULL)
readings(id INT PRIMARY KEY, batch_id INT REFERENCES batches(id), value NUMERIC)
-- 基线: batches = (1,true),(2,false); readings = (1,1,10.5),(2,1,11.0),(3,1,9.8)
```

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/rounds` | 开新一轮（重置基线；最多 2 轮并行，超出返回 409） |
| POST | `/rounds/{id}/fork` | **从活动轮次开新轮**：复制该轮已提交快照的 `batches`/`readings` |
| GET | `/rounds` / `/rounds/{id}` | 轮次列表 / 详情（含全部步骤记录） |
| POST | `/rounds/{id}/tx/{A\|B}/begin` | 开启该路事务（独立连接） |
| POST | `/rounds/{id}/tx/{A\|B}/sql` | 执行单条可信 SQL，body `{"sql": "..."}`，立即返回步骤记录 |
| POST | `/rounds/{id}/tx/{A\|B}/commit` · `/rollback` | 提交 / 回滚该路事务 |
| GET | `/rounds/{id}/steps/{seq}` | 查询单步结果 |
| POST | `/rounds/{id}/stop` · `/finish` | 停止 / 正常结束本轮（及时受理，异步清理） |

## 从活动轮次开新轮（fork）

补录一批测值后，常需要沿用这份数据验证修正后的装载 SQL。
`POST /rounds/{id}/fork` 以活动轮次为源，开一个数据相同但完全独立的新轮：

- **同一已提交快照**：两张表在**同一个 `REPEATABLE READ` 事务**里一次性读完，
  因此 `batches` 与 `readings` 必然来自同一个已提交状态，绝不会复制到半批；
  源轮未提交的改动不可见。
- **源轮不受影响**：复制只读、独立开事务，源轮仍可继续提交或回滚，复制前后都可操作。
- **结构一致**：新轮沿用同样的建表语句（`batches` 主键、`readings` 主键及
  `batch_id -> batches.id` 外键），有独立 schema 和独立 A/B 事务，
  **步骤记录从空开始**。
- **并发上限沿用**：fork 与普通轮次共用"最多 2 轮并行"的名额，超出返回 409。
- **完整后才开放**：两张表全部复制成功并通过同一个事务提交后，新轮才注册可见；
  轮次详情里带 `source_round_id` 与 `copied: {"batches": n, "readings": n}`。
- **失败不残留**：源轮已被清理（schema 不存在）返回 409，复制失败返回 500，
  两者都会说明原因、`DROP SCHEMA ... CASCADE` 清掉残留数据并归还名额。
- **源轮状态**：源轮必须处于 `active`。已停止 / 已清理的源轮返回 409 并说明
  它当前的状态；fork 出的轮次本身也可再被 fork。

## 语义保证

- **阻塞不互相拖累**：每路事务一个专属 worker 线程 + 专属连接，语句入队执行；
  一路语句阻塞时另一路照常推进。
- **失败归属明确**：死锁 / 语句失败以原始错误（error class、SQLSTATE、原文）记在
  出错事务的记录上；绝不换连接冒充原事务续跑，连接断开则该路标记 `dead`。
- **单步满 10 秒自动停整轮**（`STEP_TIMEOUT_SECONDS` 可调）：取消在跑语句、
  未开始的排队步骤标记 `canceled` 不执行、未提交事务回滚、连接释放后才报终态
  `finished`（reason: `completed`/`stopped`/`timeout`/`shutdown`）。
- **记录不可改写**：步骤进入终态（`done`/`error`/`canceled`）后迟到结果一律丢弃；
  已提交事务保留原记录；一轮停止期间另一轮仍可正常操作。
- **失败轮不遗留**：连接关闭释放锁（15 秒未释放则 `pg_terminate_backend` 兜底），
  schema 整体 `DROP ... CASCADE`，下一轮从干净基线开始。

## 示例

```bash
python3 examples/half_batch.py        # 半批读取：坏边界 vs 修正后的单提交边界
python3 examples/deadlock.py          # 死锁：标明受害事务 (40P01)
python3 examples/blocking_timeout.py  # 阻塞语句 + 10 秒自动停止 + 下一轮无残留
python3 examples/fork_round.py        # 补录后 fork：新轮复制同一已提交快照
```

## 自测

```bash
# 需要 API 与 PostgreSQL 已启动（例如 docker compose up）
python3 tests/fork_scenarios.py
```

覆盖：已提交补录完整复制（快照一致）、键约束沿用、步骤记录与事务路径重置、
源轮 fork 后仍可提交/回滚、并发名额共用、源轮先被清理（409 并归还名额）、
复制故障（500、清残留、归还名额）、源轮未提交改动不进快照。

## 说明与限制

- SQL 为"可信输入"，按原样执行；仅做朴素的单语句检查（不允许语句中间出现 `;`，
  字符串字面量中的分号不支持）。
- 记录保存在内存中，服务重启即丢失。
- 每路事务每轮只能 begin 一次；commit/rollback 后该路不再接受新步骤。
