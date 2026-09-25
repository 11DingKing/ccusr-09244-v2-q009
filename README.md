# 机器人作业数据回流服务

这是一个面向机器人作业数据团队的服务端应用，负责管理机型、场景、技能、作业记录、人工标注和数据集。服务使用 FastAPI 提供本地 HTTP 接口，以 SQLite 保存业务数据；质量评分、数据集审核、版本、订阅和统计分析均在同一进程内完成。

## 目录

- `main.py`：应用入口、健康检查和路由注册。
- `app/models`：业务实体及其关系（含 `idempotency_records` 幂等记录表）。
- `app/routers`：基础资源、作业、数据集、分析接口和幂等接入/观测接口。
- `app/services`：评分、统计、策略目录、时间窗口与请求幂等工具。
- `app/seed_data.py`：可重复执行的示例数据初始化逻辑。
- `scripts/init_sample_data.py`：初始化脚本的兼容入口。

## 配置与运行

默认数据库文件为项目根目录的 `robot_data.db`，可以通过 `DATABASE_URL` 指定 SQLite 文件。安装依赖后运行 `python3 main.py`，服务默认监听 `8000` 端口；`GET /health` 返回服务状态，接口文档位于 `/docs`。

初始化示例数据可执行 `python3 scripts/init_sample_data.py`。该命令会重建本地数据库并写入机型、场景、技能、作业、标注及数据集示例。

## 幂等接入

边缘网关重发作业数据时，通过业务键获得统一的幂等语义：

- `POST /api/v1/ingest/operations`：单条接入，请求体在作业字段之外携带 `idempotency_key`（可选 `ttl_seconds`）。首次请求在同一事务内写入作业数据与幂等记录，返回 `status=saved`；相同键 + 相同载荷返回首次结果（`status=replayed`，不产生新数据）；相同键 + 不同载荷返回 `409 PAYLOAD_CONFLICT`。
- `POST /api/v1/ingest/operations/batch`：批量接入，请求体为 `{"items": [...]}`。每个条目独立携带业务键，`results` 严格保留原输入下标；无效引用、缺键、冲突或过期条目不影响其他条目，整批在同一事务提交。条目结果取值 `saved/replayed/conflict/expired/invalid/error`。
- 载荷比对使用规范 JSON 摘要（排序键、紧凑分隔），字段顺序差异不影响判定。
- 过期与回收：记录超过 `IDEMPOTENCY_TTL_SECONDS`（默认 7 天）后不再重放，返回 `410 KEY_EXPIRED`；超过 TTL 后还有 `IDEMPOTENCY_GRACE_SECONDS`（默认 1 天）宽限期，只有越过宽限期的记录才会被 `POST /api/v1/idempotency/reap` 回收（只删幂等记录，业务数据保留）。键被复用后，携带旧载荷的请求仍会因摘要不同而冲突，不会误拿到新数据。
- 观测接口：`GET /api/v1/idempotency/stats` 查看保存/重放/冲突命中与到期分布，`GET /api/v1/idempotency/records` 按 scope、业务键、业务数据ID、状态（active/expired/reusable）分页查询记录。

幂等记录与业务数据在同一数据库事务落库，服务重启后继续有效。

## 验证

运行 `python3 -m pytest -q` 执行服务和领域工具测试，运行 `python3 -m compileall -q app main.py scripts` 检查编译。测试只使用临时 SQLite 数据库，不需要额外服务。
