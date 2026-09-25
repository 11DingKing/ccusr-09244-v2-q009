# 机器人作业数据回流服务

这是一个面向机器人作业数据团队的服务端应用，负责管理机型、场景、技能、作业记录、人工标注和数据集。服务使用 FastAPI 提供本地 HTTP 接口，以 SQLite 保存业务数据；质量评分、数据集审核、版本、订阅和统计分析均在同一进程内完成。

## 目录

- `main.py`：应用入口、健康检查和路由注册。
- `app/models`：业务实体及其关系。
- `app/routers`：基础资源、作业、数据集、分析和幂等记录接口。
- `app/services`：评分、统计、策略目录、接入幂等与时间窗口工具。
- `app/seed_data.py`：可重复执行的示例数据初始化逻辑。
- `scripts/init_sample_data.py`：初始化脚本的兼容入口。

## 配置与运行

默认数据库文件为项目根目录的 `robot_data.db`，可以通过 `DATABASE_URL` 指定 SQLite 文件。安装依赖后运行 `python3 main.py`，服务默认监听 `8000` 端口；`GET /health` 返回服务状态，接口文档位于 `/docs`。

初始化示例数据可执行 `python3 scripts/init_sample_data.py`。该命令会重建本地数据库并写入机型、场景、技能、作业、标注及数据集示例。

## 接入幂等

边缘采集网关在网络抖动后会重发最近一批作业数据。`POST /operations` 与 `POST /operations/batch` 支持在请求体中携带 `idempotency_key`（调用方业务键，批量时逐条携带，两个入口共用同一键空间）：

- **首次请求**：业务数据与幂等记录在同一事务落库，响应头 `X-Idempotency-Status: stored`。
- **相同键 + 相同载荷**：不再写入，返回首次保存的完整结果，响应头 `X-Idempotency-Status: replayed`；批量条目以 `replayed: true` 标记，且始终保留原输入位置（`index`）。
- **相同键 + 不同载荷**：判定冲突，单条返回 409，批量对应条目标记失败；不会用旧请求覆盖新数据。
- **过期回收**：记录按 `IDEMPOTENCY_TTL_HOURS`（默认 24 小时）过期，到期边界（含）即失效；`POST /idempotency-records/recycle` 显式回收全部过期键。键被回收复用后，迟到的旧载荷只会被判为冲突，不会误命中新数据。

观察接口：`GET /idempotency-records`（支持 `scope`、`idempotency_key`、`expired` 过滤）查看重放/冲突计数，`GET /idempotency-records/{id}` 查看保存的完整结果。未携带 `idempotency_key` 的请求保持原有行为，不参与去重。

## 验证

运行 `python3 -m pytest -q` 执行服务和领域工具测试，运行 `python3 -m compileall -q app main.py scripts` 检查编译。测试只使用临时 SQLite 数据库，不需要额外服务。
