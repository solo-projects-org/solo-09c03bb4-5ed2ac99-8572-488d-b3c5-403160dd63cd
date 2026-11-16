# 空气质量小时 PM2.5 采集服务

按「源地址 + 测站号 + UTC 小时清单」提交采集任务，异步抓取省站小时 PM2.5 并入库。
两个采集实例共用一个 SQLite（WAL）协调库，对外提供 FastAPI / Swagger。

## 快速启动

```bash
docker compose up -d --build
# Swagger:
#   实例 1  http://localhost:8001/docs
#   实例 2  http://localhost:8002/docs
# 省站模拟器（可交互调整慢响应/429）:
#   station-a http://localhost:9001/docs   (默认 2s 慢响应, 每 4 个请求 429, Retry-After: 3)
#   station-b http://localhost:9002/docs   (默认 0.2s 快速响应)
```

提交任务（在容器网络内源地址是 `http://station-a:9001`；从宿主机调用 API 时同样可用，
因为两个采集容器都能解析这些服务名）：

```bash
curl -sS -X POST http://localhost:8001/api/v1/jobs \
  -H 'content-type: application/json' \
  -d '{
    "source": "http://station-a:9001",
    "station_id": "A001",
    "hours": ["2026-11-15T00:00:00Z", "2026-11-15T01:00:00Z"]
  }'
```

返回 `job_id` 后查询进度与观测：

```bash
curl -sS http://localhost:8001/api/v1/jobs/<job_id>
curl -sS http://localhost:8001/api/v1/jobs/<job_id>/tasks
curl -sS http://localhost:8001/api/v1/jobs/<job_id>/observations
curl -sS 'http://localhost:8002/api/v1/observations?station_id=A001&limit=100'
curl -sS http://localhost:8002/api/v1/sources
```

任一实例都能查询另一个实例提交的任务，因为进度完全来自共享库。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 数据库与 worker 健康、实例号、在途请求数 |
| POST | `/api/v1/jobs` | 提交任务（幂等去重：同源同站同小时只生成一个任务） |
| GET | `/api/v1/jobs` | 任务列表及进度 |
| GET | `/api/v1/jobs/{job_id}` | 任务进度（总数/成功/在途/排队/重试/百分比/入库数） |
| GET | `/api/v1/jobs/{job_id}/tasks` | 每个小时的状态，可按 `status` 过滤 |
| GET | `/api/v1/jobs/{job_id}/observations` | 本任务已入库观测 |
| GET | `/api/v1/observations` | 按源/测站/小时范围查询观测 |
| GET | `/api/v1/sources` | 源的在途数、积压、429 冷却剩余秒数 |

`source` 可写 `"http://host:port"` 或 `{"protocol","host","port"}`。源的唯一身份是
`protocol + host + port`；带路径的 URL 会被 422 拒绝，避免「看起来是两个源其实是同一个」。
`hours` 支持带时区/`Z` 的 ISO-8601 或 Unix 秒，分钟与秒会被截断到整点，重复小时自动折叠。

## 关键语义与实现

**每源最多两个在途请求（跨两个实例）**
任务行在 SQLite 中以 `state='in_flight'` + `lease_until` 表示占用。认领在
`BEGIN IMMEDIATE` 事务内完成：按源计算已有未过期租约数，只放出剩余名额。
因此两个采集进程不可能同时把同一源推到 3 个请求。发请求前还有一次
`confirm_dispatch` 原子门控，确保「刚记下 429 冷却」与「发出新请求」线性化。

**429 冷却**
`Retry-After` 只接受整数秒（非整数按 1 秒处理）。冷却写入 `sources.cooldown_until`，
更新语句是 `MAX(cooldown_until, 新截止时间)`，所以：
- 新 429 永远不会缩短已有冷却；
- 旧请求成功不会提前解禁（成功路径完全不碰 `cooldown_until`）；
- 冷却存在数据库里，重启、换实例后依然生效；
- 冷却期间该源不会有新请求，但其他源和进度查询完全不受影响（查询走独立读事务，worker 用异步 httpx）。

**失败重试与进度**
非 2xx（429 之外）/超时/坏 JSON 的任务回到 `pending` 并带指数退避
（0.5s 起、封顶 30s）；`attempts` 保留。成功时观测入库与任务置为 `succeeded`
在同一个事务里完成，因此「观测可见」和「进度完成」同时生效。

**去重入库**
`observations` 上有 `UNIQUE(source_id, station_id, hour_epoch)`；成功后用
`ON CONFLICT DO NOTHING` 写入。同源同站同小时重复提交任务或重复成功回调都只留一行。

**实例退出与接续**
worker 持有租约（`AQ_LEASE_SECONDS`，且强制 ≥ HTTP 超时 + 2s）。实例被 `kill -9`
后租约过期，另一实例自动重新认领。租约刻意长于客户端超时、客户端超时又长于省站
「至多 5 秒结束处理」的上限，所以「源端仍在处理」的请求不会被提前重发。
优雅退出时先停止认领、最多等 `AQ_SHUTDOWN_GRACE_SECONDS` 让在途请求自然结束；
仍未结束的请求保留租约（不释放名额），由对端在租约到期后接续。

**慢响应/冷却不阻塞**
worker 是单个 asyncio 循环 + 并发任务，慢请求只占自己的协程；进度查询、提交任务
由 FastAPI 同步读库处理，不会被慢源卡住。

## 省站模拟器

每个实例都可独立配置，且支持运行时调整（Swagger 中的 `PATCH /_station/config`）：

| 环境变量 | 含义 |
| --- | --- |
| `STATION_DELAY_SECONDS` | 固定延迟（0–5，超过 5 被截断为 5） |
| `STATION_JITTER_SECONDS` | 额外随机抖动（合计仍被限制在 5s 内） |
| `STATION_429_EVERY` | 每 N 个请求返回 429；0 关闭 |
| `STATION_RETRY_AFTER_SECONDS` | 429 的整数 `Retry-After` |
| `STATION_FAIL_EVERY` | 每 N 个请求返回 503；0 关闭 |

请求日志（JSON Lines）记录每个请求的开始/结束时间、耗时、是否慢、是否 429、
计划行为与观测到的最大并发；查看方式：

```bash
curl -sS http://localhost:9001/_station/requests
curl -sS http://localhost:9001/_station/log.txt
```

## 采集参数

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `AQ_DB_PATH` | `./aq-data/aq.sqlite3` | 两实例必须指向同一文件（Compose 中用共享卷） |
| `AQ_INSTANCE_ID` | 主机名-PID | 写入任务 owner 与观测 |
| `AQ_HTTP_TIMEOUT_SECONDS` | 6 | 单请求总截止时间（省站上限 5s + 余量） |
| `AQ_LEASE_SECONDS` | 8 | 在途租约；实际取 `max(该值, 超时+2)` |
| `AQ_POLL_SECONDS` | 0.2 | 无任务时的认领间隔 |
| `AQ_MAX_CONCURRENCY` | 64 | 单进程协程并发上限（源级 2 额外约束） |
| `AQ_SHUTDOWN_GRACE_SECONDS` | 3 | 优雅退出等待在途请求的时间 |

## 本地测试

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

`scripts/e2e_check.py` 是对已启动 Compose 栈的黑盒验收（并发上限、429 间隔、
隔离性、去重、故障切换）。它调用 `docker compose` 停止/启动 collector-1，
需要本机有 Docker。

## 已知限制

- 单机 SQLite：两个实例必须共享同一本地卷；不支持跨主机共享。
- 调度是轮询式（默认 200ms），不是事件通知，提交后可能有极短延迟才开始。
- 未实现鉴权、TLS 与历史数据回填之外的删除/重采接口。
- `Retry-After` 按协议只支持整数秒，HTTP-date 形式按 1 秒处理。
