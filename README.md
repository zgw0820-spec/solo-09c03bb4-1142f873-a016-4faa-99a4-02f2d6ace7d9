# 机房告警升级 API

供电柜过热告警的升级通知服务：**先通知主值班员 → 主通知确已受理才开始计时 →
逾期未确认仅投递一次备班通知**。附带一个可设置慢响应、可丢回执、可按联系人
限流的本地通知网关，用于演示和验证各种异常路径。

## 快速开始

```bash
docker compose up --build -d
curl -s localhost:8000/health
open http://localhost:8000/docs      # Swagger UI
```

两个服务：

| 服务 | 地址 | 说明 |
|---|---|---|
| `api` | http://localhost:8000 | 告警 API，Swagger 在 `/docs` |
| `gateway` | http://localhost:9000 | 本地通知网关，Swagger 在 `/docs` |

数据分别持久化在命名卷 `api-data`、`gateway-data` 中，`docker compose restart`
或重建容器后仍能接续未完成的投递与确认状态。

不使用 Docker 时：

```bash
pip install -r requirements.txt
python -m uvicorn gateway.main:app --port 9000 &
DATABASE_PATH=./data/alarms.db GATEWAY_URL=http://127.0.0.1:9000 \
  python -m uvicorn app.main:app --port 8000
```

## 核心语义

### 1. 提交告警

`POST /alarms`：告警号、告警内容、主/备联系人、确认期限（**2～30 秒**）。

- 同一告警号 + **完全相同内容**的重试：返回 `200`，复用原告警，不重复通知。
- 同一告警号但内容不同：返回 `409`。
- 首次提交返回 `201`。内容指纹覆盖告警内容、metadata、主备联系人、期限。

### 2. 按联系人限流（网关容量）

多台供电柜同时告警、通知涌向同一值班联系人时，网关按容量受理：

- 同一联系人（channel + address，无 address 时按 channel + name）在任意连续
  **3 秒**内最多受理 **1 条**新通知（滑动窗口，可用 `RATE_LIMIT_WINDOW_MS` /
  `RATE_LIMIT_MAX` 或 `PUT /config` 调整，0 表示关闭）。
- 超额请求返回 **429**，响应体带 `retry_after_seconds`、响应头带 `Retry-After`；
  被限流的通知**未被受理**，网关不留记录。
- **同号重试不占额度**：已受理通知号的重复 POST 直接去重返回
  `{"status":"duplicate"}`，不消耗限流额度。
- 限流状态由持久化的受理记录重建，**网关重启后仍按原受理时刻限流**。

API 侧把 429 视为**待送**而非拒收：通知回到 `pending`（显示为
`rate_limited` / “网关限流，待补送”），按网关给的可重试时间到期补送；
其他联系人的投递照常并行，不被阻塞。被限流期间确认计时不会启动——计时仍以
网关真实受理时刻为准。备班通知在限流排队期间若告警被确认，补送会被取消
（确认早于实际发送）。

### 3. 计时只在“网关确已受理”后开始

每条通知在发起 HTTP 之前先落库（含完整请求体），通知号由数据库生成并复用。
worker 的状态机：

```
pending ──POST──> in_flight ──网关 202 accepted──> accepted   （主通知：此刻开始计时）
   ▲                  │
   │                  ├──网关 429 限流──> pending（按 retry_after_seconds 到期补送，不计时）
   │                  ├──网关明确拒收──> failed                （不盲目重试，展示原因）
   │                  └──超时/断连/回执丢失──> unknown        （不当作已送达，也不计时）
   │                                              │
   │                                       GET /notifications/{no} 查询受理结果
   │                                              ├── accepted → accepted（用网关受理时刻计时）
   │                                              ├── rejected → failed
   │                                              └── 无记录 ≥3 次 → 重投（通知号幂等）
   └──────────────────────429 限流：回到待送队列，到期自动补送──────────────────────┘
```

**回执不明永远不会触发升级**：`unknown` 状态下 `primary_accepted_at`、
`confirmation_due_at` 均为 `null`，接口状态为 `primary_unconfirmed`，并通过
`warnings` 明确提示。查询告警时会先向网关核对受理结果（只查不发）。

计时起点使用**网关记录的受理时间**，因此回执丢失或 API 重启后补查到的受理，
其期限仍从真实受理时刻起算，不会凭空延长主值班员的处理窗口。

### 4. 逾期升级：仅一次

后台 deadline 循环只做数据库操作、不发网络请求，因此网关再慢也不会推迟到期判断。
到期且未确认时，在单个事务内用条件 UPDATE 抢占并插入**唯一一条**备班通知
（`UNIQUE(alarm_no, role)` 兜底）。重复 tick、重启、并发都不会产生第二条。

### 5. 确认与到期的交错

`POST /alarms/{alarm_no}/confirm` 是一个短事务，不与网关通信，因此确认不会被慢投递阻塞。
竞态由条件 UPDATE 仲裁：

| 时序 | 结果 |
|---|---|
| 确认早于到期 | 直接 `confirmed`，不会创建备班通知 |
| 到期已生成备班行、但尚未发送 | 确认与“取消备班行”在同一事务完成，`status=confirmed`，备班显示“未投递即取消” |
| 备班已在途 | 确认成功，但**不撤回、也不再新建**；最终通过网关查询得到真实受理结果，显示 `confirmed_after_escalation`，并标记 `delivered_after_confirmation=true` 与“在途无法撤回”告警 |

### 6. 重启接续

- 重启时 `in_flight` 行（上一进程死在 POST 中）转为 `unknown`，先查网关再决定是否重投。
- 若网关已受理，直接采纳受理结果，**不会重发**；若网关无记录，重投时通知号相同，
  网关去重保证最多一次投递。
- pending/未知回执、未确认告警的到期扫描都从数据库重建，无内存队列可丢。

### 7. 慢投递不影响其他告警

- 投递是并行任务（`DELIVERY_CONCURRENCY`，默认 8），HTTP 调用不持有数据库锁。
- 确认、查询、到期扫描都只做短事务，不等待网络。
- 状态查询中的网关核对并发执行，最坏耗时约一次查询超时，而非逐条累加。

## 接口一览

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/alarms` | 提交告警（201；幂等重试 200；内容冲突 409） |
| GET | `/alarms/{alarm_no}` | 告警、通知、确认、计时状态 |
| POST | `/alarms/{alarm_no}/confirm` | 确认（幂等） |
| GET | `/alarms/{alarm_no}/notifications` | 该告警的通知明细 |
| GET | `/notifications/{notification_no}` | 按通知号查询网关受理结果 |
| GET | `/alarms` | 列出告警，可按 `status` 过滤 |
| GET | `/health` | 健康检查 |

状态值：`primary_queued`、`primary_sending`、`primary_unconfirmed`、
`primary_rejected`、`awaiting_confirmation`、`escalated`、`confirmed`、
`confirmed_after_escalation`。

## 通知网关（测试用）

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/notifications` | 受理通知，按 `notification_no` 去重；重复请求返回 `{"status":"duplicate"}`；同一联系人超限时返回 429 与 `retry_after_seconds` |
| GET | `/notifications/{no}` | 查询受理结果（回执丢失后的权威来源） |
| GET | `/notifications` | 列出所有通知记录（`?role=&alarm_no=`） |
| GET/PUT | `/config` | 运行时调整故障注入与限流参数 |
| DELETE | `/admin/reset` | 清空记录（测试便利） |

```bash
# 慢响应：受理延时 2.5s，API 的 POST 超时为 2s → API 看到“回执不明”
curl -X PUT localhost:9000/config -H 'content-type: application/json' \
  -d '{"slow_ms":2500,"drop_receipt":false,"reject":false}'

# 丢回执：确已受理，但连接挂起 3s 后断开，客户端拿不到回执
curl -X PUT localhost:9000/config -H 'content-type: application/json' \
  -d '{"slow_ms":0,"drop_receipt":true,"drop_hold_ms":3000}'

# 拒收
curl -X PUT localhost:9000/config -H 'content-type: application/json' \
  -d '{"reject":true}'

# 限流：同一联系人 3 秒最多 1 条（默认即开启；0 表示关闭窗口）
curl -X PUT localhost:9000/config -H 'content-type: application/json' \
  -d '{"rate_limit_window_ms":3000,"rate_limit_max":1}'
```

注意 `PUT /config` 是整体替换：未提供的字段恢复默认值（限流默认
`3000ms / 1 条`）。网关记录持久化到卷中，因此 API 与网关都重启后仍可查询
历史受理结果，限流也仍按原受理时刻生效。

## 演示

```bash
# 1) 正常：3 秒内确认，不升级
curl -X POST localhost:8000/alarms -H 'content-type: application/json' -d '{
  "alarm_no":"DEMO-1","content":"供电柜 A07 温度 78°C",
  "primary_contact":{"name":"主值班员 张工","channel":"app"},
  "backup_contact":{"name":"备班 李工","channel":"sms","address":"+8613800000000"},
  "ack_deadline_seconds":5}'
curl -X POST localhost:8000/alarms/DEMO-1/confirm -H 'content-type: application/json' \
  -d '{"confirmed_by":"张工"}'
curl -s localhost:8000/alarms/DEMO-1 | python3 -m json.tool

# 2) 逾期：2 秒不确认，只通知一次备班
curl -X POST localhost:8000/alarms -H 'content-type: application/json' -d '{
  "alarm_no":"DEMO-2","content":"供电柜 B02 温度 82°C",
  "primary_contact":{"name":"主值班员","channel":"app"},
  "backup_contact":{"name":"备班","channel":"sms"},
  "ack_deadline_seconds":2}'
sleep 5
curl -s localhost:8000/alarms/DEMO-2 | python3 -m json.tool
```

## 配置

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATABASE_PATH` | `./data/alarms.db` | SQLite 路径 |
| `GATEWAY_URL` | `http://127.0.0.1:9000` | 通知网关地址 |
| `GATEWAY_TIMEOUT_SECONDS` | `2.0` | 投递 POST 超时 |
| `GATEWAY_LOOKUP_TIMEOUT_SECONDS` | `1.0` | 受理结果查询超时 |
| `WORKER_INTERVAL_SECONDS` | `0.1` | worker tick 间隔 |
| `DELIVERY_LEASE_SECONDS` | `10` | 在途租约；进程死亡后由他人接管 |
| `DELIVERY_MAX_BACKOFF_SECONDS` | `1.0` | 未知回执的重查退避 |
| `DELIVERY_CONCURRENCY` | `8` | 并发投递上限 |
| `NOT_FOUND_MISSES_BEFORE_REPOST` | `3` | 网关连续“无记录”多少次后才重投 |
| `RECONCILE_ON_READ` | `true` | 查询时是否先向网关核对未知回执（只查不发） |
| `RECOVER_INFLIGHT_ON_STARTUP` | `true` | 启动时把中断的在途通知转为待查 |

网关侧（gateway 容器）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `RATE_LIMIT_WINDOW_MS` | `3000` | 同一联系人限流窗口（毫秒），0 表示关闭 |
| `RATE_LIMIT_MAX` | `1` | 窗口内同一联系人最多受理的新通知条数 |
| `GATEWAY_SLOW_MS` / `GATEWAY_DROP_RECEIPT` / `GATEWAY_REJECT` | `0` | 故障注入（同 `PUT /config`） |
| `GATEWAY_STATE_PATH` | — | 受理记录持久化路径（重启后限流与查询仍有效） |

## 测试

```bash
python -m pytest -q                     # 35 个测试，进程内假网关，真实 SQLite 文件
API_URL=http://localhost:8000 GATEWAY_URL=http://localhost:9000 \
  python -I scripts/smoke_test.py       # 对真实运行的服务做端到端冒烟
```

覆盖：幂等提交与 409、计时起点、确认阻断升级、逾期仅一次、回执不明不升级、
回执补查不重复投递、重启接续（三种中断点）、确认 vs 到期竞态、慢投递不阻塞、
拒收处理、在途备班如实披露、Swagger/OpenAPI，以及联系人限流：429 契约与
可重试时间、同号重试不占额度、限流补送且不计时、其他联系人不受影响、
备班限流补送与确认取消、并发突发不漏不重、API/网关重启后限流与补送仍正确。

## 已知限制

- SQLite 单写者：适合单实例部署。多副本共享一个数据库需要换成 PostgreSQL，
  并重新审视事务隔离（`RECOVER_INFLIGHT_ON_STARTUP` 也需关闭）。
- 网关拒收主通知后不自动重发，只标记并告警，等待人工核实联系人——这是有意为之，
  避免对错误号码反复外呼。
- 备班通知不设二次升级（需求只要求一次）。
- API 本身没有鉴权；生产部署前应补上。
