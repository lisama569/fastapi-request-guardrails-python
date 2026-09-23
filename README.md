# fastapi-request-guardrails

在 FastAPI 之上演示一层面向外部调用方的请求防护与可靠性保障：
**按调用方配额限流、请求幂等、下游熔断隔离**。所有拒绝结果都带有
稳定错误码、可读原因和可预期的重试提示，内部异常与堆栈绝不透出响应。

## 运行

```bash
uv sync
uv run uvicorn main:app --reload
# 测试（日志会打印请求输入与判定依据）
uv run pytest
```

## 接口

| 方法/路径 | 说明 |
| --- | --- |
| `POST /v1/charge` | 下单（演示链路），需请求头 `X-Caller-Id` |
| `GET /guardrails/state` | 查看熔断器当前状态 |
| `GET /healthz` | 健康检查（含熔断快照） |

请求体：

```json
{ "idempotency_key": "order-123", "amount": 100, "scenario": "ok" }
```

`scenario` 用于注入本地模拟下游的行为：`ok`（正常）、`fail`（下游业务
错误）、`timeout`/`slow`（超时）。

### 错误响应（统一结构）

```json
{
  "error": "RATE_LIMITED",
  "reason": "配额耗尽：滚动 60.0s 窗口内已达 10 次上限……",
  "retry_after_seconds": 12.34,
  "request_id": "…"
}
```

| HTTP | error | 触发条件 | 可重试 |
| --- | --- | --- | --- |
| 429 | `RATE_LIMITED` | 调用方窗口配额用尽；带 `Retry-After` 头与秒数 | 到 `retry_after_seconds` 后 |
| 422 | `IDEMPOTENCY_CONFLICT` | 同键不同内容 | 否，需更换键 |
| 502 | `DOWNSTREAM_ERROR` | 下游自身返回错误 | 可安全重试 |
| 504 | `DOWNSTREAM_TIMEOUT` | 下游超过超时阈值 | 可安全重试（未产生副作用） |
| 503 | `CIRCUIT_OPEN` | 熔断打开/半开名额满 | OPEN 时按提示等待半开 |
| 401 | `UNAUTHENTICATED` | 缺少 `X-Caller-Id` | — |
| 500 | `INTERNAL_ERROR` | 未预期异常（细节仅入日志） | 可重试 |

## 限流语义

- **维度**：按请求头 `X-Caller-Id` 区分调用方，各调用方额度独立。
- **算法**：单调用方滚动窗口（sliding window）。窗口内成功登记的
  请求数达到 `GUARDRAIL_QUOTA_LIMIT` 后，新请求返回 429；被拒请求
  **不占用**额度。
- **重试提示**：429 响应给出 `retry_after_seconds`（最早一条请求滑出
  窗口还需的时间），并同步输出 HTTP `Retry-After` 头（向上取整秒）。
- **并发一致性**：「清理窗口 → 计数 → 登记」在同一把异步锁内原子完成，
  高并发下放行数严格等于额度，不超发、不漏判（见
  `test_concurrent_quota_no_over_issuance`）。
- **配额与幂等的次序**：先限流、后幂等。即回放/冲突请求也消耗窗口额度——
  限流约束的是“进入服务的请求速率”，幂等约束的是“副作用执行次数”。

## 幂等规则

- 调用方在请求体中提供 `idempotency_key`；服务端对参与判定的内容
  （`amount`、`scenario`）做哈希。
- **同键同内容**：首次请求执行下游并缓存成功终态（TTL 内）；重复提交
  直接返回同一结果（`replayed=true`，`request_id`/`downstream_ref`
  完全一致），下游只被调用一次。
- **同键异内容**：立即返回 422 `IDEMPOTENCY_CONFLICT`，**绝不覆盖**
  既有结果；原结果仍可正常回放。
- **处理中重复**：首请求未完成时，同键请求在锁外等待其结果，不触发
  第二次执行；处理中到达的异内容请求立即 422。
- **失败不缓存**：下游错误、超时、熔断拒绝均未产生副作用，服务端释放
  in-flight 标记，调用方可用同键安全重试（等待中的请求随后获得执行权）。
- **TTL**：成功结果保留 `GUARDRAIL_IDEMPOTENCY_TTL_SECONDS` 秒，过期
  后同键视为新请求。

## 熔断状态

针对本地模拟的下游依赖维护三态熔断器：

```
CLOSED ──连续失败达阈值──▶ OPEN ──冷却时间到──▶ HALF_OPEN
  ▲                            ▲                  │
  └──────探测成功──────────────┴────探测失败──────┘
```

- `CLOSED`：正常放行；连续失败（含超时与下游错误）累计达到
  `GUARDRAIL_CB_FAILURE_THRESHOLD` 后转到 `OPEN`。
- `OPEN`：直接快速失败返回 503，不调用下游；响应中的
  `retry_after_seconds` 表示距半开探测的时间。
- `HALF_OPEN`：冷却 `GUARDRAIL_CB_OPEN_COOLDOWN_SECONDS` 后进入，
  仅放行 `GUARDRAIL_CB_HALF_OPEN_PROBE` 个探测请求；探测成功立即
  关闭熔断并清零失败计数，任一探测失败则重新打开并重新计时。
  半开期间超出探测名额的请求同样返回 503（原因为“名额已满”）。
- 超时（504）、下游业务错误（502）、熔断拒绝（503）错误码互不相同，
  调用方可据此区分处理。

## 配置项（环境变量，均有默认值）

| 环境变量 | 默认 | 含义 |
| --- | --- | --- |
| `GUARDRAIL_QUOTA_LIMIT` | 10 | 每调用方每窗口请求上限 |
| `GUARDRAIL_QUOTA_WINDOW_SECONDS` | 60.0 | 滚动窗口长度（秒） |
| `GUARDRAIL_IDEMPOTENCY_TTL_SECONDS` | 3600.0 | 成功幂等结果保留时长（秒） |
| `GUARDRAIL_CB_FAILURE_THRESHOLD` | 5 | 触发熔断的连续失败次数 |
| `GUARDRAIL_CB_OPEN_COOLDOWN_SECONDS` | 30.0 | OPEN→HALF_OPEN 冷却（秒） |
| `GUARDRAIL_CB_HALF_OPEN_PROBE` | 1 | 半开时允许的探测请求数 |
| `GUARDRAIL_DOWNSTREAM_TIMEOUT_SECONDS` | 2.0 | 下游调用超时（秒） |

## 重启语义（明确且稳定）

配额计数、幂等缓存、熔断状态均保存在**单进程内存**中、不落盘：

- 重启后滚动窗口立即清空，从 0 重新累计；
- 幂等缓存清空，重启前的键不再命中（需调用方在服务升级窗口避免重用
  键，或后续外接共享存储）；
- 熔断器回到 `CLOSED`，失败计数清零。

## 目录

```
app/
  config.py                 配置与重启语义
  schemas.py                请求/响应模型
  errors.py                 错误码、异常与异常处理器
  logging_setup.py          日志（测试输出请求输入与判定依据）
  downstream.py             本地模拟下游（ok/fail/timeout/slow）
  factory.py                应用装配与 /v1/charge 主流程
  guardrails/
    rate_limiter.py         滚动窗口配额
    idempotency.py          in-flight 去重 + 终态缓存
    circuit_breaker.py      三态熔断器
tests/                      并发配额/幂等/熔断/错误处理测试
```
