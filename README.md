# fastapi-request-guardrails

在 FastAPI 服务上增加一层面向外部调用方的请求防护与可靠性保障：**按调用方分级限流、请求幂等、
服务整体容量保护、下游熔断**，并保证并发、重复提交、依赖故障下的行为一致且可解释。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload
uv run pytest          # 34 个用例：分级配额 / 并发配额 / 重复与冲突提交 / 全局容量 / 并发隔离 / 熔断与半开恢复 / 同标识首失败结果一致
```

核心接口：

| 接口 | 说明 |
| --- | --- |
| `POST /orders` | 受多重防护保护的下单接口（需 `X-Caller-Id`，可选 `Idempotency-Key`） |
| `GET /health/downstream` | 查看熔断状态、下游模式、当前调用方配额档位/已用/剩余、全局容量视图 |
| `POST /test/downstream-mode/{ok\|fail\|timeout\|flaky}` | 切换本地模拟下游的故障模式（测试/运维辅助） |

## 一、限流语义（按调用方分级配额）

- **维度**：调用方由请求头 `X-Caller-Id` 标识（缺省 `anonymous`），各调用方独立计数。
- **分级配额**：`rate_limit_overrides` 可为指定调用方单独设额度（放宽或收紧），
  例如 `{"vip": 100, "trial": 2}`；未单独配置的调用方走 `rate_limit` 默认档，
  **默认配置下行为与之前完全一致**。
- **算法**：固定时间窗口。窗口起点按 `rate_limit_window_seconds` 对齐到整窗，
  每窗口允许对应档位的请求数；第 `limit+1` 个请求在**同一把锁内**完成
  "判窗口 → 读计数 → 比较 → 自增 → 落盘"，因此并发下**不会超发也不会漏判**。
- **拒绝行为**：超额返回 `429`，错误体 `reason="rate_limit_exceeded"`，
  带 `retry_after_seconds`（到窗口边界的剩余秒数）与 `Retry-After` 响应头，
  调用方可据此在窗口边界后重试。
- **状态可查**：`GET /health/downstream` 返回当前调用方的
  `caller_quota_tier`（`default`/`override`）、`caller_quota_limit`、
  `caller_used_quota`、`caller_quota_remaining`，方便排查"走的是哪一档、窗口还剩多少"。
- **重启语义（明确且稳定）**：计数持久化到 `rate_limit_state_file`（JSON,原子写）。
  服务重启后若仍处于同一窗口，已消耗额度继续生效；跨过窗口边界后计数自然归零。
  状态文件损坏时按空额度启动，不影响服务可用性。配置为 `None` 时退化为纯内存。

## 二、幂等规则

- 调用方通过 `Idempotency-Key` 请求头提交幂等键（不传则每次请求正常执行）。
- 请求体的 `item`+`quantity` 经规范化哈希得到 **fingerprint**，规则如下：
  1. **首次提交**：登记 fingerprint，执行业务，结果（状态码 + 响应体）只写一次；
  2. **同 key 同内容并发提交（首个仍在处理中）**：后到的请求等待首个完成，
     **不会被当成新请求再触发一次下游调用**，结果约定如下：
     - 首个成功 → 所有并发提交复用**同一份**成功结果（状态码与响应体完全一致）；
     - 首个失败（无论哪种失败）→ 所有并发提交收到**同一份**失败
       （reason / message / retry_after 完全一致），对外只有一个确定、可解释的结果。
     已认识的失败原因各自保持可区分：`rate_limit_exceeded` /
     `global_capacity_exceeded` / `circuit_breaker_open` / `downstream_timeout` /
     `downstream_error`；首个请求若因**非预期异常**失败，所有并发提交统一收到
     `500 {"reason":"internal_error","message":"internal server error"}`，
     内部异常的**类型、原始消息、堆栈一律不透出**（只记服务端日志）；
  3. **首个已完成后的同 key 同内容重复提交**：直接返回**完全一致**的状态码与
     响应体，不再产生副作用；
  4. **同 key 内容不一致**：明确返回 `409 reason="idempotency_conflict"`，
     **不覆盖**先前结果；随后用原始内容重放仍能拿到首次结果；
  5. 首次请求失败（含非预期异常）后占位被移除（**失败结果不缓存、标识不被一直
     占用**），之后携带同一 key 的重试作为新一轮首次请求正常执行；无论成功失败，
     **真实下单全程只发生一次**。
- 结果在 `idempotency_ttl_seconds` 内有效，TTL 到期后同 key 可开启新一轮请求。
- 实现为进程内带锁存储，适用于单实例语义；多实例部署需替换为 Redis 等共享存储。

## 三、服务整体容量保护

按调用方限流之外，再加一层**服务级**容量保护，防止多个调用方合起来把下游压垮：

- **全局在途上限**：任何时刻进入下游的在途请求数不超过 `global_max_inflight`；
  超出立即拒绝 `503 reason="global_capacity_exceeded"`，与"调用方自己超额"
  （429 `rate_limit_exceeded`）和"熔断中"（503 `circuit_breaker_open`）都可区分。
- **可预期重试提示**：拒绝响应带 `retry_after_seconds`（一个下游 deadline，
  即在途名额最长被占用的时长）与 `Retry-After` 响应头。
- **公平性**：单个调用方的在途数另有上限 `global_per_caller_inflight_cap`
  （缺省自动取全局上限的一半，至少为 1），量级特别大的调用方无法把整体额度
  长期占住，其他调用方始终进得来。
- **默认不启用**（`global_max_inflight=None`），不配置时行为与之前完全一致。
- `GET /health/downstream` 的 `global_capacity` 字段暴露
  `enabled / max_inflight / per_caller_cap / inflight_total`，便于压测观测。

## 四、并发隔离

- `POST /orders` 为同步路由，由 Starlette 线程池执行，慢下游不会阻塞事件循环；
  下游调用在线程池（16 worker）上并行推进。
- 一批慢请求并发到达时，**整体完成时间 ≈ 单个请求耗时**（有对应单测断言），
  一个慢调用方不会把其他调用方拖住。

## 五、熔断状态与故障隔离

对本地模拟下游（`app/downstream.py`，支持 `ok/fail/timeout/flaky` 四种模式）做三态熔断：

| 状态 | 行为 |
| --- | --- |
| `closed` | 正常放行；连续失败达到 `circuit_failure_threshold` 后开路 |
| `open` | 快速失败，直接返回 `503 reason="circuit_breaker_open"`（带重试提示），不调用下游 |
| `half_open` | 冷却 `circuit_open_seconds` 后进入；同一时刻只放行 **1 个探测**，连续成功 `circuit_half_open_success_threshold` 次后闭合；探测期间任一失败立即重新开路并重新计时冷却 |

依赖故障与防护拒绝以**可区分**方式对外暴露，内部异常与堆栈不进入响应：

| 场景 | HTTP | `error.reason` |
| --- | --- | --- |
| 调用方等待超过 deadline | 504 | `downstream_timeout` |
| 下游自身返回错误 | 502 | `downstream_error` |
| 熔断开路快速失败 | 503 | `circuit_breaker_open` |
| 服务整体容量满（含单调用方份额占满） | 503 | `global_capacity_exceeded` |
| 调用方配额超额 | 429 | `rate_limit_exceeded` |
| 幂等内容冲突 | 409 | `idempotency_conflict` |
| 未知内部错误 | 500 | `internal_error`（仅记日志，不透出异常类型/消息/堆栈） |

## 六、配置项（`app/config.py`）

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `rate_limit` | 5 | 默认档：每调用方每窗口配额 |
| `rate_limit_overrides` | `{}` | 分级配额：指定调用方的单独额度（覆盖默认档） |
| `rate_limit_window_seconds` | 10.0 | 固定窗口长度（秒） |
| `rate_limit_state_file` | `.ratelimit.json` | 窗口计数持久化文件，`None` 表示纯内存 |
| `global_max_inflight` | `None` | 全局在途上限；`None` 表示不启用容量保护（默认行为不变） |
| `global_per_caller_inflight_cap` | `None` | 单调用方在途份额上限；`None` 自动取全局上限的一半（至少 1） |
| `idempotency_ttl_seconds` | 3600.0 | 幂等结果保留时长 |
| `circuit_failure_threshold` | 3 | closed 下连续失败多少次开路 |
| `circuit_open_seconds` | 5.0 | open 冷却时长，到期进入 half_open |
| `circuit_half_open_success_threshold` | 2 | half_open 连续成功多少次闭合（逐步放行） |
| `downstream_timeout_seconds` | 0.5 | 下游调用 deadline（秒），同时作为容量拒绝的重试提示 |

## 七、代码结构

```
app/
  config.py          配置项（含分级配额与全局容量参数）
  errors.py          可预期错误类型（reason / status_code / retry_after）
  models.py          请求响应模型
  rate_limiter.py    固定窗口分级限流（锁 + 文件持久化）
  idempotency.py     幂等键存储（fingerprint 校验 / 处理中等待 / 失败传播 / TTL）
  capacity.py        服务整体容量保护（全局在途上限 + 单调用方份额公平）
  circuit_breaker.py 三态熔断器（连续失败开路 / 探测半开 / 逐步恢复）
  downstream.py      本地模拟下游（ok/fail/timeout/flaky）
  service.py         编排：限流 → 幂等 → 全局容量 → 熔断 → 带超时下游调用
  api.py             FastAPI 路由与统一错误处理
tests/               限流(6) + 分级配额(5) + 幂等(5) + 全局容量(4) + 并发隔离(6) + 熔断(6) + 错误契约(2)
```

## 八、本地验证新场景

```bash
# 全部用例（含原有 32 个回归），开头按业务模块列出用例、结尾按模块汇总通过情况
uv run pytest

# 只看新增场景（-s 可同时看到每个请求的输入与判定依据日志）
uv run pytest tests/test_tiered_quota.py -s      # 分级配额：放宽/收紧/默认档/状态接口
uv run pytest tests/test_global_capacity.py -s   # 全局容量拒绝、公平性、状态视图
uv run pytest tests/test_concurrency.py -s       # 慢请求并行；并发同标识首个失败
```

`tests/test_concurrency.py` 中与并发同标识相关的 4 条用例，可按首个请求结果
分两类单独验证：

```bash
# 场景 A：并发同标识、首个成功 -> 所有提交同一份成功结果，真实下单只发生一次
uv run pytest -s tests/test_concurrency.py::test_in_flight_duplicate_does_not_double_execute

# 场景 B：并发同标识、首个失败 -> 等待者拿到与首个完全相同的失败，且不重复下单
#   B1: 失败属于"已认识"的下游报错（502 downstream_error）
uv run pytest -s tests/test_concurrency.py::test_concurrent_same_key_first_fails_deterministic_outcome
uv run pytest -s tests/test_concurrency.py::test_concurrent_same_key_first_fails_never_double_orders
#   B2: 下游抛出非预期异常（两份响应一致为 500 internal_error，无内部细节泄露；
#       失败后同标识重试成功且全程只下一次单）
uv run pytest -s tests/test_concurrency.py::test_concurrent_same_key_unexpected_error_unified_outcome
uv run pytest -s tests/test_concurrency.py::test_concurrent_same_key_unexpected_error_never_double_orders
```

> 注意：场景 A 用例在 `tests/test_idempotency.py`；B2 正是本次修复的链路——
> 修复前等待中的重复请求会拿到 `400 guardrail_error` 且响应体直接携带内部异常原文。

手动验证（服务运行在 `localhost:8000`）：

```bash
# 分级配额与状态接口（在 app/config.py 的 Settings 里配 rate_limit_overrides 后生效）
curl -i -X POST localhost:8000/orders -H 'X-Caller-Id: vip' \
     -H 'Content-Type: application/json' -d '{"item":"book","quantity":1}'
curl -s localhost:8000/health/downstream -H 'X-Caller-Id: vip'
# -> caller_quota_tier / caller_quota_limit / caller_used_quota / caller_quota_remaining

# 并发同标识重复提交（首个成功：应返回完全一致的结果，且只下一次单）
curl -s -X POST localhost:8000/orders -H 'X-Caller-Id: a' -H 'Idempotency-Key: k1' \
     -H 'Content-Type: application/json' -d '{"item":"book","quantity":1}' &
curl -s -X POST localhost:8000/orders -H 'X-Caller-Id: a' -H 'Idempotency-Key: k1' \
     -H 'Content-Type: application/json' -d '{"item":"book","quantity":1}' &
wait

# 并发同标识、首个失败：先切到下游报错模式再并发提交，
# 两个响应应为完全一致的 502 downstream_error（失败原因可区分、结果确定）
curl -s -X POST localhost:8000/test/downstream-mode/fail
curl -s -X POST localhost:8000/orders -H 'X-Caller-Id: b' -H 'Idempotency-Key: k2' \
     -H 'Content-Type: application/json' -d '{"item":"book","quantity":1}' &
curl -s -X POST localhost:8000/orders -H 'X-Caller-Id: b' -H 'Idempotency-Key: k2' \
     -H 'Content-Type: application/json' -d '{"item":"book","quantity":1}' &
wait
# 恢复 ok 后用同一标识重试：应 201 成功（失败不缓存，重试是新一轮首次请求）
curl -s -X POST localhost:8000/test/downstream-mode/ok
curl -i -X POST localhost:8000/orders -H 'X-Caller-Id: b' -H 'Idempotency-Key: k2' \
     -H 'Content-Type: application/json' -d '{"item":"book","quantity":1}'
```

> 非预期异常（下游抛出 `RuntimeError`/`TimeoutError` 以外的异常）无法通过
> `/test/downstream-mode` 手动注入，其并发一致性由
> `test_concurrent_same_key_unexpected_error_*` 两条用例稳定复现并守护。

单测执行时会以 INFO 级别打印**请求输入与判定依据**（窗口已用额度与命中档位、ALLOW/REJECT、
fingerprint 匹配/冲突、容量在途与份额、状态迁移与探测计数），可通过 `uv run pytest -s` 查看。
