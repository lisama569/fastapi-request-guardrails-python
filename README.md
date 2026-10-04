# fastapi-request-guardrails

在 FastAPI 服务上增加一层面向外部调用方的请求防护与可靠性保障：**按调用方分级限流、
服务整体容量保护、请求幂等（含并发首失败）、下游熔断**，
并保证并发、慢下游、重复提交、依赖故障下的行为一致且可解释。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload
uv run pytest          # 36 个用例：分级配额 / 全局容量 / 并发隔离 / 幂等首失败 / 熔断
```

核心接口：

| 接口 | 说明 |
| --- | --- |
| `POST /orders` | 受多重防护保护的下单接口（需 `X-Caller-Id`，可选 `Idempotency-Key`），同步端点在线程池中执行，慢请求不阻塞事件循环 |
| `GET /health/downstream` | 查看熔断状态、下游模式、**当前调用方配额档位/已用/窗口剩余**、**全局容量在途用量** |
| `POST /test/downstream-mode/{ok\|fail\|timeout\|flaky}` | 切换本地模拟下游的故障模式（测试/运维辅助） |

## 一、限流语义（按调用方配额 + 分级）

- **维度**：调用方由请求头 `X-Caller-Id` 标识（缺省 `anonymous`），各调用方独立计数。
- **分级配额（新增）**：`rate_limits` 可为指定调用方单独设额度（放宽或收紧）；
  未配置的调用方一律走默认档 `rate_limit`，**行为与旧版本完全一致**。
  也可通过环境变量 `RATE_LIMIT_OVERRIDES` 配置，见下文配置说明。
- **算法**：固定时间窗口。窗口起点按 `rate_limit_window_seconds` 对齐到整窗，
  每窗口允许的请求数取该调用方命中的档位；超额请求在**同一把锁内**完成
  "判窗口 → 读计数 → 比较 → 自增 → 落盘"，因此并发下**不会超发也不会漏判**。
- **拒绝行为**：超额返回 `429`，错误体 `reason="rate_limit_exceeded"`，
  带 `retry_after_seconds`（到窗口边界的剩余秒数）与 `Retry-After` 响应头。
- **状态可观测（增强）**：`GET /health/downstream` 的 `caller_quota` 字段给出
  `tier`（`default` 或 `custom:<额度>`）、`used`、`limit`、`remaining`（本窗口剩余）、
  `window_reset_seconds`（窗口重置剩余时间）；旧字段 `caller_used_quota` 保留兼容。
- **重启语义**：计数持久化到 `rate_limit_state_file`（JSON，原子写）。
  服务重启后若仍处于同一窗口，已消耗额度继续生效；跨过窗口边界后计数自然归零。
  状态文件损坏时按空额度启动。配置为 `None` 时退化为纯内存。

## 二、服务整体容量保护（全局在途上限 + 公平份额，新增）

按调用方各算各的之外，再加一层**全服务维度**的容量保护，防止多个调用方合流压垮下游：

- **全局在途上限** `global_max_inflight`：同一时刻允许真正打到下游的请求总数。
  槽位只在"熔断放行后、发起下游调用前"占用，在请求落定（成功/失败/超时/熔断拒绝之外的
  所有路径）的同一代码块释放；**超时后槽位立即回收**，线程池中仍在睡眠的慢任务不虚占额度。
- **公平份额** `global_capacity_caller_share`：单个调用方同时持有的在途槽位不得超过
  `max(1, ceil(global_max_inflight * share))`。槽位按瞬时在途占用、完成即还，
  因此任何量大的调用方都无法把全局额度长期占住——它达到自身份额后被拒，
  其他调用方仍可使用剩余全局槽位。
- **拒绝契约**：超额返回 `429 reason="global_capacity_exceeded"`
  （带 `retry_after_seconds` 与 `Retry-After`，取一个下游 deadline，即"最久等这么久
  必有槽位释放"）。message 中区分 `global_inflight_full`（全局饱和）与
  `caller_share_full`（单调用方份额用尽），且该 reason 与调用方窗口配额超额的
  `rate_limit_exceeded`、熔断中的 `circuit_breaker_open` 三者**明确可区分**。
- **并发隔离**：下单端点为同步函数，FastAPI 将其放入线程池执行；服务自身也用
  余量充足的线程池（32）提交下游调用。一批慢请求同时到达时各自在独立线程并行推进，
  整体墙钟接近单个请求耗时，**不会因为一个慢调用方而把其他调用方串成一队**。
  `global_max_inflight <= 0` 可关闭该层（退化为旧行为）。

## 三、幂等规则（含并发在途与首个失败）

- 调用方通过 `Idempotency-Key` 请求头提交幂等键（不传则每次请求正常执行）。
- 请求体的 `item`+`quantity` 经规范化哈希得到 **fingerprint**，规则如下：
  1. **首次提交（leader）**：登记 fingerprint，执行业务，结果（状态码 + 响应体）只写一次；
  2. **同 key 同内容的并发在途提交（follower）**：等待 leader 落定并复用**同一个确定结果**，
     自己绝不调用下游、绝不产生副作用。**leader 最终成功则全部返回同一成功结果；
     leader 最终失败（业务错误/超时/熔断/容量拒绝等）则所有 follower 得到与 leader
     完全一致的失败（同 reason、同状态码、同重试提示）**，不会一会儿成功一会儿失败，
     也不会把 follower 的提交当成新请求再下一次单；
  3. **leader 失败落定后用同一标识重试**：失败结果不会被永久缓存，同 key 同内容的新提交
     作为新的 leader 正常执行；重试成功后结果落定，真实下单在整个过程中**只发生一次**；
  4. **同 key 内容不一致**：无论在途还是已落定，一律返回 `409 reason="idempotency_conflict"`，
     **不覆盖**先前结果；随后用原始内容重放仍能拿到首次结果。
- 成功结果在 `idempotency_ttl_seconds` 内有效，TTL 到期后同 key 可开启新一轮请求。
- 实现为进程内带锁存储，适用于单实例语义；多实例部署需替换为 Redis 等共享存储。

## 四、熔断状态与故障隔离

对本地模拟下游（`app/downstream.py`，支持 `ok/fail/timeout/flaky` 四种模式）做三态熔断：

| 状态 | 行为 |
| --- | --- |
| `closed` | 正常放行；连续失败达到 `circuit_failure_threshold` 后开路 |
| `open` | 快速失败，直接返回 `503 reason="circuit_breaker_open"`（带重试提示），不占用容量槽位、不调用下游 |
| `half_open` | 冷却 `circuit_open_seconds` 后进入；同一时刻只放行 **1 个探测**，连续成功 `circuit_half_open_success_threshold` 次后闭合；探测期间任一失败立即重新开路并重新计时冷却 |

依赖故障以多种**可区分**方式对外暴露，内部异常与堆栈不进入响应：

| 场景 | HTTP | `error.reason` |
| --- | --- | --- |
| 调用方等待超过 deadline | 504 | `downstream_timeout` |
| 下游自身返回错误 | 502 | `downstream_error` |
| 熔断开路快速失败 | 503 | `circuit_breaker_open` |
| 调用方窗口配额超额 | 429 | `rate_limit_exceeded` |
| 服务全局容量饱和 / 单调用方份额用尽（新增） | 429 | `global_capacity_exceeded` |
| 幂等内容冲突 | 409 | `idempotency_conflict` |
| 幂等首请求仍在处理（等待超时的极端情况） | 409 | `idempotency_in_progress` |
| 未知内部错误 | 500 | `internal_error`（仅记日志，不透出异常类型/消息/堆栈） |

## 五、配置项（`app/config.py`）

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `rate_limit` | 5 | 默认档：未单独配置的调用方每窗口配额 |
| `rate_limits` | `{}` | **新增**：分级配额，`{调用方: 额度}`，如 `{"vip":20,"restricted":2}`；也可用环境变量 `RATE_LIMIT_OVERRIDES='{"vip":20}'` |
| `rate_limit_window_seconds` | 10.0 | 固定窗口长度（秒） |
| `rate_limit_state_file` | `.ratelimit.json` | 窗口计数持久化文件，`None` 表示纯内存 |
| `idempotency_ttl_seconds` | 3600.0 | 幂等成功结果保留时长 |
| `global_max_inflight` | 8 | **新增**：全局在途下游请求上限，`<=0` 关闭容量层 |
| `global_capacity_caller_share` | 0.5 | **新增**：单调用方可占全局在途的比例（份额=`ceil(上限*比例)`，至少 1）；`1.0` 表示不限制单调用方 |
| `circuit_failure_threshold` | 3 | closed 下连续失败多少次开路 |
| `circuit_open_seconds` | 5.0 | open 冷却时长，到期进入 half_open |
| `circuit_half_open_success_threshold` | 2 | half_open 连续成功多少次闭合（逐步放行） |
| `downstream_timeout_seconds` | 0.5 | 下游调用 deadline（秒），同时作为容量拒绝的可预期重试提示 |

**兼容性**：不设置 `rate_limits` 时所有调用方走 `rate_limit` 默认档；不设置
`global_max_inflight`（或显式 `<=0`）时没有全局容量层；旧的错误 reason、
`Retry-After` 头与 `caller_used_quota` 字段均保持不变。

## 六、代码结构

```
app/
  config.py          配置项（含分级配额、全局容量参数、环境变量覆盖）
  errors.py          可预期错误类型（reason / status_code / retry_after / 快照重建注册表）
  models.py          请求响应模型
  rate_limiter.py    固定窗口分级限流（锁 + 文件持久化 + 档位/剩余查询）
  global_capacity.py 全局在途容量 + 单调用方公平份额（新增）
  idempotency.py     幂等键存储（leader/follower / 成功结果 / 失败快照 / TTL）
  circuit_breaker.py 三态熔断器（连续失败开路 / 探测半开 / 逐步恢复）
  downstream.py      本地模拟下游（ok/fail/timeout/flaky）
  service.py         编排：配额 → 幂等 → 熔断 → 容量 → 带超时下游调用（线程池并行）
  api.py             FastAPI 路由与统一错误处理（同步下单端点 + 增强状态接口）
tests/               旧用例 19 个（无回归） + 新增 17 个：
                     分级配额(7) + 全局容量与并发隔离(5) + 并发幂等首失败(5)
```

## 七、本地验证新场景

```bash
uv run pytest -s                       # 查看请求输入与判定依据日志
uv run pytest tests/test_tiered_quota.py            # 分级配额 + 状态接口档位/剩余
uv run pytest tests/test_global_capacity.py         # 全局容量拒绝 / 公平份额 / 慢请求并行墙钟
uv run pytest tests/test_idempotency_concurrent.py  # 同标识并发、首个失败/超时、失败后重试
```

启动服务后手工验证：

```bash
uv run uvicorn main:app --reload

# 1) 状态接口：看某调用方走哪一档、窗口剩多少、全局容量是否开启
curl -s localhost:8000/health/downstream -H 'X-Caller-Id: vip' | python -m json.tool

# 2) 全局容量拒绝（先把下游切成慢模式，再并发打满，观察可区分原因）
curl -s -X POST localhost:8000/test/downstream-mode/timeout
for i in $(seq 1 12); do
  curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8000/orders \
    -H 'X-Caller-Id: c'$i -H 'Content-Type: application/json' -d '{"item":"x","quantity":1}' &
done; wait
curl -s localhost:8000/health/downstream -H 'X-Caller-Id: c1' | python -m json.tool
curl -s -X POST localhost:8000/test/downstream-mode/ok   # 恢复

# 3) 并发同标识、首个失败后重试
curl -s -X POST localhost:8000/test/downstream-mode/fail
for i in 1 2 3; do
  curl -s -X POST localhost:8000/orders -H 'X-Caller-Id: r'$i \
    -H 'Idempotency-Key: order-77' -H 'Content-Type: application/json' \
    -d '{"item":"book","quantity":1}' &
done; wait    # 首个失败，并发的其余提交拿到同一个 downstream_error
curl -s -X POST localhost:8000/test/downstream-mode/ok
curl -s -X POST localhost:8000/orders -H 'X-Caller-Id: r1' \
  -H 'Idempotency-Key: order-77' -H 'Content-Type: application/json' \
  -d '{"item":"book","quantity":1}'   # 同标识重试成功，真实下单只发生一次
```

单测执行时会以 INFO 级别打印**请求输入与判定依据**（命中档位 tier、窗口已用额度、
ALLOW/REJECT、容量 caller_inflight/global_inflight、幂等 leader/follower 与成败落定、
熔断状态迁移），可通过 `uv run pytest -s` 查看。
