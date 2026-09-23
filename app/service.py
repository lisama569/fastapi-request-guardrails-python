"""业务编排：限流 -> 幂等 -> 熔断 -> 下游调用。

成功返回 (status_code, body)；所有可预期失败抛出 GuardrailError 子类，
内部异常不向上游透出堆栈。
"""

from __future__ import annotations

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

from .circuit_breaker import CircuitBreaker
from .config import Settings, get_settings
from .downstream import DownstreamClient
from .errors import CircuitBreakerOpen, DownstreamError, DownstreamTimeout
from .idempotency import IdempotencyStore, KeyState
from .rate_limiter import RateLimiter

logger = logging.getLogger("guardrails.service")


def fingerprint_payload(item: str, quantity: int) -> str:
    raw = json.dumps({"item": item, "quantity": quantity}, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


class OrderService:
    def __init__(
        self,
        rate_limiter: RateLimiter | None = None,
        idempotency: IdempotencyStore | None = None,
        breaker: CircuitBreaker | None = None,
        downstream: DownstreamClient | None = None,
        settings: Settings | None = None,
        executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self.rate_limiter = rate_limiter or RateLimiter(self._settings)
        self.idempotency = idempotency or IdempotencyStore(self._settings)
        self.breaker = breaker or CircuitBreaker(self._settings)
        self.downstream = downstream or DownstreamClient(
            timeout=self._settings.downstream_timeout_seconds
        )
        self._executor = executor or ThreadPoolExecutor(max_workers=16)
        self._side_effects = 0

    @property
    def side_effect_count(self) -> int:
        return self._side_effects

    def create_order(
        self, caller: str, item: str, quantity: int, idem_key: str | None
    ) -> tuple[int, dict]:
        logger.info(
            "incoming create_order caller=%s item=%s quantity=%d idem_key=%s",
            caller, item, quantity, idem_key,
        )

        # 1) 调用方配额
        self.rate_limiter.check(caller)

        # 2) 幂等判定
        state: KeyState | None = None
        if idem_key is not None:
            fp = fingerprint_payload(item, quantity)
            state = self.idempotency.begin(idem_key, fp)
            if state.done.is_set() and state.status is not None:
                logger.info("replay stored result for idem_key=%s status=%d", idem_key, state.status)
                return state.status, dict(state.body)  # type: ignore[arg-type]
            # state.done 未置位可能是：本请求是首次（自己登记），或首次仍在执行
            if state.request_fingerprint != fp:
                # begin() 已处理冲突，理论上到不了这里
                raise RuntimeError("unreachable")
            if state.status is not None:
                return state.status, dict(state.body)  # type: ignore[arg-type]
            # 等待场景结束后被 fail() 唤醒：done=True 且 status=None -> 放行重试
        # 注意：begin 对"处理中重复请求"会等待；若等到结果则上面直接返回，
        # 若首次请求失败释放了占位，则本次作为新的首次请求继续执行（用同一 state 不安全，
        # 因此等待超时/失败释放时我们重新 begin 一次）。
        if idem_key is not None and state is not None and state.done.is_set() and state.status is None:
            fp = fingerprint_payload(item, quantity)
            state = self.idempotency.begin(idem_key, fp)

        try:
            status, body = self._invoke_downstream(item, quantity)
        except Exception:
            if idem_key is not None and state is not None and state.status is None:
                self.idempotency.fail(state)
            raise

        if idem_key is not None and state is not None:
            self.idempotency.complete(state, status, body)
        return status, body

    # ------------------------------------------------------------------ internals
    def _invoke_downstream(self, item: str, quantity: int) -> tuple[int, dict]:
        # 3) 熔断判定
        if not self.breaker.allow_request():
            raise CircuitBreakerOpen(
                "downstream dependency circuit is open; retry later",
                retry_after=self._settings.circuit_open_seconds,
            )

        # 4) 带超时的下游调用：超时 / 依赖错误 / 成功分别可区分
        future = self._executor.submit(self.downstream.call, f"{item}:{quantity}")
        try:
            ref = future.result(timeout=self._settings.downstream_timeout_seconds)
        except FutureTimeout:
            self.breaker.record_failure()
            logger.info(
                "downstream invocation TIMEOUT after %.2fs",
                self._settings.downstream_timeout_seconds,
            )
            raise DownstreamTimeout(
                "downstream dependency timed out",
                retry_after=self._settings.circuit_open_seconds,
            )
        except TimeoutError as exc:
            # 下游自身判定的超时（与调用方 deadline 语义同属超时类，统一为 504）
            self.breaker.record_failure()
            logger.info("downstream reported its own timeout: %s", exc)
            raise DownstreamTimeout("downstream dependency timed out") from None
        except RuntimeError:
            self.breaker.record_failure()
            logger.info("downstream returned business error")
            raise DownstreamError("downstream dependency returned an error") from None

        self.breaker.record_success()
        # 5) 唯一副作用点：受幂等键保护，重复提交不会重复执行到这里
        self._side_effects += 1
        order_id = f"order-{self._side_effects:06d}"
        body = {
            "order_id": order_id,
            "item": item,
            "quantity": quantity,
            "downstream_ref": ref,
        }
        logger.info("side effect applied order_id=%s downstream_ref=%s", order_id, ref)
        return 201, body
