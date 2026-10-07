"""业务编排：限流 -> 幂等 -> 全局容量 -> 熔断 -> 下游调用。

成功返回 (status_code, body)；所有可预期失败抛出 GuardrailError 子类，
内部异常不向上游透出堆栈。
"""

from __future__ import annotations

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

from .capacity import GlobalCapacityGuard
from .circuit_breaker import CircuitBreaker
from .config import Settings, get_settings
from .downstream import DownstreamClient
from .errors import (
    CircuitBreakerOpen,
    DownstreamError,
    DownstreamTimeout,
    GuardrailError,
    InternalError,
)
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
        capacity: GlobalCapacityGuard | None = None,
        settings: Settings | None = None,
        executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self.rate_limiter = rate_limiter or RateLimiter(self._settings)
        self.idempotency = idempotency or IdempotencyStore(self._settings)
        self.breaker = breaker or CircuitBreaker(self._settings)
        self.capacity = capacity or GlobalCapacityGuard(self._settings)
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

        # 1) 调用方配额（分级：override 档或 default 档）
        self.rate_limiter.check(caller)

        # 2) 幂等判定：重复提交要么复用已存结果，要么收到与首个请求相同的失败
        state: KeyState | None = None
        if idem_key is not None:
            fp = fingerprint_payload(item, quantity)
            state = self.idempotency.begin(idem_key, fp)
            if state.status is not None:
                logger.info("replay stored result for idem_key=%s status=%d", idem_key, state.status)
                return state.status, dict(state.body)  # type: ignore[arg-type]
            # 否则本请求是首次（自己登记），负责执行

        # 3) 全局容量 -> 4) 熔断 -> 5) 下游调用；任何失败都通过 fail() 传播给等待者
        try:
            with self.capacity.acquire(caller):
                status, body = self._invoke_downstream(item, quantity)
        except GuardrailError as exc:
            if state is not None:
                self.idempotency.fail(state, exc)
            raise
        except Exception as exc:
            # 非预期异常：与可预期失败走同一条统一失败处理。
            # 原始异常只进服务端日志（含堆栈）；对外与等待中的重复请求都拿到
            # 同一个脱敏后的 InternalError，类型/消息/堆栈一律不外泄。
            logger.exception("unexpected internal error during create_order: %r", exc)
            internal = InternalError()
            if state is not None:
                self.idempotency.fail(state, internal)
            raise internal from None

        if state is not None:
            self.idempotency.complete(state, status, body)
        return status, body

    # ------------------------------------------------------------------ internals
    def _invoke_downstream(self, item: str, quantity: int) -> tuple[int, dict]:
        # 4) 熔断判定
        if not self.breaker.allow_request():
            raise CircuitBreakerOpen(
                "downstream dependency circuit is open; retry later",
                retry_after=self._settings.circuit_open_seconds,
            )

        # 5) 带超时的下游调用：超时 / 依赖错误 / 成功分别可区分
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
        # 6) 唯一副作用点：受幂等键保护，重复提交不会重复执行到这里
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
