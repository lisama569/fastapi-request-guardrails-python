"""业务编排：调用方配额 -> 幂等判定 -> 熔断 -> 全局容量 -> 带超时下游调用。

并发模型要点（单进程）：
- FastAPI 端点以同步函数注册，请求在线程池中执行，慢请求只阻塞自己的
  工作线程，不会把事件循环或其他调用方串成一队；
- 全局容量槽位只在"真正准备调用下游"时占用，并在下游调用落定
  （成功 / 失败 / 超时 / 拒绝）的同一代码块内释放，因此超时候的在途计数
  立即回收，不会让线程池里仍在睡眠的慢任务虚占下游额度；
- 幂等追随者（在途并发同标识）只等待 leader 的结果，自身不占容量槽位、
  不调用下游，真实下单永远只发生一次。

成功返回 (status_code, body)；可预期失败抛 GuardrailError 子类；
未知异常在本层归一化为 InternalServerError（仍会被兜底处理器转换为
无差别的 internal_error 响应）。
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

from .circuit_breaker import CircuitBreaker
from .config import Settings, get_settings
from .downstream import DownstreamClient
from .errors import (
    CircuitBreakerOpen,
    DownstreamError,
    DownstreamTimeout,
    GuardrailError,
    IdempotencyInProgress,
    InternalServerError,
)
from .global_capacity import CapacityToken, GlobalCapacityLimiter
from .idempotency import (
    BeginOutcome,
    IdempotencyStore,
    KeyState,
    Role,
    snapshot_error,
)
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
        capacity: GlobalCapacityLimiter | None = None,
        downstream: DownstreamClient | None = None,
        settings: Settings | None = None,
        executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self.rate_limiter = rate_limiter or RateLimiter(self._settings)
        self.idempotency = idempotency or IdempotencyStore(self._settings)
        self.breaker = breaker or CircuitBreaker(self._settings)
        self.capacity = capacity or GlobalCapacityLimiter(self._settings)
        self.downstream = downstream or DownstreamClient(
            timeout=self._settings.downstream_timeout_seconds
        )
        # 工作线程数需明显大于全局在途上限，保证拿到容量槽位的请求
        # 立刻有线程执行，慢请求在各自线程并行推进而非排队串行
        self._executor = executor or ThreadPoolExecutor(max_workers=32)
        self._side_effects = 0
        self._side_effects_lock = threading.Lock()

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

        # 1) 调用方配额（默认档 / 专属档）
        self.rate_limiter.check(caller)

        # 2) 幂等判定：决定本请求是执行者（leader）还是结果复用者（follower）
        outcome: BeginOutcome | None = None
        if idem_key is not None:
            fp = fingerprint_payload(item, quantity)
            outcome = self.idempotency.begin(idem_key, fp)
            if outcome.role is Role.FOLLOWER:
                state = outcome.state
                if state.error is not None:
                    # 在途追随者：拿到与 leader 完全一致的失败结果
                    logger.info(
                        "replay leader failure for idem_key=%s reason=%s",
                        idem_key, state.error.reason,
                    )
                    raise state.error.to_error()
                if state.status is not None:
                    logger.info(
                        "replay stored result for idem_key=%s status=%d",
                        idem_key, state.status,
                    )
                    return state.status, dict(state.body)
                # begin 等待超时（极端情况）：无可复用结果，不允许自己执行，
                # 明确告诉调用方稍后用同一标识重试拿结果，绝不产生第二次下单
                raise IdempotencyInProgress(
                    f"request with Idempotency-Key '{idem_key}' is still being processed; "
                    "retry later to obtain its result",
                )

        state: KeyState | None = outcome.state if outcome is not None else None

        # 3) leader 真正执行业务；任何失败都写入幂等失败快照，让在途追随者复用
        try:
            status, body = self._invoke_downstream(caller, item, quantity)
        except GuardrailError as exc:
            if state is not None:
                self.idempotency.fail(state, snapshot_error(exc))
            raise
        except Exception:
            logger.exception("unexpected error while invoking downstream")
            internal = InternalServerError("internal server error")
            if state is not None:
                self.idempotency.fail(state, snapshot_error(internal))
            raise internal from None

        if state is not None:
            self.idempotency.complete(state, status, body)
        return status, body

    # ------------------------------------------------------------------ internals
    def _invoke_downstream(self, caller: str, item: str, quantity: int) -> tuple[int, dict]:
        # 3a) 熔断判定
        if not self.breaker.allow_request():
            raise CircuitBreakerOpen(
                "downstream dependency circuit is open; retry later",
                retry_after=self._settings.circuit_open_seconds,
            )

        # 3b) 全局容量槽位：仅在熔断放行后、提交下游调用前占用
        token: CapacityToken | None = None
        try:
            token = self.capacity.acquire(caller)

            future = self._executor.submit(self.downstream.call, f"{item}:{quantity}")
            try:
                ref = future.result(timeout=self._settings.downstream_timeout_seconds)
            except FutureTimeout:
                # 槽位立即释放；线程池中的慢任务继续跑完，但不再虚占下游在途额度
                future.cancel()
                self.breaker.record_failure()
                logger.info(
                    "downstream invocation TIMEOUT after %.2fs",
                    self._settings.downstream_timeout_seconds,
                )
                raise DownstreamTimeout(
                    "downstream dependency timed out",
                    retry_after=self._settings.circuit_open_seconds,
                )
            except TimeoutError:
                # 下游自身判定的超时（语义同属超时类，统一为 504）
                self.breaker.record_failure()
                logger.info("downstream reported its own timeout")
                raise DownstreamTimeout("downstream dependency timed out") from None
            except RuntimeError:
                self.breaker.record_failure()
                logger.info("downstream returned business error")
                raise DownstreamError("downstream dependency returned an error") from None

            self.breaker.record_success()
            # 唯一副作用点：只有走到这里的 leader 会下单；
            # 追随者重放不经过本方法，重复提交永远不会重复下单
            with self._side_effects_lock:
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
        finally:
            # 成功 / 失败 / 超时 / 容量拒绝之外的所有落定路径都在这里回收槽位
            if token is not None:
                self.capacity.release(token)
