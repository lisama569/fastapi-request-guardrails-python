"""对外错误类型与拒绝原因常量；内部异常不向响应透出堆栈。"""

from __future__ import annotations


class GuardrailError(Exception):
    """所有可预期防护错误的基类。"""

    reason: str = "guardrail_error"
    status_code: int = 400

    # reason -> 具体异常类型，供幂等失败快照原样重建
    _registry: dict[str, type["GuardrailError"]] = {}

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        GuardrailError._registry[cls.reason] = cls

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.retry_after = retry_after


class RateLimitExceeded(GuardrailError):
    reason = "rate_limit_exceeded"
    status_code = 429


class GlobalCapacityExceeded(GuardrailError):
    """服务整体容量保护拒绝：区别于调用方自身超额与熔断开路。

    两种触发情形（message 不同、reason 相同）：
    - 全局在途槽位耗尽（global_inflight_full）；
    - 单调用方公平份额用尽（caller_share_full）。
    """

    reason = "global_capacity_exceeded"
    status_code = 429


class IdempotencyConflict(GuardrailError):
    reason = "idempotency_conflict"
    status_code = 409


class IdempotencyInProgress(GuardrailError):
    reason = "idempotency_in_progress"
    status_code = 409


class CircuitBreakerOpen(GuardrailError):
    reason = "circuit_breaker_open"
    status_code = 503


class DownstreamTimeout(GuardrailError):
    reason = "downstream_timeout"
    status_code = 504


class DownstreamError(GuardrailError):
    reason = "downstream_error"
    status_code = 502


class InternalServerError(GuardrailError):
    """未知内部错误的对内归一化类型（对外仍只暴露 internal_error，不透堆栈）。

    幂等首请求若以未知异常失败，需要以可复刻快照形式让在途追随者拿到
    同一个确定结果，因此在服务层归一化为本类型。
    """

    reason = "internal_error"
    status_code = 500
