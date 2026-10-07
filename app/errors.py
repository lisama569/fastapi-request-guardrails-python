"""对外错误类型与拒绝原因常量；内部异常不向响应透出堆栈。"""

from __future__ import annotations


class GuardrailError(Exception):
    """所有可预期防护错误的基类。"""

    reason: str = "guardrail_error"
    status_code: int = 400

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.retry_after = retry_after


class RateLimitExceeded(GuardrailError):
    reason = "rate_limit_exceeded"
    status_code = 429


class IdempotencyConflict(GuardrailError):
    reason = "idempotency_conflict"
    status_code = 409


class IdempotencyInProgress(GuardrailError):
    reason = "idempotency_in_progress"
    status_code = 409


class GlobalCapacityExceeded(GuardrailError):
    reason = "global_capacity_exceeded"
    status_code = 503


class CircuitBreakerOpen(GuardrailError):
    reason = "circuit_breaker_open"
    status_code = 503


class DownstreamTimeout(GuardrailError):
    reason = "downstream_timeout"
    status_code = 504


class DownstreamError(GuardrailError):
    reason = "downstream_error"
    status_code = 502


class InternalError(GuardrailError):
    """非预期内部异常的统一对外形态。

    reason / 状态码 / message 全部固定，绝不携带原始异常的类型、消息或堆栈；
    原始细节只允许出现在服务端日志里。同一份实例会分发给并发同标识的重复请求，
    保证所有并发提交拿到完全一致的结果。
    """

    reason = "internal_error"
    status_code = 500
    _GENERIC_MESSAGE = "internal server error"

    def __init__(self, message: str | None = None, *, retry_after: float | None = None) -> None:
        # 忽略调用方传入的任何具体描述，对外永远是无差别的固定文案
        super().__init__(self._GENERIC_MESSAGE, retry_after=retry_after)
