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
    """非预期内部异常的脱敏形式：固定文案，类型/消息/堆栈一律不外泄。

    响应体与 API 层兜底 Exception 处理器完全一致（500 internal_error），
    因此无论是否经过幂等层，对外契约保持不变。
    """

    reason = "internal_error"
    status_code = 500

    def __init__(self) -> None:
        super().__init__("internal server error")
