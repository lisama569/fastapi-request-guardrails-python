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


class CircuitBreakerOpen(GuardrailError):
    reason = "circuit_breaker_open"
    status_code = 503


class DownstreamTimeout(GuardrailError):
    reason = "downstream_timeout"
    status_code = 504


class DownstreamError(GuardrailError):
    reason = "downstream_error"
    status_code = 502
