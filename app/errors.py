"""错误码、防护异常与 FastAPI 异常处理器。

对外只暴露稳定的错误码与人类可读原因，内部异常/堆栈仅写入日志，
绝不进入 HTTP 响应体。
"""

import logging
from enum import Enum

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

log = logging.getLogger("guardrails.errors")


class ErrorCode(str, Enum):
    RATE_LIMITED = "RATE_LIMITED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    DOWNSTREAM_TIMEOUT = "DOWNSTREAM_TIMEOUT"
    DOWNSTREAM_ERROR = "DOWNSTREAM_ERROR"
    CIRCUIT_OPEN = "CIRCUIT_OPEN"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    UNAUTHENTICATED = "UNAUTHENTICATED"


class GuardrailError(Exception):
    """所有防护类错误的基类。"""

    status_code: int = 400
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR

    def __init__(self, reason: str = "", retry_after: float | None = None):
        super().__init__(reason)
        self.reason = reason
        self.retry_after = retry_after


class RateLimitedError(GuardrailError):
    status_code = 429
    error_code = ErrorCode.RATE_LIMITED


class IdempotencyConflictError(GuardrailError):
    status_code = 422
    error_code = ErrorCode.IDEMPOTENCY_CONFLICT


class DownstreamTimeoutError(GuardrailError):
    status_code = 504
    error_code = ErrorCode.DOWNSTREAM_TIMEOUT


class DownstreamError(GuardrailError):
    status_code = 502
    error_code = ErrorCode.DOWNSTREAM_ERROR


class CircuitOpenError(GuardrailError):
    status_code = 503
    error_code = ErrorCode.CIRCUIT_OPEN


class UnauthenticatedError(GuardrailError):
    status_code = 401
    error_code = ErrorCode.UNAUTHENTICATED


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(GuardrailError)
    async def _on_guardrail(request: Request, exc: GuardrailError) -> JSONResponse:
        body = {
            "error": exc.error_code.value,
            "reason": exc.reason,
            "retry_after_seconds": exc.retry_after,
            "request_id": _request_id(request),
        }
        headers: dict[str, str] = {}
        if exc.retry_after is not None:
            # Retry-After 不支持小数秒，向上取整
            headers["Retry-After"] = str(max(1, int(exc.retry_after + 0.999)))
        log.info(
            "拒绝请求 request_id=%s code=%s reason=%s retry_after=%s",
            body["request_id"],
            exc.error_code.value,
            exc.reason,
            exc.retry_after,
        )
        return JSONResponse(
            status_code=exc.status_code, content=body, headers=headers
        )

    @app.exception_handler(Exception)
    async def _on_unexpected(request: Request, exc: Exception) -> JSONResponse:
        # 堆栈只进日志，不进响应
        log.exception(
            "未处理异常 request_id=%s type=%s",
            _request_id(request),
            type(exc).__name__,
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": ErrorCode.INTERNAL_ERROR.value,
                "reason": "服务内部错误，请稍后重试",
                "retry_after_seconds": None,
                "request_id": _request_id(request),
            },
        )


class InternalServerError(GuardrailError):
    status_code = 500
    error_code = ErrorCode.INTERNAL_ERROR
