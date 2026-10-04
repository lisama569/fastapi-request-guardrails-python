"""FastAPI 装配：路由、统一错误处理（不泄露内部堆栈）、可观测状态接口。

对调用方约定：
- 调用方身份通过 X-Caller-Id 请求头传递（缺省 "anonymous"）；
- 幂等键通过 Idempotency-Key 请求头传递（可选）；
- 所有可预期错误返回统一结构 {"error": {"reason", "message", "retry_after_seconds"}}。
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from .config import get_settings
from .errors import GuardrailError
from .models import CreateOrderRequest
from .service import OrderService

logger = logging.getLogger("guardrails")


def create_app(order_service: OrderService) -> FastAPI:
    app = FastAPI(title="request-guardrails", version="0.1.0")

    @app.exception_handler(GuardrailError)
    async def guardrail_error_handler(request: Request, exc: GuardrailError) -> JSONResponse:
        # 只输出可预期的 reason / message / 重试提示，内部异常与堆栈绝不透出
        body: dict = {"error": {"reason": exc.reason, "message": exc.message}}
        if exc.retry_after is not None:
            body["error"]["retry_after_seconds"] = round(float(exc.retry_after), 3)
        logger.info(
            "reject %s %s reason=%s retry_after=%s",
            request.method, request.url.path, exc.reason, exc.retry_after,
        )
        headers = (
            {"Retry-After": str(max(1, round(exc.retry_after)))}
            if exc.retry_after is not None
            else None
        )
        return JSONResponse(status_code=exc.status_code, content=body, headers=headers)

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        # 兜底：内部错误只记日志（含堆栈），对外仅给无差别的 internal_error
        logger.exception("unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=500,
            content={"error": {"reason": "internal_error", "message": "internal server error"}},
        )

    @app.get("/")
    async def read_root() -> dict:
        return {"service": "request-guardrails", "status": "ok"}

    @app.post("/orders", status_code=201)
    def create_order(
        payload: CreateOrderRequest,
        x_caller_id: str = Header(default="anonymous", alias="X-Caller-Id"),
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        # 同步端点：FastAPI 会把请求放到线程池执行，慢下游只阻塞当前请求线程，
        # 事件循环与其他调用方的请求不会被一并拖住排队
        logger.info(
            "http request POST /orders caller=%s key=%s payload=%s",
            x_caller_id, idempotency_key, payload.model_dump(),
        )
        status, resp_body = order_service.create_order(
            caller=x_caller_id,
            item=payload.item,
            quantity=payload.quantity,
            idem_key=idempotency_key,
        )
        # 幂等重放首次为 201 时仍返回 201，保证"一致结果"
        return JSONResponse(status_code=status, content=resp_body)

    @app.get("/health/downstream")
    async def downstream_health(x_caller_id: str = Header(default="anonymous", alias="X-Caller-Id")) -> dict:
        return {
            "circuit_state": order_service.breaker.state().value,
            "downstream_mode": order_service.downstream.mode,
            # 当前调用方配额：命中档位、已用、本窗口剩余、窗口重置剩余时间
            "caller_quota": order_service.rate_limiter.quota_status(x_caller_id),
            # 兼容旧字段：历史调用方仍可直接读 caller_used_quota
            "caller_used_quota": order_service.rate_limiter.usage(x_caller_id),
            # 服务整体容量保护：全局在途 / 上限 / 空闲 / 单调用方份额
            "global_capacity": order_service.capacity.capacity_status(),
        }

    @app.post("/test/downstream-mode/{mode}")
    async def set_downstream_mode(mode: str) -> dict:
        """测试 / 运维辅助：切换下游故障模式，并重置熔断器。"""
        order_service.downstream.set_mode(mode)
        order_service.breaker.reset()
        return {"downstream_mode": mode, "circuit_state": order_service.breaker.state().value}

    return app


settings = get_settings()
service = OrderService(settings=settings)
app = create_app(service)
