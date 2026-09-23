"""应用装配与 HTTP 路由。

/v1/charge 请求处理顺序：
    1. 调用方识别（X-Caller-Id）
    2. 按调用方滚动窗口配额判定（被拒请求不计数）
    3. 幂等判定（同键同内容去重 / 同键异内容冲突）
    4. 熔断许可（CLOSED / HALF_OPEN 探测 / OPEN 拒绝）
    5. 调用下游，区分超时 / 下游错误
    6. 成功终态入幂等缓存；失败则释放 in-flight

配额在幂等之前判定，因此回放与冲突请求同样消耗窗口额度——
“进入服务的请求速率”与“副作用执行速率”是两件事，限流约束前者。
"""

import time

from fastapi import FastAPI, Header, Request

from app.config import Settings, get_settings
from app.downstream import (
    DownstreamClient,
    DownstreamFailure,
    DownstreamTimeout,
)
from app.errors import (
    CircuitOpenError,
    DownstreamError,
    DownstreamTimeoutError,
    IdempotencyConflictError,
    RateLimitedError,
    UnauthenticatedError,
    InternalServerError,
    register_exception_handlers,
)
from app.guardrails.circuit_breaker import CircuitBreaker
from app.guardrails.idempotency import (
    BeginState,
    IdempotencyStore,
    StoredOutcome,
    hash_payload,
)
from app.guardrails.rate_limiter import RateLimiter
from app.logging_setup import configure_logging, get_logger
from app.schemas import ChargeRequest, ChargeResult

log = get_logger("api")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging()

    app = FastAPI(title="fastapi-request-guardrails", version="0.1.0")
    app.state.settings = settings
    app.state.rate_limiter = RateLimiter(
        limit=settings.quota_limit,
        window_seconds=settings.quota_window_seconds,
    )
    app.state.idempotency = IdempotencyStore(settings.idempotency_ttl_seconds)
    app.state.circuit_breaker = CircuitBreaker(
        failure_threshold=settings.cb_failure_threshold,
        open_cooldown_seconds=settings.cb_open_cooldown_seconds,
        half_open_probe=settings.cb_half_open_probe,
    )
    app.state.downstream = DownstreamClient(settings.downstream_timeout_seconds)

    register_exception_handlers(app)

    @app.middleware("http")
    async def assign_request_id(request: Request, call_next):
        request.state.request_id = __import__("uuid").uuid4().hex
        log.info(
            "收到请求 request_id=%s method=%s path=%s caller=%s",
            request.state.request_id,
            request.method,
            request.url.path,
            request.headers.get("x-caller-id"),
        )
        return await call_next(request)

    @app.get("/")
    async def root():
        return {"service": "fastapi-request-guardrails", "status": "ok"}

    @app.get("/healthz")
    async def healthz():
        snap = await app.state.circuit_breaker.state_snapshot()
        return {"status": "ok", "circuit": snap}

    @app.get("/guardrails/state")
    async def guardrails_state():
        """可观测性：熔断器当前状态。"""

        return {"circuit": await app.state.circuit_breaker.state_snapshot()}

    @app.post("/v1/charge", response_model=ChargeResult)
    async def charge(
        request: Request,
        payload: ChargeRequest,
        x_caller_id: str | None = Header(default=None),
    ):
        request_id = request.state.request_id
        caller = x_caller_id
        if not caller:
            raise UnauthenticatedError("缺少调用方标识头 X-Caller-Id")

        now = time.monotonic()
        log.info(
            "下单输入 request_id=%s caller=%s key=%s amount=%s scenario=%s",
            request_id,
            caller,
            payload.idempotency_key,
            payload.amount,
            payload.scenario,
        )

        # 1) 配额
        decision = await app.state.rate_limiter.check(caller, now)
        if not decision.allowed:
            log.info(
                "判定依据 request_id=%s 配额拒绝 used=%d/%d retry_after=%.3f",
                request_id,
                decision.used,
                decision.limit,
                decision.retry_after_seconds or 0.0,
            )
            raise RateLimitedError(decision.reason, decision.retry_after_seconds)
        log.info(
            "判定依据 request_id=%s 配额放行 remaining=%d",
            request_id,
            decision.remaining,
        )

        # 2) 幂等
        request_hash = hash_payload(payload.amount, payload.scenario)
        state, stored = await app.state.idempotency.begin(
            payload.idempotency_key, request_hash, now
        )
        if state is BeginState.CONFLICT:
            log.info("判定依据 request_id=%s 幂等冲突 key=%s", request_id,
                     payload.idempotency_key)
            raise IdempotencyConflictError(
                "相同 Idempotency-Key 已用于不同的请求内容"
                "（amount/scenario 不一致），拒绝覆盖既有结果"
            )
        if state is BeginState.REPLAYED:
            assert stored is not None
            body = dict(stored.body)
            body["replayed"] = True
            log.info(
                "判定依据 request_id=%s 幂等回放 key=%s",
                request_id,
                payload.idempotency_key,
            )
            return ChargeResult(**body)

        # 3) 熔断许可
        permission = await app.state.circuit_breaker.allow(now)
        if not permission.allowed:
            await app.state.idempotency.fail_open(payload.idempotency_key)
            log.info("判定依据 request_id=%s 熔断拒绝 %s", request_id,
                     permission.state.value)
            raise CircuitOpenError(
                permission.reason, permission.retry_after_seconds
            )

        # 4) 调用下游
        try:
            resp = await app.state.downstream.charge(
                payload.scenario, payload.amount
            )
        except DownstreamTimeout:
            await app.state.circuit_breaker.record_failure(now)
            await app.state.idempotency.fail_open(payload.idempotency_key)
            log.info("判定依据 request_id=%s 下游超时", request_id)
            raise DownstreamTimeoutError(
                f"下游在 {settings.downstream_timeout_seconds}s 内未响应，"
                "请求未产生扣费副作用，可安全重试"
            ) from None
        except DownstreamFailure as exc:
            await app.state.circuit_breaker.record_failure(now)
            await app.state.idempotency.fail_open(payload.idempotency_key)
            log.info("判定依据 request_id=%s 下游错误: %s", request_id, exc)
            raise DownstreamError(str(exc)) from None
        except Exception:
            # 未预期异常不记熔断；堆栈仅入日志，转成统一 500 对外，
            # 并释放 in-flight 使同键可重试
            log.exception(
                "处理请求时发生未预期异常 request_id=%s", request_id
            )
            await app.state.idempotency.fail_open(payload.idempotency_key)
            raise InternalServerError(
                "服务内部错误，请稍后重试"
            ) from None

        # 5) 成功：记录熔断恢复 + 缓存终态
        await app.state.circuit_breaker.record_success()
        result = ChargeResult(
            request_id=request_id,
            caller=caller,
            amount=payload.amount,
            idempotency_key=payload.idempotency_key,
            downstream_ref=resp.ref,
            replayed=False,
        )
        await app.state.idempotency.finish(
            payload.idempotency_key,
            StoredOutcome(
                status_code=200,
                body=result.model_dump(),
                request_hash=request_hash,
            ),
            now,
        )
        log.info(
            "判定依据 request_id=%s 首单成功 downstream_ref=%s",
            request_id,
            resp.ref,
        )
        return result

    return app
