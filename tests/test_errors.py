"""横切测试：鉴权、请求校验、未预期异常不泄漏堆栈。"""

import pytest

from tests.conftest import HEADERS, charge_body, make_app


@pytest.mark.asyncio
async def test_missing_caller_header_is_401(client):
    r = await client.post("/v1/charge", json=charge_body("no-caller"))
    assert r.status_code == 401
    assert r.json()["error"] == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_invalid_amount_returns_422_validation_error(client):
    r = await client.post(
        "/v1/charge",
        json={"idempotency_key": "k", "amount": 0, "scenario": "ok"},
        headers=HEADERS,
    )
    assert r.status_code == 422
    # 这是请求体校验错误，不能与幂等冲突码混淆
    body = r.text
    assert "IDEMPOTENCY_CONFLICT" not in body


@pytest.mark.asyncio
async def test_unexpected_exception_never_leaks():
    """下游抛出非预期异常时，响应只能是统一 500，不含内部类型/堆栈。"""

    app = make_app(cb_failure_threshold=100)

    class Boom:
        async def charge(self, scenario, amount):
            raise RuntimeError("secret-internal-detail-/tmp/path.py:42")

    app.state.downstream = Boom()

    import httpx

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/v1/charge", json=charge_body("boom"), headers=HEADERS
        )
    assert r.status_code == 500
    body = r.json()
    assert body["error"] == "INTERNAL_ERROR"
    assert "secret-internal-detail" not in r.text
    assert "Traceback" not in r.text
    assert ".py" not in r.text


@pytest.mark.asyncio
async def test_unexpected_exception_releases_inflight():
    """非预期异常后同键请求不应被永久卡在 in-flight。"""

    app = make_app(cb_failure_threshold=100)

    class BoomOnce:
        def __init__(self):
            self.calls = 0

        async def charge(self, scenario, amount):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("boom")
            from app.downstream import DownstreamResponse

            return DownstreamResponse(ref="ok-after-boom")

    boom = BoomOnce()
    app.state.downstream = boom

    import httpx

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r1 = await c.post("/v1/charge", json=charge_body("boom-key"),
                          headers=HEADERS)
        r2 = await c.post("/v1/charge", json=charge_body("boom-key"),
                          headers=HEADERS)
    assert r1.status_code == 500
    assert r2.status_code == 200
    assert r2.json()["downstream_ref"] == "ok-after-boom"


@pytest.mark.asyncio
async def test_all_error_codes_are_distinguishable(client):
    # 401
    r = await client.post("/v1/charge", json=charge_body("z"))
    assert r.json()["error"] == "UNAUTHENTICATED"
