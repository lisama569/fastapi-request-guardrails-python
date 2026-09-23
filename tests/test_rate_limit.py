"""配额限流测试：并发不超发、不漏判，原因可区分。"""

import asyncio

import pytest

from app.guardrails.rate_limiter import RateLimiter
from tests.conftest import HEADERS, charge_body, make_app


@pytest.mark.asyncio
async def test_quota_rejects_over_limit_with_retry_after(client):
    # 客户端夹具配额 100，这里自建限额=3 的 app
    app = make_app(quota_limit=3, quota_window_seconds=60.0)
    import httpx

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        codes = []
        for i in range(4):
            r = await c.post(
                "/v1/charge", json=charge_body(f"seq-{i}"), headers=HEADERS
            )
            codes.append(r.status_code)
        assert codes[:3] == [200, 200, 200]
        rejected = r
        assert rejected.status_code == 429
        body = rejected.json()
        assert body["error"] == "RATE_LIMITED"
        assert "配额" in body["reason"]
        assert body["retry_after_seconds"] is not None
        assert rejected.headers["Retry-After"].isdigit()


@pytest.mark.asyncio
async def test_concurrent_quota_no_over_issuance():
    """并发 limit+超额 请求：放行数必须恰好等于限额。"""

    limit = 10
    app = make_app(quota_limit=limit, quota_window_seconds=60.0)
    import httpx

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        async def one(i: int):
            return await c.post(
                "/v1/charge",
                json=charge_body(f"conc-{i}"),
                headers={"X-Caller-Id": "race"},
            )

        responses = await asyncio.gather(*(one(i) for i in range(limit + 25)))

    allowed = [r for r in responses if r.status_code == 200]
    limited = [r for r in responses if r.status_code == 429]
    assert len(allowed) == limit, "并发下出现超发"
    assert len(limited) == 25
    assert {r.json()["error"] for r in limited} == {"RATE_LIMITED"}


@pytest.mark.asyncio
async def test_quota_is_per_caller(client):
    app = make_app(quota_limit=1)
    import httpx

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r1 = await c.post("/v1/charge", json=charge_body("a"),
                          headers={"X-Caller-Id": "alice"})
        r2 = await c.post("/v1/charge", json=charge_body("b"),
                          headers={"X-Caller-Id": "alice"})
        r3 = await c.post("/v1/charge", json=charge_body("c"),
                          headers={"X-Caller-Id": "bob"})
    assert r1.status_code == 200
    assert r2.status_code == 429
    assert r3.status_code == 200, "不同调用方额度必须独立"


@pytest.mark.asyncio
async def test_sliding_window_expiry_with_fake_clock():
    """直接对限流器注入虚拟时间，验证窗口滑过后额度恢复。"""

    rl = RateLimiter(limit=2, window_seconds=10.0)
    d0 = await rl.check("c", now=0.0)
    d1 = await rl.check("c", now=5.0)
    d2 = await rl.check("c", now=9.0)
    assert d0.allowed and d1.allowed
    assert not d2.allowed
    # 最早一次（t=0）滑出窗口
    d3 = await rl.check("c", now=10.001)
    assert d3.allowed
    assert d3.used == 2


@pytest.mark.asyncio
async def test_rejected_requests_do_not_consume_quota():
    rl = RateLimiter(limit=1, window_seconds=10.0)
    await rl.check("c", now=0.0)
    rejected = await rl.check("c", now=1.0)
    assert not rejected.allowed
    rejected2 = await rl.check("c", now=2.0)
    assert not rejected2.allowed
    # t=0 滑出后只应剩 0 个占用（两次被拒都未计数）
    allowed = await rl.check("c", now=10.001)
    assert allowed.allowed and allowed.used == 1
