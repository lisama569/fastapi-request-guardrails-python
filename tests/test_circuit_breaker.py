"""熔断测试：连续失败打开、OPEN 快速失败、冷却后半开探测恢复。

并验证三类失败面对外可区分：
    DOWNSTREAM_TIMEOUT(504) / DOWNSTREAM_ERROR(502) / CIRCUIT_OPEN(503)。
"""

import asyncio

import httpx
import pytest

from tests.conftest import HEADERS, charge_body, make_app


async def _client(app):
    transport = httpx.ASGITransport(app=app)
    c = httpx.AsyncClient(transport=transport, base_url="http://t")
    return c, transport


@pytest.mark.asyncio
async def test_timeout_and_downstream_error_are_distinct():
    app = make_app(cb_failure_threshold=100, downstream_timeout_seconds=0.1)
    c, transport = await _client(app)
    try:
        r_timeout = await c.post(
            "/v1/charge",
            json=charge_body("t", scenario="timeout"),
            headers=HEADERS,
        )
        r_fail = await c.post(
            "/v1/charge",
            json=charge_body("f", scenario="fail"),
            headers=HEADERS,
        )
    finally:
        await c.aclose()
    assert r_timeout.status_code == 504
    assert r_timeout.json()["error"] == "DOWNSTREAM_TIMEOUT"
    assert r_fail.status_code == 502
    assert r_fail.json()["error"] == "DOWNSTREAM_ERROR"


@pytest.mark.asyncio
async def test_circuit_opens_after_threshold_and_fast_fails():
    app = make_app(cb_failure_threshold=3, cb_open_cooldown_seconds=60)
    c, _ = await _client(app)
    try:
        for i in range(3):
            r = await c.post(
                "/v1/charge",
                json=charge_body(f"f{i}", scenario="fail"),
                headers=HEADERS,
            )
            assert r.status_code == 502
        # 第 4 次即使内容是“正常”的，也应在调用下游前被熔断拒绝
        r = await c.post(
            "/v1/charge", json=charge_body("blocked", scenario="ok"),
            headers=HEADERS,
        )
        assert r.status_code == 503
        body = r.json()
        assert body["error"] == "CIRCUIT_OPEN"
        assert body["retry_after_seconds"] is not None
        snap = (await c.get("/guardrails/state")).json()
        assert snap["circuit"]["state"] == "OPEN"
    finally:
        await c.aclose()


@pytest.mark.asyncio
async def test_half_open_probe_success_closes_circuit():
    cooldown = 0.15
    app = make_app(
        cb_failure_threshold=2,
        cb_open_cooldown_seconds=cooldown,
        cb_half_open_probe=1,
    )
    c, _ = await _client(app)
    try:
        for i in range(2):
            await c.post(
                "/v1/charge",
                json=charge_body(f"o{i}", scenario="fail"),
                headers=HEADERS,
            )
        r_open = await c.post(
            "/v1/charge", json=charge_body("o3"), headers=HEADERS
        )
        assert r_open.status_code == 503

        # 冷却结束：首个请求作为半开探测放行，成功后熔断关闭
        await asyncio.sleep(cooldown + 0.05)
        r_probe = await c.post(
            "/v1/charge", json=charge_body("probe", scenario="ok"),
            headers=HEADERS,
        )
        assert r_probe.status_code == 200
        snap = (await c.get("/guardrails/state")).json()
        assert snap["circuit"]["state"] == "CLOSED"
        assert snap["circuit"]["consecutive_failures"] == 0

        # 关闭后流量正常
        r_after = await c.post(
            "/v1/charge", json=charge_body("after", scenario="ok"),
            headers=HEADERS,
        )
        assert r_after.status_code == 200
    finally:
        await c.aclose()


@pytest.mark.asyncio
async def test_half_open_probe_failure_reopens():
    cooldown = 0.15
    app = make_app(
        cb_failure_threshold=2,
        cb_open_cooldown_seconds=cooldown,
        cb_half_open_probe=1,
    )
    c, _ = await _client(app)
    try:
        for i in range(2):
            await c.post(
                "/v1/charge",
                json=charge_body(f"x{i}", scenario="fail"),
                headers=HEADERS,
            )
        await asyncio.sleep(cooldown + 0.05)
        r_probe = await c.post(
            "/v1/charge", json=charge_body("xp", scenario="fail"),
            headers=HEADERS,
        )
        assert r_probe.status_code == 502
        r_next = await c.post(
            "/v1/charge", json=charge_body("xn", scenario="ok"),
            headers=HEADERS,
        )
        assert r_next.status_code == 503
        snap = (await c.get("/guardrails/state")).json()
        assert snap["circuit"]["state"] == "OPEN"
    finally:
        await c.aclose()


@pytest.mark.asyncio
async def test_half_open_limited_probe_slots():
    cooldown = 0.15
    app = make_app(
        cb_failure_threshold=1,
        cb_open_cooldown_seconds=cooldown,
        cb_half_open_probe=1,
    )
    c, _ = await _client(app)
    try:
        await c.post(
            "/v1/charge", json=charge_body("y0", scenario="fail"),
            headers=HEADERS,
        )
        await asyncio.sleep(cooldown + 0.05)

        # 用挂起的假下游占住唯一探测名额
        gate = asyncio.Event()

        class SlowProbe:
            async def charge(self, scenario, amount):
                from app.downstream import DownstreamResponse

                await gate.wait()
                return DownstreamResponse(ref="slow")

        app.state.downstream = SlowProbe()
        probe = asyncio.create_task(
            c.post("/v1/charge", json=charge_body("probe-slow"),
                   headers=HEADERS)
        )
        await asyncio.sleep(0.05)
        r_extra = await c.post(
            "/v1/charge", json=charge_body("probe-extra"), headers=HEADERS
        )
        assert r_extra.status_code == 503
        assert r_extra.json()["error"] == "CIRCUIT_OPEN"
        gate.set()
        assert (await probe).status_code == 200
    finally:
        await c.aclose()
