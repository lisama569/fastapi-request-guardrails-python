"""处理中（in-flight）重复请求的去重测试。

用可挂起的假下游卡住首请求，再并发打入同键请求，断言：
    - 下游只被执行一次；
    - 等待者最终拿到与首请求一致的结果；
    - 处理中到达的异内容请求立即 422。
"""

import asyncio

import pytest

from app.downstream import DownstreamResponse
from tests.conftest import HEADERS, charge_body, make_app


class GatedDownstream:
    """按 scenario=='gated' 挂起，直到测试放行。"""

    def __init__(self):
        self.calls = 0
        self.gate = asyncio.Event()
        self._ref = 0

    async def charge(self, scenario: str, amount: int) -> DownstreamResponse:
        if scenario == "gated":
            self.calls += 1
            await self.gate.wait()
            self._ref += 1
            return DownstreamResponse(ref=f"gated-ref-{self._ref}")
        self._ref += 1
        return DownstreamResponse(ref=f"ref-{self._ref}")


@pytest.mark.asyncio
async def test_concurrent_same_key_executes_once():
    app = make_app(quota_limit=100)
    gated = GatedDownstream()
    app.state.downstream = gated

    import httpx

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        async def call():
            return await c.post(
                "/v1/charge",
                json=charge_body("inflight-1", scenario="gated"),
                headers=HEADERS,
            )

        t1 = asyncio.create_task(call())
        t2 = asyncio.create_task(call())
        # 等两个请求都进入服务、首请求卡在下游
        await asyncio.sleep(0.1)
        assert gated.calls == 1, "处理中的重复请求触发了第二次下游执行"

        gated.gate.set()
        r1, r2 = await asyncio.gather(t1, t2)

    assert r1.status_code == r2.status_code == 200
    b1, b2 = r1.json(), r2.json()
    flags = sorted([b1["replayed"], b2["replayed"]])
    assert flags == [False, True]
    assert b1["downstream_ref"] == b2["downstream_ref"] == "gated-ref-1"
    assert gated.calls == 1


@pytest.mark.asyncio
async def test_conflict_while_inflight_rejected_immediately():
    app = make_app(quota_limit=100)
    gated = GatedDownstream()
    app.state.downstream = gated

    import httpx

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        t1 = asyncio.create_task(
            c.post(
                "/v1/charge",
                json=charge_body("inflight-2", amount=100, scenario="gated"),
                headers=HEADERS,
            )
        )
        await asyncio.sleep(0.05)
        r_conflict = await c.post(
            "/v1/charge",
            json=charge_body("inflight-2", amount=999, scenario="gated"),
            headers=HEADERS,
        )
        gated.gate.set()
        r1 = await t1

    assert r_conflict.status_code == 422
    assert r_conflict.json()["error"] == "IDEMPOTENCY_CONFLICT"
    assert r1.status_code == 200
    assert gated.calls == 1


@pytest.mark.asyncio
async def test_inflight_failure_allows_waiter_to_execute():
    """首请求在下游失败（释放 in-flight），等待者应获得执行权而非永久阻塞。"""

    app = make_app(quota_limit=100)

    class FlakyDownstream:
        def __init__(self):
            self.calls = 0
            self.failed_once = asyncio.Event()

        async def charge(self, scenario: str, amount: int):
            self.calls += 1
            if self.calls == 1:
                from app.downstream import DownstreamFailure

                self.failed_once.set()
                raise DownstreamFailure("首次失败")
            return DownstreamResponse(ref="recovered-ref")

    flaky = FlakyDownstream()
    app.state.downstream = flaky
    # 熔断阈值调大，避免本例的单次失败触发熔断干扰判定
    app.state.circuit_breaker.failure_threshold = 100

    import httpx

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        async def first():
            return await c.post(
                "/v1/charge",
                json=charge_body("inflight-3", scenario="fail"),
                headers=HEADERS,
            )

        t1 = asyncio.create_task(first())
        await flaky.failed_once.wait()

        async def waiter():
            # 等待首请求释放 in-flight 后重试（同键、内容相同）
            return await c.post(
                "/v1/charge",
                json=charge_body("inflight-3", scenario="ok"),
                headers=HEADERS,
            )

        r1, r2 = await asyncio.gather(t1, waiter())

    assert r1.status_code == 502
    assert r2.status_code == 200
    assert r2.json()["downstream_ref"] == "recovered-ref"
