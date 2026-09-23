"""幂等测试：重复重放、同键异内容冲突、失败可重试。"""

import pytest

from tests.conftest import HEADERS, charge_body, make_app


@pytest.mark.asyncio
async def test_duplicate_returns_same_result_with_replayed(client):
    r1 = await client.post("/v1/charge", json=charge_body("dup-1"), headers=HEADERS)
    r2 = await client.post("/v1/charge", json=charge_body("dup-1"), headers=HEADERS)
    assert r1.status_code == r2.status_code == 200
    b1, b2 = r1.json(), r2.json()
    assert b1["replayed"] is False
    assert b2["replayed"] is True
    # 一致结果：request_id 与下游凭证都必须相同
    assert b2["request_id"] == b1["request_id"]
    assert b2["downstream_ref"] == b1["downstream_ref"]


@pytest.mark.asyncio
async def test_same_key_different_payload_conflicts(client):
    r1 = await client.post(
        "/v1/charge", json=charge_body("conflict", amount=100), headers=HEADERS
    )
    r2 = await client.post(
        "/v1/charge", json=charge_body("conflict", amount=200), headers=HEADERS
    )
    assert r1.status_code == 200
    assert r2.status_code == 422
    assert r2.json()["error"] == "IDEMPOTENCY_CONFLICT"
    # 冲突不得覆盖既有结果：原结果仍可回放
    r3 = await client.post(
        "/v1/charge", json=charge_body("conflict", amount=100), headers=HEADERS
    )
    assert r3.status_code == 200
    assert r3.json()["downstream_ref"] == r1.json()["downstream_ref"]


@pytest.mark.asyncio
async def test_failed_request_not_deduplicated(client):
    """下游业务错误不缓存：同键重试应重新执行。"""

    r1 = await client.post(
        "/v1/charge",
        json=charge_body("retry-me", scenario="fail"),
        headers=HEADERS,
    )
    assert r1.status_code == 502
    r2 = await client.post(
        "/v1/charge",
        json=charge_body("retry-me", scenario="ok"),
        headers=HEADERS,
    )
    assert r2.status_code == 200
    assert r2.json()["replayed"] is False


@pytest.mark.asyncio
async def test_conflict_distinct_from_rate_limit_and_downstream(client):
    r = await client.post(
        "/v1/charge", json=charge_body("d", amount=1), headers=HEADERS
    )
    r_conflict = await client.post(
        "/v1/charge", json=charge_body("d", amount=2), headers=HEADERS
    )
    codes = {r.status_code, r_conflict.status_code}
    assert 422 in codes
    assert r_conflict.json()["error"] not in (
        "RATE_LIMITED", "DOWNSTREAM_ERROR", "CIRCUIT_OPEN")
