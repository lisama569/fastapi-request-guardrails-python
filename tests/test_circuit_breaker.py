"""熔断测试：连续失败开路、冷却后半开探测、逐步放行后恢复、三类错误可区分。"""

from __future__ import annotations

from app.circuit_breaker import CircuitState
from app.errors import CircuitBreakerOpen, DownstreamError, DownstreamTimeout


def test_open_after_consecutive_failures_and_reject_fast(client, service, components):
    components["downstream"].set_mode("fail")
    for i in range(3):
        r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-1"})
        assert r.status_code == 502
        print(f"[输入] 下游fail 第{i + 1}次 [依据] 依赖业务错误 -> 502 downstream_error")

    assert service.breaker.state() is CircuitState.OPEN

    # 熔断开路：不再调用下游，直接 503
    r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-1"})
    assert r.status_code == 503
    error = r.json()["error"]
    assert error["reason"] == "circuit_breaker_open"
    assert error["retry_after_seconds"] == 5.0
    print(f"[输入] 开路后请求 [依据] 熔断器快速失败 -> 503 {error['reason']}")


def test_timeout_is_distinguishable_from_dependency_error(client, service, components):
    components["downstream"].set_mode("timeout")
    r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-2"})
    assert r.status_code == 504
    assert r.json()["error"]["reason"] == "downstream_timeout"
    print("[输入] 下游timeout [依据] 调用方deadline触发 -> 504 downstream_timeout")

    service.breaker.reset()
    components["downstream"].set_mode("fail")
    r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-2"})
    assert r.status_code == 502
    assert r.json()["error"]["reason"] == "downstream_error"
    print("[输入] 下游fail [依据] 依赖自身返回错误 -> 502 downstream_error")


def test_half_open_gradual_release_then_close(client, service, components, fake_clock):
    components["downstream"].set_mode("fail")
    for _ in range(3):
        client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-3"})
    assert service.breaker.state() is CircuitState.OPEN

    # 冷却期未到：仍然开路
    fake_clock.advance(4.9)
    assert service.breaker.state() is CircuitState.OPEN
    r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-3"})
    assert r.status_code == 503
    print("[输入] 冷却4.9s后请求 [依据] 未达open_seconds=5s -> 仍503")

    # 冷却到期：进入 half_open，首个探测成功
    fake_clock.advance(0.2)
    assert service.breaker.state() is CircuitState.HALF_OPEN
    components["downstream"].set_mode("ok")
    r1 = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-3p"})
    assert r1.status_code == 201
    # 只需 2 次连续成功，第一次后仍为 half_open（逐步放行）
    assert service.breaker.state() is CircuitState.HALF_OPEN
    print("[输入] half_open 第1个探测成功 [依据] 成功1/2 -> 保持half_open继续探测")

    r2 = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-3p"})
    assert r2.status_code == 201
    assert service.breaker.state() is CircuitState.CLOSED
    print("[输入] half_open 第2个探测成功 [依据] 成功2/2 -> 闭合恢复")


def test_half_open_failure_reopens_immediately(client, service, components, fake_clock):
    components["downstream"].set_mode("fail")
    for _ in range(3):
        client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-4"})
    fake_clock.advance(5.1)
    assert service.breaker.state() is CircuitState.HALF_OPEN

    # 探测请求仍然失败 -> 立刻重新开路
    r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-4"})
    assert r.status_code == 502
    assert service.breaker.state() is CircuitState.OPEN

    # 重新计时冷却：立即请求仍是 503
    r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-4"})
    assert r.status_code == 503
    print("[输入] half_open探测失败 [依据] 立即重新开路并重新计时 -> 503")


def test_single_probe_in_half_open(components, fake_clock):
    breaker = components["breaker"]
    for _ in range(3):
        breaker.record_failure()
    assert not breaker.allow_request()  # open 冷却未到
    fake_clock.advance(5.1)
    assert breaker.allow_request() is True   # 第一个探测放行
    assert breaker.allow_request() is False  # 探测未完成时其余请求拒绝
    print("[输入] half_open 同时两个请求 [依据] 仅允许1个探测 -> True, False")


def test_health_endpoint_reports_state(client, service, components):
    components["downstream"].set_mode("fail")
    for _ in range(3):
        client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cb-6"})
    h = client.get("/health/downstream", headers={"X-Caller-Id": "cb-6"})
    assert h.json()["circuit_state"] == "open"
    assert h.json()["downstream_mode"] == "fail"
    print(f"[输入] GET /health/downstream [依据] 状态接口 -> {h.json()}")
