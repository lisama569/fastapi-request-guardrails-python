"""全局容量保护测试：整体在途上限、可区分拒绝原因、可预期重试提示、调用方间公平。"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest


@pytest.fixture()
def slow_downstream():
    """把下游改成可控制的慢调用：entered 表示已进入，release 放行。"""
    entered = threading.Event()
    release = threading.Event()

    def install(service, original):
        def slow_call(payload):
            entered.set()
            release.wait(timeout=5.0)
            return original(payload)

        return slow_call

    return {"entered": entered, "release": release, "install": install}


def _post(client, caller, results, idx):
    r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": caller})
    results[idx] = r


def test_global_capacity_reject_reason_and_retry_hint(build, monkeypatch, slow_downstream):
    """全局在途满：拒绝原因与限流/熔断可区分，且带可预期的重试提示。"""
    client, svc, components = build(
        rate_limit=100,
        global_max_inflight=2,
        global_per_caller_inflight_cap=1,
    )
    monkeypatch.setattr(
        svc.downstream, "call",
        slow_downstream["install"](svc, svc.downstream.call),
    )

    results: dict[int, object] = {}
    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(_post, client, "cap-a", results, 1)
        slow_downstream["entered"].wait(timeout=2.0)
        f2 = pool.submit(_post, client, "cap-b", results, 2)
        time.sleep(0.1)  # 两个请求都已占用全局在途名额

        # 第三个调用方：全局已满 -> 503 global_capacity_exceeded
        r3 = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cap-c"})
        assert r3.status_code == 503
        error = r3.json()["error"]
        assert error["reason"] == "global_capacity_exceeded"
        # 与"调用方自己超额"(429 rate_limit_exceeded) 和 "熔断中"(circuit_breaker_open) 都可区分
        assert error["reason"] != "rate_limit_exceeded"
        assert error["reason"] != "circuit_breaker_open"
        assert error["retry_after_seconds"] == pytest.approx(0.3, abs=0.01)
        assert r3.headers["Retry-After"] == "1"
        print(f"[输入] 全局在途2/2时 caller=cap-c 请求 [依据] 整体容量满 -> 503 {error}")

        slow_downstream["release"].set()
        f1.result()
        f2.result()

    assert results[1].status_code == 201
    assert results[2].status_code == 201
    # 在途名额释放后，新请求正常放行
    r4 = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "cap-c"})
    assert r4.status_code == 201
    print("[输入] 在途释放后 caller=cap-c 重试 [依据] 名额已归还 -> 201")


def test_per_caller_share_prevents_hogging(build, monkeypatch, slow_downstream):
    """公平性：大调用方不能长期占住整体额度，其他调用方仍能进入。"""
    client, svc, components = build(
        rate_limit=100,
        global_max_inflight=3,
        global_per_caller_inflight_cap=1,
        downstream_timeout_seconds=3.0,  # 慢下游在测试内被持有，deadline 放宽避免误超时
    )
    real_call = svc.downstream.call  # 未打补丁的真实下游调用
    monkeypatch.setattr(
        svc.downstream, "call",
        slow_downstream["install"](svc, real_call),
    )

    results: dict[int, object] = {}
    with ThreadPoolExecutor(max_workers=1) as pool:
        f1 = pool.submit(_post, client, "big", results, 1)
        slow_downstream["entered"].wait(timeout=2.0)
        time.sleep(0.1)  # big 已占用 1 个在途名额（自己的份额已满）

        # big 的第二个请求：全局还有空位，但它自己的份额已满 -> 拒绝
        r_big2 = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "big"})
        assert r_big2.status_code == 503
        assert r_big2.json()["error"]["reason"] == "global_capacity_exceeded"
        print(f"[输入] big 占住1个在途后再发请求 [依据] 单调用方份额1/1已满 -> {r_big2.json()['error']['reason']}")

        # 其他调用方不受 big 影响：全局仍有空位 -> 放行
        entered2 = threading.Event()
        release2 = threading.Event()

        def slow_call2(payload):
            entered2.set()
            release2.wait(timeout=5.0)
            return real_call(payload)

        monkeypatch.setattr(svc.downstream, "call", slow_call2)
        with ThreadPoolExecutor(max_workers=1) as pool2:
            f2 = pool2.submit(_post, client, "small", results, 2)
            assert entered2.wait(timeout=2.0), "small 的请求必须能进入下游，不被 big 拖住"
            print("[输入] big 占住份额时 small 请求 [依据] 全局仍有余量且 small 份额空闲 -> 进入下游")
            release2.set()
            f2.result()

        slow_downstream["release"].set()
        f1.result()

    assert results[1].status_code == 201
    assert results[2].status_code == 201


def test_capacity_disabled_by_default_keeps_old_behavior(build):
    """默认不启用全局容量保护：行为与之前完全一致（无 503 容量拒绝）。"""
    client, svc, components = build()
    assert svc.capacity.enabled is False
    for i in range(5):
        r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": f"d-{i}"})
        assert r.status_code == 201
    h = client.get("/health/downstream", headers={"X-Caller-Id": "d-0"})
    assert h.json()["global_capacity"]["enabled"] is False
    print(f"[输入] 默认配置连发5次 [依据] 容量保护未启用 -> 全部201, stats={h.json()['global_capacity']}")


def test_capacity_stats_visible_in_health(build, monkeypatch, slow_downstream):
    """状态接口暴露全局容量视图（上限、单调用方份额、当前在途）。"""
    client, svc, components = build(
        rate_limit=100,
        global_max_inflight=4,
        global_per_caller_inflight_cap=None,  # 自动取一半
    )
    monkeypatch.setattr(
        svc.downstream, "call",
        slow_downstream["install"](svc, svc.downstream.call),
    )

    results: dict[int, object] = {}
    with ThreadPoolExecutor(max_workers=1) as pool:
        f1 = pool.submit(_post, client, "stat-a", results, 1)
        slow_downstream["entered"].wait(timeout=2.0)
        time.sleep(0.1)
        h = client.get("/health/downstream", headers={"X-Caller-Id": "stat-a"})
        cap = h.json()["global_capacity"]
        assert cap == {"enabled": True, "max_inflight": 4, "per_caller_cap": 2, "inflight_total": 1}
        print(f"[输入] 1个在途时 GET /health/downstream [依据] -> global_capacity={cap}")
        slow_downstream["release"].set()
        f1.result()

    h2 = client.get("/health/downstream", headers={"X-Caller-Id": "stat-a"})
    assert h2.json()["global_capacity"]["inflight_total"] == 0
