"""并发隔离测试：慢请求并行推进不互相排队；并发同标识首个失败时结果确定且副作用仅一次。"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.api import create_app


def _fresh_client(service) -> TestClient:
    """每个并发线程使用独立 TestClient（独立 portal），共享同一个 service。"""
    return TestClient(create_app(service), raise_server_exceptions=False)


def test_slow_requests_run_in_parallel_not_serialized(build, monkeypatch):
    """一批慢请求并发到达：整体完成时间接近单个请求耗时，而不是逐个排队。"""
    client, svc, components = build(rate_limit=100, downstream_timeout_seconds=5.0)
    real_call = svc.downstream.call

    def slow_call(payload):
        time.sleep(0.3)  # 模拟慢下游
        return real_call(payload)

    monkeypatch.setattr(svc.downstream, "call", slow_call)

    n = 6
    results: dict[int, object] = {}

    def one(idx: int):
        c = _fresh_client(svc)
        with c:
            results[idx] = c.post(
                "/orders",
                json={"item": "x", "quantity": 1},
                headers={"X-Caller-Id": f"par-{idx}"},
            )

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(one, range(n)))
    elapsed = time.monotonic() - start

    assert all(r.status_code == 201 for r in results.values())
    assert svc.side_effect_count == n
    # 串行执行需要 6 * 0.3 = 1.8s；并行应接近单个请求的 0.3s
    assert elapsed < 1.0, f"整体耗时 {elapsed:.2f}s 接近串行，说明慢请求把链路串起来了"
    print(f"[输入] {n}个慢请求(每个0.3s)并发 [依据] 整体耗时={elapsed:.2f}s ≈ 单个耗时 -> 并行未串行")


def test_slow_caller_does_not_block_others(build, monkeypatch):
    """一个调用方的慢请求在途时，其他调用方的快请求照常完成。"""
    client, svc, components = build(rate_limit=100, downstream_timeout_seconds=5.0)
    real_call = svc.downstream.call
    entered = threading.Event()
    release = threading.Event()

    def call(payload):
        if payload.startswith("slow"):
            entered.set()
            release.wait(timeout=5.0)
        return real_call(payload)

    monkeypatch.setattr(svc.downstream, "call", call)

    results: dict[str, object] = {}

    def slow_request():
        c = _fresh_client(svc)
        with c:
            results["slow"] = c.post(
                "/orders", json={"item": "slow-item", "quantity": 1},
                headers={"X-Caller-Id": "slow-caller"},
            )

    with ThreadPoolExecutor(max_workers=1) as pool:
        f = pool.submit(slow_request)
        entered.wait(timeout=2.0)
        time.sleep(0.05)

        start = time.monotonic()
        r = client.post(
            "/orders", json={"item": "fast-item", "quantity": 1},
            headers={"X-Caller-Id": "fast-caller"},
        )
        fast_elapsed = time.monotonic() - start
        assert r.status_code == 201
        assert fast_elapsed < 1.0, f"快请求被慢调用方拖住：{fast_elapsed:.2f}s"
        print(f"[输入] slow-caller在途时 fast-caller 请求 [依据] 快请求耗时={fast_elapsed:.2f}s -> 未被拖住")

        release.set()
        f.result()
    assert results["slow"].status_code == 201


def test_concurrent_same_key_first_fails_deterministic_outcome(build, monkeypatch):
    """并发同标识、首个最终失败：等待中的重复请求收到同一份失败，不多下一次单。"""
    client, svc, components = build(rate_limit=100)
    components["downstream"].set_mode("fail")
    real_call = svc.downstream.call
    entered = threading.Event()
    release = threading.Event()

    def blocking_call(payload):
        entered.set()
        release.wait(timeout=5.0)
        return real_call(payload)  # mode=fail -> RuntimeError

    monkeypatch.setattr(svc.downstream, "call", blocking_call)

    headers = {"X-Caller-Id": "idem-fail", "Idempotency-Key": "key-fail-1"}
    results: dict[int, object] = {}

    def first():
        c = _fresh_client(svc)
        with c:
            results[1] = c.post("/orders", json={"item": "pen", "quantity": 1}, headers=headers)

    def duplicate():
        entered.wait(timeout=2.0)
        time.sleep(0.05)  # 确保首个请求已登记 key 且在处理中
        c = _fresh_client(svc)
        with c:
            results[2] = c.post("/orders", json={"item": "pen", "quantity": 1}, headers=headers)

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(first)
        f2 = pool.submit(duplicate)
        time.sleep(0.2)
        release.set()
        f1.result()
        f2.result()

    r1, r2 = results[1], results[2]
    # 同一标识的并发提交：对外只有一个确定、可解释的结果（两个响应完全一致）
    assert r1.status_code == r2.status_code == 502
    assert r1.json() == r2.json()
    assert r1.json()["error"]["reason"] == "downstream_error"
    assert svc.side_effect_count == 0, "失败的请求不产生副作用"
    print(f"[输入] 同key并发且首个失败 key=key-fail-1 [依据] 两个响应一致 -> {r1.json()['error']['reason']}, 副作用={svc.side_effect_count}")

    # 失败不缓存：同一标识重试可正常成功，且只真正发生一次
    components["downstream"].set_mode("ok")
    svc.breaker.reset()
    monkeypatch.setattr(svc.downstream, "call", real_call)
    r3 = client.post("/orders", json={"item": "pen", "quantity": 1}, headers=headers)
    assert r3.status_code == 201
    assert svc.side_effect_count == 1

    # 成功结果被缓存：再重放返回同一结果，不再产生副作用
    r4 = client.post("/orders", json={"item": "pen", "quantity": 1}, headers=headers)
    assert r4.status_code == 201
    assert r4.json() == r3.json()
    assert svc.side_effect_count == 1
    print(f"[输入] 失败后同key重试再重放 [依据] 重试201且副作用={svc.side_effect_count}, 重放结果一致")


def test_concurrent_same_key_first_fails_never_double_orders(build, monkeypatch):
    """首个失败时，并发到达的重复提交不会被当成新请求再下一次单。"""
    client, svc, components = build(rate_limit=100)
    components["downstream"].set_mode("fail")
    real_call = svc.downstream.call
    entered = threading.Event()
    release = threading.Event()
    calls = {"n": 0}
    lock = threading.Lock()

    def blocking_call(payload):
        with lock:
            calls["n"] += 1
        entered.set()
        release.wait(timeout=5.0)
        return real_call(payload)

    monkeypatch.setattr(svc.downstream, "call", blocking_call)

    headers = {"X-Caller-Id": "idem-fail2", "Idempotency-Key": "key-fail-2"}
    results: dict[int, object] = {}

    def post(idx):
        c = _fresh_client(svc)
        with c:
            results[idx] = c.post("/orders", json={"item": "cup", "quantity": 1}, headers=headers)

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(post, 1)
        entered.wait(timeout=2.0)
        time.sleep(0.05)
        f2 = pool.submit(post, 2)
        time.sleep(0.2)
        release.set()
        f1.result()
        f2.result()

    assert calls["n"] == 1, "并发同标识只能有一个请求真正打到下游"
    assert results[1].json() == results[2].json()
    assert svc.side_effect_count == 0
    print(f"[输入] 同key并发2个且下游fail [依据] 下游调用次数={calls['n']}, 副作用={svc.side_effect_count}")
