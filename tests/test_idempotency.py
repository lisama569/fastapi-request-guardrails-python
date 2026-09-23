"""幂等测试：重复提交一次副作用且结果一致；内容冲突明确拒绝；处理中不重复执行。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.errors import IdempotencyConflict


def test_duplicate_submission_single_side_effect(client, service):
    headers = {"X-Caller-Id": "idem-1", "Idempotency-Key": "key-001"}
    r1 = client.post("/orders", json={"item": "book", "quantity": 2}, headers=headers)
    r2 = client.post("/orders", json={"item": "book", "quantity": 2}, headers=headers)
    assert r1.status_code == r2.status_code == 201
    assert r1.json() == r2.json(), "重复提交必须返回一致结果"
    assert service.side_effect_count == 1, "相同标识只能产生一次副作用"
    print(f"[输入] 同key同内容提交两次 key=key-001 [依据] 命中已存结果 -> 副作用次数={service.side_effect_count}")


def test_same_key_different_payload_rejected(client, service):
    headers = {"X-Caller-Id": "idem-2", "Idempotency-Key": "key-002"}
    r1 = client.post("/orders", json={"item": "book", "quantity": 1}, headers=headers)
    assert r1.status_code == 201

    r2 = client.post("/orders", json={"item": "book", "quantity": 99}, headers=headers)
    assert r2.status_code == 409
    error = r2.json()["error"]
    assert error["reason"] == "idempotency_conflict"
    # 旧结果不被覆盖
    assert service.side_effect_count == 1
    print(f"[输入] key=key-002 首次quantity=1 重复quantity=99 [依据] fingerprint 不一致 -> 409 {error['reason']}")


def test_in_flight_duplicate_does_not_double_execute(client, service, monkeypatch):
    """首次请求处理中到达的重复请求：等待并复用结果，不产生第二个副作用。"""
    import app.service as service_mod
    import time as time_mod

    entered = threading.Event()
    release = threading.Event()

    original = service.downstream.call

    def slow_call(payload):
        entered.set()
        release.wait(timeout=2.0)
        return original(payload)

    monkeypatch.setattr(service.downstream, "call", slow_call)

    results: list[tuple[int, dict]] = []

    def call_first():
        with client:  # 每个线程独立 portal
            r = client.post(
                "/orders",
                json={"item": "pen", "quantity": 1},
                headers={"X-Caller-Id": "idem-3", "Idempotency-Key": "key-003"},
            )
            results.append((r.status_code, r.json()))

    def call_duplicate():
        entered.wait(timeout=2.0)
        time_mod.sleep(0.05)  # 确保首次已登记 key 且在处理中
        with client:
            r = client.post(
                "/orders",
                json={"item": "pen", "quantity": 1},
                headers={"X-Caller-Id": "idem-3", "Idempotency-Key": "key-003"},
            )
            results.append((r.status_code, r.json()))

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(call_first)
        f2 = pool.submit(call_duplicate)
        release.set()
        for f in as_completed([f1, f2]):
            f.result()

    assert len(results) == 2
    assert results[0] == results[1]
    assert service.side_effect_count == 1, "处理中的重复请求不得重复执行"
    print(f"[输入] 同key并发两次 key=key-003 [依据] 第二次等待首次完成 -> 副作用次数={service.side_effect_count}")


def test_without_idempotency_key_each_request_executes(client, service):
    for _ in range(3):
        r = client.post("/orders", json={"item": "cup", "quantity": 1}, headers={"X-Caller-Id": "idem-4"})
        assert r.status_code == 201
    assert service.side_effect_count == 3
    print("[输入] 不带 Idempotency-Key 的 3 次请求 [依据] 无幂等约束 -> 副作用次数=3")


def test_conflict_does_not_overwrite_and_later_key_works(client, service):
    h1 = {"X-Caller-Id": "idem-5", "Idempotency-Key": "key-005"}
    first = client.post("/orders", json={"item": "a", "quantity": 1}, headers=h1)
    conflict = client.post("/orders", json={"item": "b", "quantity": 1}, headers=h1)
    assert conflict.status_code == 409
    replay = client.post("/orders", json={"item": "a", "quantity": 1}, headers=h1)
    assert replay.status_code == 201
    assert replay.json() == first.json()
    assert service.side_effect_count == 1
    print("[输入] key=key-005 冲突后再以原内容重放 [依据] 原结果保持不变 -> 返回首次结果")
