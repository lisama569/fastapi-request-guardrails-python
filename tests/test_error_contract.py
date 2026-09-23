"""错误契约测试：三类可区分原因 + 内部异常不泄露堆栈。"""

from __future__ import annotations


def test_rate_limit_reason(client, service):
    for _ in range(5):
        client.post("/orders", json={"item": "z", "quantity": 1}, headers={"X-Caller-Id": "ec-1"})
    r = client.post("/orders", json={"item": "z", "quantity": 1}, headers={"X-Caller-Id": "ec-1"})
    assert r.status_code == 429
    assert r.json()["error"]["reason"] == "rate_limit_exceeded"
    print(f"[输入] 超额请求 [依据] 配额耗尽 -> {r.json()}")


def test_internal_exception_never_leaks(client, service, monkeypatch):
    def boom(payload):
        raise KeyError("secret-internal-detail-xyz")

    service.breaker.reset()
    monkeypatch.setattr(service.downstream, "call", boom)
    r = client.post("/orders", json={"item": "z", "quantity": 1}, headers={"X-Caller-Id": "ec-2"})
    # 未知内部错误：兜底为 internal_error，不暴露异常类型、消息或堆栈
    assert r.status_code == 500
    body = r.json()["error"]
    assert body["reason"] == "internal_error"
    assert "secret-internal-detail-xyz" not in str(body)
    assert "Traceback" not in str(body)
    print(f"[输入] 下游抛出未知内部异常 [依据] 兜底处理 -> {body}")
