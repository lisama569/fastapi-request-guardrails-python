"""限流测试：配额拒绝原因、重试提示、并发不超发不漏判、重启语义。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.config import Settings
from app.errors import RateLimitExceeded
from app.rate_limiter import RateLimiter


def test_quota_allows_then_rejects_with_distinguishable_reason(components):
    limiter = components["limiter"]
    for i in range(5):
        limiter.check("alice")  # 前 5 个放行

    try:
        limiter.check("alice")
        raise AssertionError("第 6 个请求必须被拒绝")
    except RateLimitExceeded as exc:
        assert exc.reason == "rate_limit_exceeded"
        assert exc.status_code == 429
        assert 0 <= exc.retry_after <= 10.0
        print(f"[输入] caller=alice 第6次请求 [依据] 窗口已用5/5 -> {exc.reason}, retry_after={exc.retry_after:.2f}")


def test_concurrent_requests_no_overspend(components, settings):
    """同窗口并发 20 个请求：恰好 5 个成功、15 个拒绝，不超发不漏判。"""
    limiter = components["limiter"]
    results: list[str] = []
    lock = threading.Lock()

    def one_request(idx: int) -> str:
        caller = f"concurrent-{idx % 3}"
        try:
            limiter.check(caller)
            verdict = "ALLOW"
        except RateLimitExceeded:
            verdict = "REJECT"
        with lock:
            results.append(verdict)
        print(f"[输入] 并发请求 idx={idx} caller={caller} [依据] -> {verdict}")
        return verdict

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = [pool.submit(one_request, i) for i in range(60)]
        verdicts = [f.result() for f in as_completed(futures)]

    assert verdicts.count("ALLOW") == 3 * settings.rate_limit
    assert verdicts.count("REJECT") == 60 - 3 * settings.rate_limit
    for caller in ("concurrent-0", "concurrent-1", "concurrent-2"):
        assert limiter.usage(caller) == settings.rate_limit


def test_429_response_carries_retry_after_header(client):
    for i in range(5):
        r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "bob"})
        assert r.status_code == 201
    r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "bob"})
    assert r.status_code == 429
    body = r.json()["error"]
    assert body["reason"] == "rate_limit_exceeded"
    assert "retry_after_seconds" in body
    assert r.headers["Retry-After"].isdigit()
    print(f"[输入] POST /orders caller=bob [依据] 429 body={body}")


def test_quota_isolated_per_caller(components):
    limiter = components["limiter"]
    for _ in range(5):
        limiter.check("a")
    try:
        limiter.check("a")
        raise AssertionError("a 应被限流")
    except RateLimitExceeded:
        pass
    # b 是独立调用方，额度不受影响
    limiter.check("b")
    print("[输入] caller=a 已用尽后 caller=b 请求 [依据] 调用方维度独立计数 -> ALLOW")


def test_state_survives_restart_within_same_window(settings, components):
    limiter_a = components["limiter"]
    for _ in range(3):
        limiter_a.check("carol")

    # 模拟服务重启：用同一状态文件新建限流器
    limiter_b = RateLimiter(settings)
    for _ in range(2):
        limiter_b.check("carol")  # 3 + 2 = 5，仍在同一窗口
    try:
        limiter_b.check("carol")
        raise AssertionError("重启后同窗口剩余额度应为 2，第 6 个必须拒绝")
    except RateLimitExceeded:
        print("[输入] 重启后同窗口 caller=carol 第6次 [依据] 文件持久化计数=5/5 -> REJECT")


def test_corrupt_state_file_starts_fresh(tmp_path):
    path = tmp_path / "broken.json"
    path.write_text("{not-json", encoding="utf-8")
    s = Settings(rate_limit_state_file=str(path))
    limiter = RateLimiter(s)
    limiter.check("dave")  # 不抛异常，按空额度启动
    assert limiter.usage("dave") == 1
