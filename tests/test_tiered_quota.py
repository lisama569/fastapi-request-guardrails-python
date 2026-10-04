"""分级配额测试：指定调用方单独额度（放宽/收紧），未配置的走默认档且行为不变。"""

from __future__ import annotations

from app.errors import RateLimitExceeded


def test_override_caller_gets_relaxed_quota(build):
    """vip 单独配 8 个额度：默认档 5 个会拒的请求，vip 能继续放行。"""
    client, svc, components = build(rate_limit_overrides={"vip": 8})
    limiter = components["limiter"]

    for i in range(8):
        limiter.check("vip")  # 8 个全部放行（默认档只有 5 个）
    try:
        limiter.check("vip")
        raise AssertionError("vip 第 9 个请求必须被拒绝")
    except RateLimitExceeded as exc:
        assert exc.reason == "rate_limit_exceeded"
        print(f"[输入] caller=vip override档limit=8 连发9次 [依据] 前8 ALLOW 第9 -> {exc.reason}")
    assert limiter.usage("vip") == 8


def test_override_caller_gets_tightened_quota(build):
    """trial 单独收紧到 2 个额度。"""
    client, svc, components = build(rate_limit_overrides={"trial": 2})
    limiter = components["limiter"]

    limiter.check("trial")
    limiter.check("trial")
    try:
        limiter.check("trial")
        raise AssertionError("trial 第 3 个请求必须被拒绝")
    except RateLimitExceeded as exc:
        print(f"[输入] caller=trial override档limit=2 第3次 [依据] 已用2/2 -> {exc.reason}")
    assert limiter.quota_info("trial") == {"tier": "override", "limit": 2, "used": 2, "remaining": 0}


def test_unconfigured_caller_keeps_default_quota(build):
    """未单独配置的调用方仍走默认档，行为与之前一致。"""
    client, svc, components = build(rate_limit_overrides={"vip": 8})
    limiter = components["limiter"]

    for _ in range(5):
        limiter.check("plain")
    try:
        limiter.check("plain")
        raise AssertionError("plain 第 6 个请求必须被拒绝")
    except RateLimitExceeded:
        pass
    info = limiter.quota_info("plain")
    assert info == {"tier": "default", "limit": 5, "used": 5, "remaining": 0}
    print(f"[输入] caller=plain 未配置override [依据] 走默认档5 -> {info}")


def test_health_endpoint_shows_tier_and_remaining(build):
    """状态接口能看出当前调用方走哪一档、窗口还剩多少。"""
    client, svc, components = build(rate_limit_overrides={"vip": 8})

    for _ in range(3):
        r = client.post("/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "vip"})
        assert r.status_code == 201

    h = client.get("/health/downstream", headers={"X-Caller-Id": "vip"})
    body = h.json()
    assert body["caller_quota_tier"] == "override"
    assert body["caller_quota_limit"] == 8
    assert body["caller_used_quota"] == 3
    assert body["caller_quota_remaining"] == 5
    print(f"[输入] GET /health/downstream caller=vip [依据] -> {body}")

    h2 = client.get("/health/downstream", headers={"X-Caller-Id": "nobody"})
    body2 = h2.json()
    assert body2["caller_quota_tier"] == "default"
    assert body2["caller_quota_limit"] == 5
    assert body2["caller_used_quota"] == 0
    assert body2["caller_quota_remaining"] == 5
    print(f"[输入] GET /health/downstream caller=nobody [依据] -> {body2}")


def test_override_quota_isolated_between_callers(build):
    """vip 用尽自己的放宽额度，不影响其他调用方的默认额度。"""
    client, svc, components = build(rate_limit_overrides={"vip": 3})
    limiter = components["limiter"]

    for _ in range(3):
        limiter.check("vip")
    try:
        limiter.check("vip")
        raise AssertionError("vip 应被限流")
    except RateLimitExceeded:
        pass
    limiter.check("plain")  # 默认档调用方不受影响
    assert limiter.quota_info("plain")["remaining"] == 4
    print("[输入] vip用尽override档后 plain 请求 [依据] 档位各自独立 -> ALLOW")
