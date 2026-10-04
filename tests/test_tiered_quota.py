"""分级配额测试：专属档放宽/收紧、未配置走默认档（兼容旧行为）、
状态接口可见档位与窗口剩余。"""

from __future__ import annotations

from app.errors import RateLimitExceeded


def test_custom_tier_relaxed_limit(client, service):
    """vip 配置 20/窗口：前 20 个放行，默认档 5 的限制不影响它。"""
    for i in range(20):
        r = client.post(
            "/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "vip"},
        )
        assert r.status_code == 201, f"vip 第 {i + 1} 个应放行"
    r = client.post(
        "/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "vip"},
    )
    assert r.status_code == 429
    assert r.json()["error"]["reason"] == "rate_limit_exceeded"
    print("[输入] vip(专属档20) 第21个 [依据] used=20/20 -> rate_limit_exceeded")


def test_custom_tier_tightened_limit(components):
    """tight 配置 2/窗口：第 3 个即拒绝。"""
    limiter = components["limiter"]
    limiter.check("tight")
    limiter.check("tight")
    try:
        limiter.check("tight")
        raise AssertionError("tight 第 3 个必须拒绝")
    except RateLimitExceeded as exc:
        assert exc.reason == "rate_limit_exceeded"
        print(f"[输入] tight(专属档2) 第3个 [依据] used=2/2 -> {exc.reason}")


def test_unconfigured_caller_uses_default_tier(components):
    """未单独配置的调用方仍走默认档 5，行为与旧版本完全一致。"""
    limiter = components["limiter"]
    for _ in range(5):
        limiter.check("somebody-new")
    try:
        limiter.check("somebody-new")
        raise AssertionError("默认档第 6 个必须拒绝")
    except RateLimitExceeded:
        pass
    assert limiter.tier_for("somebody-new") == "default"
    assert limiter.limit_for("somebody-new") == 5
    print("[输入] 未配置调用方 [依据] tier=default limit=5 -> 行为同旧版")


def test_tiers_are_independent(components):
    """专属档与默认档、专属档彼此之间独立计数。"""
    limiter = components["limiter"]
    for _ in range(5):
        limiter.check("plain")
    for _ in range(2):
        limiter.check("tight")
    # plain 已用尽默认档，不影响 vip 与其他默认档调用方
    limiter.check("vip")
    limiter.check("another-plain")
    assert limiter.usage("plain") == 5
    assert limiter.usage("tight") == 2
    assert limiter.usage("vip") == 1
    print("[输入] plain=5/5 tight=2/2 vip=1/20 [依据] 各档独立计数")


def test_health_endpoint_shows_tier_used_remaining_and_window(client, service):
    """状态接口：看得出走的是哪一档、已用多少、窗口还剩多少。"""
    for _ in range(3):
        client.post(
            "/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "vip"},
        )
    h = client.get("/health/downstream", headers={"X-Caller-Id": "vip"}).json()
    quota = h["caller_quota"]
    assert quota["tier"] == "custom:20"
    assert quota["used"] == 3
    assert quota["limit"] == 20
    assert quota["remaining"] == 17
    assert 0 < quota["window_reset_seconds"] <= 10.0
    # 旧字段保持兼容
    assert h["caller_used_quota"] == 3
    print(f"[输入] GET /health/downstream caller=vip [依据] -> {quota}")

    # 默认档调用方
    client.post(
        "/orders", json={"item": "x", "quantity": 1}, headers={"X-Caller-Id": "plain-h"},
    )
    h2 = client.get("/health/downstream", headers={"X-Caller-Id": "plain-h"}).json()
    assert h2["caller_quota"]["tier"] == "default"
    assert h2["caller_quota"]["limit"] == 5
    assert h2["caller_quota"]["remaining"] == 4
    print(f"[输入] GET /health/downstream caller=plain-h [依据] -> {h2['caller_quota']}")


def test_rate_limits_via_env_var(monkeypatch, tmp_path):
    """RATE_LIMIT_OVERRIDES 环境变量可配置分级配额；非法值忽略不致命。"""
    import importlib

    import app.config as config_mod

    monkeypatch.setenv("RATE_LIMIT_OVERRIDES", '{"env-caller": 42}')
    importlib.reload(config_mod)
    try:
        settings = config_mod.get_settings()
        assert settings.rate_limits == {"env-caller": 42}
        from app.rate_limiter import RateLimiter

        limiter = RateLimiter(
            config_mod.Settings(
                rate_limits=settings.rate_limits,
                rate_limit_state_file=str(tmp_path / "env.json"),
            )
        )
        assert limiter.limit_for("env-caller") == 42
        assert limiter.tier_for("env-caller") == "custom:42"
        assert limiter.limit_for("anyone-else") == 5
        print("[输入] RATE_LIMIT_OVERRIDES={\"env-caller\":42} [依据] env-caller=42 其余默认5")

        # 非法 JSON 不致命：回退为空覆盖
        monkeypatch.setenv("RATE_LIMIT_OVERRIDES", "{broken")
        assert config_mod._load_rate_limits_from_env() == {}
        print("[输入] RATE_LIMIT_OVERRIDES 非法值 [依据] 忽略告警 -> 空覆盖，服务可用")
    finally:
        importlib.reload(config_mod)  # 还原，避免污染其他用例


def test_capacity_layer_disabled_when_max_inflight_non_positive(tmp_path):
    """global_max_inflight<=0：容量层关闭，退化旧行为（不产生容量拒绝）。"""
    from app.config import Settings
    from app.global_capacity import GlobalCapacityLimiter

    s = Settings(global_max_inflight=0, rate_limit_state_file=str(tmp_path / "off.json"))
    cap = GlobalCapacityLimiter(s)
    assert cap.enabled is False
    assert cap.acquire("a") is None
    cap.release(None)  # 无槽位也不报错
    assert cap.capacity_status() == {"enabled": False}
    print("[输入] global_max_inflight=0 [依据] 容量层关闭 -> acquire 恒放行")
