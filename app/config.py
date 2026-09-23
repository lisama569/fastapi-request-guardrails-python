"""全局配置项。

重启语义（明确且稳定）：
    配额计数、幂等结果、熔断状态全部保存在**单进程内存**中，不落盘。
    服务重启后：配额窗口立即清空并重新累计；幂等缓存清空（旧键不再命中）；
    熔断器回到 CLOSED。该行为可预期、无外部存储一致性问题。
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """服务运行参数。"""

    # 配额：每个调用方每个滚动窗口允许的请求数
    quota_limit: int = 10
    # 配额滚动窗口长度（秒）
    quota_window_seconds: float = 60.0

    # 已完成幂等结果的保留时长（秒）；过期后同键视为新请求
    idempotency_ttl_seconds: float = 3600.0

    # 熔断：连续失败多少次后打开
    cb_failure_threshold: int = 5
    # 熔断打开后多久进入半开（秒）
    cb_open_cooldown_seconds: float = 30.0
    # 半开时允许同时放行的探测请求数
    cb_half_open_probe: int = 1
    # 下游调用超时（秒）
    downstream_timeout_seconds: float = 2.0


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


def get_settings() -> Settings:
    """从 GUARDRAIL_ 前缀的环境变量读取配置，缺省值见 Settings。"""

    return Settings(
        quota_limit=_env_int("GUARDRAIL_QUOTA_LIMIT", 10),
        quota_window_seconds=_env_float("GUARDRAIL_QUOTA_WINDOW_SECONDS", 60.0),
        idempotency_ttl_seconds=_env_float(
            "GUARDRAIL_IDEMPOTENCY_TTL_SECONDS", 3600.0
        ),
        cb_failure_threshold=_env_int("GUARDRAIL_CB_FAILURE_THRESHOLD", 5),
        cb_open_cooldown_seconds=_env_float(
            "GUARDRAIL_CB_OPEN_COOLDOWN_SECONDS", 30.0
        ),
        cb_half_open_probe=_env_int("GUARDRAIL_CB_HALF_OPEN_PROBE", 1),
        downstream_timeout_seconds=_env_float(
            "GUARDRAIL_DOWNSTREAM_TIMEOUT_SECONDS", 2.0
        ),
    )
