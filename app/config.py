"""全局配置项（限流窗口、幂等 TTL、熔断参数、下游超时）。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # 限流：每个调用方每个窗口允许的请求数
    rate_limit: int = 5
    rate_limit_window_seconds: float = 10.0
    # 窗口计数持久化文件（重启后语义稳定）；None 表示仅内存
    rate_limit_state_file: str | None = ".ratelimit.json"

    # 幂等结果保留时长
    idempotency_ttl_seconds: float = 3600.0

    # 熔断
    circuit_failure_threshold: int = 3
    circuit_open_seconds: float = 5.0
    circuit_half_open_success_threshold: int = 2
    circuit_sliding_window_size: int = 10

    # 下游调用超时
    downstream_timeout_seconds: float = 0.5


def get_settings() -> Settings:
    return Settings()
