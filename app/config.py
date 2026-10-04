"""全局配置项（限流窗口、幂等 TTL、熔断参数、下游超时）。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    # 限流：每个调用方每个窗口允许的请求数
    rate_limit: int = 5
    rate_limit_window_seconds: float = 10.0
    # 窗口计数持久化文件（重启后语义稳定）；None 表示仅内存
    rate_limit_state_file: str | None = ".ratelimit.json"
    # 分级配额：指定调用方单独额度（放宽或收紧），未配置的走 rate_limit 默认档
    rate_limit_overrides: dict[str, int] = field(default_factory=dict)

    # 服务整体容量保护：同时打到下游的最大在途请求数；None 表示不启用（默认行为不变）
    global_max_inflight: int | None = None
    # 公平性：单个调用方允许占用的最大在途数；None 表示自动取 global_max_inflight 的一半（至少 1）
    global_per_caller_inflight_cap: int | None = None

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
