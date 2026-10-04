"""全局配置项（限流窗口与分级配额、幂等 TTL、熔断参数、全局容量、下游超时）。"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field

logger = logging.getLogger("guardrails.config")


@dataclass(frozen=True)
class Settings:
    # 限流：每个调用方每个窗口允许的请求数（默认档）
    rate_limit: int = 5
    rate_limit_window_seconds: float = 10.0
    # 分级配额：为指定调用方单独设额度；未列出的调用方走 rate_limit 默认档
    # 也可通过环境变量 RATE_LIMIT_OVERRIDES（JSON，如 {"vip":20,"restricted":2}）覆盖
    rate_limits: dict[str, int] = field(default_factory=dict)
    # 窗口计数持久化文件（重启后语义稳定）；None 表示仅内存
    rate_limit_state_file: str | None = ".ratelimit.json"

    # 幂等结果保留时长
    idempotency_ttl_seconds: float = 3600.0

    # 熔断
    circuit_failure_threshold: int = 3
    circuit_open_seconds: float = 5.0
    circuit_half_open_success_threshold: int = 2
    circuit_sliding_window_size: int = 10

    # 服务整体容量保护：同一时刻允许打到下游的在途请求上限；<=0 表示关闭该层
    global_max_inflight: int = 8
    # 公平性：单个调用方最多持有全局在途额度的比例（防止量大的调用方长期占满）；
    # 实际份额上限 = max(1, ceil(global_max_inflight * share))，1.0 表示不限制单调用方
    global_capacity_caller_share: float = 0.5

    # 下游调用超时（同时作为全局容量拒绝时最坏情况下的可预期重试提示）
    downstream_timeout_seconds: float = 0.5


def _load_rate_limits_from_env() -> dict[str, int]:
    raw = os.environ.get("RATE_LIMIT_OVERRIDES")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("RATE_LIMIT_OVERRIDES must be a JSON object")
        return {str(caller): int(limit) for caller, limit in data.items()}
    except (ValueError, TypeError, json.JSONDecodeError):
        logger.warning("ignoring invalid RATE_LIMIT_OVERRIDES=%r", raw)
        return {}


def get_settings() -> Settings:
    overrides = _load_rate_limits_from_env()
    if overrides:
        return Settings(rate_limits=overrides)
    return Settings()
