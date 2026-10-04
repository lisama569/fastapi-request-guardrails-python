"""按调用方维度的固定窗口配额限制（并发安全，可持久化到文件，支持分级配额）。

语义：
- 对每个 caller 维护一个固定时间窗口内的已占用计数；
- 每个调用方的额度由 limit_for(caller) 决定：settings.rate_limits 中单独配置的
  调用方走专属档（tier="custom:<limit>"），未配置的走默认档 rate_limit
  （tier="default"），与历史行为完全一致；
- check() 在锁内完成"判窗口 -> 读计数 -> 比较 -> 自增 -> 落盘"，
  因此同一窗口内并发请求不会超发，也不会漏判；
- 计数持久化到 JSON 文件：服务重启后若仍处于同一窗口，已消耗额度继续生效，
  窗口边界过后计数自然归零（重启语义明确、稳定）。
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time

from .config import Settings, get_settings
from .errors import RateLimitExceeded

logger = logging.getLogger("guardrails.ratelimit")


class RateLimiter:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._lock = threading.Lock()
        # caller -> [window_start_epoch, used_count]
        self._buckets: dict[str, list[float | int]] = {}
        self._load()

    # ------------------------------------------------------------------ state
    def _load(self) -> None:
        path = self._settings.rate_limit_state_file
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            for caller, bucket in data.items():
                self._buckets[caller] = [float(bucket[0]), int(bucket[1])]
            logger.info("ratelimit state restored from %s: %d callers", path, len(self._buckets))
        except (OSError, ValueError, KeyError, TypeError):
            # 状态文件损坏时按空额度启动，不影响服务可用性
            logger.warning("ratelimit state file unreadable, starting fresh")
            self._buckets.clear()

    def _persist_locked(self) -> None:
        path = self._settings.rate_limit_state_file
        if not path:
            return
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._buckets, fh)
        os.replace(tmp, path)

    # ------------------------------------------------------------------ quota
    def limit_for(self, caller: str) -> int:
        """返回 caller 适用的窗口额度（专属档优先，否则默认档）。"""
        return self._settings.rate_limits.get(caller, self._settings.rate_limit)

    def tier_for(self, caller: str) -> str:
        """返回 caller 命中的配额档位标识（供状态接口 / 日志打印 tier=）。"""
        if caller in self._settings.rate_limits:
            return f"custom:{self._settings.rate_limits[caller]}"
        return "default"

    def _window_start(self, now: float) -> float:
        window = self._settings.rate_limit_window_seconds
        return (now // window) * window

    # ------------------------------------------------------------------ api
    def check(self, caller: str) -> None:
        """占用一个配额；超额抛出 RateLimitExceeded 并带 Retry-After 提示。"""
        window = self._settings.rate_limit_window_seconds
        limit = self.limit_for(caller)
        tier = self.tier_for(caller)
        now = time.time()
        window_start = self._window_start(now)

        with self._lock:
            bucket = self._buckets.get(caller)
            if bucket is None or float(bucket[0]) != window_start:
                bucket = [window_start, 0]
                self._buckets[caller] = bucket
            used = int(bucket[1])
            if used >= limit:
                retry_after = max(0.0, window_start + window - now)
                logger.info(
                    "rate-limit decision caller=%s tier=%s window_start=%.1f used=%d limit=%d -> REJECT retry_after=%.2f",
                    caller, tier, window_start, used, limit, retry_after,
                )
                raise RateLimitExceeded(
                    f"caller '{caller}' exceeded quota of {limit} requests per {window:g}s window",
                    retry_after=retry_after,
                )
            bucket[1] = used + 1
            self._persist_locked()
            logger.info(
                "rate-limit decision caller=%s tier=%s window_start=%.1f used=%d limit=%d -> ALLOW",
                caller, tier, window_start, used + 1, limit,
            )

    def usage(self, caller: str) -> int:
        """返回 caller 当前窗口已用配额（供状态查看）。"""
        now = time.time()
        window_start = self._window_start(now)
        with self._lock:
            bucket = self._buckets.get(caller)
            if bucket is None or float(bucket[0]) != window_start:
                return 0
            return int(bucket[1])

    def quota_status(self, caller: str) -> dict:
        """状态查看：已用 / 总额度 / 剩余 / 档位 / 窗口信息（不占用配额）。"""
        now = time.time()
        window = self._settings.rate_limit_window_seconds
        window_start = self._window_start(now)
        limit = self.limit_for(caller)
        with self._lock:
            bucket = self._buckets.get(caller)
            used = (
                int(bucket[1])
                if bucket is not None and float(bucket[0]) == window_start
                else 0
            )
        return {
            "caller": caller,
            "tier": self.tier_for(caller),
            "used": used,
            "limit": limit,
            "remaining": max(0, limit - used),
            "window_seconds": window,
            "window_reset_seconds": max(0.0, window_start + window - now),
        }

    def reset(self, caller: str | None = None) -> None:
        """清空计数（测试 / 管理接口使用）。"""
        with self._lock:
            if caller is None:
                self._buckets.clear()
            else:
                self._buckets.pop(caller, None)
            self._persist_locked()
