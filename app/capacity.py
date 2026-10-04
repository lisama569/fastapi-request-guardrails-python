"""服务整体容量保护：限制同时打到下游的在途请求数（并发安全，含公平性）。

语义：
- 全局信号量：任何时刻进入下游的在途请求不超过 global_max_inflight；
- 公平性：单个调用方的在途数另有上限（默认取全局额度的一半，至少 1），
  避免某个量级特别大的调用方把整体额度长期占住、其他调用方一直进不来；
- 超额（全局满 或 单调用方占满自己的份额）立即拒绝 GlobalCapacityExceeded，
  原因与"调用方自身配额超额"(rate_limit_exceeded)、"熔断中"(circuit_breaker_open)
  可区分，并带可预期的 retry_after（一个下游 deadline，即许可最长被占用时长）；
- acquire/release 必须成对（配合 try/finally 或上下文管理器使用）。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from .config import Settings, get_settings
from .errors import GlobalCapacityExceeded

logger = logging.getLogger("guardrails.capacity")


class GlobalCapacityGuard:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._lock = threading.Lock()
        self._inflight_total = 0
        self._inflight_per_caller: dict[str, int] = {}

    # ------------------------------------------------------------------ config
    @property
    def enabled(self) -> bool:
        return self._settings.global_max_inflight is not None

    @property
    def max_inflight(self) -> int | None:
        return self._settings.global_max_inflight

    def per_caller_cap(self) -> int | None:
        """单调用方在途上限；未启用全局保护时返回 None。"""
        total = self._settings.global_max_inflight
        if total is None:
            return None
        if self._settings.global_per_caller_inflight_cap is not None:
            return self._settings.global_per_caller_inflight_cap
        return max(1, total // 2)

    # ------------------------------------------------------------------ api
    @contextmanager
    def acquire(self, caller: str) -> Iterator[None]:
        """占用一个全局在途名额；超额抛 GlobalCapacityExceeded，退出时自动释放。"""
        self._enter(caller)
        try:
            yield
        finally:
            self._release(caller)

    def _enter(self, caller: str) -> None:
        if not self.enabled:
            return
        limit = self._settings.global_max_inflight
        assert limit is not None
        cap = self.per_caller_cap()
        assert cap is not None
        retry_after = self._settings.downstream_timeout_seconds
        with self._lock:
            caller_used = self._inflight_per_caller.get(caller, 0)
            if caller_used >= cap:
                logger.info(
                    "capacity decision caller=%s caller_inflight=%d/%d total=%d/%d -> REJECT (per-caller share) retry_after=%.2f",
                    caller, caller_used, cap, self._inflight_total, limit, retry_after,
                )
                raise GlobalCapacityExceeded(
                    f"caller '{caller}' holds its full share of global capacity "
                    f"({caller_used}/{cap} in flight); retry shortly",
                    retry_after=retry_after,
                )
            if self._inflight_total >= limit:
                logger.info(
                    "capacity decision caller=%s caller_inflight=%d/%d total=%d/%d -> REJECT (global full) retry_after=%.2f",
                    caller, caller_used, cap, self._inflight_total, limit, retry_after,
                )
                raise GlobalCapacityExceeded(
                    f"service at global downstream capacity "
                    f"({self._inflight_total}/{limit} in flight); retry shortly",
                    retry_after=retry_after,
                )
            self._inflight_total += 1
            self._inflight_per_caller[caller] = caller_used + 1
            logger.info(
                "capacity decision caller=%s caller_inflight=%d/%d total=%d/%d -> ALLOW",
                caller, caller_used + 1, cap, self._inflight_total, limit,
            )

    def _release(self, caller: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._inflight_total -= 1
            remaining = self._inflight_per_caller.get(caller, 0) - 1
            if remaining <= 0:
                self._inflight_per_caller.pop(caller, None)
            else:
                self._inflight_per_caller[caller] = remaining

    # ------------------------------------------------------------------ introspection
    def stats(self) -> dict:
        """供状态接口查看：是否启用、上限、当前在途。"""
        with self._lock:
            return {
                "enabled": self.enabled,
                "max_inflight": self._settings.global_max_inflight,
                "per_caller_cap": self.per_caller_cap(),
                "inflight_total": self._inflight_total,
            }

    def reset(self) -> None:
        with self._lock:
            self._inflight_total = 0
            self._inflight_per_caller.clear()
