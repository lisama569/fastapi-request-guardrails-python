"""服务整体容量保护：限制同一时刻真正打到下游的在途请求数。

两层含义、一个拒绝原因：
- 全局限额 global_max_inflight：所有调用方合计在途请求上限，防止多调用方
  合流把下游压垮；
- 公平份额：单个调用方同时持有的在途槽位不得超过
  max(1, ceil(global_max_inflight * global_capacity_caller_share))，
  避免量大的调用方把全局额度长期占满、其他调用方进不来。槽位随请求
  完成（成功 / 失败 / 超时）立即释放，因此份额按"在途瞬时占用"轮转，
  不会被任何调用方长期锁死。

两种触发情形共用 reason="global_capacity_exceeded"（HTTP 429），
通过 message 区分 global_inflight_full / caller_share_full，
并带可预期的 retry_after_seconds（最坏等一个下游超时的时长）。
该原因与"调用方窗口配额超额"(rate_limit_exceeded)、
"熔断中"(circuit_breaker_open) 三者明确可区分。
"""

from __future__ import annotations

import logging
import math
import threading
from dataclasses import dataclass

from .config import Settings, get_settings
from .errors import GlobalCapacityExceeded

logger = logging.getLogger("guardrails.capacity")


@dataclass(frozen=True)
class CapacityToken:
    caller: str


class GlobalCapacityLimiter:
    def __init__(self, settings: Settings | None = None, retry_after: float | None = None) -> None:
        self._settings = settings or get_settings()
        # 拒绝时给出的可预期重试提示：默认取一个下游超时，即最久等这么久必有槽位释放
        self._retry_after = (
            retry_after if retry_after is not None else self._settings.downstream_timeout_seconds
        )
        self._lock = threading.Lock()
        self._global_inflight = 0
        self._caller_inflight: dict[str, int] = {}

    # ------------------------------------------------------------------ quota
    @property
    def enabled(self) -> bool:
        return self._settings.global_max_inflight > 0

    @property
    def global_limit(self) -> int:
        return max(0, self._settings.global_max_inflight)

    def caller_capacity(self) -> int:
        """单个调用方可同时持有的在途槽位上限（至少 1）。"""
        limit = self.global_limit
        if limit <= 0:
            return 0
        share = self._settings.global_capacity_caller_share
        if share >= 1.0:
            return limit
        return max(1, math.ceil(limit * share))

    # ------------------------------------------------------------------ api
    def acquire(self, caller: str) -> CapacityToken | None:
        """尝试占用一个全局在途槽位；失败抛 GlobalCapacityExceeded。

        返回 token（释放时使用）；容量层关闭时返回 None。
        """
        if not self.enabled:
            return None
        global_limit = self.global_limit
        caller_cap = self.caller_capacity()
        with self._lock:
            caller_used = self._caller_inflight.get(caller, 0)
            # 先判公平份额，再判全局槽位，判定依据都要可日志解释
            if caller_used >= caller_cap:
                logger.info(
                    "capacity decision caller=%s caller_inflight=%d/%d global_inflight=%d/%d "
                    "-> REJECT caller_share_full retry_after=%.2f",
                    caller, caller_used, caller_cap,
                    self._global_inflight, global_limit, self._retry_after,
                )
                raise GlobalCapacityExceeded(
                    f"caller '{caller}' is using its fair share of {caller_cap} concurrent "
                    f"downstream slots; retry after up to {self._retry_after:g}s",
                    retry_after=self._retry_after,
                )
            if self._global_inflight >= global_limit:
                logger.info(
                    "capacity decision caller=%s caller_inflight=%d/%d global_inflight=%d/%d "
                    "-> REJECT global_inflight_full retry_after=%.2f",
                    caller, caller_used, caller_cap,
                    self._global_inflight, global_limit, self._retry_after,
                )
                raise GlobalCapacityExceeded(
                    f"service global downstream capacity of {global_limit} concurrent requests "
                    f"is saturated; retry after up to {self._retry_after:g}s",
                    retry_after=self._retry_after,
                )
            self._global_inflight += 1
            self._caller_inflight[caller] = caller_used + 1
            logger.info(
                "capacity decision caller=%s caller_inflight=%d/%d global_inflight=%d/%d -> ALLOW",
                caller, caller_used + 1, caller_cap,
                self._global_inflight, global_limit,
            )
            return CapacityToken(caller)

    def release(self, token: CapacityToken | None) -> None:
        """归还槽位：成功 / 失败 / 超时路径都必须调用，且只释放一次。"""
        if token is None:
            return
        with self._lock:
            used = self._caller_inflight.get(token.caller, 0)
            if used <= 1:
                self._caller_inflight.pop(token.caller, None)
            else:
                self._caller_inflight[token.caller] = used - 1
            self._global_inflight = max(0, self._global_inflight - 1)
            logger.info(
                "capacity release caller=%s caller_inflight=%d global_inflight=%d/%d",
                token.caller, self._caller_inflight.get(token.caller, 0),
                self._global_inflight, self.global_limit,
            )

    def capacity_status(self) -> dict:
        """状态查看：全局在途用量与单调用方在途用量 / 份额。"""
        with self._lock:
            if not self.enabled:
                return {"enabled": False}
            return {
                "enabled": True,
                "global_inflight": self._global_inflight,
                "global_limit": self.global_limit,
                "global_available": self.global_limit - self._global_inflight,
                "caller_share_limit": self.caller_capacity(),
            }

    def caller_inflight(self, caller: str) -> int:
        with self._lock:
            return self._caller_inflight.get(caller, 0)

    def reset(self) -> None:
        with self._lock:
            self._global_inflight = 0
            self._caller_inflight.clear()
