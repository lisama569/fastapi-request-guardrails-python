"""按调用方维度的滚动窗口配额限流器。

并发语义：
    check() 全程持有同一把 asyncio.Lock——“窗口清理 + 计数 + 登记”是一个
    原子临界区，因此同一滚动窗口内无论多少协程并发，都不会超发，也不会
    因竞争漏判。allow 时请求时间戳立即落账，被拒绝的请求不计数。

时间基准：
    以 monotonic 秒为单位（由调用方传入，便于测试），滚动窗口 =
    [now - window_seconds, now]。
"""

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass

from app.logging_setup import get_logger

log = get_logger("ratelimit")


@dataclass
class QuotaDecision:
    allowed: bool
    remaining: int          # 本窗口剩余额度
    limit: int
    window_seconds: float
    used: int               # 本窗口已用额度（含本次，若放行）
    retry_after_seconds: float | None
    reason: str


class RateLimiter:
    def __init__(self, limit: int, window_seconds: float):
        self.limit = limit
        self.window_seconds = window_seconds
        self._lock = asyncio.Lock()
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    async def check(self, caller: str, now: float) -> QuotaDecision:
        async with self._lock:
            hits = self._hits[caller]
            boundary = now - self.window_seconds
            while hits and hits[0] <= boundary:
                hits.popleft()

            if len(hits) < self.limit:
                hits.append(now)
                decision = QuotaDecision(
                    allowed=True,
                    remaining=self.limit - len(hits),
                    limit=self.limit,
                    window_seconds=self.window_seconds,
                    used=len(hits),
                    retry_after_seconds=None,
                    reason=(
                        f"放行：窗口内已用 {len(hits)}/{self.limit}，"
                        f"剩余 {self.limit - len(hits)}"
                    ),
                )
            else:
                retry_after = hits[0] + self.window_seconds - now
                decision = QuotaDecision(
                    allowed=False,
                    remaining=0,
                    limit=self.limit,
                    window_seconds=self.window_seconds,
                    used=len(hits),
                    retry_after_seconds=max(0.0, retry_after),
                    reason=(
                        f"配额耗尽：滚动 {self.window_seconds}s 窗口内已达 "
                        f"{self.limit} 次上限，最早请求于 "
                        f"{retry_after:.3f}s 后滑出窗口"
                    ),
                )
            log.info(
                "配额判定 caller=%s now=%.3f used=%d allowed=%s retry_after=%s",
                caller,
                now,
                decision.used,
                decision.allowed,
                decision.retry_after_seconds,
            )
            return decision

    async def reset(self, caller: str | None = None) -> None:
        async with self._lock:
            if caller is None:
                self._hits.clear()
            else:
                self._hits.pop(caller, None)
