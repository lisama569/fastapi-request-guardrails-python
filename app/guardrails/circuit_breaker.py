"""下游依赖熔断器。

状态机：
    CLOSED --连续失败达阈值--> OPEN
    OPEN   --冷却时间到--> HALF_OPEN（仅放行 half_open_probe 个探测请求）
    HALF_OPEN --探测成功--> CLOSED（立即关闭，失败计数清零）
    HALF_OPEN --任一探测失败--> OPEN（重新计时冷却）

所有状态迁移在同一把 asyncio.Lock 内完成，并发下状态唯一、可解释。
"""

import asyncio
from dataclasses import dataclass
from enum import Enum

from app.logging_setup import get_logger

log = get_logger("circuit")


class State(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class Permission:
    allowed: bool
    state: State
    retry_after_seconds: float | None
    reason: str


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int,
        open_cooldown_seconds: float,
        half_open_probe: int,
    ):
        self.failure_threshold = failure_threshold
        self.open_cooldown_seconds = open_cooldown_seconds
        self.half_open_probe = half_open_probe
        self._lock = asyncio.Lock()
        self._state = State.CLOSED
        self._failures = 0
        self._opened_at: float | None = None
        self._probes_in_flight = 0

    async def allow(self, now: float) -> Permission:
        async with self._lock:
            if self._state is State.CLOSED:
                return Permission(
                    True, State.CLOSED, None, "熔断器关闭，放行"
                )

            if self._state is State.OPEN:
                assert self._opened_at is not None
                elapsed = now - self._opened_at
                if elapsed < self.open_cooldown_seconds:
                    retry_after = self.open_cooldown_seconds - elapsed
                    log.info(
                        "熔断打开，拒绝 elapsed=%.3fs retry_after=%.3fs",
                        elapsed,
                        retry_after,
                    )
                    return Permission(
                        False,
                        State.OPEN,
                        retry_after,
                        f"熔断打开：连续失败已达 {self.failure_threshold} 次，"
                        f"{retry_after:.3f}s 后进入半开探测",
                    )
                # 冷却结束 -> 半开
                self._state = State.HALF_OPEN
                self._probes_in_flight = 0
                log.info("冷却结束，熔断器进入 HALF_OPEN")

            # HALF_OPEN：按探测名额放行
            if self._probes_in_flight < self.half_open_probe:
                self._probes_in_flight += 1
                log.info(
                    "半开放行探测 in_flight=%d/%d",
                    self._probes_in_flight,
                    self.half_open_probe,
                )
                return Permission(
                    True,
                    State.HALF_OPEN,
                    None,
                    f"半开探测放行（{self._probes_in_flight}/"
                    f"{self.half_open_probe}）",
                )
            log.info("半开探测名额已满，拒绝")
            return Permission(
                False,
                State.HALF_OPEN,
                None,
                "半开探测名额已满，请等待在途探测得出结论",
            )

    async def record_success(self) -> None:
        async with self._lock:
            if self._state is State.HALF_OPEN:
                log.info("探测成功，熔断器关闭")
                self._state = State.CLOSED
                self._failures = 0
                self._opened_at = None
                self._probes_in_flight = 0
            else:
                self._failures = 0

    async def record_failure(self, now: float) -> None:
        async with self._lock:
            self._failures += 1
            if self._state is State.HALF_OPEN:
                log.info("探测失败，熔断器重新打开")
                self._state = State.OPEN
                self._opened_at = now
                self._probes_in_flight = 0
            elif self._failures >= self.failure_threshold:
                log.info(
                    "连续失败 %d 次达阈值，熔断器打开", self._failures
                )
                self._state = State.OPEN
                self._opened_at = now
                self._probes_in_flight = 0

    async def state_snapshot(self) -> dict:
        async with self._lock:
            return {
                "state": self._state.value,
                "consecutive_failures": self._failures,
                "failure_threshold": self.failure_threshold,
                "opened_at": self._opened_at,
                "probes_in_flight": self._probes_in_flight,
                "half_open_probe": self.half_open_probe,
                "open_cooldown_seconds": self.open_cooldown_seconds,
            }

    async def reset(self) -> None:
        async with self._lock:
            self._state = State.CLOSED
            self._failures = 0
            self._opened_at = None
            self._probes_in_flight = 0
