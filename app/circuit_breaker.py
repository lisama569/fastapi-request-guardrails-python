"""下游依赖熔断器：closed / open / half_open 三态。

语义：
- closed：正常放行；统计滑动窗口内的连续失败次数，达到阈值即开路；
- open：直接拒绝（快速失败），open_seconds 后进入 half_open；
- half_open：放行探测请求（同一时刻最多 1 个），连续成功达到阈值则闭合，
  任一失败立即重新开路并重新计时冷却。
"""

from __future__ import annotations

import logging
import threading
import time
from enum import Enum

from .config import Settings, get_settings

logger = logging.getLogger("guardrails.circuit")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, settings: Settings | None = None, clock=time.monotonic) -> None:
        self._settings = settings or get_settings()
        self._clock = clock
        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._half_open_successes = 0
        self._opened_at: float | None = None
        self._half_open_inflight = False

    # ------------------------------------------------------------------ api
    def allow_request(self) -> bool:
        """判断当前是否放行；half_open 下仅放行一个探测请求。"""
        with self._lock:
            if self._state is CircuitState.CLOSED:
                return True
            if self._state is CircuitState.OPEN:
                assert self._opened_at is not None
                elapsed = self._clock() - self._opened_at
                if elapsed >= self._settings.circuit_open_seconds:
                    self._transition_locked(CircuitState.HALF_OPEN, reason="cooldown elapsed")
                    self._half_open_inflight = True
                    return True
                logger.info(
                    "circuit decision state=open elapsed=%.2f/%s -> REJECT",
                    elapsed, self._settings.circuit_open_seconds,
                )
                return False
            # HALF_OPEN：只放行一个探测
            if self._half_open_inflight:
                logger.info("circuit decision state=half_open probe already inflight -> REJECT")
                return False
            self._half_open_inflight = True
            return True

    def record_success(self) -> None:
        with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_inflight = False
                self._half_open_successes += 1
                logger.info(
                    "circuit half_open success %d/%d",
                    self._half_open_successes, self._settings.circuit_half_open_success_threshold,
                )
                if self._half_open_successes >= self._settings.circuit_half_open_success_threshold:
                    self._transition_locked(CircuitState.CLOSED, reason="probe successes reached")
            else:
                self._consecutive_failures = 0

    def record_failure(self) -> None:
        with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_inflight = False
                self._transition_locked(CircuitState.OPEN, reason="half_open probe failed")
                return
            self._consecutive_failures += 1
            logger.info(
                "circuit failure %d/%d",
                self._consecutive_failures, self._settings.circuit_failure_threshold,
            )
            if self._consecutive_failures >= self._settings.circuit_failure_threshold:
                self._transition_locked(CircuitState.OPEN, reason="failure threshold reached")

    def state(self) -> CircuitState:
        with self._lock:
            if self._state is CircuitState.OPEN and self._opened_at is not None:
                if self._clock() - self._opened_at >= self._settings.circuit_open_seconds:
                    return CircuitState.HALF_OPEN
            return self._state

    def reset(self) -> None:
        with self._lock:
            self._state = CircuitState.CLOSED
            self._consecutive_failures = 0
            self._half_open_successes = 0
            self._opened_at = None
            self._half_open_inflight = False

    # ------------------------------------------------------------------ internals
    def _transition_locked(self, new_state: CircuitState, reason: str) -> None:
        logger.info("circuit transition %s -> %s (%s)", self._state.value, new_state.value, reason)
        self._state = new_state
        if new_state is CircuitState.OPEN:
            self._opened_at = self._clock()
            self._half_open_successes = 0
        elif new_state is CircuitState.HALF_OPEN:
            self._half_open_successes = 0
            self._half_open_inflight = False
        elif new_state is CircuitState.CLOSED:
            self._consecutive_failures = 0
            self._half_open_successes = 0
            self._opened_at = None
            self._half_open_inflight = False
