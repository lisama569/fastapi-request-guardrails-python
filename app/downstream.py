"""本地模拟的下游依赖，支持 ok / fail / timeout / flaky 四种故障模式。

- ok：正常返回引用号；
- fail：依赖自身返回业务错误（抛 RuntimeError）；
- timeout：模拟响应超过 deadline（阻塞后抛 TimeoutError）；
- flaky：在 ok / fail 之间交替，用于半开恢复探测的测试。
"""

from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger("guardrails.downstream")

_VALID_MODES = ("ok", "fail", "timeout", "flaky")


class DownstreamClient:
    def __init__(self, timeout: float = 0.5, delay: float = 0.6, clock=time.sleep) -> None:
        self._mode = "ok"
        self._timeout = timeout
        self._delay = delay
        self._sleep = clock
        self._lock = threading.Lock()
        self._counter = 0

    def set_mode(self, mode: str) -> None:
        if mode not in _VALID_MODES:
            raise ValueError(f"unknown downstream mode: {mode}")
        with self._lock:
            self._mode = mode
            self._counter = 0
        logger.info("downstream mode set to %s", mode)

    @property
    def mode(self) -> str:
        with self._lock:
            return self._mode

    def call(self, payload: str) -> str:
        """成功返回下游引用号；超时抛 TimeoutError，业务失败抛 RuntimeError。"""
        with self._lock:
            mode = self._mode
            self._counter += 1
            seq = self._counter

        logger.info("downstream call seq=%d payload=%s mode=%s", seq, payload, mode)
        if mode == "ok":
            return f"ref-{seq}"
        if mode == "timeout":
            self._sleep(self._delay)
            raise TimeoutError("downstream response exceeded deadline")
        if mode == "flaky":
            if seq % 2 == 1:
                return f"ref-{seq}"
            raise RuntimeError("downstream rejected request (flaky)")
        raise RuntimeError("downstream returned error")
