"""基于 Idempotency-Key 的请求幂等存储（并发安全，TTL 过期）。

规则：
- 首次携带 key 的请求：登记 fingerprint 后执行，结果（状态码 + 响应体）只写一次；
- 同 key 同 fingerprint 的重复提交：
  * 首次仍在处理 -> 等待其完成后返回同一结果（绝不重复执行副作用）；
  * 首次已完成   -> 直接返回已存结果（状态码与响应体完全一致）；
- 同 key 不同 fingerprint -> 明确拒绝 IdempotencyConflict，不覆盖既有结果。
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field

from .config import Settings, get_settings
from .errors import IdempotencyConflict

logger = logging.getLogger("guardrails.idempotency")


@dataclass
class KeyState:
    request_fingerprint: str
    created_at: float
    status: int | None = None
    body: object = None
    done: threading.Event = field(default_factory=threading.Event)


class IdempotencyStore:
    def __init__(self, settings: Settings | None = None, clock=time.time) -> None:
        self._settings = settings or get_settings()
        self._clock = clock
        self._lock = threading.Lock()
        self._states: dict[str, KeyState] = {}

    def begin(self, key: str, fingerprint: str, wait_timeout: float | None = None) -> KeyState:
        """登记或复用一个幂等键。

        返回 KeyState；调用方通过 state.done / state.status 判断自己是否需要执行。
        - 返回的 state.status is None 且 done 未置位：当前调用方负责执行；
        - fingerprint 不一致时抛 IdempotencyConflict。
        """
        with self._lock:
            existing = self._states.get(key)
            if existing is not None:
                # 惰性过期：TTL 到期后旧结果可被同 key 的新请求替换
                if self._clock() - existing.created_at >= self._settings.idempotency_ttl_seconds:
                    del self._states[key]
                    existing = None
            if existing is None:
                state = KeyState(request_fingerprint=fingerprint, created_at=self._clock())
                self._states[key] = state
                logger.info("idempotency key=%s registered fingerprint=%s", key, fingerprint)
                return state
            if existing.request_fingerprint != fingerprint:
                logger.info(
                    "idempotency key=%s CONFLICT incoming=%s stored=%s",
                    key, fingerprint, existing.request_fingerprint,
                )
                raise IdempotencyConflict(
                    "Idempotency-Key reused with a different request payload"
                )

        # 同内容重复提交：等待首次请求完成（等待不持锁，避免阻塞首次写入）
        if not existing.done.wait(timeout=wait_timeout):
            logger.info("idempotency key=%s still in progress on duplicate", key)
        else:
            logger.info("idempotency key=%s replay returns stored status=%s", key, existing.status)
        return existing

    def complete(self, state: KeyState, status: int, body: object) -> None:
        """写入首次请求的最终结果并唤醒所有等待的重复请求。"""
        with self._lock:
            state.status = status
            state.body = body
            state.done.set()
        logger.info("idempotency stored result status=%d", status)

    def fail(self, state: KeyState) -> None:
        """首次请求执行失败：移除占位，允许调用方携带同 key 重试。"""
        with self._lock:
            for key, value in list(self._states.items()):
                if value is state:
                    del self._states[key]
                    break
            state.done.set()
        logger.info("idempotency placeholder released after failure")

    def reset(self) -> None:
        with self._lock:
            self._states.clear()
