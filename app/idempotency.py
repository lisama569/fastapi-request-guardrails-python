"""基于 Idempotency-Key 的请求幂等存储（并发安全，TTL 过期）。

一个幂等键在存储中有两个阶段：
- 在途（pending）：首次请求（leader）正在执行；并发到达的同内容请求成为
  追随者（follower），等待 leader 落定并复用**同一个确定结果**
  ——无论 leader 最终成功还是失败；
- 已落定（terminal）：
  * 成功：结果（状态码 + 响应体）只写一次，TTL 内同 key 同内容重复提交
    永远返回同一结果，真实副作用只发生一次；
  * 失败：保存一份"失败快照"（reason/status/retry_after）。仍处于在途等待
    的追随者拿到与 leader 一致的失败结果；失败落定之后新到达的同 key 同内容
    请求视为一次重试，允许重新执行（失败结果不会永久缓存）。

内容冲突：同 key 不同 fingerprint（无论在途还是已落定）一律明确拒绝
409 idempotency_conflict，绝不覆盖既有结果。
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .config import Settings, get_settings
from .errors import GuardrailError, IdempotencyConflict

logger = logging.getLogger("guardrails.idempotency")


class Role(str, Enum):
    LEADER = "leader"      # 负责真正执行的请求（首次 或 失败落定后的重试）
    FOLLOWER = "follower"  # 在途并发提交，复用 leader 的结果，自己绝不执行


@dataclass
class ErrorSnapshot:
    """leader 失败结果的可复刻快照（只含对外可解释字段，不含堆栈）。"""

    reason: str
    status_code: int
    message: str
    retry_after: float | None = None

    def to_error(self) -> GuardrailError:
        # 用当前注册的错误类型重建，保证 reason/status_code 与 leader 完全一致；
        # 未知类型退化为基类，仍可被统一错误处理器序列化
        cls = GuardrailError._registry.get(self.reason, GuardrailError)
        return cls(self.message, retry_after=self.retry_after)


@dataclass
class KeyState:
    request_fingerprint: str
    created_at: float
    status: int | None = None
    body: Any = None
    error: ErrorSnapshot | None = None
    done: threading.Event = field(default_factory=threading.Event)

    @property
    def succeeded(self) -> bool:
        return self.done.is_set() and self.error is None and self.status is not None

    @property
    def failed(self) -> bool:
        return self.done.is_set() and self.error is not None


@dataclass
class BeginOutcome:
    role: Role
    state: KeyState


class IdempotencyStore:
    def __init__(self, settings: Settings | None = None, clock=time.time) -> None:
        self._settings = settings or get_settings()
        self._clock = clock
        self._lock = threading.Lock()
        self._states: dict[str, KeyState] = {}

    def begin(self, key: str, fingerprint: str, wait_timeout: float | None = None) -> BeginOutcome:
        """登记或复用一个幂等键，返回角色（leader / follower）与当前状态。

        - 键不存在 / 已落定失败后的同内容重试 / TTL 过期后的新一轮：
          返回 Role.LEADER，调用方负责执行（follower 永远不会拿到这个角色）；
        - 键在途且 fingerprint 一致：等待 leader 落定后返回 Role.FOLLOWER；
        - 键已落定成功且 fingerprint 一致：直接返回 Role.FOLLOWER（重放）；
        - fingerprint 不一致：抛 IdempotencyConflict。
        """
        with self._lock:
            existing = self._states.get(key)
            if existing is not None:
                # 惰性过期：TTL 到期后（成功结果或失败快照）同 key 可开启新一轮
                if self._clock() - existing.created_at >= self._settings.idempotency_ttl_seconds:
                    del self._states[key]
                    existing = None
            if existing is None:
                state = KeyState(request_fingerprint=fingerprint, created_at=self._clock())
                self._states[key] = state
                logger.info("idempotency key=%s registered fingerprint=%s role=leader", key, fingerprint)
                return BeginOutcome(Role.LEADER, state)
            if existing.request_fingerprint != fingerprint:
                logger.info(
                    "idempotency key=%s CONFLICT incoming=%s stored=%s",
                    key, fingerprint, existing.request_fingerprint,
                )
                raise IdempotencyConflict(
                    "Idempotency-Key reused with a different request payload"
                )

            if existing.failed:
                # 失败落定之后新到达的同内容请求：这是一次重试，替换为新的在途状态
                state = KeyState(request_fingerprint=fingerprint, created_at=self._clock())
                self._states[key] = state
                logger.info(
                    "idempotency key=%s retry after stored failure fingerprint=%s role=leader",
                    key, fingerprint,
                )
                return BeginOutcome(Role.LEADER, state)

            if existing.succeeded:
                logger.info("idempotency key=%s replay returns stored status=%s", key, existing.status)
                return BeginOutcome(Role.FOLLOWER, existing)

        # 在途：追随者等待 leader 落定（等待不持锁，避免阻塞 leader 写入）
        if not existing.done.wait(timeout=wait_timeout):
            logger.info("idempotency key=%s follower wait timed out", key)
        elif existing.error is not None:
            logger.info("idempotency key=%s follower receives leader failure reason=%s", key, existing.error.reason)
        else:
            logger.info("idempotency key=%s follower receives leader success status=%s", key, existing.status)
        return BeginOutcome(Role.FOLLOWER, existing)

    def complete(self, state: KeyState, status: int, body: object) -> None:
        """写入 leader 的成功结果并唤醒所有等待的追随者。"""
        with self._lock:
            state.status = status
            state.body = body
            state.error = None
            state.done.set()
        logger.info("idempotency stored SUCCESS status=%d", status)

    def fail(self, state: KeyState, error: ErrorSnapshot) -> None:
        """写入 leader 的失败快照并唤醒所有等待的追随者。

        失败结果不删除：在途追随者必须能拿到与 leader 一致的失败；
        失败落定之后到达的同内容重试会在 begin() 中替换状态重新执行。
        """
        with self._lock:
            state.error = error
            state.status = None
            state.body = None
            state.done.set()
        logger.info("idempotency stored FAILURE reason=%s status=%d", error.reason, error.status_code)

    def reset(self) -> None:
        with self._lock:
            self._states.clear()


def snapshot_error(exc: GuardrailError) -> ErrorSnapshot:
    """把 leader 的可预期失败转成可复刻快照。"""
    return ErrorSnapshot(
        reason=exc.reason,
        status_code=exc.status_code,
        message=exc.message,
        retry_after=exc.retry_after,
    )
