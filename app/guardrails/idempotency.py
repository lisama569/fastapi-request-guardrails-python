"""请求幂等存储。

规则：
    1. 相同 idempotency_key + 相同请求内容：
       - 首请求执行，成功终态被缓存（TTL 内）；
       - 重复提交直接返回缓存的同一结果，不再触发下游；
       - 首请求仍在处理中时，重复请求等待其结果，绝不重复执行。
    2. 相同 key + 不同内容 -> 422 IDEMPOTENCY_CONFLICT，不覆盖旧结果。
    3. 仅“确认成功”的终态会被缓存；下游错误、超时、熔断拒绝不产生副作用，
       释放 in-flight，调用方可安全重试。
    4. 超过 TTL 的成功结果视为过期，同键重新成为新请求。
"""

import asyncio
import hashlib
import json
from dataclasses import dataclass
from enum import Enum

from app.logging_setup import get_logger

log = get_logger("idempotency")


class BeginState(str, Enum):
    NEW = "new"                 # 本请求获得执行权
    IN_FLIGHT = "in_flight"     # 已有同内容请求执行中，等待后拿到结果
    REPLAYED = "replayed"       # 命中已完成的成功结果
    CONFLICT = "conflict"       # 同键不同内容


@dataclass
class StoredOutcome:
    status_code: int
    body: dict
    request_hash: str


def hash_payload(amount: int, scenario: str) -> str:
    """对参与幂等判定的请求内容做稳定哈希（不含 key 本身）。"""

    canonical = json.dumps(
        {"amount": amount, "scenario": scenario},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class IdempotencyStore:
    def __init__(self, ttl_seconds: float):
        self.ttl_seconds = ttl_seconds
        self._lock = asyncio.Lock()
        self._done: dict[str, StoredOutcome] = {}
        self._saved_at: dict[str, float] = {}
        self._inflight: dict[str, asyncio.Event] = {}
        self._hash_by_key: dict[str, str] = {}

    async def begin(
        self, key: str, request_hash: str, now: float
    ) -> tuple[BeginState, StoredOutcome | None]:
        event_to_wait: asyncio.Event | None = None
        async with self._lock:
            # 懒清理过期成功结果
            saved = self._saved_at.get(key)
            if saved is not None and now - saved > self.ttl_seconds:
                self._done.pop(key, None)
                self._saved_at.pop(key, None)
                self._hash_by_key.pop(key, None)
                log.info("幂等结果过期 key=%s ttl=%ss", key, self.ttl_seconds)
                saved = None

            if key in self._done:
                if self._hash_by_key[key] != request_hash:
                    log.info("幂等冲突（命中已完成结果）key=%s", key)
                    return BeginState.CONFLICT, None
                log.info("幂等回放 key=%s", key)
                return BeginState.REPLAYED, self._done[key]

            if key in self._inflight:
                if self._hash_by_key.get(key) != request_hash:
                    log.info("幂等冲突（另一请求处理中）key=%s", key)
                    return BeginState.CONFLICT, None
                event_to_wait = self._inflight[key]
            else:
                event = asyncio.Event()
                self._inflight[key] = event
                self._hash_by_key[key] = request_hash
                log.info("幂等登记新请求 key=%s hash=%s", key, request_hash[:12])
                return BeginState.NEW, None

        # 锁外等待首请求完成
        assert event_to_wait is not None
        log.info("重复请求等待首请求完成 key=%s", key)
        await event_to_wait.wait()
        # 首请求结束后在锁内重新判定：成功则回放，失败则竞争成为新请求
        async with self._lock:
            if key in self._done:
                log.info("等待后幂等回放 key=%s", key)
                return BeginState.REPLAYED, self._done[key]
            if key in self._inflight:
                # 理论上不会发生（事件在键移除后才 set），防御性处理
                return BeginState.IN_FLIGHT, None
            event = asyncio.Event()
            self._inflight[key] = event
            self._hash_by_key[key] = request_hash
            log.info("首请求失败未缓存，等待者获得执行权 key=%s", key)
            return BeginState.NEW, None

    async def finish(self, key: str, outcome: StoredOutcome, now: float) -> None:
        """登记成功终态并唤醒所有等待者。"""

        async with self._lock:
            self._done[key] = outcome
            self._saved_at[key] = now
            self._hash_by_key[key] = outcome.request_hash
            event = self._inflight.pop(key, None)
        if event is not None:
            event.set()
        log.info("幂等结果已缓存 key=%s", key)

    async def fail_open(self, key: str) -> None:
        """执行未产生成功终态：释放 in-flight，允许重试。"""

        async with self._lock:
            event = self._inflight.pop(key, None)
            self._hash_by_key.pop(key, None)
        if event is not None:
            event.set()
        log.info("幂等执行未成功，释放 in-flight key=%s", key)

    async def reset(self) -> None:
        async with self._lock:
            self._done.clear()
            self._saved_at.clear()
            self._inflight.clear()
            self._hash_by_key.clear()
