"""服务整体容量保护测试：全局在途上限、单调用方公平份额、拒绝原因可区分、
槽位随请求落定释放、慢请求并行不串行（一个慢调用方不拖垮其他调用方）。

为稳定制造"占住在途槽位"的场景，这里直接在多线程中调用 OrderService，
下游替换为可阻塞的假实现，deadline 放宽到 2s。
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from app.config import Settings
from app.circuit_breaker import CircuitBreaker
from app.downstream import DownstreamClient
from app.errors import (
    CircuitBreakerOpen,
    GlobalCapacityExceeded,
    RateLimitExceeded,
)
from app.global_capacity import GlobalCapacityLimiter
from app.idempotency import IdempotencyStore
from app.rate_limiter import RateLimiter
from app.service import OrderService


class BlockingDownstream(DownstreamClient):
    """进入 call() 即放行 entered，随后阻塞到 hold 释放，再返回正常引用号。"""

    def __init__(self) -> None:
        super().__init__(timeout=10.0)
        self.entered = threading.Event()
        self.hold = threading.Event()
        self._seq_lock = threading.Lock()
        self._seq = 0

    def call(self, payload: str) -> str:
        with self._seq_lock:
            self._seq += 1
            seq = self._seq
        self.entered.set()
        self.hold.wait(timeout=5.0)
        return f"ref-{seq}"


def _make_service(tmp_path, *, max_inflight=8, share=0.5, timeout=2.0, downstream=None):
    settings = Settings(
        rate_limit=100,  # 容量测试里配额给足，专注验证容量层
        rate_limit_window_seconds=10.0,
        rate_limit_state_file=str(tmp_path / "ratelimit-cap.json"),
        global_max_inflight=max_inflight,
        global_capacity_caller_share=share,
        downstream_timeout_seconds=timeout,
        circuit_failure_threshold=3,
        circuit_open_seconds=5.0,
    )
    downstream = downstream or BlockingDownstream()
    svc = OrderService(
        rate_limiter=RateLimiter(settings),
        idempotency=IdempotencyStore(settings),
        breaker=CircuitBreaker(settings),
        capacity=GlobalCapacityLimiter(settings),
        downstream=downstream,
        settings=settings,
        executor=ThreadPoolExecutor(max_workers=32),
    )
    return svc, settings, downstream


def test_global_capacity_full_rejected_with_distinguishable_reason(tmp_path):
    svc, settings, downstream = _make_service(tmp_path)
    results: list[tuple[int, object]] = []

    def occupy(idx):
        try:
            status, body = svc.create_order(f"cap-{idx}", "x", 1, None)
            results.append((status, body))
        except Exception as exc:  # noqa: BLE001
            results.append((-1, exc))

    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = [pool.submit(occupy, i) for i in range(8)]
        assert downstream.entered.wait(timeout=2.0)
        # 等 8 个都占稳在途槽位
        deadline = time.time() + 2.0
        while svc.capacity.capacity_status()["global_inflight"] < 8 and time.time() < deadline:
            time.sleep(0.01)
        assert svc.capacity.capacity_status()["global_inflight"] == 8

        # 第 9 个请求：全局槽位耗尽
        with pytest.raises(GlobalCapacityExceeded) as exc_info:
            svc.create_order("cap-extra", "x", 1, None)
        assert exc_info.value.reason == "global_capacity_exceeded"
        assert exc_info.value.status_code == 429
        assert exc_info.value.retry_after == settings.downstream_timeout_seconds
        print(f"[输入] 全局8槽位占满后第9个请求 [依据] global_inflight=8/8 -> {exc_info.value.reason}")

        downstream.hold.set()
        for f in as_completed(futs):
            f.result()

    assert all(status == 201 for status, _ in results), results
    assert svc.capacity.capacity_status()["global_inflight"] == 0
    # 全部落定后槽位释放，新请求可正常进入
    status, _ = svc.create_order("cap-after", "x", 1, None)
    assert status == 201
    print("[输入] 占槽请求全部释放后新请求 [依据] global_inflight=0/8 -> 201 ALLOW")


def test_single_caller_cannot_hoard_capacity_fair_share(tmp_path):
    """公平性：big 在途达到份额上限 ceil(8*0.5)=4 后被拒；
    此时全局仍有空闲，other 调用方依旧能立刻进入。"""
    svc, _, downstream = _make_service(tmp_path)
    entered = [threading.Event() for _ in range(4)]
    other_entered = threading.Event()
    big_seq = [0]
    count_lock = threading.Lock()

    def routed(payload):
        # payload 形如 "big:1" / "other:1"，据此识别调用方
        if payload.startswith("big"):
            with count_lock:
                big_seq[0] += 1
                idx = big_seq[0] - 1
            entered[idx].set()
            downstream.hold.wait(timeout=5.0)
            return "ref-big"
        other_entered.set()
        downstream.hold.wait(timeout=5.0)
        return "ref-other"

    svc.downstream.call = routed
    results: list[tuple[str, int]] = []

    def occupy_big(_idx):
        status, _ = svc.create_order("big", "big", 1, None)
        results.append(("big", status))

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(occupy_big, i) for i in range(4)]
        for ev in entered:
            assert ev.wait(timeout=2.0)
        assert svc.capacity.caller_inflight("big") == 4

        # big 第 5 个在途请求：公平份额拒绝（全局其实还有 4 个空位）
        with pytest.raises(GlobalCapacityExceeded) as exc_info:
            svc.create_order("big", "big", 1, None)
        assert exc_info.value.reason == "global_capacity_exceeded"
        print(f"[输入] big 在途4/4后第5个 [依据] caller_share=4 -> {exc_info.value.reason}")

        # other 不受影响，应立即拿到槽位并进入下游
        def occupy_other():
            status, _ = svc.create_order("other", "other", 1, None)
            results.append(("other", status))

        f_other = pool.submit(occupy_other)
        assert other_entered.wait(timeout=2.0), "其他调用方必须能拿到槽位"
        assert svc.capacity.caller_inflight("other") == 1

        downstream.hold.set()
        f_other.result()
        for f in futs:
            f.result()

    assert sorted(results) == [("big", 201)] * 4 + [("other", 201)]
    print("[输入] big 占满自身份额时 other 请求 [依据] 全局4空闲+份额隔离 -> 201")


def test_capacity_reason_distinct_from_rate_limit_and_circuit(tmp_path):
    """global_capacity_exceeded / rate_limit_exceeded / circuit_breaker_open 互不混淆。"""
    # 1) 容量拒绝
    svc, _, downstream = _make_service(tmp_path)
    with ThreadPoolExecutor(max_workers=9) as pool:
        futs = [pool.submit(svc.create_order, f"d-{i}", "x", 1, None) for i in range(8)]
        assert downstream.entered.wait(timeout=2.0)
        deadline = time.time() + 2.0
        while svc.capacity.capacity_status()["global_inflight"] < 8 and time.time() < deadline:
            time.sleep(0.01)
        with pytest.raises(GlobalCapacityExceeded) as exc_info:
            svc.create_order("d-extra", "x", 1, None)
        assert exc_info.value.reason == "global_capacity_exceeded"
        print(f"[输入] 容量饱和 [依据] -> {exc_info.value.reason}")
        downstream.hold.set()
        for f in as_completed(futs):
            f.result()

    # 2) 调用方窗口配额拒绝（小配额独立服务）
    s2 = Settings(
        rate_limit=2,
        rate_limit_window_seconds=10.0,
        rate_limit_state_file=str(tmp_path / "r2.json"),
        global_max_inflight=8,
        downstream_timeout_seconds=2.0,
    )
    limiter = RateLimiter(s2)
    limiter.check("quota-x")
    limiter.check("quota-x")
    with pytest.raises(RateLimitExceeded) as exc_info:
        limiter.check("quota-x")
    assert exc_info.value.reason == "rate_limit_exceeded"
    print(f"[输入] 同caller第3次(配额2) [依据] -> {exc_info.value.reason}")

    # 3) 熔断拒绝
    svc.breaker.reset()
    for _ in range(3):
        svc.breaker.record_failure()
    with pytest.raises(CircuitBreakerOpen) as exc_info:
        svc._invoke_downstream("cb-x", "x", 1)
    assert exc_info.value.reason == "circuit_breaker_open"
    print(f"[输入] 熔断中 [依据] -> {exc_info.value.reason}")


def test_slow_downstream_requests_run_in_parallel_not_serialized(tmp_path):
    """压测最在意的点：多个不同调用方的慢请求同时发出，
    整体墙钟接近单个请求耗时（并行），而不是逐个排队（串行）。"""

    class SlowDownstream(DownstreamClient):
        def __init__(self, delay: float) -> None:
            super().__init__(timeout=10.0)
            self._delay = delay
            self._lock = threading.Lock()
            self._seq = 0

        def call(self, payload: str) -> str:
            with self._lock:
                self._seq += 1
                seq = self._seq
            time.sleep(self._delay)  # 慢下游：每个请求都耗时 0.4s
            return f"ref-{seq}"

    delay = 0.4
    svc, settings, _ = _make_service(tmp_path, timeout=2.0, downstream=SlowDownstream(delay))

    def one(idx):
        start = time.perf_counter()
        status, _ = svc.create_order(f"parallel-{idx}", "x", 1, None)
        return status, time.perf_counter() - start

    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=12) as pool:
        outcomes = [f.result() for f in as_completed([pool.submit(one, i) for i in range(8)])]
    wall = time.perf_counter() - wall_start

    assert all(status == 201 for status, _ in outcomes), outcomes
    assert svc.capacity.capacity_status()["global_inflight"] == 0
    print(
        f"[输入] 8个慢请求(不同caller)并发 单请求={delay}s [依据] 各自独立线程并行占槽 "
        f"-> 墙钟={wall:.2f}s（串行会是~{8 * delay:.1f}s）"
    )
    # 串行墙钟会接近 8*0.4=3.2s；并行应只略大于单个请求耗时
    assert wall < delay * 3, f"慢请求疑似被串行化：墙钟 {wall:.2f}s"


def test_slow_caller_does_not_block_other_callers(tmp_path):
    """一个调用方持续发慢请求时，其他调用方的请求仍能快速完成，
    不会被一起拖进同一个慢队列。"""
    svc, _, downstream = _make_service(tmp_path)
    big_entered = [threading.Event() for _ in range(4)]
    big_seq = [0]
    lock = threading.Lock()

    def routed(payload):
        # payload 形如 "big:1" / "other:1"
        if payload.startswith("big"):
            with lock:
                big_seq[0] += 1
                idx = big_seq[0] - 1
            big_entered[idx].set()
            downstream.hold.wait(timeout=5.0)
            return "ref-big"
        # other 调用方走立即返回的快速路径，绝不等待 big
        return "ref-other-fast"

    svc.downstream.call = routed

    def big_req(_idx):
        return svc.create_order("big", "big", 1, None)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(big_req, i) for i in range(4)]
        for ev in big_entered:
            assert ev.wait(timeout=2.0)
        # big 的 4 个请求都在途阻塞；此时 other 走快速下游
        start = time.perf_counter()
        status, body = svc.create_order("other", "other", 1, None)
        elapsed = time.perf_counter() - start
        assert status == 201
        assert body["downstream_ref"] == "ref-other-fast"
        # other 不应等待 big 的慢请求（任何量级的排队都不允许）
        assert elapsed < 0.2, f"other 被慢调用方拖住：{elapsed:.2f}s"
        print(f"[输入] big 4个慢请求在途时 other 快速请求 [依据] 线程隔离并行 -> {elapsed*1000:.0f}ms 201")

        downstream.hold.set()
        for f in as_completed(futs):
            f.result()
