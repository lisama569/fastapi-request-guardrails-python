"""并发幂等测试：同一个幂等标识并发提交，首个请求失败时——
- 在途追随者拿到与首个请求"同一个确定、可解释"的失败结果；
- 不会把追随者的提交当作新请求再下一次单（真实下单零次）；
- 失败落定后用同一标识重试可以正常成功，且真实下单只发生一次。
另覆盖首个成功时 N 个并发提交只产生一次副作用。

直接在多线程中调用 OrderService，下游替换为可阻塞 / 可控成败的假实现。
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.config import Settings
from app.circuit_breaker import CircuitBreaker
from app.downstream import DownstreamClient
from app.errors import DownstreamError
from app.global_capacity import GlobalCapacityLimiter
from app.idempotency import IdempotencyStore
from app.rate_limiter import RateLimiter
from app.service import OrderService


class GatedDownstream(DownstreamClient):
    """进入 call 即 set entered；阻塞到 hold；之后按 outcome 决定成败。"""

    def __init__(self, outcome: str = "fail") -> None:
        super().__init__(timeout=10.0)
        self.entered = threading.Event()
        # 默认直接放行（首个失败这种单请求场景用）；并发场景测试内自行 clear/set
        self.hold = threading.Event()
        self.hold.set()
        self.outcome = outcome
        self.calls = 0
        self._lock = threading.Lock()

    def call(self, payload: str) -> str:
        with self._lock:
            self.calls += 1
        self.entered.set()
        self.hold.wait(timeout=5.0)
        if self.outcome == "fail":
            raise RuntimeError("downstream rejected request")
        return "ref-ok"


def _make_service(tmp_path, downstream):
    settings = Settings(
        rate_limit=100,
        rate_limit_window_seconds=10.0,
        rate_limit_state_file=str(tmp_path / "ratelimit-idem.json"),
        global_max_inflight=16,
        global_capacity_caller_share=1.0,
        downstream_timeout_seconds=2.0,
        circuit_failure_threshold=10,  # 本用例不希望熔断干扰
    )
    svc = OrderService(
        rate_limiter=RateLimiter(settings),
        idempotency=IdempotencyStore(settings),
        breaker=CircuitBreaker(settings),
        capacity=GlobalCapacityLimiter(settings),
        downstream=downstream,
        settings=settings,
        executor=ThreadPoolExecutor(max_workers=32),
    )
    return svc


def test_concurrent_same_key_first_fails_followers_get_same_failure(tmp_path):
    downstream = GatedDownstream(outcome="fail")
    downstream.hold.clear()  # 首个请求需阻塞到追随者到齐
    svc = _make_service(tmp_path, downstream)
    key = "idem-fail-001"
    outcomes: list[tuple[str, str, int]] = []  # (角色, reason或OK, status)

    def leader():
        try:
            svc.create_order("caller-leader", "book", 1, key)
            outcomes.append(("leader", "unexpected-success", 201))
        except DownstreamError as exc:
            outcomes.append(("leader", exc.reason, exc.status_code))

    def follower(idx):
        try:
            svc.create_order(f"caller-fol-{idx}", "book", 1, key)
            outcomes.append((f"follower-{idx}", "unexpected-success", 201))
        except DownstreamError as exc:
            outcomes.append((f"follower-{idx}", exc.reason, exc.status_code))

    with ThreadPoolExecutor(max_workers=8) as pool:
        f_lead = pool.submit(leader)
        assert downstream.entered.wait(timeout=2.0), "首个请求必须已进入下游"
        time.sleep(0.05)  # 确保追随者到达时首个请求确实在途

        f_followers = [pool.submit(follower, i) for i in range(3)]
        time.sleep(0.1)  # 追随者均已登记并在等待
        assert downstream.calls == 1, "追随者绝不能触发下游调用"

        downstream.hold.set()  # 放行首个请求 -> 失败落定
        f_lead.result()
        for f in f_followers:
            f.result()

    assert len(outcomes) == 4
    # 对外只有一个确定结果：4 个请求全部得到同一个可解释失败
    assert all(kind == "downstream_error" for _, kind, _ in outcomes), outcomes
    assert all(status == 502 for _, _, status in outcomes), outcomes
    # 真实下单 0 次，追随者没有被当成新请求
    assert svc.side_effect_count == 0
    assert downstream.calls == 1
    print(f"[输入] 同key并发4个，首个失败 [依据] 3个追随者复用失败快照 -> {outcomes}，下单次数=0")


def test_retry_same_key_after_failure_succeeds_and_orders_once(tmp_path):
    downstream = GatedDownstream(outcome="fail")
    svc = _make_service(tmp_path, downstream)
    key = "idem-fail-002"

    # 首个失败
    try:
        svc.create_order("caller-a", "pen", 2, key)
        raise AssertionError("首个请求应当失败")
    except DownstreamError as exc:
        assert exc.reason == "downstream_error"
    assert svc.side_effect_count == 0

    # 在途追随者同样拿到失败（失败落定的同一轮）
    follower_errors: list[str] = []

    def follower(idx):
        try:
            svc.create_order(f"caller-b-{idx}", "pen", 2, key)
        except DownstreamError as exc:
            follower_errors.append(exc.reason)

    with ThreadPoolExecutor(max_workers=4) as pool:
        fs = [pool.submit(follower, i) for i in range(2)]
        for f in as_completed(fs):
            f.result()
    assert follower_errors == ["downstream_error", "downstream_error"]

    # 失败落定后：同标识同内容重试 -> 作为新的首次请求执行并成功
    downstream.outcome = "ok"
    downstream.entered.clear()
    status, body = svc.create_order("caller-a", "pen", 2, key)
    assert status == 201
    assert svc.side_effect_count == 1, "失败后的成功重试只能真实下单一次"
    print(f"[输入] 首个失败后同key重试 [依据] 失败不永久缓存 -> 201 下单次数=1 body={body}")

    # 再提交：重放成功结果，不再下单
    status2, body2 = svc.create_order("caller-c", "pen", 2, key)
    assert (status2, body2) == (status, body)
    assert svc.side_effect_count == 1
    print("[输入] 成功后再提交同key [依据] 命中已存成功结果 -> 同一结果，下单次数仍=1")


def test_concurrent_same_key_first_succeeds_single_order_identical_results(tmp_path):
    downstream = GatedDownstream(outcome="ok")
    downstream.hold.clear()  # 首个请求需阻塞到追随者到齐
    svc = _make_service(tmp_path, downstream)
    key = "idem-ok-001"
    results: list[tuple[int, dict]] = []
    results_lock = threading.Lock()

    def request(idx):
        status, body = svc.create_order(f"caller-ok-{idx}", "cup", 3, key)
        with results_lock:
            results.append((status, body))

    with ThreadPoolExecutor(max_workers=8) as pool:
        f0 = pool.submit(request, 0)
        assert downstream.entered.wait(timeout=2.0)
        time.sleep(0.05)
        rest = [pool.submit(request, i) for i in range(1, 5)]
        time.sleep(0.1)
        assert downstream.calls == 1, "追随者必须只等待，不能再打下游"
        downstream.hold.set()
        f0.result()
        for f in as_completed(rest):
            f.result()

    assert len(results) == 5
    assert all(status == 201 for status, _ in results)
    bodies = [body for _, body in results]
    assert all(b == bodies[0] for b in bodies), "同标识并发必须给出同一个确定结果"
    assert svc.side_effect_count == 1, "真实下单只发生一次"
    assert downstream.calls == 1
    print(f"[输入] 同key并发5个首个成功 [依据] 4追随者复用成功 -> 同一 {bodies[0]['order_id']}，下单次数=1")


def test_concurrent_same_key_first_times_out_followers_get_same_timeout(tmp_path):
    """首个请求是超时失败时，追随者同样拿到 504 downstream_timeout，而非别的结果。"""

    class TimeoutDownstream(DownstreamClient):
        entered = None

        def __init__(self):
            super().__init__(timeout=10.0)
            self.entered = threading.Event()
            self.hold = threading.Event()
            self.calls = 0
            self._lock = threading.Lock()

        def call(self, payload):
            with self._lock:
                self.calls += 1
            self.entered.set()
            self.hold.wait(timeout=5.0)
            time.sleep(1.0)  # 超过 deadline
            return "never"

    settings = Settings(
        rate_limit=100,
        rate_limit_window_seconds=10.0,
        rate_limit_state_file=str(tmp_path / "ratelimit-timeout.json"),
        global_max_inflight=16,
        global_capacity_caller_share=1.0,
        downstream_timeout_seconds=0.3,
        circuit_failure_threshold=10,
    )
    downstream = TimeoutDownstream()
    svc = OrderService(
        rate_limiter=RateLimiter(settings),
        idempotency=IdempotencyStore(settings),
        breaker=CircuitBreaker(settings),
        capacity=GlobalCapacityLimiter(settings),
        downstream=downstream,
        settings=settings,
        executor=ThreadPoolExecutor(max_workers=32),
    )
    key = "idem-timeout-001"
    reasons: list[str] = []

    def request(idx):
        try:
            svc.create_order(f"caller-t-{idx}", "mug", 1, key)
            reasons.append("unexpected-success")
        except Exception as exc:  # noqa: BLE001
            reasons.append(exc.reason)

    with ThreadPoolExecutor(max_workers=4) as pool:
        f0 = pool.submit(request, 0)
        assert downstream.entered.wait(timeout=2.0)
        time.sleep(0.05)
        followers = [pool.submit(request, i) for i in range(1, 3)]
        downstream.hold.set()  # 让慢下游开始睡；首个请求会在 0.3s deadline 超时
        f0.result(timeout=5.0)
        for f in as_completed(followers):
            f.result(timeout=5.0)

    assert reasons == ["downstream_timeout"] * 3, reasons
    assert svc.side_effect_count == 0
    assert downstream.calls == 1
    print(f"[输入] 同key并发3个首个超时 [依据] 追随者复用超时快照 -> {reasons}，下单次数=0")


def test_concurrent_same_key_leader_capacity_rejected_then_retry_once(tmp_path):
    """leader 因全局容量饱和失败 -> follower 拿到同一个 global_capacity_exceeded；
    槽位释放后同标识重试成功，整个过程真实下单一次。"""
    from app.errors import GlobalCapacityExceeded

    # 全局只允许 1 个在途；用一个持续占槽的"背景请求"制造饱和
    settings = Settings(
        rate_limit=100,
        rate_limit_window_seconds=10.0,
        rate_limit_state_file=str(tmp_path / "ratelimit-cap-idem.json"),
        global_max_inflight=1,
        global_capacity_caller_share=1.0,
        downstream_timeout_seconds=2.0,
        circuit_failure_threshold=10,
    )

    class Gated(DownstreamClient):
        def __init__(self):
            super().__init__(timeout=10.0)
            self.entered = threading.Event()
            self.hold = threading.Event()
            self.calls = 0
            self._l = threading.Lock()

        def call(self, payload):
            with self._l:
                self.calls += 1
            self.entered.set()
            self.hold.wait(timeout=5.0)
            return "ref-ok"

    downstream = Gated()
    svc = OrderService(
        rate_limiter=RateLimiter(settings),
        idempotency=IdempotencyStore(settings),
        breaker=CircuitBreaker(settings),
        capacity=GlobalCapacityLimiter(settings),
        downstream=downstream,
        settings=settings,
        executor=ThreadPoolExecutor(max_workers=32),
    )

    def background():
        svc.create_order("bg-caller", "bg", 1, None)

    key = "idem-cap-001"
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_bg = pool.submit(background)
        assert downstream.entered.wait(timeout=2.0)
        assert svc.capacity.capacity_status()["global_inflight"] == 1

        reasons: list[str] = []

        def request(idx):
            try:
                svc.create_order(f"idem-c-{idx}", "book", 1, key)
                reasons.append("unexpected-success")
            except GlobalCapacityExceeded as exc:
                reasons.append(exc.reason)

        f0 = pool.submit(request, 0)  # leader：登记幂等键后容量拒绝
        time.sleep(0.1)
        f1 = pool.submit(request, 1)  # follower：应得到同一容量拒绝
        f0.result(timeout=3.0)
        f1.result(timeout=3.0)
        assert reasons == ["global_capacity_exceeded", "global_capacity_exceeded"], reasons
        assert svc.side_effect_count == 0
        print(f"[输入] 同key并发时全局饱和 [依据] leader/follower 同为容量拒绝 -> {reasons}，下单=0")

        # 槽位释放后用同一标识重试：作为新 leader 成功，且只下单一次
        downstream.hold.set()
        f_bg.result(timeout=3.0)
        bg_done = svc.side_effect_count  # 背景占槽请求自身也成功下单一次
        status, body = svc.create_order("idem-c-0", "book", 1, key)
        assert status == 201
        assert svc.side_effect_count == bg_done + 1, "幂等标识对应的真实下单只能有一次"
        print(f"[输入] 容量释放后同key重试 [依据] 失败落定后允许重试 -> 201 {body['order_id']}，同标识下单=1")
