"""测试夹具：隔离状态文件、可控时间、统一服务装配与日志打印。"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.capacity import GlobalCapacityGuard
from app.circuit_breaker import CircuitBreaker
from app.config import Settings
from app.downstream import DownstreamClient
from app.idempotency import IdempotencyStore
from app.rate_limiter import RateLimiter
from app.service import OrderService


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        rate_limit=5,
        rate_limit_window_seconds=10.0,
        rate_limit_state_file=str(tmp_path / "ratelimit.json"),
        idempotency_ttl_seconds=3600.0,
        circuit_failure_threshold=3,
        circuit_open_seconds=5.0,
        circuit_half_open_success_threshold=2,
        downstream_timeout_seconds=0.3,
    )


@pytest.fixture()
def fake_clock():
    """monotonic 风格的可控时钟，供熔断器使用。"""
    current = [1000.0]

    def now() -> float:
        return current[0]

    def advance(seconds: float) -> None:
        current[0] += seconds

    now.advance = advance  # type: ignore[attr-defined]
    return now


@pytest.fixture()
def components(settings, fake_clock):
    breaker = CircuitBreaker(settings, clock=fake_clock)
    downstream = DownstreamClient(timeout=settings.downstream_timeout_seconds)
    return {
        "limiter": RateLimiter(settings),
        "idempotency": IdempotencyStore(settings),
        "breaker": breaker,
        "downstream": downstream,
        "clock": fake_clock,
    }


@pytest.fixture()
def service(settings, components):
    executor = ThreadPoolExecutor(max_workers=16)
    svc = OrderService(
        rate_limiter=components["limiter"],
        idempotency=components["idempotency"],
        breaker=components["breaker"],
        downstream=components["downstream"],
        settings=settings,
        executor=executor,
    )
    yield svc
    executor.shutdown(wait=False)


@pytest.fixture()
def client(service):
    # raise_server_exceptions=False：验证兜底 500 响应契约（生产行为）
    return TestClient(create_app(service), raise_server_exceptions=False)


@pytest.fixture()
def build(tmp_path, fake_clock):
    """按自定义 Settings 装配 (client, service, components) 的工厂，用于新场景测试。"""
    executors: list[ThreadPoolExecutor] = []

    def _build(**overrides) -> tuple[TestClient, OrderService, dict]:
        params = dict(
            rate_limit=5,
            rate_limit_window_seconds=10.0,
            rate_limit_state_file=str(tmp_path / "ratelimit.json"),
            idempotency_ttl_seconds=3600.0,
            circuit_failure_threshold=3,
            circuit_open_seconds=5.0,
            circuit_half_open_success_threshold=2,
            downstream_timeout_seconds=0.3,
        )
        params.update(overrides)
        settings = Settings(**params)
        components = {
            "limiter": RateLimiter(settings),
            "idempotency": IdempotencyStore(settings),
            "breaker": CircuitBreaker(settings, clock=fake_clock),
            "downstream": DownstreamClient(timeout=settings.downstream_timeout_seconds),
            "capacity": GlobalCapacityGuard(settings),
            "clock": fake_clock,
        }
        executor = ThreadPoolExecutor(max_workers=16)
        executors.append(executor)
        svc = OrderService(
            rate_limiter=components["limiter"],
            idempotency=components["idempotency"],
            breaker=components["breaker"],
            downstream=components["downstream"],
            capacity=components["capacity"],
            settings=settings,
            executor=executor,
        )
        client = TestClient(create_app(svc), raise_server_exceptions=False)
        return client, svc, components

    yield _build
    for ex in executors:
        ex.shutdown(wait=False)


def pytest_configure(config):
    # 单测日志：打印请求输入与判定依据（限流 / 幂等 / 熔断）
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger("guardrails")
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    root.propagate = False


# 业务模块 -> 中文说明：汇总时按业务分组展示用例与通过情况
_BUSINESS_LABELS = {
    "test_rate_limit": "调用方限流",
    "test_tiered_quota": "分级配额",
    "test_idempotency": "幂等提交",
    "test_global_capacity": "整体容量保护",
    "test_concurrency": "并发隔离与同标识结果一致",
    "test_circuit_breaker": "熔断与半开恢复",
    "test_error_contract": "错误契约与内部错误不外泄",
}


def pytest_report_collectionfinish(config, start_path, items):
    # 收集完成后按业务模块列出覆盖的用例（先换行，避免与进度点挤在同一行）
    from collections import defaultdict

    grouped: dict[str, list[str]] = defaultdict(list)
    for item in items:
        module = item.nodeid.split("::", 1)[0].rsplit("/", 1)[-1].replace(".py", "")
        grouped[module].append(item.name)
    lines = ["", "按业务模块覆盖的用例："]
    for module, names in grouped.items():
        label = _BUSINESS_LABELS.get(module, module)
        lines.append(f"  [{label}] {module}.py ({len(names)} 条)")
        for name in names:
            lines.append(f"      - {name}")
    return "\n".join(lines)


def pytest_runtest_logreport(report):
    # 每条用例调用阶段实时打印通过情况；guardrails logger 自带直写 stderr 的
    # handler 且 propagate=False，不受 pytest 日志捕获影响，不加 -s 也能看到。
    # 用 nodeid 取模块名（收集路径稳定，不受 tests/__init__.py 包解析影响）。
    if report.when != "call":
        return
    module = report.nodeid.split("::", 1)[0].rsplit("/", 1)[-1].replace(".py", "")
    mark = "PASS" if report.passed else "FAIL"
    logging.getLogger("guardrails.test").info("[%s] %s::%s", mark, module, report.head_line)


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    # 结束时按业务模块汇总通过/失败数，整体回归情况一目了然
    from collections import defaultdict

    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"passed": 0, "failed": 0, "skipped": 0})
    for key in ("passed", "failed", "skipped"):
        for report in terminalreporter.stats.get(key, []):
            if report.when != "call":
                continue
            module = report.nodeid.split("::", 1)[0].rsplit("/", 1)[-1].replace(".py", "")
            stats[module][key] += 1

    terminalreporter.write_sep("=", "按业务模块汇总")
    for module, counts in sorted(stats.items()):
        label = _BUSINESS_LABELS.get(module, module)
        terminalreporter.write_line(
            f"  [{label}] {module}: 通过 {counts['passed']} / 失败 {counts['failed']} / 跳过 {counts['skipped']}"
        )
