"""测试公共夹具。

默认配置：配额宽松、冷却短、超时短，具体用例可自建 app 覆盖。
"""


import httpx
import pytest

from app.config import Settings
from app.factory import create_app


def make_app(**overrides):
    base = dict(
        quota_limit=100,
        quota_window_seconds=60.0,
        idempotency_ttl_seconds=3600.0,
        cb_failure_threshold=3,
        cb_open_cooldown_seconds=0.2,
        cb_half_open_probe=1,
        downstream_timeout_seconds=0.3,
    )
    base.update(overrides)
    return create_app(Settings(**base))


@pytest.fixture
def app():
    return make_app()


@pytest.fixture
async def client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as ac:
        yield ac


def charge_body(key: str, amount: int = 100, scenario: str = "ok") -> dict:
    return {"idempotency_key": key, "amount": amount, "scenario": scenario}


HEADERS = {"X-Caller-Id": "alice"}
