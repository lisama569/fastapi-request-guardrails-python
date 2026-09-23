"""本地模拟的下游依赖客户端。

支持四种场景，便于测试区分三类失败面：
    ok      —— 正常返回
    fail    —— 下游自身返回业务错误（DownstreamFailure）
    timeout —— 下游挂起，触发调用方超时（DownstreamTimeout）
    slow    —— 下游慢于超时阈值，同样触发超时
"""

import asyncio
import uuid
from dataclasses import dataclass


class DownstreamTimeout(Exception):
    """下游调用超过约定超时时间。"""


class DownstreamFailure(Exception):
    """下游自身返回的错误（非超时）。"""


@dataclass
class DownstreamResponse:
    ref: str


class DownstreamClient:
    def __init__(self, timeout: float):
        self.timeout = timeout

    async def charge(self, scenario: str, amount: int) -> DownstreamResponse:
        try:
            return await asyncio.wait_for(
                self._handle(scenario, amount), timeout=self.timeout
            )
        except asyncio.TimeoutError:
            raise DownstreamTimeout(
                f"下游调用超时（>{self.timeout}s）"
            ) from None

    async def _handle(self, scenario: str, amount: int) -> DownstreamResponse:
        if scenario == "ok":
            return DownstreamResponse(ref=f"ref-{uuid.uuid4().hex[:12]}")
        if scenario == "fail":
            raise DownstreamFailure("下游账户服务返回：账户状态异常")
        if scenario in ("timeout", "slow"):
            # 模拟挂起/慢响应，交由 wait_for 判定超时
            await asyncio.sleep(self.timeout * 2 + 1)
            return DownstreamResponse(ref="unreachable")
        raise DownstreamFailure(f"未知下游场景: {scenario}")
