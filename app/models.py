"""请求 / 响应与下游依赖的数据模型。"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

DownstreamMode = Literal["ok", "fail", "timeout", "flaky"]


class CreateOrderRequest(BaseModel):
    item: str = Field(..., min_length=1)
    quantity: int = Field(..., gt=0)


class OrderResult(BaseModel):
    order_id: str
    item: str
    quantity: int
    downstream_ref: str


class ErrorResponse(BaseModel):
    error: dict
