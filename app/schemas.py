"""请求/响应/错误数据模型（骨架）。"""

from pydantic import BaseModel, Field


class ChargeRequest(BaseModel):
    """下单/扣费请求体。"""

    amount: int = Field(gt=0)
    # 请求幂等键，由调用方提供
    idempotency_key: str = Field(min_length=1)
    # 下游故障场景注入：ok / fail / timeout / slow
    scenario: str = "ok"


class ChargeResult(BaseModel):
    """成功响应。"""

    request_id: str
    caller: str
    amount: int
    idempotency_key: str
    downstream_ref: str
    replayed: bool


class ErrorBody(BaseModel):
    """统一错误响应体，内部异常永不透出。"""

    error: str
    reason: str
    retry_after_seconds: float | None = None
    request_id: str | None = None
