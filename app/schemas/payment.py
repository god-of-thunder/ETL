from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class PaymentCreate(BaseModel):
    order_id: int
    provider: str
    amount: float


class PaymentResponse(BaseModel):
    id: int
    order_id: int
    provider: str
    transaction_id: Optional[str]
    amount: float
    status: str
    paid_at: Optional[datetime]

    class Config:
        from_attributes = True