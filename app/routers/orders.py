from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from app.db.session import get_db
from app.dependencies.auth import get_current_user
from app.models.user import User
from app.schemas.order import (
    OrderCreate,
    OrderResponse
)
from app.services.order_service import (
    create_order,
    get_my_orders,
    get_order_detail
)

router = APIRouter(
    prefix="/api/v1/orders",
    tags=["Orders"]
)


@router.post("/", response_model=OrderResponse)
def create_new_order(
    payload: OrderCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return create_order(db, current_user.id, payload)


@router.get("/", response_model=List[OrderResponse])
def get_orders(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return get_my_orders(db, current_user.id)


@router.get("/{order_id}", response_model=OrderResponse)
def get_order(
    order_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return get_order_detail(db, order_id, current_user.id)