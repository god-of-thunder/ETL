from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from app.core.database import get_db
from app.dependencies.auth import get_current_user
from app.models.user import User
from app.schemas.cart import (
    CartItemCreate,
    CartItemUpdate,
    CartItemResponse
)
from app.services.cart_service import (
    add_to_cart,
    get_cart_items,
    update_cart_item,
    remove_cart_item
)

router = APIRouter(
    prefix="/api/v1/carts",
    tags=["Cart"]
)


@router.post("/", response_model=CartItemResponse)
def add_cart_item(
    payload: CartItemCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return add_to_cart(db, current_user.id, payload)


@router.get("/", response_model=List[CartItemResponse])
def get_my_cart(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return get_cart_items(db, current_user.id)


@router.put("/{cart_item_id}")
def update_cart(
    cart_item_id: int,
    payload: CartItemUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return update_cart_item(
        db,
        cart_item_id,
        current_user.id,
        payload
    )


@router.delete("/{cart_item_id}")
def remove_cart(
    cart_item_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return remove_cart_item(db, cart_item_id, current_user.id)