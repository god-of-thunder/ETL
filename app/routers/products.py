from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from app.db.session import get_db
from app.schemas.product import (
    ProductCreate,
    ProductUpdate,
    ProductResponse
)
from app.services.product_service import (
    create_product,
    get_products,
    update_product,
    delete_product
)

router = APIRouter(
    prefix="/api/v1/products",
    tags=["Products"]
)


@router.post("/", response_model=ProductResponse)
def create_product_api(
    payload: ProductCreate,
    db: Session = Depends(get_db)
):
    return create_product(db, payload)


@router.get("/", response_model=List[ProductResponse])
def get_all_products(
    db: Session = Depends(get_db)
):
    return get_products(db)


@router.put("/{product_id}")
def update_product_api(
    product_id: int,
    payload: ProductUpdate,
    db: Session = Depends(get_db)
):
    return update_product(db, product_id, payload)


@router.delete("/{product_id}")
def delete_product_api(
    product_id: int,
    db: Session = Depends(get_db)
):
    return delete_product(db, product_id)