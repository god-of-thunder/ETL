from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.user import UserCreate, UserLogin
from app.schemas.auth import Token
from app.services.auth_service import (
    register_user,
    authenticate_user,
    create_access_token
)

router = APIRouter(
    prefix="/api/v1/auth",
    tags=["Auth"]
)


@router.post("/register")
def register(
    payload: UserCreate,
    db: Session = Depends(get_db)
):
    return register_user(db, payload)


@router.post("/login", response_model=Token)
def login(
    payload: UserLogin,
    db: Session = Depends(get_db)
):
    user = authenticate_user(db, payload.email, payload.password)

    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    access_token = create_access_token(
        data={"sub": user.email}
    )

    return {
        "access_token": access_token,
        "token_type": "bearer"
    }