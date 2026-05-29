from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base, TimestampMixin, SoftDeleteMixin


class User(Base, TimestampMixin, SoftDeleteMixin):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)

    email = Column(String, unique=True, nullable=False, index=True)

    username = Column(String, unique=True, nullable=False)

    hashed_password = Column(String, nullable=False)

    full_name = Column(String)

    phone = Column(String)

    last_login_at = Column(DateTime)

    orders = relationship("Order", back_populates="user")

    cart_items = relationship("CartItem", back_populates="user")