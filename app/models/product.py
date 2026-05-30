from sqlalchemy import Column, Integer, String, Float, Text, Boolean
from sqlalchemy.orm import relationship

from app.db.base import Base, TimestampMixin, SoftDeleteMixin


class Product(Base, TimestampMixin, SoftDeleteMixin):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)

    name = Column(String, nullable=False, index=True)

    sku = Column(String, unique=True, nullable=False)

    description = Column(Text)

    category = Column(String, index=True)

    price = Column(Float, nullable=False)

    stock = Column(Integer, default=0)

    is_active = Column(Boolean, default=True)

    order_items = relationship("OrderItem", back_populates="product")

    cart_items = relationship("CartItem", back_populates="product")