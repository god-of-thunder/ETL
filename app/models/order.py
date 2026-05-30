from sqlalchemy import Column, Integer, ForeignKey, Float, String, DateTime
from sqlalchemy.orm import relationship

from app.db.base import Base, TimestampMixin


class Order(Base, TimestampMixin):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)

    user_id = Column(Integer, ForeignKey("users.id"))

    total_amount = Column(Float, nullable=False)

    status = Column(String, default="pending")

    payment_status = Column(String, default="unpaid")

    shipping_address = Column(String)

    paid_at = Column(DateTime)

    shipped_at = Column(DateTime)

    delivered_at = Column(DateTime)

    cancelled_at = Column(DateTime)

    user = relationship("User", back_populates="orders")

    items = relationship("OrderItem", back_populates="order")

    payment = relationship(
        "Payment",
        back_populates="order",
        uselist=False
    )