from sqlalchemy import Column, Integer, ForeignKey, String, Float, DateTime
from sqlalchemy.orm import relationship

from app.db.base import Base, TimestampMixin


class Payment(Base, TimestampMixin):
    __tablename__ = "payments"

    id = Column(Integer, primary_key=True, index=True)

    order_id = Column(Integer, ForeignKey("orders.id"))

    provider = Column(String)

    transaction_id = Column(String, unique=True)

    amount = Column(Float)

    status = Column(String)

    paid_at = Column(DateTime)

    order = relationship("Order", back_populates="payment")