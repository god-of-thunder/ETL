from sqlalchemy import Column, Integer, ForeignKey, String

from app.db.base import Base, TimestampMixin


class InventoryMovement(Base, TimestampMixin):
    __tablename__ = "inventory_movements"

    id = Column(Integer, primary_key=True)

    product_id = Column(Integer, ForeignKey("products.id"))

    change_amount = Column(Integer)

    movement_type = Column(String)