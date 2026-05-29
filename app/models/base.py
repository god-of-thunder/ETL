from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, DateTime, Boolean
from datetime import datetime


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False
    )

    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )


class SoftDeleteMixin:
    is_deleted = Column(Boolean, default=False)

    deleted_at = Column(DateTime, nullable=True)