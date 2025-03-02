from sqlalchemy import Column, ForeignKey, DateTime, Integer
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID
import uuid

from app.db.base import Base


class Follow(Base):
    __tablename__ = "follows"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    follower_id = Column(Integer, ForeignKey("users.id"), index=True)
    followed_id = Column(Integer, ForeignKey("users.id"), index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now()) 