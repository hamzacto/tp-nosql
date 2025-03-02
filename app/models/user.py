from sqlalchemy import Column, String, DateTime, Integer
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid

from app.db.base import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    purchases = relationship("Purchase", back_populates="user")
    
    # Follow relationships
    followers = relationship(
        "Follow",
        foreign_keys="Follow.followed_id",
        backref="followed",
        lazy="dynamic"
    )
    following = relationship(
        "Follow",
        foreign_keys="Follow.follower_id",
        backref="follower",
        lazy="dynamic"
    ) 