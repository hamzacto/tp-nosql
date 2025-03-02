from typing import List, Dict, Any
from datetime import datetime
from pydantic import BaseModel
from uuid import UUID

from app.schemas.user import User
from app.schemas.product import Product


class UserWithFollowers(User):
    followers: List[User]


class ProductInfluence(BaseModel):
    product: Product
    influence_count: int
    influenced_users: List[User]


class UserInfluence(BaseModel):
    user: User
    influenced_products: List[Product]
    influence_level: int


class ViralProduct(BaseModel):
    product: Product
    viral_score: float
    purchase_count: int
    network_reach: int


class FollowBase(BaseModel):
    follower_id: int
    followed_id: int


class FollowCreate(FollowBase):
    pass


class Follow(FollowBase):
    id: UUID
    created_at: datetime

    class Config:
        orm_mode = True
        from_attributes = True


class UserNetwork(BaseModel):
    user_id: int
    followers: List[Dict[str, Any]]
    following: List[Dict[str, Any]] 