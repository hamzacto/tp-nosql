from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel

from app.schemas.user import User
from app.schemas.product import Product


# Shared properties
class PurchaseBase(BaseModel):
    product_id: int


# Properties to receive via API on creation
class PurchaseCreate(PurchaseBase):
    pass


# Properties to receive via API on update
class PurchaseUpdate(PurchaseBase):
    product_id: Optional[int] = None


# Properties shared by models stored in DB
class PurchaseInDBBase(PurchaseBase):
    id: int
    user_id: int
    created_at: datetime

    class Config:
        orm_mode = True
        from_attributes = True


# Properties to return to client
class Purchase(PurchaseInDBBase):
    pass


# Properties stored in DB
class PurchaseInDB(PurchaseInDBBase):
    pass


# Purchase with user and product details
class PurchaseWithDetails(Purchase):
    user: User
    product: Product 