from typing import Optional
from datetime import datetime
from pydantic import BaseModel
from uuid import UUID


# Shared properties
class ProductBase(BaseModel):
    name: str
    category: str
    price: float


# Properties to receive via API on creation
class ProductCreate(ProductBase):
    pass


# Properties to receive via API on update
class ProductUpdate(ProductBase):
    name: Optional[str] = None
    category: Optional[str] = None
    price: Optional[float] = None


# Properties shared by models stored in DB
class ProductInDBBase(ProductBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        orm_mode = True
        from_attributes = True


# Properties to return to client
class Product(ProductInDBBase):
    pass


# Properties stored in DB
class ProductInDB(ProductInDBBase):
    pass 