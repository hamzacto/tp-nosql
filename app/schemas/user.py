from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, EmailStr
from uuid import UUID


# Shared properties
class UserBase(BaseModel):
    name: str
    email: EmailStr


# Properties to receive via API on creation
class UserCreate(UserBase):
    password: str


# Properties to receive via API on update
class UserUpdate(UserBase):
    password: Optional[str] = None


# Properties shared by models stored in DB
class UserInDBBase(UserBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        orm_mode = True
        from_attributes = True


# Properties to return to client
class User(UserInDBBase):
    pass


# Properties stored in DB
class UserInDB(UserInDBBase):
    hashed_password: str


# Follow user schema
class FollowUser(BaseModel):
    target_user_id: int 