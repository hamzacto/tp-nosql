from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from neo4j import AsyncGraphDatabase

from app.db.init_db import get_db, get_neo4j_driver
from app.schemas.user import User, UserCreate, FollowUser
from app.services import user_service

router = APIRouter()


@router.post("/", response_model=User, status_code=status.HTTP_201_CREATED)
async def create_user(
    user_in: UserCreate,
    db: AsyncSession = Depends(get_db),
    neo4j_driver = Depends(get_neo4j_driver),
) -> Any:
    """
    Create a new user.
    """
    user = await user_service.get_user_by_email(db, email=user_in.email)
    if user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A user with this email already exists.",
        )
    user = await user_service.create_user(db, neo4j_driver, user_in=user_in)
    return user


@router.get("/{user_id}", response_model=User)
async def get_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Get a user by ID.
    """
    user = await user_service.get_user(db, user_id=user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    return user


@router.post("/{user_id}/follow", status_code=status.HTTP_200_OK)
async def follow_user(
    user_id: int,
    follow_data: FollowUser,
    db: AsyncSession = Depends(get_db),
    neo4j_driver = Depends(get_neo4j_driver),
) -> Any:
    """
    Follow another user.
    """
    if user_id == follow_data.target_user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A user cannot follow themselves",
        )
    
    success = await user_service.follow_user(
        db, neo4j_driver, user_id=user_id, target_user_id=follow_data.target_user_id
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="One or both users not found",
        )
    
    return {"status": "success", "message": "User followed successfully"}


@router.get("/{user_id}/followers", response_model=List[dict])
async def get_followers(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    neo4j_driver = Depends(get_neo4j_driver),
) -> Any:
    """
    Get all followers of a user.
    """
    # Check if user exists
    user = await user_service.get_user(db, user_id=user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    
    followers = await user_service.get_followers(neo4j_driver, user_id=user_id)
    return followers 