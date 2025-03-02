from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from neo4j import AsyncGraphDatabase

from app.db.init_db import get_db, get_neo4j_driver
from app.schemas.purchase import Purchase, PurchaseCreate
from app.services import purchase_service, user_service, product_service

router = APIRouter()


@router.post("/users/{user_id}/purchases", response_model=Purchase, status_code=status.HTTP_201_CREATED)
async def create_purchase(
    user_id: int,
    purchase_in: PurchaseCreate,
    db: AsyncSession = Depends(get_db),
    neo4j_driver = Depends(get_neo4j_driver),
) -> Any:
    """
    Record a purchase for a user.
    """
    # Check if user exists
    user = await user_service.get_user(db, user_id=user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    
    # Check if product exists
    product = await product_service.get_product(db, product_id=purchase_in.product_id)
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found",
        )
    
    purchase = await purchase_service.create_purchase(
        db, neo4j_driver, user_id=user_id, purchase_in=purchase_in
    )
    return purchase


@router.get("/users/{user_id}/purchases", response_model=List[Purchase])
async def get_user_purchases(
    user_id: int,
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Get all purchases made by a user.
    """
    # Check if user exists
    user = await user_service.get_user(db, user_id=user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    
    purchases = await purchase_service.get_user_purchases(
        db, user_id=user_id, skip=skip, limit=limit
    )
    return purchases 