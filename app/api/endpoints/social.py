from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from neo4j import AsyncGraphDatabase

from app.db.init_db import get_db, get_neo4j_driver
from app.services import user_service, product_service, social_service

router = APIRouter()


@router.get("/users/{user_id}/influence", response_model=Dict[str, Any])
async def get_user_influence(
    user_id: int,
    level: int = Query(2, ge=1, le=5),
    db: AsyncSession = Depends(get_db),
    neo4j_driver = Depends(get_neo4j_driver),
) -> Any:
    """
    Fetch all products bought by followers up to level n.
    This endpoint analyzes the influence of a user's purchases on their followers.
    """
    # Check if user exists
    user = await user_service.get_user(db, user_id=user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    
    influence_data = await social_service.get_user_influence_network(
        neo4j_driver, user_id=user_id, level=level
    )
    
    if not influence_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found in graph database",
        )
    
    return influence_data


@router.get("/products/viral", response_model=Dict[str, Any])
async def get_viral_products(
    level: int = Query(2, ge=1, le=5),
    neo4j_driver = Depends(get_neo4j_driver),
) -> Any:
    """
    Get the most viral products across the entire social network.
    """
    viral_products = await product_service.get_viral_products(neo4j_driver, level=level)
    return {"viral_products": viral_products, "level": level} 