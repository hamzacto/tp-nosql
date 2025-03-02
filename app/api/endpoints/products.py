from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from neo4j import AsyncGraphDatabase
from uuid import UUID

from app.db.init_db import get_db, get_neo4j_driver
from app.schemas.product import Product, ProductCreate
from app.services import product_service

router = APIRouter()


@router.post("/", response_model=Product, status_code=status.HTTP_201_CREATED)
async def create_product(
    product_in: ProductCreate,
    db: AsyncSession = Depends(get_db),
    neo4j_driver = Depends(get_neo4j_driver),
) -> Any:
    """
    Create a new product.
    """
    product = await product_service.create_product(db, neo4j_driver, product_in=product_in)
    return product


@router.get("/", response_model=List[Product])
async def get_products(
    skip: int = 0,
    limit: int = 100,
    category: str = None,
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Get all products with optional filtering by category.
    """
    if category:
        products = await product_service.get_products_by_category(db, category=category)
    else:
        products = await product_service.get_products(db, skip=skip, limit=limit)
    return products


@router.get("/{product_id}", response_model=Product)
async def get_product(
    product_id: int,
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Get a product by ID.
    """
    product = await product_service.get_product(db, product_id=product_id)
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found",
        )
    return product


@router.get("/{product_id}/viral", response_model=dict)
async def get_product_virality(
    product_id: int,
    level: int = Query(2, ge=1, le=5),
    db: AsyncSession = Depends(get_db),
    neo4j_driver = Depends(get_neo4j_driver),
) -> Any:
    """
    Find how many users bought a product within a follow network (for virality detection).
    The level parameter determines how deep in the follow network to look.
    """
    # Check if product exists
    product = await product_service.get_product(db, product_id=product_id)
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found",
        )
    
    from app.services.social_service import get_product_virality
    virality_data = await get_product_virality(neo4j_driver, product_id=product_id, level=level)
    return virality_data 