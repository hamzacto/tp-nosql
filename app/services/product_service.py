from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from neo4j import AsyncGraphDatabase
from uuid import UUID

from app.models.product import Product
from app.schemas.product import ProductCreate, ProductUpdate


async def create_product(db: AsyncSession, neo4j_driver, product_in: ProductCreate) -> Product:
    """
    Create a new product in both PostgreSQL and Neo4j.
    """
    # Create product in PostgreSQL
    db_product = Product(
        name=product_in.name,
        category=product_in.category,
        price=product_in.price,
    )
    db.add(db_product)
    await db.commit()
    await db.refresh(db_product)
    
    # Create product node in Neo4j
    async with neo4j_driver.session() as session:
        await session.run(
            """
            CREATE (p:Product {id: $id, name: $name, category: $category, price: $price})
            """,
            id=str(db_product.id),  # Convert ID to string for Neo4j
            name=db_product.name,
            category=db_product.category,
            price=db_product.price,
        )
    
    return db_product


async def get_product(db: AsyncSession, product_id: int) -> Optional[Product]:
    """
    Get a product by ID from PostgreSQL.
    """
    result = await db.execute(select(Product).filter(Product.id == product_id))
    return result.scalars().first()


async def get_product_by_name(db: AsyncSession, name: str) -> Optional[Product]:
    """
    Get a product by name from PostgreSQL.
    """
    result = await db.execute(select(Product).filter(Product.name == name))
    return result.scalars().first()


async def get_products(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[Product]:
    """
    Get all products from PostgreSQL with pagination.
    """
    result = await db.execute(select(Product).offset(skip).limit(limit))
    return result.scalars().all()


async def get_products_by_category(db: AsyncSession, category: str) -> List[Product]:
    """
    Get all products in a specific category from PostgreSQL.
    """
    result = await db.execute(select(Product).filter(Product.category == category))
    return result.scalars().all()


async def get_viral_products(neo4j_driver, level: int = 2) -> List[dict]:
    """
    Find products that have been purchased by users in a follow network.
    This helps identify viral products.
    """
    async with neo4j_driver.session() as session:
        result = await session.run(
            """
            MATCH (u1:User)-[:BOUGHT]->(p:Product)<-[:BOUGHT]-(u2:User)
            WHERE EXISTS((u1)-[:FOLLOWS*1..%d]->(u2))
            WITH p, COUNT(DISTINCT u1) AS buyers
            RETURN p.id AS id, p.name AS name, p.category AS category, p.price AS price, 
                   buyers AS purchase_count,
                   buyers * 1.0 / (CASE WHEN p.price > 0 THEN p.price ELSE 1 END) AS viral_score
            ORDER BY viral_score DESC
            LIMIT 10
            """ % level
        )
        
        products = []
        async for record in result:
            products.append({
                "id": record["id"],  # This will be a string from Neo4j
                "name": record["name"],
                "category": record["category"],
                "price": record["price"],
                "purchase_count": record["purchase_count"],
                "viral_score": record["viral_score"],
            })
        
        return products 


async def create_products_batch(db: AsyncSession, neo4j_driver, products: List[ProductCreate]) -> List[Dict[str, Any]]:
    """Create multiple products in a single transaction."""
    # Create products in PostgreSQL
    db_products = []
    for product_create in products:
        db_product = Product(
            name=product_create.name,
            category=product_create.category,
            price=product_create.price,
        )
        db_products.append(db_product)
    
    db.add_all(db_products)
    await db.commit()
    
    # Create products in Neo4j - use a more efficient approach for large batches
    async with neo4j_driver.session() as session:
        # Prepare batch parameters for a single Cypher query
        batch_params = []
        for product in db_products:
            batch_params.append({
                "id": str(product.id),
                "name": product.name,
                "category": product.category,
                "price": product.price
            })
        
        # Execute a single parameterized query for all products
        if batch_params:
            tx = await session.begin_transaction()
            try:
                # Use UNWIND for batch processing - much more efficient for large batches
                await tx.run(
                    """
                    UNWIND $batch AS product
                    MERGE (p:Product {id: product.id})
                    SET p.name = product.name, 
                        p.category = product.category, 
                        p.price = product.price
                    """,
                    batch=batch_params
                )
                await tx.commit()
            except Exception as e:
                await tx.rollback()
                raise e
    
    # Return created products
    return [
        {"id": product.id, "name": product.name, "category": product.category, "price": product.price}
        for product in db_products
    ] 