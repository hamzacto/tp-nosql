from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from neo4j import AsyncGraphDatabase
from uuid import UUID

from app.models.purchase import Purchase
from app.models.user import User
from app.models.product import Product
from app.schemas.purchase import PurchaseCreate, PurchaseUpdate
from app.services import user_service, product_service


async def create_purchase(db: AsyncSession, neo4j_driver, user_id: int, purchase_in: PurchaseCreate) -> Optional[Purchase]:
    """
    Create a new purchase in both PostgreSQL and Neo4j.
    """
    # Check if user and product exist
    user = await user_service.get_user(db, user_id)
    product = await product_service.get_product(db, purchase_in.product_id)
    
    if not user or not product:
        return None
    
    # Create purchase in PostgreSQL
    db_purchase = Purchase(
        user_id=user_id,
        product_id=purchase_in.product_id,
    )
    db.add(db_purchase)
    await db.commit()
    await db.refresh(db_purchase)
    
    # Create BOUGHT relationship in Neo4j
    async with neo4j_driver.session() as session:
        await session.run(
            """
            MATCH (u:User {id: $user_id}), (p:Product {id: $product_id})
            CREATE (u)-[r:BOUGHT {purchase_id: $purchase_id, timestamp: datetime()}]->(p)
            """,
            user_id=str(user_id),  # Convert ID to string for Neo4j
            product_id=str(purchase_in.product_id),  # Convert ID to string for Neo4j
            purchase_id=str(db_purchase.id),  # Convert Integer ID to string for Neo4j
        )
    
    return db_purchase


async def get_user_purchases(db: AsyncSession, user_id: int) -> List[Purchase]:
    """
    Get all purchases made by a user from PostgreSQL.
    """
    result = await db.execute(select(Purchase).filter(Purchase.user_id == user_id))
    return result.scalars().all()


async def get_purchase(db: AsyncSession, purchase_id: int) -> Optional[Purchase]:
    """
    Get a purchase by ID from PostgreSQL.
    """
    result = await db.execute(select(Purchase).filter(Purchase.id == purchase_id))
    return result.scalars().first()


async def get_product_purchases(db: AsyncSession, product_id: int) -> List[Purchase]:
    """
    Get all purchases of a product from PostgreSQL.
    """
    result = await db.execute(select(Purchase).filter(Purchase.product_id == product_id))
    return result.scalars().all()


async def get_purchase_network(neo4j_driver, user_id: int, level: int = 2) -> Dict[str, Any]:
    """
    Get the purchase network of a user from Neo4j.
    This shows products purchased by users in the user's follow network.
    """
    async with neo4j_driver.session() as session:
        result = await session.run(
            """
            MATCH (u:User {id: $user_id})-[:FOLLOWS*1..%d]->(friend:User)-[:BOUGHT]->(p:Product)
            RETURN p.id AS product_id, p.name AS product_name, p.category AS category, p.price AS price,
                   COUNT(DISTINCT friend) AS buyer_count
            ORDER BY buyer_count DESC
            LIMIT 10
            """ % level,
            user_id=str(user_id),  # Convert ID to string for Neo4j
        )
        
        products = []
        async for record in result:
            products.append({
                "id": record["product_id"],  # This will be a string from Neo4j
                "name": record["product_name"],
                "category": record["category"],
                "price": record["price"],
                "buyer_count": record["buyer_count"],
            })
        
        return {
            "user_id": str(user_id),  # Convert ID to string for response
            "level": level,
            "products": products,
        }


async def get_user_influence(neo4j_driver, user_id: int, level: int = 2) -> Dict[str, Any]:
    """
    Calculate the influence of a user in the purchase network.
    This shows how many users in the follow network have purchased products that the user has purchased.
    """
    async with neo4j_driver.session() as session:
        result = await session.run(
            """
            MATCH (u:User {id: $user_id})-[:BOUGHT]->(p:Product)<-[:BOUGHT]-(follower:User)
            WHERE EXISTS((follower)-[:FOLLOWS*1..%d]->(u))
            WITH u, COUNT(DISTINCT follower) AS followers, COUNT(DISTINCT p) AS products
            RETURN u.id AS user_id, u.name AS user_name, followers, products,
                   followers * 1.0 / CASE WHEN products > 0 THEN products ELSE 1 END AS influence_score
            """ % level,
            user_id=str(user_id),  # Convert ID to string for Neo4j
        )
        
        record = await result.single()
        if not record:
            return {
                "user_id": str(user_id),  # Convert ID to string for response
                "level": level,
                "followers": 0,
                "products": 0,
                "influence_score": 0,
            }
        
        return {
            "user_id": record["user_id"],  # This will be a string from Neo4j
            "name": record["user_name"],
            "level": level,
            "followers": record["followers"],
            "products": record["products"],
            "influence_score": record["influence_score"],
        } 