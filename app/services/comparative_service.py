from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, text
from neo4j import AsyncGraphDatabase
from uuid import UUID

from app.models.user import User
from app.models.product import Product
from app.models.purchase import Purchase


# PostgreSQL queries
async def pg_get_product_virality(
    db: AsyncSession, product_id: int, level: int = 2
) -> Dict[str, Any]:
    """
    Find how many users bought a product within a follow network (PostgreSQL).
    This is a complex query in PostgreSQL that requires recursive CTEs.
    """
    # This is a complex query that requires recursive CTEs in PostgreSQL
    # We'll use raw SQL for this
    query = text("""
    WITH RECURSIVE followers AS (
        -- Base case: direct followers
        SELECT follower_id, followed_id, 1 AS level
        FROM follows
        WHERE followed_id IN (
            SELECT user_id FROM purchases WHERE product_id = :product_id
        )
        
        UNION ALL
        
        -- Recursive case: followers of followers up to level n
        SELECT f.follower_id, fl.followed_id, fl.level + 1
        FROM follows f
        JOIN followers fl ON f.followed_id = fl.follower_id
        WHERE fl.level < :level
    )
    SELECT 
        p.id, 
        p.name, 
        p.category, 
        p.price,
        COUNT(DISTINCT u.id) AS purchase_count,
        COUNT(DISTINCT u.id) * 1.0 / p.price AS viral_score
    FROM products p
    JOIN purchases pu ON p.id = pu.product_id
    JOIN users u ON pu.user_id = u.id
    JOIN followers f ON u.id = f.follower_id
    WHERE p.id = :product_id
    GROUP BY p.id, p.name, p.category, p.price
    """)
    
    result = await db.execute(query, {"product_id": product_id, "level": level})
    row = result.fetchone()
    
    if not row:
        return {
            "id": str(product_id),
            "name": "",
            "category": "",
            "price": 0,
            "purchase_count": 0,
            "viral_score": 0,
        }
    
    return {
        "id": str(row[0]),  # Convert UUID to string for response
        "name": row[1],
        "category": row[2],
        "price": row[3],
        "purchase_count": row[4],
        "viral_score": row[5],
    }


async def pg_get_user_influence(
    db: AsyncSession, user_id: int, level: int = 2
) -> Dict[str, Any]:
    """
    Calculate the influence of a user in the purchase network (PostgreSQL).
    This shows how many users in the follow network have purchased products that the user has purchased.
    """
    query = text("""
    WITH RECURSIVE followers AS (
        -- Base case: direct followers
        SELECT follower_id, followed_id, 1 AS level
        FROM follows
        WHERE followed_id = :user_id
        
        UNION ALL
        
        -- Recursive case: followers of followers up to level n
        SELECT f.follower_id, fl.followed_id, fl.level + 1
        FROM follows f
        JOIN followers fl ON f.followed_id = fl.follower_id
        WHERE fl.level < :level
    ),
    user_products AS (
        -- Products purchased by the user
        SELECT product_id
        FROM purchases
        WHERE user_id = :user_id
    ),
    influenced_purchases AS (
        -- Purchases of the same products by followers
        SELECT 
            u.id AS follower_id,
            COUNT(DISTINCT p.id) AS product_count
        FROM users u
        JOIN purchases pu ON u.id = pu.user_id
        JOIN products p ON pu.product_id = p.id
        JOIN followers f ON u.id = f.follower_id
        WHERE pu.product_id IN (SELECT product_id FROM user_products)
        GROUP BY u.id
    )
    SELECT 
        u.id,
        u.name,
        COUNT(DISTINCT ip.follower_id) AS followers,
        COUNT(DISTINCT up.product_id) AS products,
        CASE 
            WHEN COUNT(DISTINCT up.product_id) > 0 
            THEN COUNT(DISTINCT ip.follower_id) * 1.0 / COUNT(DISTINCT up.product_id)
            ELSE 0
        END AS influence_score
    FROM users u
    LEFT JOIN user_products up ON 1=1
    LEFT JOIN influenced_purchases ip ON 1=1
    WHERE u.id = :user_id
    GROUP BY u.id, u.name
    """)
    
    result = await db.execute(query, {"user_id": user_id, "level": level})
    row = result.fetchone()
    
    if not row:
        return {
            "user_id": str(user_id),
            "name": "",
            "level": level,
            "followers": 0,
            "products": 0,
            "influence_score": 0,
        }
    
    return {
        "user_id": str(row[0]),  # Convert UUID to string for response
        "name": row[1],
        "level": level,
        "followers": row[2] or 0,
        "products": row[3] or 0,
        "influence_score": row[4] or 0,
    }


async def pg_get_viral_products(
    db: AsyncSession, level: int = 2
) -> List[Dict[str, Any]]:
    """
    Find products that have been purchased by users in a follow network (PostgreSQL).
    This helps identify viral products.
    """
    query = text("""
    WITH RECURSIVE follow_network AS (
        -- All follow relationships
        SELECT follower_id, followed_id, 1 AS level
        FROM follows
        
        UNION ALL
        
        -- Recursive case: followers of followers up to level n
        SELECT f.follower_id, fn.followed_id, fn.level + 1
        FROM follows f
        JOIN follow_network fn ON f.followed_id = fn.follower_id
        WHERE fn.level < :level
    ),
    viral_products AS (
        -- Products purchased by users in the follow network
        SELECT 
            p.id,
            p.name,
            p.category,
            p.price,
            COUNT(DISTINCT pu.user_id) AS purchase_count,
            COUNT(DISTINCT pu.user_id) * 1.0 / p.price AS viral_score
        FROM products p
        JOIN purchases pu ON p.id = pu.product_id
        JOIN users u ON pu.user_id = u.id
        JOIN follow_network fn ON u.id = fn.follower_id
        GROUP BY p.id, p.name, p.category, p.price
        ORDER BY viral_score DESC
        LIMIT 10
    )
    SELECT id, name, category, price, purchase_count, viral_score
    FROM viral_products
    """)
    
    result = await db.execute(query, {"level": level})
    rows = result.fetchall()
    
    products = []
    for row in rows:
        products.append({
            "id": str(row[0]),  # Convert UUID to string for response
            "name": row[1],
            "category": row[2],
            "price": row[3],
            "purchase_count": row[4],
            "viral_score": row[5],
        })
    
    return products


# Neo4j queries
async def neo4j_get_product_virality(
    neo4j_driver, product_id: int, level: int = 2
) -> Dict[str, Any]:
    """
    Find how many users bought a product within a follow network (Neo4j).
    This is a simple query in Neo4j using pattern matching.
    """
    async with neo4j_driver.session() as session:
        result = await session.run(
            """
            MATCH (p:Product {id: $product_id})
            OPTIONAL MATCH (u1:User)-[:BOUGHT]->(p)<-[:BOUGHT]-(u2:User)
            WHERE EXISTS((u1)-[:FOLLOWS*1..%d]->(u2))
            WITH p, COUNT(DISTINCT u1) AS purchase_count
            RETURN p.id AS id, p.name AS name, p.category AS category, p.price AS price,
                   purchase_count,
                   CASE WHEN p.price > 0 THEN purchase_count * 1.0 / p.price ELSE 0 END AS viral_score
            """ % level,
            product_id=str(product_id),  # Convert UUID to string for Neo4j
        )
        
        record = await result.single()
        if not record:
            return {
                "id": str(product_id),
                "name": "",
                "category": "",
                "price": 0,
                "purchase_count": 0,
                "viral_score": 0,
            }
        
        return {
            "id": record["id"],  # This will be a string from Neo4j
            "name": record["name"],
            "category": record["category"],
            "price": record["price"],
            "purchase_count": record["purchase_count"],
            "viral_score": record["viral_score"],
        }


async def neo4j_get_user_influence(
    neo4j_driver, user_id: int, level: int = 2
) -> Dict[str, Any]:
    """
    Calculate the influence of a user in the purchase network (Neo4j).
    This shows how many users in the follow network have purchased products that the user has purchased.
    """
    async with neo4j_driver.session() as session:
        result = await session.run(
            """
            MATCH (u:User {id: $user_id})
            OPTIONAL MATCH (u)-[:BOUGHT]->(p:Product)<-[:BOUGHT]-(follower:User)
            WHERE EXISTS((follower)-[:FOLLOWS*1..%d]->(u))
            WITH u, COUNT(DISTINCT follower) AS followers, COUNT(DISTINCT p) AS products
            RETURN u.id AS user_id, u.name AS name, followers,
                   products,
                   CASE WHEN products > 0 THEN followers * 1.0 / products ELSE 0 END AS influence_score
            """ % level,
            user_id=str(user_id),  # Convert UUID to string for Neo4j
        )
        
        record = await result.single()
        if not record:
            return {
                "user_id": str(user_id),
                "name": "",
                "level": level,
                "followers": 0,
                "products": 0,
                "influence_score": 0,
            }
        
        return {
            "user_id": record["user_id"],  # This will be a string from Neo4j
            "name": record["name"],
            "level": level,
            "followers": record["followers"],
            "products": record["products"],
            "influence_score": record["influence_score"],
        }


async def neo4j_get_viral_products(
    neo4j_driver, level: int = 2
) -> List[Dict[str, Any]]:
    """
    Find products that have been purchased by users in a follow network (Neo4j).
    This helps identify viral products.
    """
    async with neo4j_driver.session() as session:
        result = await session.run(
            """
            MATCH (u1:User)-[:FOLLOWS*1..%d]->(u2:User)
            MATCH (u1)-[:BOUGHT]->(p:Product)<-[:BOUGHT]-(u2)
            WITH p, COUNT(DISTINCT u1) AS purchase_count
            RETURN p.id AS id, p.name AS name, p.category AS category, p.price AS price,
                   purchase_count,
                   CASE WHEN p.price > 0 THEN purchase_count * 1.0 / p.price ELSE 0 END AS viral_score
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