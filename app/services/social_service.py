from typing import List, Dict, Any
from neo4j import AsyncGraphDatabase

async def get_product_virality(neo4j_driver, product_id: int, level: int = 2) -> Dict[str, Any]:
    """
    Analyze the virality of a product within a social network.
    """
    async with neo4j_driver.session() as session:
        # Get product details
        product_result = await session.run(
            """
            MATCH (p:Product {id: $product_id})
            RETURN p
            """,
            product_id=product_id,
        )
        
        product_record = await product_result.single()
        if not product_record:
            return None
        
        product = product_record["p"]
        
        # Analyze virality
        virality_result = await session.run(
            """
            MATCH (u1:User)-[:BOUGHT]->(p:Product {id: $product_id})
            MATCH (u2:User)-[:BOUGHT]->(p)
            WHERE EXISTS((u2)-[:FOLLOWS*1..%d]->(u1))
            WITH p, COUNT(DISTINCT u1) AS influencers, COUNT(DISTINCT u2) AS influenced
            RETURN p.id AS id, p.name AS name, p.category AS category, p.price AS price,
                   influencers, influenced,
                   influenced * 1.0 / (CASE WHEN influencers > 0 THEN influencers ELSE 1 END) AS viral_ratio,
                   influencers + influenced AS network_reach
            """ % level,
            product_id=product_id,
        )
        
        virality_record = await virality_result.single()
        if not virality_record:
            return {
                "product": {
                    "id": product["id"],
                    "name": product["name"],
                    "category": product["category"],
                    "price": product["price"],
                },
                "viral_score": 0,
                "purchase_count": 0,
                "network_reach": 0,
                "influencers": 0,
                "influenced": 0,
            }
        
        return {
            "product": {
                "id": virality_record["id"],
                "name": virality_record["name"],
                "category": virality_record["category"],
                "price": virality_record["price"],
            },
            "viral_score": virality_record["viral_ratio"],
            "purchase_count": virality_record["influencers"] + virality_record["influenced"],
            "network_reach": virality_record["network_reach"],
            "influencers": virality_record["influencers"],
            "influenced": virality_record["influenced"],
        }


async def get_user_influence_network(neo4j_driver, user_id: int, level: int = 2) -> Dict[str, Any]:
    """
    Get the influence network of a user up to a certain level.
    """
    async with neo4j_driver.session() as session:
        # Get user details
        user_result = await session.run(
            """
            MATCH (u:User {id: $user_id})
            RETURN u
            """,
            user_id=user_id,
        )
        
        user_record = await user_result.single()
        if not user_record:
            return None
        
        user = user_record["u"]
        
        # Get products bought by followers
        influence_result = await session.run(
            """
            MATCH (u:User {id: $user_id})-[:BOUGHT]->(p:Product)
            MATCH (follower:User)-[:FOLLOWS*1..%d]->(u)
            MATCH (follower)-[:BOUGHT]->(p)
            WITH p, COUNT(DISTINCT follower) AS follower_count
            RETURN p.id AS id, p.name AS name, p.category AS category, p.price AS price,
                   follower_count
            ORDER BY follower_count DESC
            LIMIT 10
            """ % level,
            user_id=user_id,
        )
        
        influenced_products = []
        async for record in influence_result:
            influenced_products.append({
                "id": record["id"],
                "name": record["name"],
                "category": record["category"],
                "price": record["price"],
                "follower_count": record["follower_count"],
            })
        
        # Get total influence metrics
        metrics_result = await session.run(
            """
            MATCH (u:User {id: $user_id})
            MATCH (follower:User)-[:FOLLOWS*1..%d]->(u)
            WITH COUNT(DISTINCT follower) AS total_followers
            RETURN total_followers
            """ % level,
            user_id=user_id,
        )
        
        metrics_record = await metrics_result.single()
        total_followers = metrics_record["total_followers"] if metrics_record else 0
        
        return {
            "user": {
                "id": user["id"],
                "name": user["name"],
                "email": user["email"],
            },
            "influenced_products": influenced_products,
            "total_followers": total_followers,
            "influence_level": level,
        } 