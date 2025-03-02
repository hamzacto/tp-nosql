from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from neo4j import AsyncGraphDatabase
from uuid import UUID
import logging

from app.models.user import User
from app.schemas.user import UserCreate, UserUpdate
from app.utils.security import get_password_hash, verify_password
from app.models.follow import Follow


async def create_user(db: AsyncSession, neo4j_driver, user_in: UserCreate) -> User:
    """
    Create a new user in both PostgreSQL and Neo4j.
    """
    # Create user in PostgreSQL
    db_user = User(
        name=user_in.name,
        email=user_in.email,
        hashed_password=get_password_hash(user_in.password),
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    
    # Create user node in Neo4j
    async with neo4j_driver.session() as session:
        await session.run(
            """
            CREATE (u:User {id: $id, name: $name, email: $email})
            """,
            id=str(db_user.id),  # Convert ID to string for Neo4j
            name=db_user.name,
            email=db_user.email,
        )
    
    return db_user


async def get_user(db: AsyncSession, user_id: int) -> Optional[User]:
    """
    Get a user by ID from PostgreSQL.
    """
    result = await db.execute(select(User).filter(User.id == user_id))
    return result.scalars().first()


async def get_user_by_email(db: AsyncSession, email: str) -> Optional[User]:
    """
    Get a user by email from PostgreSQL.
    """
    result = await db.execute(select(User).filter(User.email == email))
    return result.scalars().first()


async def authenticate_user(db: AsyncSession, email: str, password: str) -> Optional[User]:
    """
    Authenticate a user by email and password.
    """
    user = await get_user_by_email(db, email)
    if not user:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


async def follow_user(db: AsyncSession, neo4j_driver, user_id: int, target_user_id: int) -> bool:
    """
    Create a FOLLOWS relationship between two users in both PostgreSQL and Neo4j.
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Creating follow relationship: {user_id} -> {target_user_id}")
    
    # Check if both users exist in PostgreSQL
    try:
        user = await get_user(db, user_id)
        target_user = await get_user(db, target_user_id)
        
        if not user:
            logger.error(f"User with ID {user_id} not found")
            return False
        
        if not target_user:
            logger.error(f"Target user with ID {target_user_id} not found")
            return False
        
        logger.info(f"Both users found: {user.name} -> {target_user.name}")
        
        # Create follow relationship in PostgreSQL
        # Check if relationship already exists
        stmt = select(Follow).where(
            Follow.follower_id == user_id,
            Follow.followed_id == target_user_id
        )
        result = await db.execute(stmt)
        existing_follow = result.scalars().first()
        
        if existing_follow:
            logger.info(f"Follow relationship already exists in PostgreSQL")
        else:
            # Create new follow relationship
            logger.info(f"Creating new follow relationship in PostgreSQL")
            follow = Follow(
                follower_id=user_id,
                followed_id=target_user_id
            )
            db.add(follow)
            await db.commit()
            logger.info(f"Follow relationship created in PostgreSQL")
        
        # Create FOLLOWS relationship in Neo4j
        logger.info(f"Creating follow relationship in Neo4j")
        async with neo4j_driver.session() as session:
            try:
                result = await session.run(
                    """
                    MATCH (u1:User {id: $user_id}), (u2:User {id: $target_user_id})
                    MERGE (u1)-[r:FOLLOWS]->(u2)
                    RETURN r
                    """,
                    user_id=str(user_id),  # Convert ID to string for Neo4j
                    target_user_id=str(target_user_id),  # Convert ID to string for Neo4j
                )
                
                # Check if relationship was created
                record = await result.single()
                success = record is not None
                
                if success:
                    logger.info(f"Follow relationship created in Neo4j")
                else:
                    logger.error(f"Failed to create follow relationship in Neo4j")
                
                return success
            except Exception as e:
                logger.error(f"Neo4j error: {str(e)}")
                return False
    except Exception as e:
        logger.error(f"Error in follow_user: {str(e)}")
        return False


async def get_followers(neo4j_driver, user_id: int) -> List[Dict[str, Any]]:
    """
    Get all followers of a user from Neo4j.
    """
    async with neo4j_driver.session() as session:
        result = await session.run(
            """
            MATCH (follower:User)-[:FOLLOWS]->(u:User {id: $user_id})
            RETURN follower
            """,
            user_id=str(user_id),  # Convert ID to string for Neo4j
        )
        
        followers = []
        async for record in result:
            follower = record["follower"]
            followers.append({
                "id": follower["id"],  # This will be a string from Neo4j
                "name": follower["name"],
                "email": follower["email"],
            })
        
        return followers


async def create_users_batch(db: AsyncSession, neo4j_driver, users: List[UserCreate]) -> List[Dict[str, Any]]:
    """Create multiple users in a single transaction."""
    # Create users in PostgreSQL
    db_users = []
    for user_create in users:
        hashed_password = get_password_hash(user_create.password)
        db_user = User(
            name=user_create.name,
            email=user_create.email,
            hashed_password=hashed_password,
        )
        db_users.append(db_user)
    
    db.add_all(db_users)
    await db.commit()
    
    # Create users in Neo4j - use a more efficient approach for large batches
    async with neo4j_driver.session() as session:
        # Prepare batch parameters for a single Cypher query
        batch_params = []
        for user in db_users:
            batch_params.append({
                "id": str(user.id),
                "name": user.name,
                "email": user.email
            })
        
        # Execute a single parameterized query for all users
        if batch_params:
            tx = await session.begin_transaction()
            try:
                # Use UNWIND for batch processing - much more efficient for large batches
                await tx.run(
                    """
                    UNWIND $batch AS user
                    MERGE (u:User {id: user.id})
                    SET u.name = user.name, u.email = user.email
                    """,
                    batch=batch_params
                )
                await tx.commit()
            except Exception as e:
                await tx.rollback()
                raise e
    
    # Return created users
    return [
        {"id": user.id, "name": user.name, "email": user.email}
        for user in db_users
    ] 