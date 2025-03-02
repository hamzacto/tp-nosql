import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from neo4j import AsyncGraphDatabase

from app.core.config import settings
from app.db.base import Base

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL setup
engine = create_async_engine(str(settings.DATABASE_URL), echo=True)
async_session = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Neo4j setup
neo4j_driver = AsyncGraphDatabase.driver(
    settings.NEO4J_URI,
    auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
)

async def init_db() -> None:
    """Initialize databases and create tables if they don't exist."""
    try:
        # Create PostgreSQL tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
            # Create additional PostgreSQL indexes for better performance
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_name ON users (name)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)"))
            
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_products_name ON products (name)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_products_category ON products (category)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_products_price ON products (price)"))
            
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_purchases_user_id ON purchases (user_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_purchases_product_id ON purchases (product_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_purchases_created_at ON purchases (created_at)"))
            
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_follows_follower_id ON follows (follower_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_follows_followed_id ON follows (followed_id)"))
            
        logger.info("PostgreSQL tables and indexes created successfully")
        
        # Initialize Neo4j constraints and indexes
        async with neo4j_driver.session() as session:
            # Create constraints for User nodes
            await session.run(
                "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE"
            )
            # Create constraints for Product nodes
            await session.run(
                "CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE"
            )
            
            # Create additional Neo4j indexes for better performance
            await session.run("CREATE INDEX user_name_idx IF NOT EXISTS FOR (u:User) ON (u.name)")
            await session.run("CREATE INDEX user_email_idx IF NOT EXISTS FOR (u:User) ON (u.email)")
            
            await session.run("CREATE INDEX product_name_idx IF NOT EXISTS FOR (p:Product) ON (p.name)")
            await session.run("CREATE INDEX product_category_idx IF NOT EXISTS FOR (p:Product) ON (p.category)")
            await session.run("CREATE INDEX product_price_idx IF NOT EXISTS FOR (p:Product) ON (p.price)")
            
            # Create indexes for relationships - using correct Neo4j syntax for current version
            # For Neo4j 4.4+, we need to specify what to index on the relationship
            await session.run("CREATE INDEX follows_rel_idx IF NOT EXISTS FOR ()-[r:FOLLOWS]->() ON (r.timestamp)")
            await session.run("CREATE INDEX bought_rel_idx IF NOT EXISTS FOR ()-[r:BOUGHT]->() ON (r.purchase_id)")
            
            logger.info("Neo4j constraints and indexes created successfully")
    except Exception as e:
        logger.error(f"Error initializing databases: {e}")
        raise

async def get_db() -> AsyncSession:
    """Dependency for getting async database session."""
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()

async def get_neo4j_driver():
    """Dependency for getting Neo4j driver."""
    try:
        yield neo4j_driver
    finally:
        # Driver is closed when application shuts down
        pass 