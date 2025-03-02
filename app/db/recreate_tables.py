import asyncio
import logging
from sqlalchemy.ext.asyncio import create_async_engine
from app.core.config import settings
from app.db.base import Base

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def recreate_tables():
    """Drop and recreate all tables in the database."""
    try:
        # Create engine
        engine = create_async_engine(str(settings.DATABASE_URL), echo=True)
        
        # Drop all tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            logger.info("All tables dropped successfully")
        
        # Create all tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            logger.info("All tables created successfully")
        
        # Close engine
        await engine.dispose()
        
    except Exception as e:
        logger.error(f"Error recreating tables: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(recreate_tables()) 