import asyncio
import os

# Make sure we can import from our app
os.chdir('app')
print("Changed directory to:", os.getcwd())

from utils.benchmark import DatabaseBenchmark
from database import get_db, get_neo4j_driver

async def test():
    print("Starting benchmark test...")
    async for db in get_db():
        print("Connected to database")
        benchmark = DatabaseBenchmark(db, get_neo4j_driver())
        print("Created benchmark instance")
        # Run just the recommendation_queries benchmark
        await benchmark.run_benchmark('recommendation_queries')
        return

if __name__ == "__main__":
    asyncio.run(test()) 