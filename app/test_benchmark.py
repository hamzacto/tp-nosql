import asyncio
import sys
import os

# Add the current directory to the path so we can import from app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import app
from app.utils.benchmark import DatabaseBenchmark
from app.database import get_db, get_neo4j_driver

async def test():
    # Test our changes with one benchmark
    async for db in get_db():
        benchmark = DatabaseBenchmark(db, get_neo4j_driver())
        await benchmark.run_benchmark('recommendation_queries')
        print("Benchmark complete! Data metadata:")
        print(benchmark.data_metadata)
        return

if __name__ == "__main__":
    asyncio.run(test()) 