import asyncio
import argparse
import logging
import sys
import os
import time
from typing import Dict, Any, List

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.init_db import get_db, get_neo4j_driver
from app.utils.data_generator import DataGenerator
from app.utils.performance_tester import PerformanceTester
from app.services import comparative_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("app.log")
    ]
)
logger = logging.getLogger(__name__)


async def generate_data(args):
    """Generate test data."""
    logger.info("Starting data generation...")
    
    # Get database connections
    db = None
    neo4j_driver = None
    
    try:
        # Get database connections
        db_gen = get_db()
        db = await db_gen.__anext__()
        
        neo4j_gen = get_neo4j_driver()
        neo4j_driver = await neo4j_gen.__anext__()
        
        # Create data generator
        generator = DataGenerator(db, neo4j_driver)
        
        # Generate data
        start_time = time.time()
        
        if args.users > 0:
            await generator.generate_users(args.users)
        
        if args.products > 0:
            await generator.generate_products(args.products)
        
        if args.follows > 0:
            await generator.generate_follows(args.follows)
        
        if args.purchases > 0:
            await generator.generate_purchases(args.purchases)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info(f"Data generation completed in {total_time:.2f} seconds")
    
    except Exception as e:
        logger.error(f"Error generating data: {e}")
        raise
    
    finally:
        # Close database connections
        if db:
            await db_gen.__anext__()
        
        if neo4j_driver:
            await neo4j_gen.__anext__()


async def run_performance_tests(args):
    """Run performance tests."""
    logger.info("Starting performance tests...")
    
    # Get database connections
    db = None
    neo4j_driver = None
    
    try:
        # Get database connections
        db_gen = get_db()
        db = await db_gen.__anext__()
        
        neo4j_gen = get_neo4j_driver()
        neo4j_driver = await neo4j_gen.__anext__()
        
        # Create performance tester
        tester = PerformanceTester(db, neo4j_driver)
        
        # Run tests
        if args.test_type == "all" or args.test_type == "product_virality":
            # Test product virality
            for level in range(1, args.max_level + 1):
                await tester.run_comparative_test(
                    f"Product Virality (Level {level})",
                    lambda: comparative_service.pg_get_product_virality(db, args.product_id, level),
                    lambda: comparative_service.neo4j_get_product_virality(neo4j_driver, args.product_id, level),
                    args.iterations
                )
        
        if args.test_type == "all" or args.test_type == "user_influence":
            # Test user influence
            for level in range(1, args.max_level + 1):
                await tester.run_comparative_test(
                    f"User Influence (Level {level})",
                    lambda: comparative_service.pg_get_user_influence(db, args.user_id, level),
                    lambda: comparative_service.neo4j_get_user_influence(neo4j_driver, args.user_id, level),
                    args.iterations
                )
        
        if args.test_type == "all" or args.test_type == "viral_products":
            # Test viral products
            for level in range(1, args.max_level + 1):
                await tester.run_comparative_test(
                    f"Viral Products (Level {level})",
                    lambda: comparative_service.pg_get_viral_products(db, level),
                    lambda: comparative_service.neo4j_get_viral_products(neo4j_driver, level),
                    args.iterations
                )
        
        # Print results
        tester.print_results()
        
        # Export results to CSV
        if args.output:
            tester.export_results_to_csv(args.output)
    
    except Exception as e:
        logger.error(f"Error running performance tests: {e}")
        raise
    
    finally:
        # Close database connections
        if db:
            await db_gen.__anext__()
        
        if neo4j_driver:
            await neo4j_gen.__anext__()


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Social Network Purchase Analysis CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Generate data command
    generate_parser = subparsers.add_parser(
        "generate", help="Generate test data"
    )
    generate_parser.add_argument(
        "--users", type=int, default=1000,
        help="Number of users to generate (default: 1000)"
    )
    generate_parser.add_argument(
        "--products", type=int, default=100,
        help="Number of products to generate (default: 100)"
    )
    generate_parser.add_argument(
        "--follows", type=int, default=20,
        help="Maximum number of follows per user (default: 20)"
    )
    generate_parser.add_argument(
        "--purchases", type=int, default=5,
        help="Maximum number of purchases per user (default: 5)"
    )
    
    # Performance test command
    test_parser = subparsers.add_parser(
        "test", help="Run performance tests"
    )
    test_parser.add_argument(
        "--test-type", type=str, default="all",
        choices=["all", "product_virality", "user_influence", "viral_products"],
        help="Type of test to run (default: all)"
    )
    test_parser.add_argument(
        "--product-id", type=int, default=1,
        help="Product ID for product virality test (default: 1)"
    )
    test_parser.add_argument(
        "--user-id", type=int, default=1,
        help="User ID for user influence test (default: 1)"
    )
    test_parser.add_argument(
        "--max-level", type=int, default=3,
        help="Maximum level for tests (default: 3)"
    )
    test_parser.add_argument(
        "--iterations", type=int, default=5,
        help="Number of iterations for each test (default: 5)"
    )
    test_parser.add_argument(
        "--output", type=str, default="performance_results.csv",
        help="Output file for test results (default: performance_results.csv)"
    )
    
    args = parser.parse_args()
    
    if args.command == "generate":
        asyncio.run(generate_data(args))
    elif args.command == "test":
        asyncio.run(run_performance_tests(args))
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 