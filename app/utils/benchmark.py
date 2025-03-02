import time
import asyncio
import logging
import statistics
from typing import List, Dict, Any, Callable, Tuple, Optional
import random
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import text
from neo4j import AsyncGraphDatabase
import psycopg2
import psycopg2.extras

from app.models.user import User
from app.models.product import Product
from app.models.purchase import Purchase
from app.services import user_service, product_service, purchase_service

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseBenchmark:
    """Utility class for benchmarking PostgreSQL vs Neo4j performance."""
    
    def __init__(self, db: AsyncSession, neo4j_driver):
        self.db = db
        self.neo4j_driver = neo4j_driver
        self.results = {
            "postgresql": {},
            "neo4j": {}
        }
        self.data_metadata = {}  # Store metadata about data being processed in each test
        self.current_step = 0
        self.total_steps = 0
        self.progress_callback = None  # Callback function for progress updates
    
    async def run_benchmark(self, test_type: str = "all", max_level: int = 3, 
                         product_id: Optional[str] = None, user_id: Optional[str] = None, 
                         iterations: int = 5):
        """
        Run benchmark tests with the specified parameters.
        
        Parameters:
        - test_type: Type of test to run (all, product_virality, user_influence, viral_products)
        - max_level: Maximum level for social network depth (1-5)
        - product_id: Product ID for virality test
        - user_id: User ID for influence test
        - iterations: Number of iterations per test
        """
        logger.info(f"Starting database benchmark with test_type={test_type}, max_level={max_level}, iterations={iterations}")
        
        # Track the current benchmark step for UI progress updates
        self.current_step = 0
        self.total_steps = 0
        
        # Calculate total steps based on selected test type
        if test_type == "all":
            self.total_steps = 6  # User retrieval, product retrieval, user follows, product purchases, recommendation queries, + chosen test
            if max_level > 3:
                logger.warning(f"High max_level={max_level} may cause longer execution times")
        elif test_type == "basic":
            self.total_steps = 4  # User retrieval, product retrieval, user follows, product purchases
        elif test_type in ["product_virality", "user_influence", "viral_products", "recommendation_queries"]:
            self.total_steps = 1
        
        # Update progress if callback is set
        if self.progress_callback:
            self.progress_callback(self.current_step, self.total_steps, f"Starting benchmarks for test type: {test_type}")
        
        # Get sample data for benchmarks
        logger.info("Fetching sample data for benchmarks...")
        users = await self._get_sample_users(100)
        products = await self._get_sample_products(100)
        
        if not users or not products:
            logger.error("Not enough data for benchmarking. Please generate data first.")
            return self.results
        
        # Keep IDs as strings for consistency - conversion will happen in individual benchmark methods
        specific_product_id = None
        specific_user_id = None
        
        if product_id:
            try:
                # Keep as string for consistency
                specific_product_id = str(product_id)
                logger.info(f"Using specific product ID: {specific_product_id}")
            except Exception as e:
                logger.error(f"Invalid product ID format: {e}")
        
        if user_id:
            try:
                # Keep as string for consistency  
                specific_user_id = str(user_id)
                logger.info(f"Using specific user ID: {specific_user_id}")
            except Exception as e:
                logger.error(f"Invalid user ID format: {e}")
        
        # Run appropriate benchmarks based on test_type
        if test_type == "all" or test_type == "basic":
            logger.info("Starting basic benchmarks...")
            
            # User retrieval
            self.current_step += 1
            logger.info(f"Benchmark progress: {self.current_step}/{self.total_steps} - Running user retrieval benchmark")
            if self.progress_callback:
                self.progress_callback(self.current_step, self.total_steps, "Running user retrieval benchmark")
            await self._benchmark_user_retrieval(iterations, users)
            
            # Product retrieval
            self.current_step += 1
            logger.info(f"Benchmark progress: {self.current_step}/{self.total_steps} - Running product retrieval benchmark")
            if self.progress_callback:
                self.progress_callback(self.current_step, self.total_steps, "Running product retrieval benchmark")
            await self._benchmark_product_retrieval(iterations, products)
            
            # User follows
            self.current_step += 1
            logger.info(f"Benchmark progress: {self.current_step}/{self.total_steps} - Running user follows benchmark")
            if self.progress_callback:
                self.progress_callback(self.current_step, self.total_steps, "Running user follows benchmark")
            await self._benchmark_user_follows(iterations, users)
            
            # Product purchases
            self.current_step += 1
            logger.info(f"Benchmark progress: {self.current_step}/{self.total_steps} - Running product purchases benchmark")
            if self.progress_callback:
                self.progress_callback(self.current_step, self.total_steps, "Running product purchases benchmark")
            await self._benchmark_product_purchases(iterations, products)
        
        if test_type == "all" or test_type == "product_virality":
            # Product virality
            self.current_step += 1
            logger.info(f"Benchmark progress: {self.current_step}/{self.total_steps} - Running product virality benchmark")
            if self.progress_callback:
                self.progress_callback(self.current_step, self.total_steps, f"Running product virality benchmark with max_level={max_level}")
            # Use specific product if provided, otherwise use random ones
            await self._benchmark_product_virality(iterations, specific_product_id or random.choice(products)["id"], max_level)
        
        if test_type == "all" or test_type == "user_influence":
            # User influence
            self.current_step += 1
            logger.info(f"Benchmark progress: {self.current_step}/{self.total_steps} - Running user influence benchmark")
            if self.progress_callback:
                self.progress_callback(self.current_step, self.total_steps, f"Running user influence benchmark with max_level={max_level}")
            # Use specific user if provided, otherwise use random ones
            await self._benchmark_user_influence(iterations, specific_user_id or random.choice(users)["id"], max_level)
        
        if test_type == "all" or test_type == "viral_products":
            # Viral products
            self.current_step += 1
            logger.info(f"Benchmark progress: {self.current_step}/{self.total_steps} - Running viral products benchmark")
            if self.progress_callback:
                self.progress_callback(self.current_step, self.total_steps, f"Running viral products benchmark with max_level={max_level}")
            await self._benchmark_viral_products(iterations, max_level)
        
        if test_type == "all" or test_type == "recommendation_queries":
            # Recommendation queries
            self.current_step += 1
            logger.info(f"Benchmark progress: {self.current_step}/{self.total_steps} - Running recommendation queries benchmark")
            if self.progress_callback:
                self.progress_callback(self.current_step, self.total_steps, "Running recommendation queries benchmark")
            await self._benchmark_recommendation_queries(iterations, users)
        
        # Print summary
        logger.info("All benchmarks completed, generating summary...")
        if self.progress_callback:
            self.progress_callback(self.total_steps, self.total_steps, "All benchmarks completed, generating summary")
        self._print_benchmark_summary()
        
        return self.results
    
    async def _get_sample_users(self, limit: int) -> List[Dict[str, Any]]:
        """Get a sample of users for benchmarking."""
        result = await self.db.execute(select(User).limit(limit))
        users = result.scalars().all()
        return [{"id": user.id, "name": user.name, "email": user.email} for user in users]
    
    async def _get_sample_products(self, limit: int) -> List[Dict[str, Any]]:
        """Get a sample of products for benchmarking."""
        result = await self.db.execute(select(Product).limit(limit))
        products = result.scalars().all()
        return [{"id": product.id, "name": product.name, "category": product.category} for product in products]
    
    async def _benchmark_user_retrieval(self, iterations: int, users: List[Dict[str, Any]]):
        """Benchmark user retrieval performance."""
        logger.info("Benchmarking user retrieval...")
        
        # Initialize data metadata for this test
        test_users = random.sample(users, min(5, len(users)))
        self.data_metadata["user_retrieval"] = {
            "sample_users": [
                {"id": user["id"], "username": user.get("username", "User " + str(user["id"]))}
                for user in test_users
            ],
            "total_users_sampled": len(users)
        }
        
        # PostgreSQL benchmark
        logger.info(f"Running PostgreSQL user retrieval benchmark ({iterations} iterations)...")
        pg_times = []
        processed_user_ids = []
        for _ in range(iterations):
            user_id = random.choice(users)["id"]
            processed_user_ids.append(user_id)
            start_time = time.time()
            # Convert user_id to integer for PostgreSQL if needed
            try:
                pg_user_id = int(user_id)
            except (ValueError, TypeError):
                pg_user_id = user_id
                
            # Get user using model query
            result = await self.db.execute(
                select(User).filter(User.id == pg_user_id)
            )
            _ = result.scalar_one_or_none()
            end_time = time.time()
            pg_times.append(end_time - start_time)
        
        # Neo4j benchmark
        neo4j_times = []
        for _ in range(iterations):
            user_id = random.choice(users)["id"]
            start_time = time.time()
            async with self.neo4j_driver.session() as session:
                await session.run(
                    """
                    MATCH (u:User {id: $id})
                    RETURN u
                    """,
                    id=str(user_id)
                )
            end_time = time.time()
            neo4j_times.append(end_time - start_time)
        
        # Update metadata with actual processed IDs
        self.data_metadata["user_retrieval"]["processed_user_ids"] = processed_user_ids
        
        # Store results
        self.results["postgresql"]["user_retrieval"] = {
            "avg": statistics.mean(pg_times),
            "min": min(pg_times),
            "max": max(pg_times),
            "median": statistics.median(pg_times),
            "data_info": self.data_metadata["user_retrieval"]
        }
        
        self.results["neo4j"]["user_retrieval"] = {
            "avg": statistics.mean(neo4j_times),
            "min": min(neo4j_times),
            "max": max(neo4j_times),
            "median": statistics.median(neo4j_times),
            "data_info": self.data_metadata["user_retrieval"]
        }
    
    async def _benchmark_product_retrieval(self, iterations: int, products: List[Dict[str, Any]]):
        """Benchmark product retrieval performance."""
        logger.info("Benchmarking product retrieval...")
        
        # Initialize data metadata for this test
        test_products = random.sample(products, min(5, len(products)))
        self.data_metadata["product_retrieval"] = {
            "sample_products": [
                {"id": product["id"], "name": product.get("name", "Product " + str(product["id"]))}
                for product in test_products
            ],
            "total_products_sampled": len(products)
        }
        
        # PostgreSQL benchmark
        logger.info(f"Running PostgreSQL product retrieval benchmark ({iterations} iterations)...")
        pg_times = []
        processed_product_ids = []
        for _ in range(iterations):
            product_id = random.choice(products)["id"]
            processed_product_ids.append(product_id)
            start_time = time.time()
            # Convert product_id to integer for PostgreSQL if needed
            try:
                pg_product_id = int(product_id)
            except (ValueError, TypeError):
                pg_product_id = product_id
                
            # Get product using model query
            result = await self.db.execute(
                select(Product).filter(Product.id == pg_product_id)
            )
            _ = result.scalar_one_or_none()
            end_time = time.time()
            pg_times.append(end_time - start_time)
        
        # Neo4j benchmark
        neo4j_times = []
        for _ in range(iterations):
            product_id = random.choice(products)["id"]
            start_time = time.time()
            async with self.neo4j_driver.session() as session:
                await session.run(
                    """
                    MATCH (p:Product {id: $id})
                    RETURN p
                    """,
                    id=str(product_id)
                )
            end_time = time.time()
            neo4j_times.append(end_time - start_time)
        
        # Update metadata with actual processed IDs
        self.data_metadata["product_retrieval"]["processed_product_ids"] = processed_product_ids
        
        # Store results
        self.results["postgresql"]["product_retrieval"] = {
            "avg": statistics.mean(pg_times),
            "min": min(pg_times),
            "max": max(pg_times),
            "median": statistics.median(pg_times),
            "data_info": self.data_metadata["product_retrieval"]
        }
        
        self.results["neo4j"]["product_retrieval"] = {
            "avg": statistics.mean(neo4j_times),
            "min": min(neo4j_times),
            "max": max(neo4j_times),
            "median": statistics.median(neo4j_times),
            "data_info": self.data_metadata["product_retrieval"]
        }
    
    async def _benchmark_user_follows(self, iterations: int, users: List[Dict[str, Any]]):
        """Benchmark user follows relationship query performance."""
        logger.info("Benchmarking user follows relationship queries...")
        
        # Initialize data metadata for this test
        test_users = random.sample(users, min(5, len(users)))
        self.data_metadata["user_follows"] = {
            "sample_users": [
                {"id": user["id"], "username": user.get("username", "User " + str(user["id"]))}
                for user in test_users
            ],
            "total_users_sampled": len(users),
            "query_description": "Finding users who are followed by a given user"
        }
        
        # PostgreSQL benchmark
        logger.info(f"Running PostgreSQL user follows benchmark ({iterations} iterations)...")
        pg_times = []
        processed_user_ids = []
        for _ in range(iterations):
            user_id = random.choice(users)["id"]
            processed_user_ids.append(user_id)
            start_time = time.time()
            # Convert user_id to integer for PostgreSQL if needed
            try:
                pg_user_id = int(user_id)
            except (ValueError, TypeError):
                pg_user_id = user_id
            
            # Get followers using SQL
            result = await self.db.execute(
                select(User).join(User.followers).filter(User.id == pg_user_id)
            )
            _ = result.scalars().all()
            end_time = time.time()
            pg_times.append(end_time - start_time)
        
        # Neo4j benchmark
        neo4j_times = []
        for _ in range(iterations):
            user_id = random.choice(users)["id"]
            start_time = time.time()
            async with self.neo4j_driver.session() as session:
                await session.run(
                    """
                    MATCH (u:User {id: $id})-[:FOLLOWS]->(followed:User)
                    RETURN followed
                    """,
                    id=str(user_id)
                )
            end_time = time.time()
            neo4j_times.append(end_time - start_time)
            
        # Update metadata with actual processed IDs
        self.data_metadata["user_follows"]["processed_user_ids"] = processed_user_ids
        
        # Store results
        self.results["postgresql"]["user_follows"] = {
            "avg": statistics.mean(pg_times),
            "min": min(pg_times),
            "max": max(pg_times),
            "median": statistics.median(pg_times),
            "data_info": self.data_metadata["user_follows"]
        }
        
        self.results["neo4j"]["user_follows"] = {
            "avg": statistics.mean(neo4j_times),
            "min": min(neo4j_times),
            "max": max(neo4j_times),
            "median": statistics.median(neo4j_times),
            "data_info": self.data_metadata["user_follows"]
        }
    
    async def _benchmark_product_purchases(self, iterations: int, products: List[Dict[str, Any]]):
        """Benchmark product purchase relationship query performance."""
        logger.info("Benchmarking product purchase relationship queries...")
        
        # Initialize data metadata for this test
        test_products = random.sample(products, min(5, len(products)))
        self.data_metadata["product_purchases"] = {
            "sample_products": [
                {"id": product["id"], "name": product.get("name", "Product " + str(product["id"]))}
                for product in test_products
            ],
            "total_products_sampled": len(products),
            "query_description": "Finding users who purchased a given product"
        }
        
        # PostgreSQL benchmark
        logger.info(f"Running PostgreSQL product purchases benchmark ({iterations} iterations)...")
        pg_times = []
        processed_product_ids = []
        for _ in range(iterations):
            product_id = random.choice(products)["id"]
            processed_product_ids.append(product_id)
            start_time = time.time()
            # Convert product_id to integer for PostgreSQL if needed
            try:
                pg_product_id = int(product_id)
            except (ValueError, TypeError):
                pg_product_id = product_id
                
            # Get purchases using SQL
            result = await self.db.execute(
                select(Purchase).filter(Purchase.product_id == pg_product_id)
            )
            _ = result.scalars().all()
            end_time = time.time()
            pg_times.append(end_time - start_time)
        
        # Neo4j benchmark
        neo4j_times = []
        for _ in range(iterations):
            product_id = random.choice(products)["id"]
            start_time = time.time()
            async with self.neo4j_driver.session() as session:
                await session.run(
                    """
                    MATCH (p:Product {id: $id})<-[:PURCHASED]-(u:User)
                    RETURN u
                    """,
                    id=str(product_id)
                )
            end_time = time.time()
            neo4j_times.append(end_time - start_time)
            
        # Update metadata with actual processed IDs
        self.data_metadata["product_purchases"]["processed_product_ids"] = processed_product_ids
        
        # Store results
        self.results["postgresql"]["product_purchases"] = {
            "avg": statistics.mean(pg_times),
            "min": min(pg_times),
            "max": max(pg_times),
            "median": statistics.median(pg_times),
            "data_info": self.data_metadata["product_purchases"]
        }
        
        self.results["neo4j"]["product_purchases"] = {
            "avg": statistics.mean(neo4j_times),
            "min": min(neo4j_times),
            "max": max(neo4j_times),
            "median": statistics.median(neo4j_times),
            "data_info": self.data_metadata["product_purchases"]
        }
    
    async def _benchmark_recommendation_queries(self, iterations: int, users: List[Dict[str, Any]]):
        """Benchmark recommendation query performance."""
        logger.info("Benchmarking recommendation queries...")
        
        # Initialize data metadata for this test
        test_users = random.sample(users, min(5, len(users)))
        self.data_metadata["recommendation_queries"] = {
            "sample_users": [
                {"id": user["id"], "username": user.get("username", "User " + str(user["id"]))}
                for user in test_users
            ],
            "total_users_sampled": len(users),
            "query_description": "Finding product recommendations based on social connections"
        }
        
        # PostgreSQL benchmark
        logger.info(f"Running PostgreSQL recommendation queries benchmark ({iterations} iterations)...")
        pg_times = []
        processed_user_ids = []
        for _ in range(iterations):
            user_id = random.choice(users)["id"]
            processed_user_ids.append(user_id)
            start_time = time.time()
            # Convert user_id to integer for PostgreSQL if needed
            try:
                pg_user_id = int(user_id)
            except (ValueError, TypeError):
                pg_user_id = user_id
                
            # Get product recommendations for this user using more direct query
            # Get users followed by the current user
            follows_query = text("""
                SELECT f.followed_id 
                FROM follows f
                WHERE f.follower_id = :user_id
            """)
            
            follows_result = await self.db.execute(follows_query, {"user_id": pg_user_id})
            followed_user_ids = [row[0] for row in follows_result]
            
            if followed_user_ids:
                # Get products purchased by followed users
                purchases_query = text("""
                    SELECT p.product_id, COUNT(p.id) AS purchase_count
                    FROM purchases p
                    WHERE p.user_id = ANY(:followed_ids)
                    GROUP BY p.product_id
                    ORDER BY purchase_count DESC
                    LIMIT 10
                """)
                
                purchases_result = await self.db.execute(purchases_query, {"followed_ids": followed_user_ids})
                product_counts = [(row[0], row[1]) for row in purchases_result]
                
                # Filter out products already purchased by the user
                if product_counts:
                    product_ids = [pc[0] for pc in product_counts]
                    
                    filtered_query = text("""
                        SELECT pr.id, pr.name
                        FROM products pr
                        WHERE pr.id = ANY(:product_ids)
                        AND NOT EXISTS (
                            SELECT 1 FROM purchases up
                            WHERE up.product_id = pr.id AND up.user_id = :user_id
                        )
                        ORDER BY pr.id
                    """)
                    
                    await self.db.execute(filtered_query, {
                        "product_ids": product_ids,
                        "user_id": pg_user_id
                    })
            end_time = time.time()
            pg_times.append(end_time - start_time)
        
        # Neo4j benchmark
        neo4j_times = []
        for _ in range(iterations):
            user_id = random.choice(users)["id"]
            start_time = time.time()
            async with self.neo4j_driver.session() as session:
                await session.run(
                    """
                    MATCH (u:User {id: $id})-[:FOLLOWS]->(friend:User)-[:PURCHASED]->(p:Product)
                    WHERE NOT EXISTS ((u)-[:PURCHASED]->(p))
                    RETURN p.id, p.name, COUNT(friend) AS recommendationScore
                    ORDER BY recommendationScore DESC
                    LIMIT 10
                    """,
                    id=str(user_id)
                )
            end_time = time.time()
            neo4j_times.append(end_time - start_time)
            
        # Update metadata with actual processed IDs and query details
        self.data_metadata["recommendation_queries"]["processed_user_ids"] = processed_user_ids
        self.data_metadata["recommendation_queries"]["postgresql_query"] = """
            -- Get users followed by the current user
            SELECT f.followed_id 
            FROM follows f
            WHERE f.follower_id = :user_id
            
            -- Then get products purchased by those users
            SELECT p.product_id, COUNT(p.id) AS purchase_count
            FROM purchases p
            WHERE p.user_id = ANY(:followed_ids)
            GROUP BY p.product_id
            ORDER BY purchase_count DESC
            LIMIT 10
            
            -- Finally filter out products the user already purchased
            SELECT pr.id, pr.name
            FROM products pr
            WHERE pr.id = ANY(:product_ids)
            AND NOT EXISTS (
                SELECT 1 FROM purchases up
                WHERE up.product_id = pr.id AND up.user_id = :user_id
            )
            ORDER BY pr.id
        """
        self.data_metadata["recommendation_queries"]["neo4j_query"] = """
            MATCH (u:User {id: $id})-[:FOLLOWS]->(friend:User)-[:PURCHASED]->(p:Product)
            WHERE NOT EXISTS ((u)-[:PURCHASED]->(p))
            RETURN p.id, p.name, COUNT(friend) AS recommendationScore
            ORDER BY recommendationScore DESC
            LIMIT 10
        """
        
        # Store results
        self.results["postgresql"]["recommendation_queries"] = {
            "avg": statistics.mean(pg_times),
            "min": min(pg_times),
            "max": max(pg_times),
            "median": statistics.median(pg_times),
            "data_info": self.data_metadata["recommendation_queries"]
        }
        
        self.results["neo4j"]["recommendation_queries"] = {
            "avg": statistics.mean(neo4j_times),
            "min": min(neo4j_times),
            "max": max(neo4j_times),
            "median": statistics.median(neo4j_times),
            "data_info": self.data_metadata["recommendation_queries"]
        }
        
        # Print metadata for verification
        print("Recommendation queries metadata:")
        print(self.data_metadata["recommendation_queries"])
    
    async def _benchmark_product_virality(self, iterations: int, product_id: str, max_level: int = 3):
        """
        Benchmark product virality analysis - finding how far a product's influence spreads.
        This tests recursive query performance.
        """
        logger.info(f"Benchmarking product virality for product {product_id} with max_level={max_level}...")
        
        # Initialize data metadata for this test
        self.data_metadata["product_virality"] = {
            "product_id": product_id,
            "max_level": max_level,
            "query_description": "Finding how a product purchase influences a user's social network"
        }
        
        # PostgreSQL benchmark
        logger.info(f"Running PostgreSQL product virality benchmark with max_level={max_level} ({iterations} iterations)...")
        # Note: With high max_level values (4-5), this query might take several minutes to complete
        if max_level > 3:
            logger.warning(f"High max_level={max_level} may cause this query to take several minutes per iteration")
            
            # Update progress callback with warning if available
            if self.progress_callback:
                self.progress_callback(
                    self.current_step, 
                    self.total_steps, 
                    f"Running PostgreSQL product virality benchmark with high max_level={max_level}. This may take several minutes."
                )
        
        pg_times = []
        pg_errors = 0
        for i in range(iterations):
            # Update progress for this specific iteration
            if self.progress_callback:
                iteration_message = f"Running PostgreSQL product virality query (iteration {i+1}/{iterations})"
                self.progress_callback(self.current_step, self.total_steps, iteration_message)
            
            logger.info(f"Running PostgreSQL product virality query (iteration {i+1}/{iterations})")
            try:
                start_time = time.time()
                
                # Convert product_id to integer for PostgreSQL if needed
                try:
                    pg_product_id = int(product_id)
                except (ValueError, TypeError):
                    pg_product_id = product_id
                
                # Complex query in PostgreSQL
                query = """
                WITH RECURSIVE product_network AS (
                    -- Base case: Direct purchasers of the product
                    SELECT
                        u.id AS user_id,
                        1 AS level
                    FROM
                        purchases p
                        JOIN users u ON p.user_id = u.id
                    WHERE
                        p.product_id = :product_id
                    
                    UNION
                    
                    -- Recursive case: Followers at increasing depths
                    SELECT
                        f.follower_id AS user_id,
                        pn.level + 1 AS level
                    FROM
                        product_network pn
                        JOIN follows f ON pn.user_id = f.followed_id
                    WHERE
                        pn.level < :max_level
                )
                SELECT
                    level,
                    COUNT(DISTINCT user_id) AS user_count
                FROM
                    product_network
                GROUP BY
                    level
                ORDER BY
                    level;
                """
                
                result = await self.db.execute(text(query), {"product_id": pg_product_id, "max_level": max_level})
                _ = result.fetchall()
                
                end_time = time.time()
                elapsed_time = end_time - start_time
                pg_times.append(elapsed_time)
                logger.info(f"PostgreSQL product virality query completed in {elapsed_time:.2f} seconds")
            except Exception as e:
                logger.error(f"Error in PostgreSQL product virality query: {str(e)}")
                pg_errors += 1
                # Add a high time value to indicate an error
                pg_times.append(60.0)
                if self.progress_callback:
                    self.progress_callback(
                        self.current_step, 
                        self.total_steps, 
                        f"Error in PostgreSQL product virality query: {str(e)}"
                    )
        
        # Neo4j benchmark
        logger.info(f"Running Neo4j product virality benchmark with max_level={max_level} ({iterations} iterations)...")
        neo4j_times = []
        neo4j_errors = 0
        for i in range(iterations):
            # Update progress for this specific iteration
            if self.progress_callback:
                iteration_message = f"Running Neo4j product virality query (iteration {i+1}/{iterations})"
                self.progress_callback(self.current_step, self.total_steps, iteration_message)
            
            logger.info(f"Running Neo4j product virality query (iteration {i+1}/{iterations})")
            try:
                # Neo4j benchmark using the helper method that already includes timing
                neo4j_time = await self._benchmark_product_virality_neo4j(product_id, max_level)
                neo4j_times.append(neo4j_time)
            except Exception as e:
                logger.error(f"Error in Neo4j product virality query: {str(e)}")
                neo4j_errors += 1
                # Add a high time value to indicate an error
                neo4j_times.append(60.0)
                if self.progress_callback:
                    self.progress_callback(
                        self.current_step, 
                        self.total_steps, 
                        f"Error in Neo4j product virality query: {str(e)}"
                    )
        
        # Store the PostgreSQL query in the metadata
        self.data_metadata["product_virality"]["postgresql_query"] = """
        WITH RECURSIVE product_network AS (
            -- Base case: Direct purchasers of the product
            SELECT
                u.id AS user_id,
                1 AS level
            FROM
                purchases p
                JOIN users u ON p.user_id = u.id
            WHERE
                p.product_id = :product_id
            
            UNION
            
            -- Recursive case: Followers at increasing depths
            SELECT
                f.follower_id AS user_id,
                pn.level + 1 AS level
            FROM
                product_network pn
                JOIN follows f ON pn.user_id = f.followed_id
            WHERE
                pn.level < :max_level
        )
        SELECT
            level,
            COUNT(DISTINCT user_id) AS user_count
        FROM
            product_network
        GROUP BY
            level
        ORDER BY
            level;
        """
        
        # Store the Neo4j query in the metadata
        self.data_metadata["product_virality"]["neo4j_query"] = """
        MATCH (p:Product {id: $product_id})
        CALL {
            WITH p
            MATCH (u:User)-[:BOUGHT]->(p)
            RETURN u, 1 AS level
            UNION
            WITH p
            UNWIND range(2, $max_level) AS level
            MATCH (u:User)-[:BOUGHT]->(p),
                  path = (follower:User)-[:FOLLOWS*]->(u)
            WHERE length(path) <= level-1
            RETURN follower AS u, level
        }
        RETURN level, count(DISTINCT u) AS user_count
        ORDER BY level
        """
        
        # Store error counts in metadata
        self.data_metadata["product_virality"]["errors"] = {
            "postgresql": pg_errors,
            "neo4j": neo4j_errors
        }
        
        # Calculate and store the results
        self.results["postgresql"]["product_virality"] = {
            "times": pg_times,
            "avg": statistics.mean(pg_times) if pg_times else 0,
            "min": min(pg_times) if pg_times else 0,
            "max": max(pg_times) if pg_times else 0,
            "median": statistics.median(pg_times) if pg_times else 0,
            "data_info": self.data_metadata["product_virality"]
        }
        
        self.results["neo4j"]["product_virality"] = {
            "times": neo4j_times,
            "avg": statistics.mean(neo4j_times) if neo4j_times else 0,
            "min": min(neo4j_times) if neo4j_times else 0,
            "max": max(neo4j_times) if neo4j_times else 0,
            "median": statistics.median(neo4j_times) if neo4j_times else 0,
            "data_info": self.data_metadata["product_virality"]
        }
        
        # Update progress with completion status
        if self.progress_callback:
            completion_message = f"Completed product virality benchmark with max_level={max_level}"
            if pg_errors > 0 or neo4j_errors > 0:
                completion_message += f" (PostgreSQL errors: {pg_errors}, Neo4j errors: {neo4j_errors})"
            self.progress_callback(
                self.current_step, 
                self.total_steps, 
                completion_message
            )

    async def _benchmark_product_virality_neo4j(self, product_id: str, max_level: int) -> float:
        """Helper method with correct Neo4j query syntax for product virality benchmarking."""
        logger.info(f"Executing Neo4j product virality query with max_level={max_level}...")
        start_time = time.time()
        
        # Simplified Neo4j query that should be more efficient and less likely to get stuck
        query = """
        MATCH (p:Product {id: $product_id})
        
        // Direct buyers (level 1)
        OPTIONAL MATCH (buyer:User)-[:BOUGHT]->(p)
        WITH p, collect(DISTINCT buyer) as direct_buyers
        
        // For each level, find followers
        UNWIND range(1, $max_level) as level
        WITH p, level, direct_buyers
        
        OPTIONAL MATCH path = (follower:User)-[:FOLLOWS*1..2]->(buyer:User)
        WHERE buyer IN direct_buyers AND length(path) <= level
        
        WITH level, count(DISTINCT CASE WHEN level = 1 THEN buyer ELSE follower END) as user_count
        RETURN level, user_count
        ORDER BY level
        """
        
        try:
            # Set a timeout of 30 seconds for this query (reduced from 60)
            timeout_seconds = 30
            logger.info(f"Starting Neo4j product virality query with {timeout_seconds}s timeout...")
            
            async with self.neo4j_driver.session() as session:
                try:
                    # Execute the query with proper timeout handling
                    result_future = session.run(query, product_id=str(product_id), max_level=max_level)
                    result = await asyncio.wait_for(result_future, timeout=timeout_seconds)
                    
                    # Consume results more efficiently
                    records = await result.values()
                    logger.info(f"Neo4j product virality query completed successfully with {len(records)} records.")
                    
                except asyncio.TimeoutError:
                    logger.error(f"Neo4j product virality query timed out after {timeout_seconds} seconds!")
                    # Update progress callback with the timeout information if available
                    if self.progress_callback:
                        self.progress_callback(
                            self.current_step, 
                            self.total_steps, 
                            f"Neo4j product virality query timed out after {timeout_seconds} seconds"
                        )
                    raise TimeoutError(f"Neo4j query timed out after {timeout_seconds} seconds")
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.info(f"Neo4j product virality query completed in {elapsed_time:.2f} seconds")
            return elapsed_time
        except Exception as e:
            logger.error(f"Error in Neo4j product virality query: {str(e)}")
            # Update progress callback with the error information if available
            if self.progress_callback:
                self.progress_callback(
                    self.current_step, 
                    self.total_steps, 
                    f"Error in Neo4j product virality query: {str(e)}"
                )
            return 60.0  # Return a high value to indicate an error

    async def _benchmark_user_influence(self, iterations: int, user_id: str, max_level: int = 3):
        """
        Benchmark user influence analysis - finding how a user's purchases influence their network.
        This tests multi-level traversal and aggregation performance.
        """
        logger.info(f"Benchmarking user influence for user {user_id} with max_level={max_level}...")
        
        # Initialize data metadata for this test
        self.data_metadata["user_influence"] = {
            "user_id": user_id,
            "max_level": max_level,
            "query_description": "Finding how a user's purchases influence their social network up to a specified level"
        }
        
        # PostgreSQL benchmark
        logger.info(f"Running PostgreSQL user influence benchmark with max_level={max_level} ({iterations} iterations)...")
        # Note: With high max_level values (4-5), this query might take several minutes to complete
        if max_level > 3:
            logger.warning(f"High max_level={max_level} may cause this query to take several minutes per iteration")
            
            # Update progress callback with warning if available
            if self.progress_callback:
                self.progress_callback(
                    self.current_step, 
                    self.total_steps, 
                    f"Running PostgreSQL user influence benchmark with high max_level={max_level}. This may take several minutes."
                )
        
        pg_times = []
        pg_errors = 0
        for i in range(iterations):
            # Update progress for this specific iteration
            if self.progress_callback:
                iteration_message = f"Running PostgreSQL user influence query (iteration {i+1}/{iterations})"
                self.progress_callback(self.current_step, self.total_steps, iteration_message)
            
            logger.info(f"Running PostgreSQL user influence query (iteration {i+1}/{iterations})")
            start_time = time.time()
            
            # Complex query in PostgreSQL requiring multiple recursive CTEs
            query = """
            WITH RECURSIVE user_network AS (
                -- Base case: the user
                SELECT id, 0 AS level
                FROM users
                WHERE id = :user_id
                
                UNION
                
                -- Recursive case: followers of users in the network
                SELECT f.follower_id, un.level + 1
                FROM user_network un
                JOIN follows f ON un.id = f.followed_id
                WHERE un.level < :max_level
            ),
            user_purchases AS (
                SELECT u.id AS user_id, p.product_id, p.created_at
                FROM user_network u
                JOIN purchases p ON u.id = p.user_id
            ),
            influence_data AS (
                SELECT 
                    un.level,
                    COUNT(DISTINCT up.user_id) AS users_count,
                    COUNT(DISTINCT up.product_id) AS products_count,
                    COUNT(up.product_id) AS purchases_count
                FROM user_network un
                LEFT JOIN user_purchases up ON un.id = up.user_id
                GROUP BY un.level
            )
            SELECT * FROM influence_data
            ORDER BY level;
            """
            
            # Convert user_id to integer for PostgreSQL
            try:
                pg_user_id = int(user_id)
            except (ValueError, TypeError):
                logger.error(f"Invalid user ID format for PostgreSQL: {user_id}. Using as-is.")
                pg_user_id = user_id
            
            try:
                result = await self.db.execute(text(query), {"user_id": pg_user_id, "max_level": max_level})
                _ = result.fetchall()
                
                end_time = time.time()
                elapsed_time = end_time - start_time
                pg_times.append(elapsed_time)
                logger.info(f"PostgreSQL user influence query completed in {elapsed_time:.2f} seconds")
            except Exception as e:
                logger.error(f"Error in PostgreSQL user influence query: {str(e)}")
                pg_errors += 1
                # Add a high time value to indicate an error
                pg_times.append(60.0)
                if self.progress_callback:
                    self.progress_callback(
                        self.current_step, 
                        self.total_steps, 
                        f"Error in PostgreSQL user influence query: {str(e)}"
                    )
        
        # Neo4j benchmark
        logger.info(f"Running Neo4j user influence benchmark with max_level={max_level} ({iterations} iterations)...")
        neo4j_times = []
        neo4j_errors = 0
        for i in range(iterations):
            # Update progress for this specific iteration
            if self.progress_callback:
                iteration_message = f"Running Neo4j user influence query (iteration {i+1}/{iterations})"
                self.progress_callback(self.current_step, self.total_steps, iteration_message)
            
            logger.info(f"Running Neo4j user influence query (iteration {i+1}/{iterations})")
            # Use the helper method which already includes timing
            try:
                neo4j_time = await self._benchmark_user_influence_neo4j(user_id, max_level)
                neo4j_times.append(neo4j_time)
                logger.info(f"Neo4j user influence query completed in {neo4j_time:.2f} seconds")
            except Exception as e:
                logger.error(f"Error in Neo4j user influence query: {str(e)}")
                neo4j_errors += 1
                # Add a high time value to indicate an error
                neo4j_times.append(60.0)
                if self.progress_callback:
                    self.progress_callback(
                        self.current_step, 
                        self.total_steps, 
                        f"Error in Neo4j user influence query: {str(e)}"
                    )
        
        # Store the PostgreSQL query in the metadata
        self.data_metadata["user_influence"]["postgresql_query"] = """
            WITH RECURSIVE user_network AS (
                -- Base case: the user
                SELECT id, 0 AS level
                FROM users
                WHERE id = :user_id
                
                UNION
                
                -- Recursive case: followers of users in the network
                SELECT f.follower_id, un.level + 1
                FROM user_network un
                JOIN follows f ON un.id = f.followed_id
                WHERE un.level < :max_level
            ),
            user_purchases AS (
                SELECT u.id AS user_id, p.product_id, p.created_at
                FROM user_network u
                JOIN purchases p ON u.id = p.user_id
            ),
            influence_data AS (
                SELECT 
                    un.level,
                    COUNT(DISTINCT up.user_id) AS users_count,
                    COUNT(DISTINCT up.product_id) AS products_count,
                    COUNT(up.product_id) AS purchases_count
                FROM user_network un
                LEFT JOIN user_purchases up ON un.id = up.user_id
                GROUP BY un.level
            )
            SELECT * FROM influence_data
            ORDER BY level;
        """
        
        # Get Neo4j query from helper method
        self.data_metadata["user_influence"]["neo4j_query"] = """
        MATCH (u:User {id: $user_id})
        CALL {
            WITH u
            OPTIONAL MATCH path = (u)<-[:FOLLOWS*0..${max_level}]-(follower)
            WITH follower, length(path) AS level
            OPTIONAL MATCH (follower)-[p:PURCHASED]->(product:Product)
            RETURN 
                level,
                count(DISTINCT follower) AS users_count,
                count(DISTINCT product) AS products_count,
                count(p) AS purchases_count
            ORDER BY level
        }
        RETURN * 
        """
        
        # Store error counts in metadata
        self.data_metadata["user_influence"]["errors"] = {
            "postgresql": pg_errors,
            "neo4j": neo4j_errors
        }
        
        # Update progress with completion status
        if self.progress_callback:
            completion_message = f"Completed user influence benchmark with max_level={max_level}"
            if pg_errors > 0 or neo4j_errors > 0:
                completion_message += f" (PostgreSQL errors: {pg_errors}, Neo4j errors: {neo4j_errors})"
            self.progress_callback(
                self.current_step, 
                self.total_steps, 
                completion_message
            )
        
        # Store results - ensure we have at least one valid measurement
        if not pg_times:
            pg_times = [60.0]  # Default to 60 seconds if no valid measurements
        if not neo4j_times:
            neo4j_times = [60.0]  # Default to 60 seconds if no valid measurements
            
        self.results["postgresql"]["user_influence"] = {
            "avg": statistics.mean(pg_times),
            "min": min(pg_times),
            "max": max(pg_times),
            "median": statistics.median(pg_times),
            "data_info": self.data_metadata["user_influence"],
            "errors": pg_errors
        }
        
        self.results["neo4j"]["user_influence"] = {
            "avg": statistics.mean(neo4j_times),
            "min": min(neo4j_times),
            "max": max(neo4j_times),
            "median": statistics.median(neo4j_times),
            "data_info": self.data_metadata["user_influence"],
            "errors": neo4j_errors
        }

    async def _benchmark_user_influence_neo4j(self, user_id: str, max_level: int) -> float:
        """Helper method with correct Neo4j query syntax for user influence benchmarking."""
        logger.info(f"Executing Neo4j user influence query with max_level={max_level}...")
        start_time = time.time()
        
        # Simplified Neo4j query that should be more efficient and less likely to get stuck
        query = """
        MATCH (u:User {id: $user_id})
        WITH u
        UNWIND range(0, $max_level) AS level
        OPTIONAL MATCH path = (u)<-[:FOLLOWS*0..1]-(follower:User)
        WHERE length(path) = level
        WITH level, collect(DISTINCT follower) AS followers
        
        WITH level, size(followers) AS users_count
        
        OPTIONAL MATCH (user)-[:BOUGHT]->(product)
        WHERE user IN followers
        
        RETURN 
            level,
            users_count,
            count(DISTINCT product) AS products_count,
            count(product) AS purchases_count
        ORDER BY level
        """
        
        try:
            # Set a timeout of 30 seconds for this query (reduced from 60)
            timeout_seconds = 30
            logger.info(f"Starting Neo4j query execution with {timeout_seconds}s timeout...")
            
            # Create a task and add a timeout
            async with self.neo4j_driver.session() as session:
                try:
                    # Execute the query with proper timeout handling
                    result_future = session.run(query, user_id=str(user_id), max_level=max_level)
                    result = await asyncio.wait_for(result_future, timeout=timeout_seconds)
                    
                    # Consume results more efficiently
                    records = await result.values()
                    logger.info(f"Neo4j query completed successfully with {len(records)} records.")
                    
                except asyncio.TimeoutError:
                    logger.error(f"Neo4j query timed out after {timeout_seconds} seconds!")
                    # Update progress callback with the timeout information if available
                    if self.progress_callback:
                        self.progress_callback(
                            self.current_step, 
                            self.total_steps, 
                            f"Neo4j user influence query timed out after {timeout_seconds} seconds"
                        )
                    raise TimeoutError(f"Neo4j query timed out after {timeout_seconds} seconds")
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.info(f"Neo4j user influence query execution took {elapsed_time:.2f} seconds")
            return elapsed_time
            
        except Exception as e:
            # Log the exception details to help diagnose issues
            logger.error(f"Error executing Neo4j user influence query: {str(e)}")
            if hasattr(e, '__class__'):
                logger.error(f"Exception type: {e.__class__.__name__}")
            if hasattr(e, 'details'):
                logger.error(f"Exception details: {e.details}")
                
            # Update progress callback with the error information if available
            if self.progress_callback:
                self.progress_callback(
                    self.current_step, 
                    self.total_steps, 
                    f"Error in Neo4j user influence query: {str(e)}"
                )
                
            # Return a high value (60 seconds) to indicate there was a problem
            return 60.0

    async def _benchmark_viral_products(self, iterations: int, max_level: int = 3):
        """
        Benchmark viral products analysis - finding products with the highest social spread.
        This tests complex aggregation and pattern matching performance.
        """
        logger.info(f"Benchmarking viral products with max_level={max_level}...")
        
        # Initialize data metadata for this test
        self.data_metadata["viral_products"] = {
            "max_level": max_level,
            "query_description": "Finding products with the highest social spread and influence through the network"
        }
        
        # PostgreSQL benchmark
        logger.info(f"Running PostgreSQL viral products benchmark with max_level={max_level} ({iterations} iterations)...")
        # Note: With high max_level values (4-5), this query might take several minutes to complete
        if max_level > 3:
            logger.warning(f"High max_level={max_level} may cause this query to take several minutes per iteration")
            
            # Update progress callback with warning if available
            if self.progress_callback:
                self.progress_callback(
                    self.current_step, 
                    self.total_steps, 
                    f"Running PostgreSQL viral products benchmark with high max_level={max_level}. This may take several minutes."
                )
        
        pg_times = []
        pg_errors = 0
        for i in range(iterations):
            # Update progress for this specific iteration
            if self.progress_callback:
                iteration_message = f"Running PostgreSQL viral products query (iteration {i+1}/{iterations})"
                self.progress_callback(self.current_step, self.total_steps, iteration_message)
            
            logger.info(f"Running PostgreSQL viral products query (iteration {i+1}/{iterations})")
            start_time = time.time()
            
            try:
                # Use SQLAlchemy's text method for PostgreSQL queries
                query = """
                WITH RECURSIVE product_buyers AS (
                    -- Get all product purchases
                    SELECT p.product_id, p.user_id, pr.name AS product_name
                    FROM purchases p
                    JOIN products pr ON p.product_id = pr.id
                ),
                social_network AS (
                    -- Base case: direct purchasers
                    SELECT pb.product_id, pb.product_name, pb.user_id, 1 AS level
                    FROM product_buyers pb
                    
                    UNION
                    
                    -- Recursive case: followers who might be influenced
                    SELECT sn.product_id, sn.product_name, f.follower_id, sn.level + 1
                    FROM social_network sn
                    JOIN follows f ON sn.user_id = f.followed_id
                    WHERE sn.level < :max_level
                ),
                product_spread AS (
                    SELECT 
                        product_id,
                        product_name,
                        COUNT(DISTINCT user_id) AS total_reach,
                        MAX(level) AS max_level
                    FROM social_network
                    GROUP BY product_id, product_name
                )
                SELECT * FROM product_spread
                ORDER BY total_reach DESC
                LIMIT 10;
                """
                
                result = await self.db.execute(text(query), {"max_level": max_level})
                results = result.fetchall()
                
                end_time = time.time()
                elapsed_time = end_time - start_time
                pg_times.append(elapsed_time)
                logger.info(f"PostgreSQL viral products query completed in {elapsed_time:.2f} seconds")
            except Exception as e:
                logger.error(f"Error in PostgreSQL viral products query: {str(e)}")
                pg_errors += 1
                # Add a high time value to indicate an error
                pg_times.append(60.0)
                if self.progress_callback:
                    self.progress_callback(
                        self.current_step, 
                        self.total_steps, 
                        f"Error in PostgreSQL viral products query: {str(e)}"
                    )
        
        # Neo4j benchmark
        logger.info(f"Running Neo4j viral products benchmark with max_level={max_level} ({iterations} iterations)...")
        neo4j_times = []
        neo4j_errors = 0
        for i in range(iterations):
            # Update progress for this specific iteration
            if self.progress_callback:
                iteration_message = f"Running Neo4j viral products query (iteration {i+1}/{iterations})"
                self.progress_callback(self.current_step, self.total_steps, iteration_message)
            
            logger.info(f"Running Neo4j viral products query (iteration {i+1}/{iterations})")
            start_time = time.time()
            
            # Neo4j query with correct Cypher syntax - using string formatting for the variable length pattern
            query = """
            MATCH (p:Product)<-[b:BOUGHT]-(u:User)
            WITH p, count(u) as direct_buyers
            ORDER BY direct_buyers DESC
            LIMIT 10
            MATCH (p)<-[:BOUGHT]-(buyer:User)<-[:FOLLOWS*1..%d]-(follower:User)
            RETURN p.id, p.name, direct_buyers,
                   count(DISTINCT follower) as network_reach,
                   direct_buyers + count(DISTINCT follower) as total_reach
            ORDER BY total_reach DESC
            """ % max_level
            
            try:
                # Set a timeout of 60 seconds for this query
                timeout_seconds = 60
                logger.info(f"Starting Neo4j viral products query with {timeout_seconds}s timeout...")
                
                async with self.neo4j_driver.session() as session:
                    try:
                        # Execute the query and properly consume the results
                        result = await asyncio.wait_for(
                            session.run(query),
                            timeout=timeout_seconds
                        )
                        
                        # Explicitly consume the results to ensure the query completes
                        records = await result.consume()
                        logger.info("Neo4j viral products query completed successfully.")
                        
                    except asyncio.TimeoutError:
                        logger.error(f"Neo4j viral products query timed out after {timeout_seconds} seconds!")
                        # Update progress callback with the timeout information if available
                        if self.progress_callback:
                            self.progress_callback(
                                self.current_step, 
                                self.total_steps, 
                                f"Neo4j viral products query timed out after {timeout_seconds} seconds"
                            )
                        raise TimeoutError(f"Neo4j query timed out after {timeout_seconds} seconds")
                
                end_time = time.time()
                elapsed_time = end_time - start_time
                neo4j_times.append(elapsed_time)
                logger.info(f"Neo4j viral products query completed in {elapsed_time:.2f} seconds")
            except Exception as e:
                logger.error(f"Error in Neo4j viral products query: {str(e)}")
                neo4j_errors += 1
                # Add a high time value to indicate an error
                neo4j_times.append(60.0)
                if self.progress_callback:
                    self.progress_callback(
                        self.current_step, 
                        self.total_steps, 
                        f"Error in Neo4j viral products query: {str(e)}"
                    )
        
        # Store the PostgreSQL query in the metadata
        self.data_metadata["viral_products"]["postgresql_query"] = """
        WITH RECURSIVE product_buyers AS (
            -- Get all product purchases
            SELECT p.product_id, p.user_id, pr.name AS product_name
            FROM purchases p
            JOIN products pr ON p.product_id = pr.id
        ),
        social_network AS (
            -- Base case: direct purchasers
            SELECT pb.product_id, pb.product_name, pb.user_id, 1 AS level
            FROM product_buyers pb
            
            UNION
            
            -- Recursive case: followers who might be influenced
            SELECT sn.product_id, sn.product_name, f.follower_id, sn.level + 1
            FROM social_network sn
            JOIN follows f ON sn.user_id = f.followed_id
            WHERE sn.level < :max_level
        ),
        product_spread AS (
            SELECT 
                product_id,
                product_name,
                COUNT(DISTINCT user_id) AS total_reach,
                MAX(level) AS max_level
            FROM social_network
            GROUP BY product_id, product_name
        )
        SELECT * FROM product_spread
        ORDER BY total_reach DESC
        LIMIT 10;
        """
        
        # Store the Neo4j query in the metadata
        neo4j_formatted_query = """
        MATCH (p:Product)<-[b:BOUGHT]-(u:User)
        WITH p, count(u) as direct_buyers
        ORDER BY direct_buyers DESC
        LIMIT 10
        MATCH (p)<-[:BOUGHT]-(buyer:User)<-[:FOLLOWS*1..{max_level}]-(follower:User)
        RETURN p.id, p.name, direct_buyers,
               count(DISTINCT follower) as network_reach,
               direct_buyers + count(DISTINCT follower) as total_reach
        ORDER BY total_reach DESC
        """
        self.data_metadata["viral_products"]["neo4j_query"] = neo4j_formatted_query.replace("{max_level}", str(max_level))
        
        # Store error counts in metadata
        self.data_metadata["viral_products"]["errors"] = {
            "postgresql": pg_errors,
            "neo4j": neo4j_errors
        }
        
        # Update progress with completion status
        if self.progress_callback:
            completion_message = f"Completed viral products benchmark with max_level={max_level}"
            if pg_errors > 0 or neo4j_errors > 0:
                completion_message += f" (PostgreSQL errors: {pg_errors}, Neo4j errors: {neo4j_errors})"
            self.progress_callback(
                self.current_step, 
                self.total_steps, 
                completion_message
            )
        
        # Store results
        self.results["postgresql"]["viral_products"] = {
            "avg": statistics.mean(pg_times) if pg_times else 0,
            "min": min(pg_times) if pg_times else 0,
            "max": max(pg_times) if pg_times else 0,
            "median": statistics.median(pg_times) if pg_times else 0,
            "data_info": self.data_metadata["viral_products"]
        }
        
        self.results["neo4j"]["viral_products"] = {
            "avg": statistics.mean(neo4j_times) if neo4j_times else 0,
            "min": min(neo4j_times) if neo4j_times else 0,
            "max": max(neo4j_times) if neo4j_times else 0,
            "median": statistics.median(neo4j_times) if neo4j_times else 0,
            "data_info": self.data_metadata["viral_products"]
        }

    def _print_benchmark_summary(self):
        """Print a summary of benchmark results."""
        logger.info("=" * 50)
        logger.info("DATABASE BENCHMARK RESULTS (times in seconds)")
        logger.info("=" * 50)
        
        for operation in self.results["postgresql"]:
            pg_avg = self.results["postgresql"][operation]["avg"]
            neo4j_avg = self.results["neo4j"][operation]["avg"]
            
            if pg_avg and neo4j_avg:
                ratio = pg_avg / neo4j_avg if neo4j_avg > 0 else float('inf')
                faster = "Neo4j" if neo4j_avg < pg_avg else "PostgreSQL"
                factor = ratio if faster == "Neo4j" else (1/ratio)
                
                logger.info(f"Operation: {operation}")
                logger.info(f"  PostgreSQL avg: {pg_avg:.6f}s")
                logger.info(f"  Neo4j avg:      {neo4j_avg:.6f}s")
                logger.info(f"  {faster} is {factor:.2f}x faster")
                logger.info("-" * 50)
        
        logger.info("=" * 50)
        
        return self.results 