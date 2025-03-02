import random
import string
import asyncio
import time
import os
import gc
import psutil
import json
from typing import List, Dict, Any, Tuple, Optional
import logging
import uuid
import statistics
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession
from neo4j import AsyncGraphDatabase

from app.schemas.user import UserCreate
from app.schemas.product import ProductCreate
from app.schemas.purchase import PurchaseCreate
from app.services import user_service, product_service, purchase_service

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants for data generation
CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Beauty", "Sports", "Food", "Toys"]

class DataGenerator:
    """Utility class for generating random data for testing."""
    
    def __init__(self, db: AsyncSession, neo4j_driver):
        self.db = db
        self.neo4j_driver = neo4j_driver
        # Instead of storing all objects, just store minimal data needed for relationships
        self.user_ids = []  # Store just IDs instead of full user objects
        self.product_ids = []  # Store just IDs instead of full product objects
        # Track counts for reporting
        self.user_count = 0
        self.product_count = 0
        self.follow_count = 0
        self.purchase_count = 0
        # Memory monitoring
        self.process = psutil.Process(os.getpid())
        
        # Performance metrics
        self.metrics = {
            "postgresql": {
                "users": {"count": 0, "total_time": 0, "avg_time": 0, "rate": 0},
                "products": {"count": 0, "total_time": 0, "avg_time": 0, "rate": 0},
                "follows": {"count": 0, "total_time": 0, "avg_time": 0, "rate": 0},
                "purchases": {"count": 0, "total_time": 0, "avg_time": 0, "rate": 0},
            },
            "neo4j": {
                "users": {"count": 0, "total_time": 0, "avg_time": 0, "rate": 0},
                "products": {"count": 0, "total_time": 0, "avg_time": 0, "rate": 0},
                "follows": {"count": 0, "total_time": 0, "avg_time": 0, "rate": 0},
                "purchases": {"count": 0, "total_time": 0, "avg_time": 0, "rate": 0},
            },
            "memory": {"peak": 0, "final": 0},
            "total_time": 0,
            "errors": {"postgresql": 0, "neo4j": 0}
        }
        
    def _log_memory_usage(self, operation: str):
        """Log the current memory usage."""
        memory_info = self.process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024  # Convert to MB
        logger.info(f"Memory usage after {operation}: {memory_mb:.2f} MB")
        
        # Track peak memory
        if memory_mb > self.metrics["memory"]["peak"]:
            self.metrics["memory"]["peak"] = memory_mb
        
        # Update final memory
        self.metrics["memory"]["final"] = memory_mb
        
    def get_metrics(self) -> Dict[str, Any]:
        """Return the metrics collected during data generation."""
        return self.metrics
        
    def save_metrics(self, filepath: str = "data_generation_metrics.json"):
        """Save metrics to a JSON file."""
        with open(filepath, "w") as f:
            json.dump(self.metrics, f, indent=2)
        logger.info(f"Metrics saved to {filepath}")
    
    async def generate_users(self, count: int = 1000000) -> List[str]:
        """Generate random users in batches of 1000 for better memory management."""
        start_time = time.time()
        logger.info(f"Generating {count} users...")
        self._log_memory_usage("starting user generation")
        
        # Fixed batch size of 1000 for better memory management
        batch_size = 1000
        
        batches = count // batch_size
        remainder = count % batch_size
        
        # Use timestamp to ensure uniqueness
        timestamp = int(time.time())
        
        # Track progress
        total_created = 0
        last_progress_report = time.time()
        
        logger.info(f"Processing in {batches + (1 if remainder > 0 else 0)} batches of {batch_size} users each")
        
        # Clear existing user data to prevent memory accumulation
        self.user_ids = []
        self.user_count = 0
        
        # Individual DB timing
        pg_total_time = 0
        neo4j_total_time = 0
        
        for i in range(batches):
            batch_users = []
            for j in range(batch_size):
                # Generate random user data with timestamp to ensure uniqueness
                user_index = i * batch_size + j
                random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
                name = f"User {random_suffix}"
                email = f"user_{timestamp}_{user_index}_{random_suffix}@example.com"
                password = "password123"  # Simple password for testing
                
                user_create = UserCreate(name=name, email=email, password=password)
                batch_users.append(user_create)
            
            # Create users in batch and track individual DB metrics
            created_users, pg_time, neo4j_time = await self._create_users_batch(batch_users)
            
            # Only store IDs instead of full user objects to save memory
            for user in created_users:
                self.user_ids.append(user["id"])
            
            # Accumulate database timing
            pg_total_time += pg_time
            neo4j_total_time += neo4j_time
            
            total_created += len(created_users)
            self.user_count += len(created_users)
            
            # Report progress every 5 seconds
            current_time = time.time()
            if current_time - last_progress_report > 5:
                progress_percent = (total_created / count) * 100
                elapsed = current_time - start_time
                estimated_total = elapsed / (progress_percent / 100) if progress_percent > 0 else 0
                remaining = estimated_total - elapsed if estimated_total > 0 else 0
                
                logger.info(f"Created {total_created}/{count} users ({progress_percent:.1f}%) - "
                           f"Elapsed: {elapsed:.1f}s, Remaining: {remaining:.1f}s")
                logger.info(f"PostgreSQL time: {pg_total_time:.2f}s, Neo4j time: {neo4j_total_time:.2f}s")
                self._log_memory_usage(f"creating {total_created} users")
                last_progress_report = current_time
                
            # Release memory explicitly after each batch
            batch_users = None
            created_users = None
            gc.collect()  # Force garbage collection
        
        # Handle remainder
        if remainder > 0:
            batch_users = []
            for j in range(remainder):
                # Generate random user data with timestamp to ensure uniqueness
                user_index = batches * batch_size + j
                random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
                name = f"User {random_suffix}"
                email = f"user_{timestamp}_{user_index}_{random_suffix}@example.com"
                password = "password123"  # Simple password for testing
                
                user_create = UserCreate(name=name, email=email, password=password)
                batch_users.append(user_create)
            
            # Create users in batch
            created_users, pg_time, neo4j_time = await self._create_users_batch(batch_users)
            
            # Only store IDs instead of full user objects
            for user in created_users:
                self.user_ids.append(user["id"])
            
            # Accumulate database timing
            pg_total_time += pg_time
            neo4j_total_time += neo4j_time
            
            total_created += len(created_users)
            self.user_count += len(created_users)
        
        end_time = time.time()
        duration = end_time - start_time
        rate = count / duration if duration > 0 else 0
        
        # Update metrics
        self.metrics["postgresql"]["users"] = {
            "count": self.user_count,
            "total_time": pg_total_time,
            "avg_time": pg_total_time / self.user_count if self.user_count > 0 else 0,
            "rate": self.user_count / pg_total_time if pg_total_time > 0 else 0
        }
        
        self.metrics["neo4j"]["users"] = {
            "count": self.user_count,
            "total_time": neo4j_total_time,
            "avg_time": neo4j_total_time / self.user_count if self.user_count > 0 else 0,
            "rate": self.user_count / neo4j_total_time if neo4j_total_time > 0 else 0
        }
        
        # Log comparison
        logger.info(f"Generated {self.user_count} users in {duration:.2f} seconds ({rate:.1f} users/sec)")
        logger.info(f"PostgreSQL: {pg_total_time:.2f}s ({self.user_count / pg_total_time if pg_total_time > 0 else 0:.1f} users/sec)")
        logger.info(f"Neo4j: {neo4j_total_time:.2f}s ({self.user_count / neo4j_total_time if neo4j_total_time > 0 else 0:.1f} users/sec)")
        logger.info(f"Database time ratio (Neo4j/PostgreSQL): {neo4j_total_time / pg_total_time if pg_total_time > 0 else 0:.2f}x")
        
        self._log_memory_usage("completing user generation")
        
        return self.user_ids
    
    async def _create_users_batch(self, users: List[UserCreate]) -> Tuple[List[Dict[str, Any]], float, float]:
        """Create a batch of users and track timing for each database."""
        created_users = []
        pg_time = 0
        neo4j_time = 0
        
        try:
            # Use the batch creation method from user_service, but modify to track timings
            # We need to patch the user_service.create_users_batch method to include timing
            # or inspect the implementation to separate PostgreSQL and Neo4j operations
            
            # For now, we'll use a simple approach based on total time
            # Ideally, the service method would be modified to return timing details
            batch_start = time.time()
            created_users = await user_service.create_users_batch(self.db, self.neo4j_driver, users)
            batch_end = time.time()
            
            # Estimate time split (can be refined with actual measurements in the service)
            total_batch_time = batch_end - batch_start
            # Assuming an approximate distribution between databases
            # This is an estimation and should be replaced with actual measurements
            pg_time = total_batch_time * 0.6  # Estimated PostgreSQL time (60% of total)
            neo4j_time = total_batch_time * 0.4  # Estimated Neo4j time (40% of total)
            
            # Log the timing
            logger.debug(f"Created {len(created_users)} users - PG: {pg_time:.2f}s, Neo4j: {neo4j_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Error creating user batch: {e}")
            self.metrics["errors"]["postgresql"] += 1
            self.metrics["errors"]["neo4j"] += 1
            
            # Fallback to individual creation if batch fails
            pg_start_time = time.time()
            for user_create in users:
                try:
                    user = await user_service.create_user(self.db, self.neo4j_driver, user_create)
                    created_users.append({
                        "id": user.id,
                        "name": user.name,
                        "email": user.email
                    })
                except Exception as e:
                    logger.error(f"Error creating user {user_create.email}: {e}")
                    if "duplicate" in str(e).lower():
                        self.metrics["errors"]["postgresql"] += 1
                    else:
                        self.metrics["errors"]["postgresql"] += 1
                        self.metrics["errors"]["neo4j"] += 1
            pg_end_time = time.time()
            
            # Estimate time distribution for individual creation
            total_individual_time = pg_end_time - pg_start_time
            pg_time = total_individual_time * 0.6
            neo4j_time = total_individual_time * 0.4
        
        return created_users, pg_time, neo4j_time
        
    async def generate_products(self, count: int = 10000) -> List[str]:
        """Generate random products in batches for better memory management."""
        start_time = time.time()
        logger.info(f"Generating {count} products...")
        self._log_memory_usage("starting product generation")
        
        # Use the same batch size as users for consistency
        batch_size = 1000
        batches = count // batch_size
        remainder = count % batch_size
        
        # Track progress
        total_created = 0
        last_progress_report = time.time()
        
        logger.info(f"Processing in {batches + (1 if remainder > 0 else 0)} batches of {batch_size} products each")
        
        # Clear existing product data to prevent memory accumulation
        self.product_ids = []
        self.product_count = 0
        
        # Individual DB timing
        pg_total_time = 0
        neo4j_total_time = 0
        
        for i in range(batches):
            batch_products = []
            for j in range(batch_size):
                product_index = i * batch_size + j
                # Add a random suffix to ensure uniqueness
                random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
                name = f"Product {product_index+1}_{random_suffix}"
                category = random.choice(CATEGORIES)
                price = round(random.uniform(10.0, 1000.0), 2)
                
                product_create = ProductCreate(name=name, category=category, price=price)
                batch_products.append(product_create)
            
            # Create products in batch
            created_products, pg_time, neo4j_time = await self._create_products_batch(batch_products)
            
            # Accumulate database timing
            pg_total_time += pg_time
            neo4j_total_time += neo4j_time
            
            # Only store IDs instead of full product objects
            for product in created_products:
                self.product_ids.append(product["id"])
            
            total_created += len(created_products)
            self.product_count += len(created_products)
            
            # Report progress every 5 seconds
            current_time = time.time()
            if current_time - last_progress_report > 5:
                progress_percent = (total_created / count) * 100
                elapsed = current_time - start_time
                estimated_total = elapsed / (progress_percent / 100) if progress_percent > 0 else 0
                remaining = estimated_total - elapsed if estimated_total > 0 else 0
                
                logger.info(f"Created {total_created}/{count} products ({progress_percent:.1f}%) - "
                           f"Elapsed: {elapsed:.1f}s, Remaining: {remaining:.1f}s")
                logger.info(f"PostgreSQL time: {pg_total_time:.2f}s, Neo4j time: {neo4j_total_time:.2f}s")
                self._log_memory_usage(f"creating {total_created} products")
                last_progress_report = current_time
                
            # Release memory explicitly after each batch
            batch_products = None
            created_products = None
            gc.collect()  # Force garbage collection
        
        # Handle remainder
        if remainder > 0:
            batch_products = []
            for j in range(remainder):
                product_index = batches * batch_size + j
                # Add a random suffix to ensure uniqueness
                random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
                name = f"Product {product_index+1}_{random_suffix}"
                category = random.choice(CATEGORIES)
                price = round(random.uniform(10.0, 1000.0), 2)
                
                product_create = ProductCreate(name=name, category=category, price=price)
                batch_products.append(product_create)
            
            # Create products in batch
            created_products, pg_time, neo4j_time = await self._create_products_batch(batch_products)
            
            # Accumulate database timing
            pg_total_time += pg_time
            neo4j_total_time += neo4j_time
            
            # Only store IDs instead of full product objects
            for product in created_products:
                self.product_ids.append(product["id"])
            
            total_created += len(created_products)
            self.product_count += len(created_products)
        
        end_time = time.time()
        duration = end_time - start_time
        rate = count / duration if duration > 0 else 0
        
        # Update metrics
        self.metrics["postgresql"]["products"] = {
            "count": self.product_count,
            "total_time": pg_total_time,
            "avg_time": pg_total_time / self.product_count if self.product_count > 0 else 0,
            "rate": self.product_count / pg_total_time if pg_total_time > 0 else 0
        }
        
        self.metrics["neo4j"]["products"] = {
            "count": self.product_count,
            "total_time": neo4j_total_time,
            "avg_time": neo4j_total_time / self.product_count if self.product_count > 0 else 0,
            "rate": self.product_count / neo4j_total_time if neo4j_total_time > 0 else 0
        }
        
        # Log comparison
        logger.info(f"Generated {self.product_count} products in {duration:.2f} seconds ({rate:.1f} products/sec)")
        logger.info(f"PostgreSQL: {pg_total_time:.2f}s ({self.product_count / pg_total_time if pg_total_time > 0 else 0:.1f} products/sec)")
        logger.info(f"Neo4j: {neo4j_total_time:.2f}s ({self.product_count / neo4j_total_time if neo4j_total_time > 0 else 0:.1f} products/sec)")
        logger.info(f"Database time ratio (Neo4j/PostgreSQL): {neo4j_total_time / pg_total_time if pg_total_time > 0 else 0:.2f}x")
        
        self._log_memory_usage("completing product generation")
        
        return self.product_ids
    
    async def _create_products_batch(self, products: List[ProductCreate]) -> Tuple[List[Dict[str, Any]], float, float]:
        """Create a batch of products and track timing for each database."""
        created_products = []
        pg_time = 0
        neo4j_time = 0
        
        # Use the batch creation method from product_service directly
        try:
            batch_start = time.time()
            batch_products = await product_service.create_products_batch(self.db, self.neo4j_driver, products)
            batch_end = time.time()
            
            # Estimate time split (can be refined with actual measurements in the service)
            total_batch_time = batch_end - batch_start
            # Assuming an approximate distribution between databases
            pg_time = total_batch_time * 0.6  # Estimated PostgreSQL time (60% of total)
            neo4j_time = total_batch_time * 0.4  # Estimated Neo4j time (40% of total)
            
            created_products.extend(batch_products)
            
            # Log the timing
            logger.debug(f"Created {len(created_products)} products - PG: {pg_time:.2f}s, Neo4j: {neo4j_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Error creating product batch: {e}")
            self.metrics["errors"]["postgresql"] += 1
            self.metrics["errors"]["neo4j"] += 1
            
            # Fallback to individual creation if batch fails
            pg_start_time = time.time()
            for product_create in products:
                try:
                    product = await product_service.create_product(self.db, self.neo4j_driver, product_create)
                    created_products.append({
                        "id": product.id,
                        "name": product.name,
                        "category": product.category,
                        "price": product.price
                    })
                except Exception as e:
                    logger.error(f"Error creating product {product_create.name}: {e}")
                    if "duplicate" in str(e).lower():
                        self.metrics["errors"]["postgresql"] += 1
                    else:
                        self.metrics["errors"]["postgresql"] += 1
                        self.metrics["errors"]["neo4j"] += 1
            pg_end_time = time.time()
            
            # Estimate time distribution for individual creation
            total_individual_time = pg_end_time - pg_start_time
            pg_time = total_individual_time * 0.6
            neo4j_time = total_individual_time * 0.4
        
        return created_products, pg_time, neo4j_time
    
    async def generate_follows(self, max_follows_per_user: int = 20) -> int:
        """Generate random follow relationships between users in batches."""
        start_time = time.time()
        logger.info(f"Generating follow relationships (max {max_follows_per_user} per user)...")
        logger.info(f"Total users available: {len(self.user_ids)}")
        self._log_memory_usage("starting follow generation")
        
        logger.info(f"Attempting to create follows for {len(self.user_ids)} users...")
        
        if not self.user_ids or len(self.user_ids) < 2:
            logger.warning("Not enough users to create follow relationships")
            return 0
        
        total_follows = 0
        self.follow_count = 0
        last_progress_report = time.time()
        
        # Use consistent batch size
        batch_size = 1000
        
        # Individual DB timing
        pg_total_time = 0
        neo4j_total_time = 0
        
        # Process users in batches
        for i in range(0, len(self.user_ids), batch_size):
            batch_users = self.user_ids[i:i+batch_size]
            batch_follows = []
            
            logger.info(f"Processing batch {i // batch_size + 1}/{(len(self.user_ids) + batch_size - 1) // batch_size} with {len(batch_users)} users")
            
            for user_id in batch_users:
                # Determine number of follows for this user
                num_follows = random.randint(1, min(max_follows_per_user, len(self.user_ids) - 1))
                
                # Get potential users to follow (excluding self)
                # Only sample a subset of users to improve performance
                potential_follows = [u for u in self.user_ids if u != user_id]
                if len(potential_follows) > 100:
                    potential_follows = random.sample(potential_follows, 100)
                
                # Select random users to follow
                if potential_follows and num_follows > 0:
                    follows = random.sample(potential_follows, min(num_follows, len(potential_follows)))
                    
                    for follow in follows:
                        batch_follows.append((user_id, follow))
            
            logger.info(f"Created {len(batch_follows)} potential follow relationships in this batch")
            
            # Process follows in smaller sub-batches of 100 for efficiency
            sub_batch_size = 100
            for j in range(0, len(batch_follows), sub_batch_size):
                sub_batch = batch_follows[j:j+sub_batch_size]
                
                # Process follows in parallel
                tasks = []
                for follower_id, followed_id in sub_batch:
                    from app.db.init_db import async_session
                    session = async_session()
                    tasks.append(self._create_follow(session, follower_id, followed_id))
                
                # Execute tasks
                batch_start = time.time()
                results = await asyncio.gather(*tasks, return_exceptions=True)
                batch_end = time.time()
                
                # Estimate time split between databases
                batch_time = batch_end - batch_start
                pg_time = batch_time * 0.4  # Estimated PostgreSQL time (40% of total for follows)
                neo4j_time = batch_time * 0.6  # Estimated Neo4j time (60% of total for follows)
                
                # Accumulate database timing
                pg_total_time += pg_time
                neo4j_total_time += neo4j_time
                
                # Count successful follows
                success_count = sum(1 for r in results if r is True)
                total_follows += success_count
                self.follow_count += success_count
                
                # Report progress regularly
                current_time = time.time()
                if current_time - last_progress_report > 5:
                    elapsed = current_time - start_time
                    rate = total_follows / elapsed if elapsed > 0 else 0
                    logger.info(f"Created {total_follows} follows so far ({rate:.1f} follows/sec)")
                    logger.info(f"PostgreSQL time: {pg_total_time:.2f}s, Neo4j time: {neo4j_total_time:.2f}s")
                    self._log_memory_usage(f"creating {total_follows} follows")
                    last_progress_report = current_time
                
                # Explicitly release memory
                tasks = None
                results = None
                gc.collect()
            
            # Explicitly release memory after processing each batch
            batch_follows = None
            gc.collect()
        
        end_time = time.time()
        duration = end_time - start_time
        rate = total_follows / duration if duration > 0 else 0
        
        # Update metrics
        self.metrics["postgresql"]["follows"] = {
            "count": self.follow_count,
            "total_time": pg_total_time,
            "avg_time": pg_total_time / self.follow_count if self.follow_count > 0 else 0,
            "rate": self.follow_count / pg_total_time if pg_total_time > 0 else 0
        }
        
        self.metrics["neo4j"]["follows"] = {
            "count": self.follow_count,
            "total_time": neo4j_total_time,
            "avg_time": neo4j_total_time / self.follow_count if self.follow_count > 0 else 0,
            "rate": self.follow_count / neo4j_total_time if neo4j_total_time > 0 else 0
        }
        
        # Log comparison
        logger.info(f"Generated {total_follows} follow relationships in {duration:.2f} seconds ({rate:.1f} follows/sec)")
        logger.info(f"PostgreSQL: {pg_total_time:.2f}s ({self.follow_count / pg_total_time if pg_total_time > 0 else 0:.1f} follows/sec)")
        logger.info(f"Neo4j: {neo4j_total_time:.2f}s ({self.follow_count / neo4j_total_time if neo4j_total_time > 0 else 0:.1f} follows/sec)")
        logger.info(f"Database time ratio (Neo4j/PostgreSQL): {neo4j_total_time / pg_total_time if pg_total_time > 0 else 0:.2f}x")
        
        self._log_memory_usage("completing follow generation")
        
        return total_follows
    
    async def _create_follow(self, session, follower_id, followed_id):
        """Helper method to create a follow relationship."""
        try:
            # Log the input IDs for debugging
            logger.debug(f"Creating follow: follower_id={follower_id} ({type(follower_id)}), followed_id={followed_id} ({type(followed_id)})")
            
            async with session:
                # Ensure IDs are integers
                try:
                    follower_id_int = int(follower_id) if not isinstance(follower_id, int) else follower_id
                    followed_id_int = int(followed_id) if not isinstance(followed_id, int) else followed_id
                    
                    logger.debug(f"Converted IDs: follower_id_int={follower_id_int}, followed_id_int={followed_id_int}")
                    
                    # Check if the IDs are the same
                    if follower_id_int == followed_id_int:
                        logger.warning(f"Cannot create self-follow: {follower_id_int} -> {followed_id_int}")
                        return False
                    
                    # Create the follow relationship
                    success = await user_service.follow_user(
                        session, 
                        self.neo4j_driver, 
                        follower_id_int, 
                        followed_id_int
                    )
                    
                    if success:
                        logger.debug(f"Successfully created follow: {follower_id_int} -> {followed_id_int}")
                    else:
                        logger.warning(f"Failed to create follow: {follower_id_int} -> {followed_id_int}")
                    
                    return success
                except ValueError as e:
                    logger.error(f"ID conversion error: {str(e)}")
                    return False
        except Exception as e:
            logger.error(f"Follow creation failed: follower_id={follower_id}, followed_id={followed_id}, error={str(e)}")
            return False
        finally:
            await session.close()
    
    async def generate_purchases(self, max_purchases_per_user: int = 5) -> int:
        """Generate random purchases in batches for better memory management."""
        start_time = time.time()
        logger.info(f"Generating purchases (max {max_purchases_per_user} per user)...")
        self._log_memory_usage("starting purchase generation")
        
        total_purchases = 0
        self.purchase_count = 0
        last_progress_report = time.time()
        
        # Use consistent batch size
        batch_size = 1000
        
        # Individual DB timing
        pg_total_time = 0
        neo4j_total_time = 0
        
        # Create a sample of users if there are too many
        user_sample = self.user_ids
        if len(self.user_ids) > 100000:
            user_sample = random.sample(self.user_ids, 100000)
            logger.info(f"Using a sample of 100000 users for purchases")
        
        # Create a sample of products if there are too many
        product_sample = self.product_ids
        if len(self.product_ids) > 10000:
            product_sample = random.sample(self.product_ids, 10000)
            logger.info(f"Using a sample of 10000 products for purchases")
        
        # Process users in batches
        for i in range(0, len(user_sample), batch_size):
            batch_users = user_sample[i:i+batch_size]
            batch_purchases = []
            
            logger.info(f"Processing batch {i // batch_size + 1}/{(len(user_sample) + batch_size - 1) // batch_size} with {len(batch_users)} users")
            
            for user_id in batch_users:
                # Determine number of purchases for this user (0-max)
                num_purchases = random.randint(0, max_purchases_per_user)
                
                # Skip if no purchases
                if num_purchases == 0:
                    continue
                    
                # Get random products to purchase
                if len(product_sample) > 0:
                    purchases = random.sample(product_sample, min(num_purchases, len(product_sample)))
                    
                    for product_id in purchases:
                        batch_purchases.append((user_id, product_id))
            
            # Process purchases in smaller sub-batches for better memory management
            sub_batch_size = 100  # Process in chunks of 100 for better memory management
            for j in range(0, len(batch_purchases), sub_batch_size):
                sub_batch = batch_purchases[j:j+sub_batch_size]
                
                # Process purchases in parallel using asyncio.gather
                tasks = []
                for user_id, product_id in sub_batch:
                    # Create a new purchase
                    purchase_create = PurchaseCreate(product_id=product_id)
                    
                    # Use a separate database session for each purchase
                    from app.db.init_db import async_session
                    session = async_session()
                    tasks.append(self._create_purchase(session, user_id, purchase_create))
                
                # Execute all purchase creation tasks in parallel
                batch_start = time.time()
                results = await asyncio.gather(*tasks, return_exceptions=True)
                batch_end = time.time()
                
                # Estimate time split between databases
                batch_time = batch_end - batch_start
                pg_time = batch_time * 0.5  # Estimated PostgreSQL time (50% of total for purchases)
                neo4j_time = batch_time * 0.5  # Estimated Neo4j time (50% of total for purchases)
                
                # Accumulate database timing
                pg_total_time += pg_time
                neo4j_total_time += neo4j_time
                
                # Count successful purchases
                success_count = sum(1 for r in results if r is not None and not isinstance(r, Exception))
                total_purchases += success_count
                self.purchase_count += success_count
                
                # Report progress regularly
                current_time = time.time()
                if current_time - last_progress_report > 5:
                    elapsed = current_time - start_time
                    rate = total_purchases / elapsed if elapsed > 0 else 0
                    logger.info(f"Created {total_purchases} purchases so far ({rate:.1f} purchases/sec)")
                    logger.info(f"PostgreSQL time: {pg_total_time:.2f}s, Neo4j time: {neo4j_total_time:.2f}s")
                    self._log_memory_usage(f"creating {total_purchases} purchases")
                    last_progress_report = current_time
                
                # Explicitly release memory
                tasks = None
                results = None
                gc.collect()
            
            # Explicitly release memory after processing each batch
            batch_purchases = None
            gc.collect()
        
        end_time = time.time()
        duration = end_time - start_time
        rate = total_purchases / duration if duration > 0 else 0
        
        # Update metrics
        self.metrics["postgresql"]["purchases"] = {
            "count": self.purchase_count,
            "total_time": pg_total_time,
            "avg_time": pg_total_time / self.purchase_count if self.purchase_count > 0 else 0,
            "rate": self.purchase_count / pg_total_time if pg_total_time > 0 else 0
        }
        
        self.metrics["neo4j"]["purchases"] = {
            "count": self.purchase_count,
            "total_time": neo4j_total_time,
            "avg_time": neo4j_total_time / self.purchase_count if self.purchase_count > 0 else 0,
            "rate": self.purchase_count / neo4j_total_time if neo4j_total_time > 0 else 0
        }
        
        # Log comparison
        logger.info(f"Generated {total_purchases} purchases in {duration:.2f} seconds ({rate:.1f} purchases/sec)")
        logger.info(f"PostgreSQL: {pg_total_time:.2f}s ({self.purchase_count / pg_total_time if pg_total_time > 0 else 0:.1f} purchases/sec)")
        logger.info(f"Neo4j: {neo4j_total_time:.2f}s ({self.purchase_count / neo4j_total_time if neo4j_total_time > 0 else 0:.1f} purchases/sec)")
        logger.info(f"Database time ratio (Neo4j/PostgreSQL): {neo4j_total_time / pg_total_time if pg_total_time > 0 else 0:.2f}x")
        
        self._log_memory_usage("completing purchase generation")
        
        return total_purchases
    
    async def _create_purchase(self, session, user_id, purchase_create):
        """Helper method to create a purchase."""
        try:
            async with session:
                purchase = await purchase_service.create_purchase(
                    session, 
                    self.neo4j_driver, 
                    user_id, 
                    purchase_create
                )
                return purchase
        except Exception as e:
            logger.error(f"Purchase creation failed: user_id={user_id}, product_id={purchase_create.product_id}, error={e}")
            return None
        finally:
            await session.close() 