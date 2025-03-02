from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Dict, Any, Optional, List
import time
import asyncio
import logging
from datetime import datetime
import os
import json
from collections import Counter

from app.db.init_db import get_db, get_neo4j_driver
from app.utils.data_generator import DataGenerator
from app.utils.benchmark import DatabaseBenchmark

router = APIRouter()

# Store benchmark results
benchmark_results = {}

# Dictionary to store performance metrics from data generation
generation_metrics = {}


@router.get("/random-ids", response_model=Dict[str, str])
async def get_random_ids(
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Get a random product and user ID for testing.
    """
    try:
        # Get a random product
        product_query = text("SELECT id FROM products ORDER BY RANDOM() LIMIT 1")
        product_result = await db.execute(product_query)
        product_id = product_result.scalar()
        
        # Get a random user
        user_query = text("SELECT id FROM users ORDER BY RANDOM() LIMIT 1")
        user_result = await db.execute(user_query)
        user_id = user_result.scalar()
        
        if not product_id or not user_id:
            return {
                "message": "No products or users found. Please generate data first.",
                "product_id": "",
                "user_id": ""
            }
        
        return {
            "product_id": str(product_id),
            "user_id": str(user_id)
        }
    except Exception as e:
        logging.error(f"Error getting random IDs: {str(e)}")
        return {
            "message": f"Error getting random IDs: {str(e)}. Please generate data first.",
            "product_id": "",
            "user_id": ""
        }


@router.get("/metrics/generation/{task_id}")
async def get_generation_metrics(task_id: str):
    """
    Get detailed performance metrics from data generation for PostgreSQL vs Neo4j comparison.
    """
    if task_id not in generation_metrics:
        raise HTTPException(status_code=404, detail=f"Task ID {task_id} not found")
    
    return generation_metrics[task_id]


@router.get("/metrics/generation/latest")
async def get_latest_generation_metrics():
    """
    Get the most recent data generation metrics.
    """
    if not generation_metrics:
        raise HTTPException(status_code=404, detail="No generation metrics available")
    
    # Get the most recent task_id based on timestamp
    latest_task_id = sorted(generation_metrics.keys())[-1]
    return generation_metrics[latest_task_id]


@router.post("/generate")
async def generate_data(
    background_tasks: BackgroundTasks,
    users: int = 1000,
    products: int = 100,
    max_follows: int = 20,
    max_purchases: int = 5,
    db: AsyncSession = Depends(get_db),
    neo4j_driver = Depends(get_neo4j_driver)
):
    """
    Generate test data for benchmarking.
    This is a long-running task that will be executed in the background.
    Returns performance metrics comparing PostgreSQL and Neo4j.
    """
    # Generate a unique ID for this generation task
    task_id = f"generate_{int(time.time())}"
    
    # Start the data generation in the background
    background_tasks.add_task(
        _generate_data_task,
        task_id,
        db,
        neo4j_driver,
        users,
        products,
        max_follows,
        max_purchases
    )
    
    return {
        "task_id": task_id,
        "status": "running",
        "message": "Data generation started in the background",
        "metrics_endpoint": f"/api/benchmark/metrics/generation/{task_id}"
    }


@router.get("/status/{task_id}")
async def get_task_status(task_id: str):
    """Get the status of a data generation or benchmark task."""
    logging.info(f"Checking status for task_id: {task_id}")
    
    if task_id in benchmark_results:
        status = benchmark_results[task_id]["status"]
        logging.info(f"Task found: {task_id}, status: {status}")
        
        result = dict(benchmark_results[task_id])
        
        if status == "completed":
            result["progress"] = 100
        elif status == "failed":
            result["progress"] = 0
        else:  # running
            # For benchmark tasks with detailed progress tracking
            if "current_step" in result and "total_steps" in result:
                current_step = result.get("current_step", 0)
                total_steps = result.get("total_steps", 1)
                # Calculate progress percentage (ensure we don't divide by zero)
                if total_steps > 0:
                    progress = min(int((current_step / total_steps) * 100), 99)  # Cap at 99% until completed
                else:
                    progress = 50
                result["progress"] = progress
            else:
                # For tasks without detailed progress tracking, use time-based estimate
                start_time = datetime.fromisoformat(result["start_time"])
                current_time = datetime.now()
                # Assume tasks take about 30 seconds to complete
                elapsed_seconds = (current_time - start_time).total_seconds()
                progress = min(int(elapsed_seconds / 30 * 100), 99)  # Cap at 99% until completed
                result["progress"] = progress
            
        return result
    else:
        logging.warning(f"Task not found: {task_id}")
        return {
            "task_id": task_id,
            "status": "not_found",
            "message": "Task not found or not started yet",
            "progress": 0
        }


@router.post("/run")
async def run_benchmark(
    background_tasks: BackgroundTasks,
    test_type: str = "all",
    max_level: int = 3,
    product_id: Optional[str] = None,
    user_id: Optional[str] = None,
    iterations: int = 5,
    db: AsyncSession = Depends(get_db),
    neo4j_driver = Depends(get_neo4j_driver)
):
    """
    Run the database benchmark comparing PostgreSQL and Neo4j.
    This is a long-running task that will be executed in the background.
    
    Parameters:
    - test_type: Type of test to run (all, product_virality, user_influence, viral_products)
    - max_level: Maximum level for social network depth (1-5)
    - product_id: Product ID for virality test
    - user_id: User ID for influence test
    - iterations: Number of iterations per test
    """
    # Generate a unique ID for this benchmark task
    task_id = f"benchmark_{int(time.time())}"
    
    # Start the benchmark in the background
    background_tasks.add_task(
        _run_benchmark_task,
        task_id,
        db,
        neo4j_driver,
        test_type,
        max_level,
        product_id,
        user_id,
        iterations
    )
    
    return {
        "benchmark_id": task_id,
        "status": "started",
        "message": f"Benchmark started with test_type={test_type}, max_level={max_level}, iterations={iterations}"
    }


@router.get("/results/{task_id}")
async def get_benchmark_results(task_id: str):
    """Get the results of a completed benchmark task."""
    logging.info(f"Getting results for task_id: {task_id}")
    logging.info(f"Available benchmark_results keys: {list(benchmark_results.keys())}")
    
    if task_id in benchmark_results and benchmark_results[task_id]["status"] == "completed":
        logging.info(f"Task {task_id} is completed, returning results")
        return benchmark_results[task_id]
    elif task_id in benchmark_results:
        logging.info(f"Task {task_id} is in status: {benchmark_results[task_id]['status']}")
        return {
            "task_id": task_id,
            "status": benchmark_results[task_id]["status"],
            "message": "Benchmark is still running or failed"
        }
    else:
        logging.warning(f"Task {task_id} not found in benchmark_results")
        return {
            "task_id": task_id,
            "status": "not_found",
            "message": "Benchmark task not found"
        }


@router.get("/compare")
async def compare_benchmarks(pg_task_id: str, neo4j_task_id: str):
    """Compare the results of two benchmark tasks."""
    if pg_task_id not in benchmark_results or neo4j_task_id not in benchmark_results:
        raise HTTPException(status_code=404, detail="One or both benchmark tasks not found")
    
    if benchmark_results[pg_task_id]["status"] != "completed" or benchmark_results[neo4j_task_id]["status"] != "completed":
        raise HTTPException(status_code=400, detail="One or both benchmark tasks not completed")
    
    pg_results = benchmark_results[pg_task_id]["results"]
    neo4j_results = benchmark_results[neo4j_task_id]["results"]
    
    comparison = {
        "postgresql_task": pg_task_id,
        "neo4j_task": neo4j_task_id,
        "operations": {}
    }
    
    # Compare each operation
    for operation in pg_results["postgresql"]:
        if operation in neo4j_results["neo4j"]:
            pg_avg = pg_results["postgresql"][operation]["avg"]
            neo4j_avg = neo4j_results["neo4j"][operation]["avg"]
            
            if pg_avg and neo4j_avg:
                ratio = pg_avg / neo4j_avg if neo4j_avg > 0 else float('inf')
                faster = "Neo4j" if neo4j_avg < pg_avg else "PostgreSQL"
                factor = ratio if faster == "Neo4j" else (1/ratio)
                
                comparison["operations"][operation] = {
                    "postgresql_avg": pg_avg,
                    "neo4j_avg": neo4j_avg,
                    "faster_db": faster,
                    "speedup_factor": factor
                }
    
    return comparison


async def _generate_data_task(
    task_id: str,
    db: AsyncSession,
    neo4j_driver,
    users: int,
    products: int,
    max_follows: int,
    max_purchases: int
):
    """Background task for data generation."""
    try:
        overall_start_time = time.time()
        
        benchmark_results[task_id] = {
            "task_id": task_id,
            "status": "running",
            "message": "Data generation in progress",
            "start_time": datetime.now().isoformat(),
            "progress": 0
        }
        
        # Create data generator
        generator = DataGenerator(db, neo4j_driver)
        
        # Define progress weights for each step
        progress_weights = {
            "users": 40,      # Users generation - 40% of total progress
            "products": 20,   # Products generation - 20% of total progress
            "follows": 20,    # Follows generation - 20% of total progress
            "purchases": 20   # Purchases generation - 20% of total progress
        }
        current_progress = 0
        
        # Generate users - 40% of total progress
        benchmark_results[task_id]["message"] = f"Generating {users} users..."
        benchmark_results[task_id]["progress"] = current_progress
        
        start_time = time.time()
        generated_users = await generator.generate_users(users)
        end_time = time.time()
        users_time = end_time - start_time
        
        current_progress += progress_weights["users"]
        benchmark_results[task_id]["progress"] = current_progress
        benchmark_results[task_id]["message"] = f"Generated {len(generated_users)} users in {users_time:.2f} seconds"
        logging.info(f"Generated {len(generated_users)} users in {users_time:.2f} seconds")
        
        # Generate products - 20% of total progress
        benchmark_results[task_id]["message"] = f"Generating {products} products..."
        
        start_time = time.time()
        generated_products = await generator.generate_products(products)
        end_time = time.time()
        products_time = end_time - start_time
        
        current_progress += progress_weights["products"]
        benchmark_results[task_id]["progress"] = current_progress
        benchmark_results[task_id]["message"] = f"Generated {len(generated_products)} products in {products_time:.2f} seconds"
        logging.info(f"Generated {len(generated_products)} products in {products_time:.2f} seconds")
        
        # Generate follows - 20% of total progress
        benchmark_results[task_id]["message"] = f"Generating follow relationships..."
        
        start_time = time.time()
        generated_follows = await generator.generate_follows(max_follows)
        end_time = time.time()
        follows_time = end_time - start_time
        
        current_progress += progress_weights["follows"]
        benchmark_results[task_id]["progress"] = current_progress
        benchmark_results[task_id]["message"] = f"Generated {generated_follows} follow relationships in {follows_time:.2f} seconds"
        logging.info(f"Generated {generated_follows} follow relationships in {follows_time:.2f} seconds")
        
        # Generate purchases - 20% of total progress
        benchmark_results[task_id]["message"] = f"Generating purchases..."
        
        start_time = time.time()
        generated_purchases = await generator.generate_purchases(max_purchases)
        end_time = time.time()
        purchases_time = end_time - start_time
        
        current_progress += progress_weights["purchases"]
        benchmark_results[task_id]["progress"] = current_progress
        
        overall_end_time = time.time()
        overall_duration = overall_end_time - overall_start_time
        
        benchmark_results[task_id]["message"] = f"Generated {generated_purchases} purchases in {purchases_time:.2f} seconds"
        logging.info(f"Generated {generated_purchases} purchases in {purchases_time:.2f} seconds")
        
        benchmark_results[task_id]["status"] = "completed"
        benchmark_results[task_id]["end_time"] = datetime.now().isoformat()
        benchmark_results[task_id]["has_results"] = True
        benchmark_results[task_id]["duration"] = overall_duration
        
        # Get the detailed metrics from the generator
        generator_metrics = generator.get_metrics()
        
        # Compute comparison metrics
        comparison = {
            "postgresql": {
                "total_time": sum(generator_metrics["postgresql"][op]["total_time"] for op in ["users", "products", "follows", "purchases"]),
                "operation_times": {
                    "users": generator_metrics["postgresql"]["users"]["total_time"],
                    "products": generator_metrics["postgresql"]["products"]["total_time"],
                    "follows": generator_metrics["postgresql"]["follows"]["total_time"] if "follows" in generator_metrics["postgresql"] else 0,
                    "purchases": generator_metrics["postgresql"]["purchases"]["total_time"] if "purchases" in generator_metrics["postgresql"] else 0
                },
                "rates": {
                    "users": generator_metrics["postgresql"]["users"]["rate"],
                    "products": generator_metrics["postgresql"]["products"]["rate"],
                    "follows": generator_metrics["postgresql"]["follows"]["rate"] if "follows" in generator_metrics["postgresql"] else 0,
                    "purchases": generator_metrics["postgresql"]["purchases"]["rate"] if "purchases" in generator_metrics["postgresql"] else 0
                }
            },
            "neo4j": {
                "total_time": sum(generator_metrics["neo4j"][op]["total_time"] for op in ["users", "products", "follows", "purchases"]),
                "operation_times": {
                    "users": generator_metrics["neo4j"]["users"]["total_time"],
                    "products": generator_metrics["neo4j"]["products"]["total_time"],
                    "follows": generator_metrics["neo4j"]["follows"]["total_time"] if "follows" in generator_metrics["neo4j"] else 0,
                    "purchases": generator_metrics["neo4j"]["purchases"]["total_time"] if "purchases" in generator_metrics["neo4j"] else 0
                },
                "rates": {
                    "users": generator_metrics["neo4j"]["users"]["rate"],
                    "products": generator_metrics["neo4j"]["products"]["rate"],
                    "follows": generator_metrics["neo4j"]["follows"]["rate"] if "follows" in generator_metrics["neo4j"] else 0,
                    "purchases": generator_metrics["neo4j"]["purchases"]["rate"] if "purchases" in generator_metrics["neo4j"] else 0
                }
            },
            "comparison": {
                "faster_db": "postgresql" if generator_metrics["postgresql"]["users"]["total_time"] < generator_metrics["neo4j"]["users"]["total_time"] else "neo4j",
                "overall_speedup": max(
                    generator_metrics["postgresql"]["users"]["total_time"] / generator_metrics["neo4j"]["users"]["total_time"],
                    generator_metrics["neo4j"]["users"]["total_time"] / generator_metrics["postgresql"]["users"]["total_time"]
                ),
                "operations": {
                    "users": {
                        "faster": "postgresql" if generator_metrics["postgresql"]["users"]["total_time"] < generator_metrics["neo4j"]["users"]["total_time"] else "neo4j",
                        "speedup": max(
                            generator_metrics["postgresql"]["users"]["total_time"] / generator_metrics["neo4j"]["users"]["total_time"],
                            generator_metrics["neo4j"]["users"]["total_time"] / generator_metrics["postgresql"]["users"]["total_time"]
                        )
                    },
                    "products": {
                        "faster": "postgresql" if generator_metrics["postgresql"]["products"]["total_time"] < generator_metrics["neo4j"]["products"]["total_time"] else "neo4j",
                        "speedup": max(
                            generator_metrics["postgresql"]["products"]["total_time"] / generator_metrics["neo4j"]["products"]["total_time"],
                            generator_metrics["neo4j"]["products"]["total_time"] / generator_metrics["postgresql"]["products"]["total_time"]
                        )
                    }
                }
            },
            "memory": generator_metrics["memory"],
            "errors": generator_metrics["errors"]
        }
        
        # Store combined metrics
        generation_metrics[task_id] = {
            "task_id": task_id,
            "status": "completed",
            "message": f"Data generation completed: {len(generated_users)} users, {len(generated_products)} products, {generated_follows} follows, {generated_purchases} purchases",
            "start_time": benchmark_results[task_id]["start_time"],
            "end_time": benchmark_results[task_id]["end_time"],
            "duration": overall_duration,
            "counts": {
                "users": len(generated_users),
                "products": len(generated_products),
                "follows": generated_follows,
                "purchases": generated_purchases
            },
            "timings": {
                "users": users_time,
                "products": products_time,
                "follows": follows_time,
                "purchases": purchases_time,
                "total": overall_duration
            },
            "database_metrics": generator_metrics,
            "comparison": comparison
        }
        
        # Save metrics to a file for persistence
        metrics_dir = os.path.join(os.getcwd(), "metrics")
        os.makedirs(metrics_dir, exist_ok=True)
        metrics_file = os.path.join(metrics_dir, f"generation_metrics_{task_id}.json")
        
        with open(metrics_file, "w") as f:
            json.dump(generation_metrics[task_id], f, indent=2)
        
        logging.info(f"Generation metrics saved to {metrics_file}")
        
    except Exception as e:
        logging.error(f"Error generating data: {str(e)}")
        benchmark_results[task_id]["status"] = "error"
        benchmark_results[task_id]["message"] = f"Error: {str(e)}"
        generation_metrics[task_id] = {
            "task_id": task_id,
            "status": "error",
            "message": f"Error: {str(e)}",
            "error": str(e)
        }


async def _run_benchmark_task(
    task_id: str,
    db: AsyncSession,
    neo4j_driver,
    test_type: str,
    max_level: int,
    product_id: Optional[str],
    user_id: Optional[str],
    iterations: int
):
    """Background task for running benchmarks."""
    try:
        benchmark_results[task_id] = {
            "task_id": task_id,
            "status": "running",
            "message": f"Benchmark in progress with test_type={test_type}, max_level={max_level}, iterations={iterations}",
            "start_time": datetime.now().isoformat(),
            "progress": 0,
            "current_step": 0,
            "total_steps": 1,  # Will be updated by the benchmark class
            "current_operation": "Initializing benchmark"
        }
        
        # Create benchmark instance
        benchmark = DatabaseBenchmark(db, neo4j_driver)
        
        # Custom progress updater function to be called from the benchmark class
        def update_progress(current_step, total_steps, operation_details):
            benchmark_results[task_id].update({
                "current_step": current_step,
                "total_steps": total_steps,
                "current_operation": operation_details,
                "message": f"Benchmark in progress: {operation_details} (Step {current_step}/{total_steps})"
            })
            logging.info(f"Benchmark progress for {task_id}: {current_step}/{total_steps} - {operation_details}")
        
        # Attach the updater to the benchmark instance
        benchmark.progress_callback = update_progress
        
        logging.info(f"Running benchmark for task: {task_id}")
        results = await benchmark.run_benchmark(test_type, max_level, product_id, user_id, iterations)
        logging.info(f"Benchmark completed for task: {task_id}")
        
        # Store results
        benchmark_results[task_id].update({
            "status": "completed",
            "message": "Benchmark completed successfully",
            "results": results,
            "end_time": datetime.now().isoformat(),
            "has_results": True,
            "progress": 100
        })
        
    except Exception as e:
        logging.error(f"Error running benchmark: {str(e)}")
        benchmark_results[task_id].update({
            "status": "failed",
            "message": f"Benchmark failed: {str(e)}",
            "end_time": datetime.now().isoformat(),
            "has_results": False,
            "progress": 0
        })


@router.get("/list", response_model=List[Dict[str, Any]])
async def list_benchmarks() -> Any:
    """
    List all benchmarks.
    """
    logging.info(f"Listing benchmarks, available keys: {list(benchmark_results.keys())}")
    benchmarks = []
    
    for benchmark_id in benchmark_results:
        has_results = "results" in benchmark_results[benchmark_id] if benchmark_results[benchmark_id].get("status") == "completed" else False
        logging.info(f"Benchmark {benchmark_id}: status={benchmark_results[benchmark_id].get('status', 'unknown')}, has_results={has_results}")
        
        benchmarks.append({
            "id": benchmark_id,
            "status": benchmark_results[benchmark_id].get("status", "unknown"),
            "message": benchmark_results[benchmark_id].get("message", ""),
            "has_results": has_results,
            "progress": benchmark_results[benchmark_id].get("progress", 0)
        })
    
    return benchmarks


@router.get("/tasks")
async def get_benchmark_tasks():
    """
    Get a list of all benchmark and data generation tasks.
    """
    tasks = []
    
    # Add generation tasks
    for task_id, task_data in generation_metrics.items():
        tasks.append({
            "task_id": task_id,
            "type": "generation",
            "status": task_data.get("status", "unknown"),
            "start_time": task_data.get("start_time", ""),
            "end_time": task_data.get("end_time", ""),
            "duration": task_data.get("duration", 0)
        })
    
    # Add benchmark tasks
    for task_id, task_data in benchmark_results.items():
        if task_id not in generation_metrics:  # Avoid duplicates
            tasks.append({
                "task_id": task_id,
                "type": "benchmark",
                "status": task_data.get("status", "unknown"),
                "start_time": task_data.get("start_time", ""),
                "end_time": task_data.get("end_time", ""),
                "duration": task_data.get("duration", 0)
            })
    
    # Sort by start time (most recent first)
    tasks.sort(key=lambda x: x.get("start_time", ""), reverse=True)
    
    return tasks 