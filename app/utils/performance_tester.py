import time
import logging
from typing import Dict, Any, List, Callable, Tuple
import asyncio
import statistics
from sqlalchemy.ext.asyncio import AsyncSession
from neo4j import AsyncGraphDatabase

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PerformanceTester:
    """Utility class for testing database query performance."""
    
    def __init__(self, db: AsyncSession, neo4j_driver):
        self.db = db
        self.neo4j_driver = neo4j_driver
        self.results = {}
    
    async def run_test(self, name: str, query_func: Callable, *args, **kwargs) -> Dict[str, Any]:
        """Run a performance test on a query function."""
        logger.info(f"Running performance test: {name}")
        
        # Run the query and measure time
        start_time = time.time()
        result = await query_func(*args, **kwargs)
        end_time = time.time()
        
        duration = end_time - start_time
        
        # Store the result
        test_result = {
            "name": name,
            "duration": duration,
            "result": result
        }
        
        self.results[name] = test_result
        
        logger.info(f"Test '{name}' completed in {duration:.4f} seconds")
        
        return test_result
    
    async def run_comparative_test(
        self, 
        name: str, 
        postgres_func: Callable, 
        neo4j_func: Callable, 
        iterations: int = 5,
        *args, **kwargs
    ) -> Dict[str, Any]:
        """Run a comparative performance test between PostgreSQL and Neo4j."""
        logger.info(f"Running comparative test: {name} ({iterations} iterations)")
        
        postgres_times = []
        neo4j_times = []
        
        # Run multiple iterations to get average performance
        for i in range(iterations):
            logger.info(f"Iteration {i+1}/{iterations}")
            
            # PostgreSQL test
            start_time = time.time()
            postgres_result = await postgres_func(*args, **kwargs)
            postgres_time = time.time() - start_time
            postgres_times.append(postgres_time)
            
            # Neo4j test
            start_time = time.time()
            neo4j_result = await neo4j_func(*args, **kwargs)
            neo4j_time = time.time() - start_time
            neo4j_times.append(neo4j_time)
            
            logger.info(f"  PostgreSQL: {postgres_time:.4f}s, Neo4j: {neo4j_time:.4f}s")
        
        # Calculate statistics
        postgres_avg = statistics.mean(postgres_times)
        neo4j_avg = statistics.mean(neo4j_times)
        
        postgres_median = statistics.median(postgres_times)
        neo4j_median = statistics.median(neo4j_times)
        
        postgres_min = min(postgres_times)
        neo4j_min = min(neo4j_times)
        
        postgres_max = max(postgres_times)
        neo4j_max = max(neo4j_times)
        
        # Determine which is faster
        if postgres_avg < neo4j_avg:
            faster = "PostgreSQL"
            speedup = neo4j_avg / postgres_avg
        else:
            faster = "Neo4j"
            speedup = postgres_avg / neo4j_avg
        
        # Store the result
        test_result = {
            "name": name,
            "iterations": iterations,
            "postgresql": {
                "avg_time": postgres_avg,
                "median_time": postgres_median,
                "min_time": postgres_min,
                "max_time": postgres_max,
                "times": postgres_times,
                "result": postgres_result
            },
            "neo4j": {
                "avg_time": neo4j_avg,
                "median_time": neo4j_median,
                "min_time": neo4j_min,
                "max_time": neo4j_max,
                "times": neo4j_times,
                "result": neo4j_result
            },
            "faster": faster,
            "speedup": speedup
        }
        
        self.results[name] = test_result
        
        logger.info(f"Comparative test '{name}' completed:")
        logger.info(f"  PostgreSQL avg: {postgres_avg:.4f}s")
        logger.info(f"  Neo4j avg: {neo4j_avg:.4f}s")
        logger.info(f"  {faster} is {speedup:.2f}x faster")
        
        return test_result
    
    def get_results(self) -> Dict[str, Dict[str, Any]]:
        """Get all test results."""
        return self.results
    
    def print_results(self) -> None:
        """Print all test results in a formatted way."""
        logger.info("=== Performance Test Results ===")
        
        for name, result in self.results.items():
            logger.info(f"\nTest: {name}")
            
            if "postgresql" in result and "neo4j" in result:
                # Comparative test
                logger.info(f"  Iterations: {result['iterations']}")
                logger.info(f"  PostgreSQL avg: {result['postgresql']['avg_time']:.4f}s")
                logger.info(f"  Neo4j avg: {result['neo4j']['avg_time']:.4f}s")
                logger.info(f"  Faster: {result['faster']} ({result['speedup']:.2f}x)")
            else:
                # Single test
                logger.info(f"  Duration: {result['duration']:.4f}s")
    
    def export_results_to_csv(self, filename: str) -> None:
        """Export test results to a CSV file."""
        import csv
        
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['Test Name', 'Type', 'PostgreSQL Avg (s)', 'Neo4j Avg (s)', 
                         'Faster', 'Speedup', 'PostgreSQL Min (s)', 'PostgreSQL Max (s)',
                         'Neo4j Min (s)', 'Neo4j Max (s)']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            for name, result in self.results.items():
                if "postgresql" in result and "neo4j" in result:
                    # Comparative test
                    writer.writerow({
                        'Test Name': name,
                        'Type': 'Comparative',
                        'PostgreSQL Avg (s)': f"{result['postgresql']['avg_time']:.4f}",
                        'Neo4j Avg (s)': f"{result['neo4j']['avg_time']:.4f}",
                        'Faster': result['faster'],
                        'Speedup': f"{result['speedup']:.2f}x",
                        'PostgreSQL Min (s)': f"{result['postgresql']['min_time']:.4f}",
                        'PostgreSQL Max (s)': f"{result['postgresql']['max_time']:.4f}",
                        'Neo4j Min (s)': f"{result['neo4j']['min_time']:.4f}",
                        'Neo4j Max (s)': f"{result['neo4j']['max_time']:.4f}"
                    })
                else:
                    # Single test
                    writer.writerow({
                        'Test Name': name,
                        'Type': 'Single',
                        'PostgreSQL Avg (s)': 'N/A',
                        'Neo4j Avg (s)': 'N/A',
                        'Faster': 'N/A',
                        'Speedup': 'N/A',
                        'PostgreSQL Min (s)': 'N/A',
                        'PostgreSQL Max (s)': 'N/A',
                        'Neo4j Min (s)': 'N/A',
                        'Neo4j Max (s)': 'N/A'
                    })
        
        logger.info(f"Results exported to {filename}") 