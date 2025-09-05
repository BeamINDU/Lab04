# agents/storage/scalable_database.py
"""
Scalable Database Handler with connection pooling and async support
Handles high concurrent loads efficiently
"""

import asyncio
import asyncpg
import psycopg2
from psycopg2 import pool
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

@dataclass
class DatabaseConfig:
    """Database configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "siamtemp_company_a"
    user: str = "postgres"
    password: str = "password"
    min_connections: int = 5
    max_connections: int = 20
    command_timeout: int = 60
    max_queries: int = 50000
    max_inactive_connection_lifetime: float = 300.0

# =============================================================================
# ASYNC DATABASE HANDLER WITH CONNECTION POOLING
# =============================================================================

class ScalableDatabaseHandler:
    """
    Production-ready database handler with:
    - Connection pooling
    - Async support for high concurrency
    - Query optimization
    - Circuit breaker pattern
    - Retry logic
    """
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.pool = None
        self.sync_pool = None
        self.circuit_breaker = CircuitBreaker()
        self.query_stats = {}
        
    # =========================================================================
    # ASYNC CONNECTION MANAGEMENT
    # =========================================================================
    
    async def initialize_async(self):
        """Initialize async connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                min_size=self.config.min_connections,
                max_size=self.config.max_connections,
                command_timeout=self.config.command_timeout,
                max_queries=self.config.max_queries,
                max_inactive_connection_lifetime=self.config.max_inactive_connection_lifetime
            )
            logger.info(f"✅ Async pool initialized: {self.config.min_connections}-{self.config.max_connections} connections")
            
        except Exception as e:
            logger.error(f"Failed to initialize async pool: {e}")
            raise
    
    def initialize_sync(self):
        """Initialize synchronous connection pool for backward compatibility"""
        try:
            self.sync_pool = psycopg2.pool.ThreadedConnectionPool(
                self.config.min_connections,
                self.config.max_connections,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password
            )
            logger.info("✅ Sync pool initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize sync pool: {e}")
            raise
    
    # =========================================================================
    # QUERY EXECUTION WITH CIRCUIT BREAKER
    # =========================================================================
    
    async def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict]:
        """
        Execute query with circuit breaker and retry logic
        """
        # Check circuit breaker
        if not self.circuit_breaker.can_execute():
            raise Exception("Circuit breaker is open - too many failures")
        
        # Hash query for caching
        query_hash = self._hash_query(sql, params)
        
        # Try cache first (integrate with Redis cache)
        cached = await self._get_cached_result(query_hash)
        if cached:
            return cached
        
        try:
            result = await self._execute_with_retry(sql, params)
            
            # Cache successful result
            await self._cache_result(query_hash, result)
            
            # Update circuit breaker
            self.circuit_breaker.record_success()
            
            # Track query stats
            self._track_query_stats(sql, len(result))
            
            return result
            
        except Exception as e:
            self.circuit_breaker.record_failure()
            logger.error(f"Query failed: {e}")
            raise
    
    async def _execute_with_retry(self, sql: str, params: Optional[tuple], 
                                  max_retries: int = 3) -> List[Dict]:
        """Execute query with retry logic"""
        for attempt in range(max_retries):
            try:
                async with self.pool.acquire() as connection:
                    # Set query optimization parameters
                    await connection.execute("SET work_mem = '256MB'")
                    await connection.execute("SET random_page_cost = 1.1")
                    
                    # Execute query
                    if params:
                        rows = await connection.fetch(sql, *params)
                    else:
                        rows = await connection.fetch(sql)
                    
                    # Convert to dict format
                    return [dict(row) for row in rows]
                    
            except asyncpg.TooManyConnectionsError:
                logger.warning(f"Connection pool exhausted, attempt {attempt + 1}")
                await asyncio.sleep(0.1 * (attempt + 1))
                
            except asyncpg.PostgresError as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Query failed, retrying: {e}")
                await asyncio.sleep(0.1)
        
        raise Exception(f"Query failed after {max_retries} retries")
    
    # =========================================================================
    # BATCH OPERATIONS FOR HIGH THROUGHPUT
    # =========================================================================
    
    async def execute_batch(self, queries: List[tuple]) -> List[List[Dict]]:
        """
        Execute multiple queries in parallel for high throughput
        queries: List of (sql, params) tuples
        """
        tasks = []
        for sql, params in queries:
            task = asyncio.create_task(self.execute_query(sql, params))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Batch query {i} failed: {result}")
                processed.append([])
            else:
                processed.append(result)
        
        return processed
    
    # =========================================================================
    # CACHING INTEGRATION
    # =========================================================================
    
    def _hash_query(self, sql: str, params: Optional[tuple]) -> str:
        """Generate hash for query caching"""
        import hashlib
        query_str = f"{sql}:{params}" if params else sql
        return hashlib.md5(query_str.encode()).hexdigest()
    
    async def _get_cached_result(self, query_hash: str) -> Optional[List[Dict]]:
        """Get cached result (integrate with Redis)"""
        # TODO: Integrate with Redis cache
        return None
    
    async def _cache_result(self, query_hash: str, result: List[Dict]):
        """Cache query result (integrate with Redis)"""
        # TODO: Integrate with Redis cache
        pass
    
    # =========================================================================
    # CONNECTION POOL MONITORING
    # =========================================================================
    
    async def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics"""
        if not self.pool:
            return {}
        
        return {
            'size': self.pool.get_size(),
            'free_connections': self.pool.get_idle_size(),
            'used_connections': self.pool.get_size() - self.pool.get_idle_size(),
            'max_size': self.pool.get_max_size(),
            'queries_executed': sum(self.query_stats.values())
        }
    
    def _track_query_stats(self, sql: str, result_count: int):
        """Track query statistics"""
        query_type = sql.split()[0].upper()
        if query_type not in self.query_stats:
            self.query_stats[query_type] = 0
        self.query_stats[query_type] += 1
    
    # =========================================================================
    # CLEANUP
    # =========================================================================
    
    async def close(self):
        """Close all connections"""
        if self.pool:
            await self.pool.close()
            logger.info("Async pool closed")
        
        if self.sync_pool:
            self.sync_pool.closeall()
            logger.info("Sync pool closed")

# =============================================================================
# CIRCUIT BREAKER PATTERN
# =============================================================================

class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures
    """
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'closed'  # closed, open, half_open
    
    def can_execute(self) -> bool:
        """Check if request can be executed"""
        if self.state == 'closed':
            return True
        
        if self.state == 'open':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'half_open'
                return True
            return False
        
        return self.state == 'half_open'
    
    def record_success(self):
        """Record successful execution"""
        self.failure_count = 0
        if self.state == 'half_open':
            self.state = 'closed'
    
    def record_failure(self):
        """Record failed execution"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'open'
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")

# =============================================================================
# LOAD BALANCER FOR MULTIPLE DATABASES
# =============================================================================

class DatabaseLoadBalancer:
    """
    Load balancer for multiple database instances
    For read replicas and high availability
    """
    
    def __init__(self, configs: List[DatabaseConfig]):
        self.handlers = []
        self.current_index = 0
        
        for config in configs:
            handler = ScalableDatabaseHandler(config)
            self.handlers.append(handler)
    
    async def initialize_all(self):
        """Initialize all database handlers"""
        for handler in self.handlers:
            await handler.initialize_async()
    
    async def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict]:
        """
        Execute query with round-robin load balancing
        Write queries go to primary, reads are distributed
        """
        # Determine if it's a write query
        is_write = any(keyword in sql.upper() for keyword in ['INSERT', 'UPDATE', 'DELETE'])
        
        if is_write:
            # Always use primary (first handler)
            return await self.handlers[0].execute_query(sql, params)
        else:
            # Round-robin for read queries
            handler = self.handlers[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.handlers)
            return await handler.execute_query(sql, params)
    
    async def get_all_stats(self) -> List[Dict]:
        """Get stats from all handlers"""
        stats = []
        for i, handler in enumerate(self.handlers):
            handler_stats = await handler.get_pool_stats()
            handler_stats['instance'] = i
            stats.append(handler_stats)
        return stats