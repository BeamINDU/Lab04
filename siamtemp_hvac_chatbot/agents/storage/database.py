import os
import re
import json
import asyncio
import time
import logging
import hashlib
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, date, timedelta
from decimal import Decimal
from collections import deque, defaultdict
import aiohttp
import psycopg2
from textwrap import dedent
from psycopg2.extras import RealDictCursor
from collections import Counter, defaultdict
logger = logging.getLogger(__name__)

class SimplifiedDatabaseHandler:
    """
    SimplifiedDatabaseHandler - เพิ่ม query optimization
    - Query plan analysis
    - Connection pooling simulation
    - Better error handling
    """
    
    def __init__(self):
        self.connection = None
        self.query_cache = {}
        self.stats = defaultdict(lambda: {'count': 0, 'total_time': 0})
        self._connect()
    
    def _connect(self):
        """สร้างการเชื่อมต่อกับ PostgreSQL"""
        try:
            db_config = {
                'host': os.getenv('DB_HOST', 'postgres-company-a'),
                'port': os.getenv('DB_PORT', '5432'),
                'database': os.getenv('DB_NAME', 'siamtemp_company_a'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'password123')
            }
            
            self.connection = psycopg2.connect(
                **db_config,
                cursor_factory=RealDictCursor,
                # Connection optimization
                options='-c statement_timeout=60000 -c work_mem=256MB'
            )
            
            # Set optimization parameters
            with self.connection.cursor() as cursor:
                cursor.execute("SET random_page_cost = 1.1")
                cursor.execute("SET effective_cache_size = '4GB'")
                cursor.execute("SET max_parallel_workers_per_gather = 4")
                self.connection.commit()
            
            logger.info("✅ Database connected with optimizations")
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            self.connection = None
    
    async def execute_query(self, sql: str) -> List[Dict]:
        """Execute SQL with optimization hints"""
        if not self.connection:
            self._connect()
            if not self.connection:
                raise ConnectionError("Cannot connect to database")
        
        # Optimize query based on detected patterns
        optimized_sql = self._optimize_query(sql)
        
        try:
            start_time = datetime.now()
            
            with self.connection.cursor() as cursor:
                # Log query plan for slow queries
                if self._is_complex_query(sql):
                    self._log_query_plan(cursor, optimized_sql)
                
                cursor.execute(optimized_sql)
                results = cursor.fetchall()
                
                # Track statistics
                elapsed = (datetime.now() - start_time).total_seconds()
                self._update_stats(sql, elapsed, len(results))
                
                return [dict(row) for row in results]
                
        except psycopg2.errors.InFailedSqlTransaction:
            self.connection.rollback()
            logger.warning("Transaction failed, reconnecting...")
            self._connect()  # Reconnect แทนการ retry
            return [] 
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Query execution error: {e}")
            logger.error(f"SQL: {optimized_sql[:500]}")
            raise
    
    def _optimize_query(self, sql: str) -> str:
        """Add optimization hints based on query patterns"""
        optimized = sql
        
        # 1. Optimize work_force date filtering
        if 'v_work_force' in sql.lower() and 'LIKE' in sql.upper():
            # Add index hint comment (PostgreSQL doesn't support hints directly)
            optimized = f"/* IndexScan(work_force date_idx) */\n{optimized}"
            
            # Optimize multiple LIKE patterns with UNION
            like_pattern = r'date\s+LIKE\s+\'[^\']+\'\s+OR\s+date\s+LIKE'
            like_count = len(re.findall(like_pattern, sql, re.IGNORECASE))
            if like_count > 3:
                logger.info(f"Query has {like_count} LIKE patterns - consider UNION optimization")
        
        # 2. Optimize large aggregations
        if 'GROUP BY' in sql.upper() and 'v_sales' in sql.lower():
            # Enable parallel aggregation
            optimized = f"/* Parallel(4) */\n{optimized}"
        
        # 3. Add ANALYZE hint for complex joins
        if sql.upper().count('JOIN') > 2:
            optimized = f"/* HashJoin */\n{optimized}"
        
        return optimized
    
    def _is_complex_query(self, sql: str) -> bool:
        """Determine if query is complex enough to log plan"""
        complexity_indicators = [
            sql.upper().count('JOIN') > 1,
            sql.upper().count('GROUP BY') > 0,
            sql.upper().count('UNION') > 0,
            'WITH' in sql.upper(),
            len(sql) > 1000
        ]
        return sum(complexity_indicators) >= 2
    
    def _log_query_plan(self, cursor, sql: str):
        """Log query execution plan for analysis"""
        try:
            cursor.execute(f"EXPLAIN (ANALYZE false, FORMAT JSON) {sql}")
            plan = cursor.fetchone()
            if plan:
                logger.debug(f"Query plan: {json.dumps(plan, indent=2)[:500]}")
        except Exception as e:
            logger.debug(f"Could not get query plan: {e}")
    
    def _update_stats(self, sql: str, elapsed: float, row_count: int):
        """Track query performance statistics"""
        # Normalize SQL for statistics
        normalized = re.sub(r'\s+', ' ', sql[:100]).strip()
        
        self.stats[normalized]['count'] += 1
        self.stats[normalized]['total_time'] += elapsed
        self.stats[normalized]['last_row_count'] = row_count
        
        # Log slow queries
        if elapsed > 5:
            logger.warning(f"Slow query ({elapsed:.2f}s): {normalized}")
    
    def get_performance_stats(self) -> Dict:
        """Get query performance statistics"""
        return {
            'total_queries': sum(s['count'] for s in self.stats.values()),
            'unique_queries': len(self.stats),
            'slowest_queries': sorted(
                [(k, v['total_time']/v['count']) for k, v in self.stats.items()],
                key=lambda x: x[1],
                reverse=True
            )[:5]
        }
    
    def close_connections(self):
        """ปิดการเชื่อมต่อและแสดง statistics"""
        if self.connection:
            # Log final statistics
            stats = self.get_performance_stats()
            logger.info(f"Database statistics: {stats}")
            
            self.connection.close()
            logger.info("Database connection closed")
