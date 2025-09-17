# agents/llm/database_wrapper.py
"""
Database Wrapper ที่จัดการปัญหา async/sync
"""

import asyncio
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class SyncDatabaseWrapper:
    """
    Wrapper ที่แปลง async database calls เป็น sync calls
    สำหรับใช้ใน LLM Orchestrator
    """
    
    def __init__(self, async_db_handler):
        self.async_db = async_db_handler
        
    def execute_query(self, sql: str) -> List[Dict]:
        """
        Execute query synchronously by running async call in event loop
        """
        try:
            # ตรวจสอบว่ามี event loop รันอยู่หรือไม่
            try:
                loop = asyncio.get_running_loop()
                # ถ้ามี loop รันอยู่ ให้สร้าง task ใหม่
                task = asyncio.create_task(self.async_db.execute_query(sql))
                # รันจนเสร็จ (อันตราย แต่จำเป็น)
                return asyncio.run_coroutine_threadsafe(
                    self.async_db.execute_query(sql), loop
                ).result(timeout=60)
                
            except RuntimeError:
                # ไม่มี event loop รันอยู่ ให้สร้างใหม่
                return asyncio.run(self.async_db.execute_query(sql))
                
        except Exception as e:
            logger.error(f"Database wrapper error: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            result = self.execute_query("SELECT 1 as test LIMIT 1;")
            return len(result) > 0
        except:
            return False

# Alternative: Force sync wrapper
class ForceSyncDatabaseWrapper:
    """
    Force synchronous wrapper - ใช้เมื่อ async ไม่ทำงาน
    """
    
    def __init__(self, async_db_handler):
        self.async_db = async_db_handler
        # สร้าง connection ใหม่แบบ sync
        self._create_sync_connection()
    
    def _create_sync_connection(self):
        """สร้าง sync connection แยก"""
        import os
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        try:
            db_config = {
                'host': os.getenv('DB_HOST', 'postgres-company-a'),
                'port': os.getenv('DB_PORT', '5432'),
                'database': os.getenv('DB_NAME', 'siamtemp_company_a'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'password123')
            }
            
            self.sync_connection = psycopg2.connect(
                **db_config,
                cursor_factory=RealDictCursor
            )
            
            logger.info("✅ Sync database wrapper connected")
            
        except Exception as e:
            logger.error(f"❌ Sync wrapper connection failed: {e}")
            self.sync_connection = None
    
    def execute_query(self, sql: str) -> List[Dict]:
        """Execute query synchronously"""
        if not self.sync_connection:
            self._create_sync_connection()
            if not self.sync_connection:
                raise ConnectionError("Cannot connect to database")
        
        try:
            with self.sync_connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Sync query failed: {e}")
            # Try to reconnect
            self._create_sync_connection()
            raise
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            result = self.execute_query("SELECT 1 as test LIMIT 1;")
            return len(result) > 0
        except:
            return False
    
    def close(self):
        """Close sync connection"""
        if self.sync_connection:
            self.sync_connection.close()
            logger.info("Sync database wrapper closed")

# สำหรับใช้ใน LLM Orchestrator
def create_database_wrapper(async_db_handler):
    """
    สร้าง database wrapper ที่เหมาะสม
    """
    try:
        # ลองใช้ sync wrapper ก่อน
        wrapper = SyncDatabaseWrapper(async_db_handler)
        if wrapper.test_connection():
            logger.info("Using SyncDatabaseWrapper")
            return wrapper
    except Exception as e:
        logger.warning(f"SyncDatabaseWrapper failed: {e}")
    
    try:
        # ใช้ force sync wrapper
        wrapper = ForceSyncDatabaseWrapper(async_db_handler)
        if wrapper.test_connection():
            logger.info("Using ForceSyncDatabaseWrapper")
            return wrapper
    except Exception as e:
        logger.error(f"ForceSyncDatabaseWrapper failed: {e}")
    
    # ถ้าไม่ได้ ให้ใช้ async db ตรงๆ (อาจมีปัญหา)
    logger.warning("Using async database handler directly")
    return async_db_handler