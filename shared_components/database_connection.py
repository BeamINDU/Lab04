# shared_components/database_connection.py
# Database Connection Handler สำหรับระบบ HVAC AI

import os
import psycopg2
from typing import Dict, Any, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseConnectionManager:
    """จัดการการเชื่อมต่อฐานข้อมูลสำหรับระบบ Multi-tenant"""
    
    def __init__(self):
        self.connection_cache = {}
        self.tenant_configs = self._load_tenant_db_configs()
        
    def _load_tenant_db_configs(self) -> Dict[str, Dict[str, Any]]:
        """โหลดการตั้งค่าฐานข้อมูลของแต่ละ tenant"""
        
        return {
            'company-a': {
                'host': os.getenv('POSTGRES_HOST_COMPANY_A', 'postgres-company-a'),
                'port': int(os.getenv('POSTGRES_PORT_COMPANY_A', '5432')),
                'database': os.getenv('POSTGRES_DB_COMPANY_A', 'siamtemp_company_a'),
                'user': os.getenv('POSTGRES_USER_COMPANY_A', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD_COMPANY_A', 'password123'),
                'name': 'Siamtemp Bangkok HQ'
            },
            'company-b': {
                'host': os.getenv('POSTGRES_HOST_COMPANY_B', 'postgres-company-b'),
                'port': int(os.getenv('POSTGRES_PORT_COMPANY_B', '5433')),
                'database': os.getenv('POSTGRES_DB_COMPANY_B', 'siamtemp_company_b'),
                'user': os.getenv('POSTGRES_USER_COMPANY_B', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD_COMPANY_B', 'password123'),
                'name': 'Siamtemp Chiang Mai Regional'
            },
            'company-c': {
                'host': os.getenv('POSTGRES_HOST_COMPANY_C', 'postgres-company-c'),
                'port': int(os.getenv('POSTGRES_PORT_COMPANY_C', '5434')),
                'database': os.getenv('POSTGRES_DB_COMPANY_C', 'siamtemp_company_c'),
                'user': os.getenv('POSTGRES_USER_COMPANY_C', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD_COMPANY_C', 'password123'),
                'name': 'Siamtemp International'
            }
        }
    
    def get_connection(self, tenant_id: str):
        """สร้างการเชื่อมต่อฐานข้อมูลสำหรับ tenant ที่ระบุ"""
        
        if tenant_id not in self.tenant_configs:
            raise ValueError(f"Unknown tenant_id: {tenant_id}")
        
        config = self.tenant_configs[tenant_id]
        
        try:
            connection = psycopg2.connect(
                host=config['host'],
                port=config['port'],
                database=config['database'],
                user=config['user'],
                password=config['password']
            )
            
            logger.info(f"Connected to {config['name']} database")
            return connection
            
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to {tenant_id} database: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected database connection error for {tenant_id}: {e}")
            raise
    
    def test_connection(self, tenant_id: str) -> bool:
        """ทดสอบการเชื่อมต่อฐานข้อมูล"""
        
        try:
            conn = self.get_connection(tenant_id)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            logger.info(f"Database connection test successful for {tenant_id}")
            return result[0] == 1
            
        except Exception as e:
            logger.error(f"Database connection test failed for {tenant_id}: {e}")
            return False
    
    def get_database_info(self, tenant_id: str) -> Dict[str, Any]:
        """ดึงข้อมูลเกี่ยวกับฐานข้อมูล"""
        
        try:
            conn = self.get_connection(tenant_id)
            cursor = conn.cursor()
            
            # Database version
            cursor.execute("SELECT version()")
            db_version = cursor.fetchone()[0]
            
            # Table count
            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            table_count = cursor.fetchone()[0]
            
            # Database size
            cursor.execute(f"""
                SELECT pg_size_pretty(pg_database_size('{self.tenant_configs[tenant_id]["database"]}'))
            """)
            db_size = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            return {
                'tenant_id': tenant_id,
                'tenant_name': self.tenant_configs[tenant_id]['name'],
                'database': self.tenant_configs[tenant_id]['database'],
                'version': db_version,
                'table_count': table_count,
                'size': db_size,
                'connection_test': 'passed',
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get database info for {tenant_id}: {e}")
            return {
                'tenant_id': tenant_id,
                'error': str(e),
                'connection_test': 'failed',
                'timestamp': datetime.now().isoformat()
            }

# shared_components/__init__.py
from .database_connection import DatabaseConnectionManager

__all__ = ['DatabaseConnectionManager']

# แก้ไขใน dual_model_dynamic_ai.py
# เปลี่ยนจาก:
def _create_sync_connection(self, tenant_id: str):
    """🔗 สร้าง sync database connection (ไม่ใช้ async)"""
    
    try:
        import psycopg2
        
        # ใช้ environment variables หรือ default values
        connection_params = {
            'host': os.getenv(f'POSTGRES_HOST_{tenant_id.upper().replace("-", "_")}', 'postgres-company-a'),
            'port': int(os.getenv(f'POSTGRES_PORT_{tenant_id.upper().replace("-", "_")}', '5432')),
            'database': os.getenv(f'POSTGRES_DB_{tenant_id.upper().replace("-", "_")}', 'siamtemp_company_a'),
            'user': os.getenv(f'POSTGRES_USER_{tenant_id.upper().replace("-", "_")}', 'postgres'),
            'password': os.getenv(f'POSTGRES_PASSWORD_{tenant_id.upper().replace("-", "_")}', 'password123')
        }
        
        logger.info(f"🔗 Creating sync connection to {connection_params['host']}:{connection_params['port']}")
        return psycopg2.connect(**connection_params)
        
    except Exception as e:
        logger.error(f"❌ Sync connection failed: {e}")
        raise

# เป็น:
def _create_sync_connection(self, tenant_id: str):
    """🔗 สร้าง sync database connection ผ่าน shared component"""
    
    try:
        from shared_components import DatabaseConnectionManager
        
        db_manager = DatabaseConnectionManager()
        connection = db_manager.get_connection(tenant_id)
        
        logger.info(f"🔗 Connected via DatabaseConnectionManager for {tenant_id}")
        return connection
        
    except Exception as e:
        logger.error(f"❌ Shared connection failed: {e}")
        raise

# แก้ไขใน enhanced_multi_agent_service.py
# เพิ่ม endpoint ใหม่สำหรับ database testing

@app.get("/database-info/{tenant_id}")
async def get_database_info(tenant_id: str):
    """ดึงข้อมูลฐานข้อมูลของ tenant ที่ระบุ"""
    
    try:
        from shared_components import DatabaseConnectionManager
        
        db_manager = DatabaseConnectionManager()
        info = db_manager.get_database_info(tenant_id)
        
        return info
        
    except Exception as e:
        return {
            "error": f"Failed to get database info: {str(e)}",
            "tenant_id": tenant_id
        }

@app.get("/database-test/{tenant_id}")
async def test_database_connection(tenant_id: str):
    """ทดสอบการเชื่อมต่อฐานข้อมูล"""
    
    try:
        from shared_components import DatabaseConnectionManager
        
        db_manager = DatabaseConnectionManager()
        test_result = db_manager.test_connection(tenant_id)
        
        return {
            "tenant_id": tenant_id,
            "connection_test": "passed" if test_result else "failed",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "tenant_id": tenant_id,
            "connection_test": "failed",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }