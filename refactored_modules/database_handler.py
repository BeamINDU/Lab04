import psycopg2
import time
import json
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime, timedelta, date  # ✅ Fixed import
from .tenant_config import TenantConfig

logger = logging.getLogger(__name__)

class EnhancedDatabaseHandler:
    """🗄️ Enhanced Database Handler with quick fixes"""
    
    def __init__(self, tenant_configs: Dict[str, TenantConfig]):
        self.tenant_configs = tenant_configs
        self.connection_pool = {}
        self.schema_cache = {}
        self.query_stats = {}
        self.performance_cache = {}
        
        # Configuration
        self.cache_ttl = 3600  # 1 hour
        self.max_connection_retries = 3
        self.query_timeout = 30  # seconds
        
        logger.info("✅ Enhanced Database Handler initialized")
    
    # ========================================================================
    # 🔌 SIMPLIFIED CONNECTION MANAGEMENT
    # ========================================================================
    
    def get_database_connection(self, tenant_id: str, retry_count: int = 0) -> psycopg2.extensions.connection:
        """Simplified database connection"""
        
        if tenant_id not in self.tenant_configs:
            raise ValueError(f"Unknown tenant: {tenant_id}")
            
        config = self.tenant_configs[tenant_id]
        
        try:
            logger.info(f"🔌 Creating database connection for {tenant_id}")
            conn = psycopg2.connect(
                host=config.db_host,
                port=config.db_port,
                database=config.db_name,
                user=config.db_user,
                password=config.db_password,
                connect_timeout=10
            )
            
            conn.set_session(autocommit=True)
            return conn
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to database for {tenant_id}: {e}")
            raise
    
    # ========================================================================
    # 🎯 SIMPLIFIED QUERY EXECUTION
    # ========================================================================
    
    def execute_sql_query(self, tenant_id: str, query: str) -> List[Dict[str, Any]]:
        """Simplified SQL execution with basic error handling"""
        
        start_time = time.time()
        
        try:
            logger.info(f"🗄️ Executing SQL for {tenant_id}: {query[:100]}...")
            
            conn = self.get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            
            # Process results with FIXED Decimal handling
            results = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                processed_row = self._process_row_data_simple(row_dict)
                results.append(processed_row)
            
            cursor.close()
            conn.close()
            
            execution_time = time.time() - start_time
            logger.info(f"✅ Query executed successfully: {len(results)} rows in {execution_time:.2f}s")
            return results
            
        except Exception as e:
            logger.error(f"❌ Unexpected error executing SQL for {tenant_id}: {e}")
            raise
    
    def _process_row_data_simple(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """🔧 SIMPLIFIED row data processing"""
        
        processed_row = {}
        
        for key, value in row_dict.items():
            # Handle Decimal conversion
            if isinstance(value, Decimal):
                processed_row[key] = float(value)
            
            # Handle date/datetime formatting - FIXED
            elif isinstance(value, (date, datetime)):  # ✅ Fixed datetime usage
                processed_row[key] = value.isoformat()
            
            # Handle None values
            elif value is None:
                processed_row[key] = None
            
            # Handle string trimming
            elif isinstance(value, str):
                processed_row[key] = value.strip()
            
            # Keep other types as-is
            else:
                processed_row[key] = value
        
        return processed_row
    
    # ========================================================================
    # 🔍 SIMPLIFIED SCHEMA DISCOVERY
    # ========================================================================
    
    async def get_live_schema_info(self, tenant_id: str) -> Dict[str, Any]:
        """Simplified schema discovery"""
        
        cache_key = f"{tenant_id}_schema"
        
        # Check cache
        if self._is_schema_cache_valid(cache_key):
            logger.info(f"📊 Using cached schema for {tenant_id}")
            return self.schema_cache[cache_key]['data']
        
        try:
            logger.info(f"🔍 Discovering schema for {tenant_id}")
            schema_info = await self._discover_basic_schema(tenant_id)
            
            # Cache the results
            self.schema_cache[cache_key] = {
                'data': schema_info,
                'timestamp': time.time()
            }
            
            return schema_info
            
        except Exception as e:
            logger.error(f"❌ Schema discovery failed for {tenant_id}: {e}")
            return self._get_fallback_schema(tenant_id)
    
    async def _discover_basic_schema(self, tenant_id: str) -> Dict[str, Any]:
        """Basic schema discovery"""
        
        try:
            conn = self.get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            schema_info = {
                'tables': {},
                'sample_data': {},
                'discovered_at': datetime.now().isoformat(),
                'tenant_id': tenant_id
            }
            
            # Get basic table structure
            cursor.execute("""
                SELECT table_name, column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name IN ('employees', 'projects', 'employee_projects')
                ORDER BY table_name, ordinal_position
            """)
            
            for row in cursor.fetchall():
                table_name, column_name, data_type, is_nullable = row
                
                if table_name not in schema_info['tables']:
                    schema_info['tables'][table_name] = {'columns': []}
                
                schema_info['tables'][table_name]['columns'].append({
                    'name': column_name,
                    'type': data_type,
                    'nullable': is_nullable == 'YES'
                })
            
            # Get sample data for important columns
            await self._get_basic_sample_data(cursor, schema_info)
            
            cursor.close()
            conn.close()
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Basic schema discovery error: {e}")
            raise
    
    async def _get_basic_sample_data(self, cursor, schema_info: Dict):
        """Get basic sample data"""
        
        try:
            # Get sample departments
            cursor.execute("SELECT DISTINCT department FROM employees LIMIT 5")
            departments = [row[0] for row in cursor.fetchall()]
            
            # Get sample positions
            cursor.execute("SELECT DISTINCT position FROM employees LIMIT 10")
            positions = [row[0] for row in cursor.fetchall()]
            
            schema_info['sample_data'] = {
                'employees': {
                    'department': departments,
                    'position': positions
                }
            }
            
        except Exception as e:
            logger.warning(f"Could not get sample data: {e}")
            schema_info['sample_data'] = {}
    
    def _is_schema_cache_valid(self, cache_key: str) -> bool:
        """Check if schema cache is still valid"""
        
        if cache_key not in self.schema_cache:
            return False
        
        cache_age = time.time() - self.schema_cache[cache_key]['timestamp']
        return cache_age < self.cache_ttl
    
    def _get_fallback_schema(self, tenant_id: str) -> Dict[str, Any]:
        """Fallback schema when discovery fails"""
        
        return {
            'tables': {
                'employees': {
                    'columns': [
                        {'name': 'id', 'type': 'integer', 'nullable': False},
                        {'name': 'name', 'type': 'character varying', 'nullable': False},
                        {'name': 'department', 'type': 'character varying', 'nullable': False},
                        {'name': 'position', 'type': 'character varying', 'nullable': False},
                        {'name': 'salary', 'type': 'numeric', 'nullable': False}
                    ]
                }
            },
            'sample_data': {
                'employees': {
                    'department': ['IT', 'Sales', 'Management'],
                    'position': ['Frontend Developer', 'Backend Developer', 'Designer']
                }
            },
            'discovered_at': datetime.now().isoformat(),
            'tenant_id': tenant_id,
            'fallback': True
        }
    
    # ========================================================================
    # 🔧 SIMPLIFIED VALIDATION
    # ========================================================================
    
    def validate_sql_query(self, sql: str, tenant_id: str) -> Dict[str, Any]:
        """Simplified SQL validation"""
        
        validation_result = {
            'is_valid': False,
            'error': None,
            'warnings': []
        }
        
        try:
            sql_upper = sql.upper().strip()
            
            # Basic security checks
            dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
            for keyword in dangerous_keywords:
                if keyword in sql_upper:
                    validation_result['error'] = f"Dangerous operation detected: {keyword}"
                    return validation_result
            
            # Must be SELECT
            if not sql_upper.startswith('SELECT'):
                validation_result['error'] = "Only SELECT queries are allowed"
                return validation_result
            
            # Check for LIMIT
            if 'LIMIT' not in sql_upper:
                validation_result['warnings'].append("Query without LIMIT may return many rows")
            
            validation_result['is_valid'] = True
            return validation_result
            
        except Exception as e:
            validation_result['error'] = f"Validation error: {str(e)}"
            return validation_result
    
    # ========================================================================
    # 📊 SIMPLIFIED HEALTH CHECK
    # ========================================================================
    
    async def health_check(self) -> Dict[str, Any]:
        """Simplified health check"""
        
        health_status = {
            'overall_status': 'healthy',
            'tenant_status': {},
            'last_check': datetime.now().isoformat()
        }
        
        for tenant_id in self.tenant_configs.keys():
            try:
                conn = self.get_database_connection(tenant_id)
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                conn.close()
                
                health_status['tenant_status'][tenant_id] = 'healthy'
                
            except Exception as e:
                health_status['tenant_status'][tenant_id] = f'unhealthy: {str(e)}'
                health_status['overall_status'] = 'degraded'
        
        return health_status

# Create alias for backward compatibility
DatabaseHandler = EnhancedDatabaseHandler