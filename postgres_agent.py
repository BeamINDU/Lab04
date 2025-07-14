import psycopg2
import json
import os
from typing import Dict, List, Any, Optional
import boto3
import logging
from contextlib import contextmanager
from tenant_manager import get_tenant_config, get_tenant_database_config

logger = logging.getLogger(__name__)

class PostgreSQLAgent:
    def __init__(self, tenant_id: Optional[str] = None):
        # Current tenant
        self.current_tenant_id = tenant_id
        self._connection_pools = {}  # Store connections per tenant
        
        # Claude for SQL generation
        self.bedrock_runtime = boto3.client(
            'bedrock-runtime',
            region_name=os.getenv('AWS_DEFAULT_REGION', 'ap-southeast-1')
        )
        
        # Database schema information (same for all tenants)
        self.schema_info = {
            "employees": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสพนักงาน"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อพนักงาน"},
                    {"name": "department", "type": "VARCHAR", "description": "แผนก (IT, Sales, HR, Management)"},
                    {"name": "position", "type": "VARCHAR", "description": "ตำแหน่งงาน"},
                    {"name": "salary", "type": "DECIMAL", "description": "เงินเดือน"},
                    {"name": "hire_date", "type": "DATE", "description": "วันที่เข้าทำงาน"},
                    {"name": "email", "type": "VARCHAR", "description": "อีเมล"}
                ],
                "description": "ข้อมูลพนักงานบริษัท"
            },
            "projects": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "รหัสโปรเจค"},
                    {"name": "name", "type": "VARCHAR", "description": "ชื่อโปรเจค"},
                    {"name": "client", "type": "VARCHAR", "description": "ลูกค้า"},
                    {"name": "budget", "type": "DECIMAL", "description": "งบประมาณ"},
                    {"name": "status", "type": "VARCHAR", "description": "สถานะ (active, completed, cancelled)"},
                    {"name": "start_date", "type": "DATE", "description": "วันเริ่มโปรเจค"},
                    {"name": "end_date", "type": "DATE", "description": "วันสิ้นสุดโปรเจค"},
                    {"name": "tech_stack", "type": "VARCHAR", "description": "เทคโนโลยีที่ใช้"}
                ],
                "description": "ข้อมูลโปรเจคของบริษัท"
            },
            "employee_projects": {
                "columns": [
                    {"name": "employee_id", "type": "INTEGER", "description": "รหัสพนักงาน"},
                    {"name": "project_id", "type": "INTEGER", "description": "รหัสโปรเจค"},
                    {"name": "role", "type": "VARCHAR", "description": "บทบาทในโปรเจค"},
                    {"name": "allocation", "type": "DECIMAL", "description": "สัดส่วนงาน (0-1)"}
                ],
                "description": "ความสัมพันธ์ระหว่างพนักงานและโปรเจค"
            }
        }

    def set_tenant(self, tenant_id: str):
        """Set current tenant for operations"""
        self.current_tenant_id = tenant_id
        logger.info(f"PostgreSQL Agent switched to tenant: {tenant_id}")

    def get_current_tenant_id(self) -> str:
        """Get current tenant ID"""
        if not self.current_tenant_id:
            raise ValueError("No tenant ID set. Call set_tenant() first.")
        return self.current_tenant_id

    def get_tenant_db_config(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Get database configuration for specified tenant"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            db_config = get_tenant_database_config(tenant_id)
            logger.debug(f"Retrieved DB config for tenant: {tenant_id}")
            return db_config
        except Exception as e:
            logger.error(f"Failed to get DB config for tenant {tenant_id}: {e}")
            raise

    @contextmanager
    def get_connection(self, tenant_id: Optional[str] = None):
        """Get database connection for specified tenant with context manager"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        conn = None
        try:
            db_config = self.get_tenant_db_config(tenant_id)
            conn = psycopg2.connect(**db_config)
            logger.debug(f"Database connection established for tenant: {tenant_id}")
            yield conn
        except Exception as e:
            logger.error(f"Database connection error for tenant {tenant_id}: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
                logger.debug(f"Database connection closed for tenant: {tenant_id}")

    def generate_sql(self, question: str, tenant_id: Optional[str] = None) -> str:
        """สร้าง SQL จาก natural language question"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        # Get tenant info for context
        tenant_config = get_tenant_config(tenant_id)
        
        # สร้าง schema description
        schema_desc = ""
        for table_name, table_info in self.schema_info.items():
            schema_desc += f"\nTable: {table_name}\n"
            schema_desc += f"Description: {table_info['description']}\n"
            schema_desc += "Columns:\n"
            for col in table_info['columns']:
                schema_desc += f"  - {col['name']} ({col['type']}): {col['description']}\n"
        
        # Add tenant context to prompt
        tenant_context = f"""
บริบทของบริษัท: {tenant_config.name}
Database: {tenant_config.database_config['database']}
"""
        
        prompt = f"""คุณเป็น SQL expert ที่เชี่ยวชาญในการแปลงคำถามภาษาไทยเป็น SQL query

{tenant_context}

Database Schema:
{schema_desc}

คำถาม: {question}

กรุณาสร้าง SQL query ที่ถูกต้อง โดย:
1. ใช้เฉพาะ tables และ columns ที่มีใน schema
2. ตอบให้ตรงกับคำถาม
3. ใช้ JOIN เมื่อจำเป็น
4. ส่งคืนเฉพาะ SQL query เท่านั้น ไม่ต้องอธิบาย

SQL Query:"""

        try:
            # Get tenant-specific API keys if available
            tenant_config = get_tenant_config(tenant_id)
            api_keys = tenant_config.api_keys
            
            claude_request = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 500,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            response = self.bedrock_runtime.invoke_model(
                modelId='apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
                body=json.dumps(claude_request),
                contentType='application/json'
            )
            
            response_body = json.loads(response['body'].read())
            sql_query = response_body['content'][0]['text'].strip()
            
            # Clean up SQL query (remove markdown formatting if any)
            if sql_query.startswith('```'):
                sql_query = sql_query.split('\n')[1:-1]
                sql_query = '\n'.join(sql_query)
            
            logger.debug(f"Generated SQL for tenant {tenant_id}: {sql_query}")
            return sql_query
            
        except Exception as e:
            logger.error(f"Error generating SQL for tenant {tenant_id}: {e}")
            return None

    def execute_query(self, sql: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Execute SQL query และส่งคืนผลลัพธ์"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            with self.get_connection(tenant_id) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # Get results
                if sql.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    
                    # Convert to list of dictionaries
                    data = []
                    for row in results:
                        data.append(dict(zip(columns, row)))
                    
                    logger.info(f"Query executed successfully for tenant {tenant_id}, returned {len(data)} rows")
                    
                    return {
                        "success": True,
                        "data": data,
                        "columns": columns,
                        "row_count": len(data),
                        "tenant_id": tenant_id
                    }
                else:
                    # For non-SELECT queries
                    conn.commit()
                    logger.info(f"Non-SELECT query executed for tenant {tenant_id}")
                    
                    return {
                        "success": True,
                        "message": "Query executed successfully",
                        "affected_rows": cursor.rowcount,
                        "tenant_id": tenant_id
                    }
                    
        except Exception as e:
            logger.error(f"Query execution error for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": None,
                "tenant_id": tenant_id
            }

    def format_results(self, results: Dict[str, Any], question: str, tenant_id: Optional[str] = None) -> str:
        """แปลงผลลัพธ์ SQL เป็นคำตอบภาษาไทยที่เข้าใจง่าย"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        if not results["success"]:
            return f"ขออภัย เกิดข้อผิดพลาดในการค้นหาข้อมูล: {results.get('error', 'Unknown error')}"
        
        data = results.get("data", [])
        if not data:
            return "ไม่พบข้อมูลที่ตรงกับคำถามของคุณ"
        
        # Get tenant info for context
        tenant_config = get_tenant_config(tenant_id)
        
        # สร้าง prompt สำหรับ Claude ในการแปลงผลลัพธ์
        prompt = f"""บริษัท: {tenant_config.name}
คำถาม: {question}

ผลลัพธ์จากฐานข้อมูล:
{json.dumps(data, ensure_ascii=False, indent=2, default=str)}

กรุณาสรุปผลลัพธ์เป็นคำตอบภาษาไทยที่เข้าใจง่าย โดย:
1. ตอบตรงประเด็นคำถาม
2. แสดงตัวเลขและข้อมูลที่สำคัญ
3. ใช้ภาษาที่เป็นธรรมชาติ
4. หากมีข้อมูลมาก ให้สรุปเป็นสาระสำคัญ
5. ระบุว่าข้อมูลมาจากบริษัท {tenant_config.name}

คำตอบ:"""

        try:
            claude_request = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            response = self.bedrock_runtime.invoke_model(
                modelId='apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
                body=json.dumps(claude_request),
                contentType='application/json'
            )
            
            response_body = json.loads(response['body'].read())
            answer = response_body['content'][0]['text'].strip()
            
            logger.debug(f"Formatted results for tenant {tenant_id}")
            return answer
            
        except Exception as e:
            logger.error(f"Error formatting results for tenant {tenant_id}: {e}")
            # Fallback to simple formatting
            return self._simple_format(data, question, tenant_config.name)

    def _simple_format(self, data: List[Dict], question: str, tenant_name: str) -> str:
        """Simple formatting fallback"""
        if len(data) == 1 and len(data[0]) == 1:
            # Single value result
            value = list(data[0].values())[0]
            return f"ผลลัพธ์จาก {tenant_name}: {value}"
        
        # Multiple results
        result_text = f"ข้อมูลจาก {tenant_name} - พบข้อมูลทั้งหมด {len(data)} รายการ:\n\n"
        for i, row in enumerate(data[:5], 1):  # Show first 5 results
            result_text += f"{i}. "
            result_text += ", ".join([f"{k}: {v}" for k, v in row.items()])
            result_text += "\n"
        
        if len(data) > 5:
            result_text += f"\n... และอีก {len(data) - 5} รายการ"
        
        return result_text

    def query(self, question: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Main method สำหรับรับคำถามและส่งคืนคำตอบ"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            # Generate SQL
            sql = self.generate_sql(question, tenant_id)
            if not sql:
                return {
                    "success": False,
                    "answer": "ไม่สามารถสร้าง SQL query จากคำถามนี้ได้",
                    "sql": None,
                    "data": None,
                    "tenant_id": tenant_id
                }
            
            logger.info(f"Generated SQL for tenant {tenant_id}: {sql}")
            
            # Execute query
            results = self.execute_query(sql, tenant_id)
            
            # Format answer
            if results["success"]:
                answer = self.format_results(results, question, tenant_id)
                return {
                    "success": True,
                    "answer": answer,
                    "sql": sql,
                    "data": results["data"],
                    "tenant_id": tenant_id
                }
            else:
                return {
                    "success": False,
                    "answer": f"เกิดข้อผิดพลาดในการดึงข้อมูล: {results.get('error')}",
                    "sql": sql,
                    "data": None,
                    "tenant_id": tenant_id
                }
                
        except Exception as e:
            logger.error(f"Error in query for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "answer": f"เกิดข้อผิดพลาด: {str(e)}",
                "sql": None,
                "data": None,
                "tenant_id": tenant_id
            }

    def test_connection(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Test database connection for a tenant"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            with self.get_connection(tenant_id) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 as test, current_database() as db_name")
                result = cursor.fetchone()
                
                return {
                    "success": True,
                    "tenant_id": tenant_id,
                    "database": result[1] if result else "unknown",
                    "message": "Connection successful"
                }
        except Exception as e:
            logger.error(f"Connection test failed for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "tenant_id": tenant_id,
                "error": str(e),
                "message": "Connection failed"
            }

    def get_tenant_stats(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Get database statistics for a tenant"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            stats = {}
            with self.get_connection(tenant_id) as conn:
                cursor = conn.cursor()
                
                # Get table counts
                for table_name in self.schema_info.keys():
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    stats[f"{table_name}_count"] = count
                
                # Get database size
                cursor.execute("""
                    SELECT pg_size_pretty(pg_database_size(current_database())) as db_size
                """)
                db_size = cursor.fetchone()[0]
                stats['database_size'] = db_size
                
                return {
                    "success": True,
                    "tenant_id": tenant_id,
                    "stats": stats
                }
                
        except Exception as e:
            logger.error(f"Failed to get stats for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "tenant_id": tenant_id,
                "error": str(e)
            }


# Multi-tenant convenience functions
def create_postgres_agent(tenant_id: str) -> PostgreSQLAgent:
    """Create PostgreSQL agent for specific tenant"""
    agent = PostgreSQLAgent(tenant_id)
    return agent

def query_postgres_for_tenant(question: str, tenant_id: str) -> Dict[str, Any]:
    """Quick query function for specific tenant"""
    agent = create_postgres_agent(tenant_id)
    return agent.query(question, tenant_id)

def test_tenant_database(tenant_id: str) -> Dict[str, Any]:
    """Test database connection for specific tenant"""
    agent = create_postgres_agent(tenant_id)
    return agent.test_connection(tenant_id)


# Test usage
if __name__ == "__main__":
    # Test multi-tenant functionality
    test_tenants = ['company-a', 'company-b', 'company-c']
    test_questions = [
        "มีพนักงานกี่คน?",
        "เงินเดือนเฉลี่ยของพนักงานเท่าไหร่?",
        "โปรเจคที่กำลังดำเนินการอยู่มีอะไรบ้าง?"
    ]
    
    for tenant_id in test_tenants:
        print(f"\n{'='*60}")
        print(f"🏢 Testing Tenant: {tenant_id}")
        print(f"{'='*60}")
        
        # Test connection
        try:
            connection_result = test_tenant_database(tenant_id)
            print(f"🔗 Connection: {connection_result}")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            continue
        
        # Test queries
        agent = create_postgres_agent(tenant_id)
        for question in test_questions:
            print(f"\n❓ คำถาม: {question}")
            try:
                result = agent.query(question, tenant_id)
                print(f"✅ คำตอบ: {result['answer']}")
                if result.get('sql'):
                    print(f"🔍 SQL: {result['sql']}")
            except Exception as e:
                print(f"❌ Error: {e}")
        
        # Get stats
        try:
            stats = agent.get_tenant_stats(tenant_id)
            if stats['success']:
                print(f"\n📊 Stats: {stats['stats']}")
        except Exception as e:
            print(f"❌ Stats error: {e}")
        
        print("-" * 60)