import json
import psycopg2
import boto3
import logging
from typing import Dict, Any, List, Optional
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum

from tenant_manager import get_tenant_config, get_tenant_database_config

logger = logging.getLogger(__name__)

# ============================================================================
# Schema & Configuration
# ============================================================================

class TableType(Enum):
    CORE = "core"
    ENHANCED = "enhanced"
    INTERNATIONAL = "international"
    VIEW = "view"

@dataclass
class TableInfo:
    name: str
    description: str
    key_columns: List[str]
    table_type: TableType

class SchemaRegistry:
    """Compact schema registry with essential tables only"""
    
    TABLES = {
        # Core Business Tables
        "employees": TableInfo(
            "employees", "ข้อมูลพนักงาน (id, name, department, position, salary, hire_date, email)",
            ["id", "name", "department", "position", "salary"], TableType.CORE
        ),
        "projects": TableInfo(
            "projects", "โปรเจคบริษัท (id, name, client, budget, status, start_date, end_date, tech_stack)",
            ["id", "name", "client", "budget", "status"], TableType.CORE
        ),
        "employee_projects": TableInfo(
            "employee_projects", "พนักงานในโปรเจค (employee_id, project_id, role, allocation)",
            ["employee_id", "project_id", "role"], TableType.CORE
        ),
        
        # Enhanced Tables
        "departments": TableInfo(
            "departments", "แผนกองค์กร (id, name, manager_id, budget, location)",
            ["id", "name", "manager_id", "budget"], TableType.ENHANCED
        ),
        "clients": TableInfo(
            "clients", "ลูกค้า (id, name, industry, contact_person, contract_value, country)",
            ["id", "name", "industry", "contract_value"], TableType.ENHANCED
        ),
        "timesheets": TableInfo(
            "timesheets", "บันทึกเวลา (employee_id, project_id, work_date, hours_worked, hourly_rate)",
            ["employee_id", "project_id", "hours_worked"], TableType.ENHANCED
        ),
        "expenses": TableInfo(
            "expenses", "ค่าใช้จ่าย (project_id, employee_id, amount, category, expense_date)",
            ["project_id", "amount", "category"], TableType.ENHANCED
        ),
        
        # Views
        "employee_performance": TableInfo(
            "employee_performance", "ประสิทธิภาพพนักงาน (VIEW)", 
            ["name", "projects_count", "total_hours"], TableType.VIEW
        ),
        "project_financial_summary": TableInfo(
            "project_financial_summary", "สรุปการเงินโปรเจค (VIEW)",
            ["name", "budget", "total_cost"], TableType.VIEW
        )
    }
    
    @classmethod
    def get_schema_prompt(cls, tenant_type: str = "standard") -> str:
        """Generate compact schema description"""
        tables = []
        for name, info in cls.TABLES.items():
            # Skip international tables for non-international tenants
            if info.table_type == TableType.INTERNATIONAL and tenant_type != "international":
                continue
            tables.append(f"{name}: {info.description}")
        
        return "\n".join(tables)

# ============================================================================
# SQL Generator - Ultra Compact
# ============================================================================

class SQLGenerator:
    """Compact SQL generation with Claude"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.config = get_tenant_config(tenant_id)
        self.bedrock = boto3.client('bedrock-runtime', region_name='ap-southeast-1')
        
        # Determine tenant type for schema
        self.tenant_type = "international" if tenant_id == "company-c" else "standard"
    
    def generate(self, question: str) -> Optional[str]:
        """Generate SQL from natural language question"""
        schema = SchemaRegistry.get_schema_prompt(self.tenant_type)
        
        prompt = f"""คุณเป็น SQL expert สำหรับบริษัท {self.config.name}

Schema: {schema}

คำถาม: {question}

กฎ:
1. ใช้เฉพาะตารางใน schema
2. JOIN เมื่อจำเป็น (LEFT JOIN สำหรับข้อมูลที่อาจไม่มี)
3. ใช้ WHERE, GROUP BY, ORDER BY ตามความเหมาะสม
4. ส่งคืนเฉพาะ SQL query

SQL:"""

        try:
            response = self.bedrock.invoke_model(
                modelId='apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 300,
                    "messages": [{"role": "user", "content": prompt}]
                })
            )
            
            result = json.loads(response['body'].read())
            sql = result['content'][0]['text'].strip()
            
            # Clean SQL
            if sql.startswith('```'):
                sql = '\n'.join(sql.split('\n')[1:-1])
            
            return sql
            
        except Exception as e:
            logger.error(f"SQL generation failed for {self.tenant_id}: {e}")
            return None

# ============================================================================
# Database Manager - Ultra Efficient
# ============================================================================

class DatabaseManager:
    """Efficient database operations with connection pooling"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self._db_config = None
    
    @property
    def db_config(self) -> Dict[str, Any]:
        """Lazy load database config"""
        if self._db_config is None:
            self._db_config = get_tenant_database_config(self.tenant_id)
        return self._db_config
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            yield conn
        except Exception as e:
            logger.error(f"DB connection error for {self.tenant_id}: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, sql: str) -> Dict[str, Any]:
        """Execute SQL and return structured results"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                
                if sql.strip().upper().startswith('SELECT'):
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    
                    # Convert to list of dicts
                    data = [dict(zip(columns, row)) for row in rows]
                    
                    return {
                        "success": True,
                        "data": data,
                        "columns": columns,
                        "row_count": len(data)
                    }
                else:
                    conn.commit()
                    return {
                        "success": True,
                        "message": "Query executed successfully",
                        "affected_rows": cursor.rowcount
                    }
                    
        except Exception as e:
            logger.error(f"Query execution failed for {self.tenant_id}: {e}")
            return {"success": False, "error": str(e), "data": None}
    
    def test_connection(self) -> Dict[str, Any]:
        """Test database connectivity"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1, current_database()")
                result = cursor.fetchone()
                
                return {
                    "success": True,
                    "database": result[1],
                    "tenant_id": self.tenant_id
                }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "tenant_id": self.tenant_id
            }

# ============================================================================
# Response Formatter - Smart & Compact
# ============================================================================

class ResponseFormatter:
    """Smart response formatting with Claude"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.config = get_tenant_config(tenant_id)
        self.bedrock = boto3.client('bedrock-runtime', region_name='ap-southeast-1')
    
    def format_results(self, results: Dict[str, Any], question: str) -> str:
        """Format SQL results into natural language"""
        if not results["success"]:
            return f"ขออภัย เกิดข้อผิดพลาด: {results.get('error', 'Unknown error')}"
        
        data = results.get("data", [])
        if not data:
            return "ไม่พบข้อมูลที่ตรงกับคำถาม"
        
        # Simple formatting for basic queries
        if len(data) == 1 and len(data[0]) == 1:
            value = list(data[0].values())[0]
            return f"ผลลัพธ์จาก {self.config.name}: {value}"
        
        # Use Claude for complex formatting
        try:
            prompt = f"""บริษัท: {self.config.name}
คำถาม: {question}
ข้อมูล: {json.dumps(data[:5], ensure_ascii=False, default=str)}

สรุปเป็นคำตอบภาษาไทยที่เข้าใจง่าย แสดงตัวเลขสำคัญ หากข้อมูลมากให้สรุปเป็นสาระสำคัญ

คำตอบ:"""

            response = self.bedrock.invoke_model(
                modelId='apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 500,
                    "messages": [{"role": "user", "content": prompt}]
                })
            )
            
            result = json.loads(response['body'].read())
            return result['content'][0]['text'].strip()
            
        except Exception as e:
            logger.error(f"Response formatting failed for {self.tenant_id}: {e}")
            return self._simple_format(data)
    
    def _simple_format(self, data: List[Dict]) -> str:
        """Simple fallback formatting"""
        if len(data) <= 3:
            result = f"ข้อมูลจาก {self.config.name}:\n"
            for i, row in enumerate(data, 1):
                row_text = ", ".join([f"{k}: {v}" for k, v in row.items()])
                result += f"{i}. {row_text}\n"
            return result
        else:
            return f"พบข้อมูล {len(data)} รายการจาก {self.config.name}"

# ============================================================================
# Main PostgreSQL Agent - Ultra Compact
# ============================================================================

class PostgreSQLAgent:
    """Ultra-compact multi-tenant PostgreSQL agent"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.sql_generator = SQLGenerator(tenant_id)
        self.db_manager = DatabaseManager(tenant_id)
        self.formatter = ResponseFormatter(tenant_id)
    
    def query(self, question: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Main query method - streamlined pipeline"""
        tenant_id = tenant_id or self.tenant_id
        
        try:
            # 1. Generate SQL
            sql = self.sql_generator.generate(question)
            if not sql:
                return {
                    "success": False,
                    "answer": "ไม่สามารถสร้าง SQL query ได้",
                    "tenant_id": tenant_id
                }
            
            # 2. Execute query
            results = self.db_manager.execute_query(sql)
            
            # 3. Format response
            if results["success"]:
                answer = self.formatter.format_results(results, question)
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
                    "answer": f"เกิดข้อผิดพลาด: {results.get('error')}",
                    "sql": sql,
                    "tenant_id": tenant_id
                }
                
        except Exception as e:
            logger.error(f"PostgreSQL Agent error for {tenant_id}: {e}")
            return {
                "success": False,
                "answer": f"เกิดข้อผิดพลาด: {str(e)}",
                "tenant_id": tenant_id
            }
    
    def test_connection(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Test database connection"""
        return self.db_manager.test_connection()
    
    def get_stats(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Get basic database statistics"""
        try:
            stats = {}
            for table_name in ["employees", "projects", "departments", "clients"]:
                result = self.db_manager.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                if result["success"] and result["data"]:
                    stats[f"{table_name}_count"] = result["data"][0]["count"]
            
            return {"success": True, "tenant_id": self.tenant_id, "stats": stats}
            
        except Exception as e:
            return {"success": False, "tenant_id": self.tenant_id, "error": str(e)}

# ============================================================================
# Convenience Functions
# ============================================================================

def create_postgres_agent(tenant_id: str) -> PostgreSQLAgent:
    """Factory function for creating PostgreSQL agents"""
    return PostgreSQLAgent(tenant_id)

def query_postgres_for_tenant(question: str, tenant_id: str) -> Dict[str, Any]:
    """Quick query function"""
    agent = PostgreSQLAgent(tenant_id)
    return agent.query(question, tenant_id)

def test_tenant_database(tenant_id: str) -> Dict[str, Any]:
    """Quick connection test"""
    agent = PostgreSQLAgent(tenant_id)
    return agent.test_connection(tenant_id)

# ============================================================================
# Test Runner
# ============================================================================

if __name__ == "__main__":
    # Quick test
    test_tenants = ['company-a', 'company-b', 'company-c']
    test_questions = ["มีพนักงานกี่คน?", "เงินเดือนเฉลี่ยเท่าไหร่?"]
    
    for tenant_id in test_tenants:
        print(f"\n🏢 Testing {tenant_id}")
        agent = PostgreSQLAgent(tenant_id)
        
        # Test connection
        conn_test = agent.test_connection()
        print(f"Connection: {'✅' if conn_test['success'] else '❌'}")
        
        if conn_test['success']:
            for question in test_questions:
                result = agent.query(question)
                status = '✅' if result['success'] else '❌'
                print(f"{status} {question}: {result['answer'][:50]}...")