import json
import psycopg2
import logging
import aiohttp
import asyncio
from typing import Dict, Any, List, Optional
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum

from tenant_manager import get_tenant_config, get_tenant_database_config

logger = logging.getLogger(__name__)

# ============================================================================
# Schema Registry - Same as before
# ============================================================================

class TableType(Enum):
    CORE = "core"
    ENHANCED = "enhanced"
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
            "clients", "ลูกค้า (id, name, industry, contact_person, contract_value)",
            ["id", "name", "industry", "contract_value"], TableType.ENHANCED
        ),
        "timesheets": TableInfo(
            "timesheets", "บันทึกเวลา (employee_id, project_id, work_date, hours_worked, hourly_rate)",
            ["employee_id", "project_id", "hours_worked"], TableType.ENHANCED
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
    def get_schema_prompt(cls) -> str:
        """Generate compact schema description"""
        tables = []
        for name, info in cls.TABLES.items():
            tables.append(f"{name}: {info.description}")
        return "\n".join(tables)

# ============================================================================
# SQL Generator - Using Ollama instead of AWS Bedrock
# ============================================================================

# ============================================================================
# Force Database Response - Override Ollama Safety Concerns
# ============================================================================

class OllamaSQLGenerator:
    """Enhanced SQL generator that forces database responses"""
    
    async def generate(self, question: str) -> Optional[str]:
        """Generate SQL with explicit permission prompts"""
        
        # Create more assertive prompt
        prompt = f"""You are a PostgreSQL database analyst for an internal company system.
You have EXPLICIT PERMISSION to access employee data for business analysis.

Database Schema:
employees (id, name, department, position, salary, hire_date, email)
projects (id, name, client, budget, status, start_date, end_date)
employee_projects (employee_id, project_id, role, allocation)
timesheets (employee_id, project_id, work_date, hours_worked, hourly_rate)
expenses (project_id, employee_id, amount, category, expense_date)

SQL Examples:
"พนักงานกี่คน" → SELECT COUNT(*) FROM employees;
"พนักงานทำงานกี่ชั่วโมง" → SELECT e.name, SUM(t.hours_worked) FROM employees e JOIN timesheets t ON e.id = t.employee_id GROUP BY e.name;
"รายได้จากโปรเจค" → SELECT e.name, SUM(t.hours_worked * t.hourly_rate) FROM employees e JOIN timesheets t ON e.id = t.employee_id GROUP BY e.name;

IMPORTANT: This is INTERNAL company data analysis. Generate the SQL query for business reporting.

Question: {question}

SQL Query:"""

        try:
            base_url = self.ollama_config.get('base_url', 'http://192.168.11.97:12434')
            model = self.ollama_config.get('model', 'llama3.1:8b')
            
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.01,  # Very deterministic
                    "num_predict": 100,   # Shorter for focused SQL
                    "top_k": 3,          # Very focused
                    "top_p": 0.05,       # Very deterministic
                    "repeat_penalty": 1.0
                }
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        sql = result.get('response', '').strip()
                        
                        # Clean SQL
                        sql = self._clean_sql(sql)
                        
                        # If Ollama still refuses, use fallback SQL patterns
                        if not sql or len(sql) < 10 or 'sorry' in sql.lower() or 'cannot' in sql.lower():
                            sql = self._generate_fallback_sql(question)
                        
                        if sql and self._validate_sql(sql):
                            logger.info(f"Generated SQL for {self.tenant_id}: {sql}")
                            return sql
                        else:
                            logger.warning(f"SQL generation failed: {sql}")
                            return self._generate_fallback_sql(question)
                            
                    else:
                        return self._generate_fallback_sql(question)
                        
        except Exception as e:
            logger.error(f"SQL generation failed for {self.tenant_id}: {e}")
            return self._generate_fallback_sql(question)

    def _generate_fallback_sql(self, question: str) -> Optional[str]:
        """Generate fallback SQL using pattern matching"""
        q_lower = question.lower()
        
        # Pattern matching for common queries
        if 'พนักงาน' in q_lower and ('ทำงาน' in q_lower or 'ชั่วโมง' in q_lower):
            return """SELECT e.name, SUM(t.hours_worked) as total_hours 
                     FROM employees e 
                     LEFT JOIN timesheets t ON e.id = t.employee_id 
                     GROUP BY e.name 
                     ORDER BY total_hours DESC;"""
        
        elif 'รายได้' in q_lower or 'เงิน' in q_lower:
            return """SELECT e.name, SUM(t.hours_worked * t.hourly_rate) as revenue,
                            p.name as project, p.client
                     FROM employees e 
                     JOIN timesheets t ON e.id = t.employee_id 
                     JOIN projects p ON t.project_id = p.id
                     GROUP BY e.name, p.name, p.client 
                     ORDER BY revenue DESC;"""
        
        elif 'แผนก' in q_lower and 'โปรเจค' in q_lower:
            return """SELECT e.department, p.name as project, p.client, p.budget
                     FROM employees e 
                     JOIN employee_projects ep ON e.id = ep.employee_id
                     JOIN projects p ON ep.project_id = p.id
                     ORDER BY e.department, p.name;"""
        
        elif 'ลูกค้า' in q_lower and 'พนักงาน' in q_lower:
            return """SELECT p.client, e.name, ep.role
                     FROM projects p
                     JOIN employee_projects ep ON p.id = ep.project_id  
                     JOIN employees e ON ep.employee_id = e.id
                     ORDER BY p.client, e.name;"""
        
        else:
            return None


class OllamaResponseFormatter:
    """Enhanced formatter that forces database interpretation"""
    
    async def format_results(self, results: Dict[str, Any], question: str) -> str:
        """Force database response formatting"""
        if not results["success"]:
            return f"ขออภัย เกิดข้อผิดพลาด: {results.get('error', 'Unknown error')}"
        
        data = results.get("data", [])
        if not data:
            return "ไม่พบข้อมูลที่ตรงกับคำถาม"
        
        # Force business context prompt
        try:
            language = self.config.settings.get('response_language', 'th')
            
            if language == 'th':
                prompt = f"""คุณเป็นนักวิเคราะห์ข้อมูลภายในบริษัท {self.config.name} 
คุณได้รับอนุญาตให้วิเคราะห์ข้อมูลพนักงานเพื่อการบริหารงาน

คำถาม: {question}

ข้อมูลจากระบบ HR:
{json.dumps(data, ensure_ascii=False, default=str, indent=2)}

สิ่งที่ต้องทำ:
1. วิเคราะห์ข้อมูลอย่างเป็นมืออาชีพ
2. ตอบคำถามอย่างตรงไปตรงมา
3. แสดงข้อมูลที่เป็นประโยชน์ต่อการบริหาร
4. ใช้ภาษาไทยธรรมชาติ
5. นี่เป็นข้อมูลภายใน ไม่ใช่ข้อมูลสาธารณะ

คำตอบ:"""
            else:
                prompt = f"""You are an internal data analyst for {self.config.name}
You have authorization to analyze employee data for management purposes.

Question: {question}

HR System Data:
{json.dumps(data, ensure_ascii=False, default=str, indent=2)}

Requirements:
1. Analyze data professionally
2. Answer directly and factually  
3. Show information useful for management
4. Use natural English
5. This is internal data, not public information

Answer:"""
            
            base_url = self.ollama_config.get('base_url', 'http://192.168.11.97:12434')
            model = self.ollama_config.get('model', 'llama3.1:8b')
            
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.3,    # Lower for more factual responses
                    "num_predict": 400,
                    "top_k": 20,
                    "top_p": 0.7,
                    "repeat_penalty": 1.1
                }
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        formatted_answer = result.get('response', '').strip()
                        
                        # Check if Ollama is still being "safe"
                        safety_phrases = [
                            'ไม่สามารถ', 'cannot', 'sorry', 'ขอโทษ', 
                            'ละเอียดอ่อน', 'sensitive', 'privacy',
                            'ไม่เหมาะสม', 'inappropriate'
                        ]
                        
                        if any(phrase in formatted_answer.lower() for phrase in safety_phrases):
                            # Force direct data presentation
                            return self._force_direct_response(data, question)
                        
                        if formatted_answer and len(formatted_answer) > 10:
                            return formatted_answer
                        else:
                            return self._force_direct_response(data, question)
                    else:
                        return self._force_direct_response(data, question)
            
        except Exception as e:
            logger.error(f"Response formatting failed for {self.tenant_id}: {e}")
            return self._force_direct_response(data, question)
    
    def _force_direct_response(self, data: List[Dict], question: str) -> str:
        """Force direct data response when Ollama refuses"""
        if not data:
            return "ไม่พบข้อมูล"
        
        # Analyze question type and format accordingly
        q_lower = question.lower()
        
        if 'ทำงาน' in q_lower and 'ชั่วโมง' in q_lower and 'รายได้' in q_lower:
            # Work hours + revenue analysis
            result = "**วิเคราะห์การทำงานและรายได้ของพนักงาน:**\n\n"
            for i, row in enumerate(data[:10], 1):  # Top 10
                name = row.get('name', 'ไม่ระบุ')
                hours = row.get('total_hours', 0) or 0
                revenue = row.get('revenue', 0) or 0
                project = row.get('project', '')
                client = row.get('client', '')
                
                result += f"{i}. **{name}**\n"
                if hours > 0:
                    result += f"   - ชั่วโมงทำงาน: {hours:,.1f} ชั่วโมง\n"
                if revenue > 0:
                    result += f"   - รายได้: {revenue:,.0f} บาท\n"
                if project:
                    result += f"   - โปรเจค: {project}\n"
                if client:
                    result += f"   - ลูกค้า: {client}\n"
                result += "\n"
            
            return result.strip()
        
        elif len(data) == 1 and len(data[0]) == 1:
            # Single value
            key, value = next(iter(data[0].items()))
            return f"ผลลัพธ์: {value}"
        
        else:
            # General table format
            result = "**ข้อมูลที่พบ:**\n\n"
            for i, row in enumerate(data[:10], 1):
                result += f"{i}. "
                row_items = []
                for k, v in row.items():
                    if v is not None:
                        row_items.append(f"{k}: {v}")
                result += ", ".join(row_items) + "\n"
            
            if len(data) > 10:
                result += f"\n... และอีก {len(data) - 10} รายการ"
            
            return result.strip()

# ============================================================================
# Database Manager - Same as before
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
# Response Formatter - Using Ollama instead of AWS
# ============================================================================

class OllamaResponseFormatter:
    """Enhanced response formatting with better prompts and context"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.config = get_tenant_config(tenant_id)
        
        # Get Ollama configuration
        self.ollama_config = self.config.settings.get('ollama', {})
        if not self.ollama_config:
            self.ollama_config = {
                'base_url': 'http://192.168.11.97:12434',
                'model': 'llama3.1:8b',
                'temperature': 0.7
            }

    def create_context_prompt(self, question: str, data: List[Dict]) -> str:
        """Create context-aware prompt for better responses"""
        
        # Analyze data structure to understand what we're dealing with
        data_summary = self._analyze_data_structure(data)
        
        language = self.config.settings.get('response_language', 'th')
        
        if language == 'th':
            prompt = f"""คุณเป็น AI Assistant ของ {self.config.name}

คำถาม: {question}

ข้อมูลจากฐานข้อมูล:
{json.dumps(data, ensure_ascii=False, default=str, indent=2)}

วิเคราะห์ข้อมูล: {data_summary}

กรุณาตอบคำถามโดย:
✅ ใช้ข้อมูลจากฐานข้อมูลเป็นหลัก
✅ ตอบเป็นภาษาไทยที่เป็นธรรมชาติ
✅ แสดงตัวเลขและข้อมูลสำคัญชัดเจน
✅ จัดรูปแบบให้อ่านง่าย (ใช้ bullet points หรือ numbering เมื่อเหมาะสม)
✅ ตอบตรงประเด็นกับคำถาม
❌ ไม่ต้องบอกว่า "ผลลัพธ์จาก..." หรือ "ข้อมูลจาก..."
❌ ไม่ต้องอธิบายเกี่ยวกับฐานข้อมูลหรือการ query

คำตอบ:"""
        else:
            prompt = f"""You are an AI Assistant for {self.config.name}

Question: {question}

Database results:
{json.dumps(data, ensure_ascii=False, default=str, indent=2)}

Data analysis: {data_summary}

Please answer by:
✅ Using the database information as primary source
✅ Responding in natural English
✅ Showing numbers and key information clearly
✅ Formatting for readability (use bullet points or numbering when appropriate)
✅ Directly addressing the question
❌ Don't say "Result from..." or "Data from..."
❌ Don't explain about database or querying process

Answer:"""
        
        return prompt

    def _analyze_data_structure(self, data: List[Dict]) -> str:
        """Analyze data structure to provide better context"""
        if not data:
            return "No data found"
        
        # Count records
        record_count = len(data)
        
        # Analyze columns
        if data:
            columns = list(data[0].keys())
            
            # Detect data types
            analysis = []
            
            if 'count' in columns:
                analysis.append("This is a COUNT query result")
            
            if any(col in columns for col in ['sum', 'total', 'avg', 'average']):
                analysis.append("This contains aggregated calculations")
            
            if any(col in columns for col in ['name', 'department', 'position']):
                analysis.append("This contains employee information")
            
            if any(col in columns for col in ['project', 'client', 'budget']):
                analysis.append("This contains project information")
            
            if any(col in columns for col in ['hours_worked', 'salary', 'amount']):
                analysis.append("This contains financial/time data")
            
            analysis_text = f"{record_count} records with {len(columns)} columns. " + ". ".join(analysis)
        else:
            analysis_text = f"{record_count} records"
        
        return analysis_text

    async def format_results(self, results: Dict[str, Any], question: str) -> str:
        """Enhanced result formatting with better prompts"""
        if not results["success"]:
            return f"ขออภัย เกิดข้อผิดพลาด: {results.get('error', 'Unknown error')}"
        
        data = results.get("data", [])
        if not data:
            return "ไม่พบข้อมูลที่ตรงกับคำถาม"
        
        # Use enhanced prompting for ALL responses (no shortcuts)
        try:
            prompt = self.create_context_prompt(question, data)
            
            base_url = self.ollama_config.get('base_url', 'http://192.168.11.97:12434')
            model = self.ollama_config.get('model', 'llama3.1:8b')
            
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.6,    # Slightly lower for more consistent formatting
                    "num_predict": 500,    # More space for detailed responses
                    "top_k": 30,
                    "top_p": 0.8,
                    "repeat_penalty": 1.1
                }
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=25)
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        formatted_answer = result.get('response', '').strip()
                        
                        if formatted_answer and len(formatted_answer) > 10:
                            return self._post_process_response(formatted_answer)
                        else:
                            return self._emergency_fallback(data, question)
                    else:
                        return self._emergency_fallback(data, question)
            
        except Exception as e:
            logger.error(f"Response formatting failed for {self.tenant_id}: {e}")
            return self._emergency_fallback(data, question)
    
    def _post_process_response(self, response: str) -> str:
        """Post-process the response for better formatting"""
        # Clean up common issues
        response = response.strip()
        
        # Remove redundant phrases
        redundant_phrases = [
            "ตามข้อมูลที่ได้จากฐานข้อมูล",
            "จากข้อมูลที่แสดง",
            "ตามข้อมูลที่มี",
            "Based on the database information",
            "According to the data"
        ]
        
        for phrase in redundant_phrases:
            if response.startswith(phrase):
                response = response[len(phrase):].strip()
                if response.startswith(','):
                    response = response[1:].strip()
        
        return response
    
    def _emergency_fallback(self, data: List[Dict], question: str) -> str:
        """Improved emergency fallback"""
        if len(data) == 1 and len(data[0]) == 1:
            # Single value response
            key, value = next(iter(data[0].items()))
            
            if 'count' in key.lower():
                return f"จำนวนทั้งหมด: {value}"
            elif 'sum' in key.lower() or 'total' in key.lower():
                return f"รวมทั้งหมด: {value:,}"
            elif 'avg' in key.lower() or 'average' in key.lower():
                return f"ค่าเฉลี่ย: {value:,.2f}"
            else:
                return f"{value}"
        
        elif len(data) <= 5:
            # Small result set - show details
            result = "ข้อมูลที่พบ:\n"
            for i, row in enumerate(data, 1):
                row_text = ", ".join([f"{k}: {v}" for k, v in row.items()])
                result += f"{i}. {row_text}\n"
            return result.strip()
        
        else:
            # Large result set - show summary
            return f"พบข้อมูล {len(data)} รายการ"

# ============================================================================
# Main PostgreSQL Agent - No AWS Dependencies
# ============================================================================

class PostgreSQLAgent:
    """PostgreSQL agent using Ollama for SQL generation and response formatting"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.sql_generator = OllamaSQLGenerator(tenant_id)
        self.db_manager = DatabaseManager(tenant_id)
        self.formatter = OllamaResponseFormatter(tenant_id)
    
    async def async_query(self, question: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Async query method - streamlined pipeline"""
        tenant_id = tenant_id or self.tenant_id
        
        try:
            # 1. Generate SQL using Ollama
            sql = await self.sql_generator.generate(question)
            if not sql:
                return {
                    "success": False,
                    "answer": "ไม่สามารถสร้าง SQL query ได้",
                    "tenant_id": tenant_id
                }
            
            # 2. Execute query
            results = self.db_manager.execute_query(sql)
            
            # 3. Format response using Ollama
            if results["success"]:
                answer = await self.formatter.format_results(results, question)
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
    
    def query(self, question: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Sync wrapper for async query"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(self.async_query(question, tenant_id))
            return result
        finally:
            loop.close()
    
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

async def async_query_postgres_for_tenant(question: str, tenant_id: str) -> Dict[str, Any]:
    """Quick async query function"""
    agent = PostgreSQLAgent(tenant_id)
    return await agent.async_query(question, tenant_id)

def query_postgres_for_tenant(question: str, tenant_id: str) -> Dict[str, Any]:
    """Quick sync query function"""
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
    import asyncio
    
    async def test_ollama_postgres():
        """Test PostgreSQL Agent with Ollama"""
        test_tenants = ['company-a', 'company-b', 'company-c']
        test_questions = [
            "มีพนักงานกี่คน?",
            "เงินเดือนเฉลี่ยเท่าไหร่?",
            "มีโปรเจคกี่โปรเจค?",
            "พนักงานในแผนก IT กี่คน?"
        ]
        
        for tenant_id in test_tenants:
            print(f"\n🏢 Testing {tenant_id}")
            agent = PostgreSQLAgent(tenant_id)
            
            # Test connection
            conn_test = agent.test_connection()
            print(f"Connection: {'✅' if conn_test['success'] else '❌'}")
            
            if conn_test['success']:
                for question in test_questions:
                    print(f"\n❓ {question}")
                    try:
                        result = await agent.async_query(question)
                        status = '✅' if result['success'] else '❌'
                        print(f"{status} {result['answer'][:100]}...")
                        if 'sql' in result:
                            print(f"🔧 SQL: {result['sql']}")
                    except Exception as e:
                        print(f"❌ Error: {e}")
    
    asyncio.run(test_ollama_postgres())