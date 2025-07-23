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
# Enhanced SQL Generator with Pattern Matching Fallback
# ============================================================================

class SmartSQLGenerator:
    """Smart SQL Generator - Pattern matching first, then Ollama"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.config = get_tenant_config(tenant_id)
        
        # Ollama configuration
        self.ollama_config = self.config.settings.get('ollama', {})
        if not self.ollama_config:
            self.ollama_config = {
                'base_url': 'http://192.168.11.97:12434',
                'model': 'llama3.1:8b'
            }
        
        # Initialize tenant-specific schema
        self.schema_info = self._get_tenant_schema()
    
    def _get_tenant_schema(self) -> Dict[str, Any]:
        """Get tenant-specific database schema information"""
        tenant_schemas = {
            'company-a': {
                'name': 'SiamTech Main Office (Bangkok)',
                'core_tables': [
                    'employees (id, name, department, position, salary, hire_date, email)',
                    'projects (id, name, client, budget, status, start_date, end_date, tech_stack)',
                    'employee_projects (employee_id, project_id, role, allocation)'
                ],
                'enhanced_tables': [
                    'departments (id, name, manager_id, budget, location)',
                    'timesheets (employee_id, project_id, work_date, hours_worked, hourly_rate, status)',
                    'expenses (project_id, employee_id, amount, category, expense_date, status)',
                    'meetings (id, title, meeting_date, organizer_id, project_id)',
                    'equipment (id, name, category, assigned_to, status)',
                    'training (id, title, start_date, end_date, cost)'
                ],
                'views': [
                    'employee_performance (name, projects_count, total_hours)',
                    'project_financial_summary (name, budget, total_cost)',
                    'department_statistics (name, employee_count, avg_salary)'
                ],
                'specialties': ['Enterprise solutions', 'Large projects', 'Main office operations']
            },
            
            'company-b': {
                'name': 'SiamTech Regional Office (Chiang Mai)',
                'core_tables': [
                    'employees (id, name, department, position, salary, hire_date, email)',
                    'projects (id, name, client, budget, status, start_date, end_date, tech_stack)',
                    'employee_projects (employee_id, project_id, role, allocation)'
                ],
                'enhanced_tables': [
                    'departments (id, name, manager_id, budget, location)',
                    'clients (id, name, industry, contact_person, contract_value, region)',
                    'skills (id, name, category, global_demand_level)',
                    'employee_skills (employee_id, skill_id, proficiency_level, certified)',
                    'timesheets (employee_id, project_id, work_date, hours_worked, hourly_rate)',
                    'expenses (project_id, employee_id, amount, category, country)',
                    'meetings (id, title, meeting_date, organizer_id, project_id, client_id)',
                    'equipment (id, name, category, assigned_to, location, country)',
                    'training (id, title, provider, category, cost, location)'
                ],
                'views': [
                    'regional_performance (name, projects_count, total_hours, region)',
                    'regional_project_summary (name, client, budget, region)',
                    'regional_client_analysis (name, industry, sector_classification)'
                ],
                'specialties': ['Tourism industry', 'Regional projects', 'Northern Thailand market']
            },
            
            'company-c': {
                'name': 'SiamTech International (Global Operations)',
                'core_tables': [
                    'employees (id, name, department, position, salary, hire_date, email)',
                    'projects (id, name, client, budget, status, start_date, end_date, tech_stack)',
                    'employee_projects (employee_id, project_id, role, allocation)'
                ],
                'enhanced_tables': [
                    'departments (id, name, manager_id, budget, location)',
                    'clients (id, name, industry, country, timezone, currency, market_type)',
                    'skills (id, name, category, global_demand_level)',
                    'employee_skills (employee_id, skill_id, proficiency_level, certification_authority)',
                    'timesheets (employee_id, project_id, work_date, hours_worked, hourly_rate_usd, currency, client_timezone)',
                    'expenses (project_id, employee_id, amount, currency, amount_usd, country)',
                    'meetings (id, title, meeting_date, timezone, meeting_platform, language)',
                    'equipment (id, name, category, assigned_to, country, shipping_status)',
                    'training (id, title, cost_usd, language, international_certification)'
                ],
                'international_tables': [
                    'international_contracts (id, client_id, project_id, total_value_usd, currency, governing_law)',
                    'international_payments (id, contract_id, amount_usd, currency, payment_method, status)',
                    'company_info (company_name, branch_type, location, total_employees)'
                ],
                'views': [
                    'international_team_performance (name, revenue_generated_usd, critical_skills)',
                    'global_project_summary (name, client_country, contract_value_usd, profit_margin_usd)',
                    'revenue_by_geography (country, currency, total_contracts_usd)'
                ],
                'specialties': ['International clients', 'Multi-currency operations', 'Global projects', 'Cross-border payments']
            }
        }
        
        return tenant_schemas.get(self.tenant_id, tenant_schemas['company-a'])
    
    def _build_schema_prompt(self) -> str:
        schema = self.schema_info
        
        prompt_parts = [
            f"Database Schema for {schema['name']}:",
            "",
            "CORE TABLES:"
        ]
        
        for table in schema['core_tables']:
            prompt_parts.append(f"- {table}")
        
        if schema.get('enhanced_tables'):
            prompt_parts.extend([
                "",
                "ENHANCED TABLES:"
            ])
            for table in schema['enhanced_tables']:
                prompt_parts.append(f"- {table}")
        
        if schema.get('international_tables'):
            prompt_parts.extend([
                "",
                "INTERNATIONAL TABLES:"
            ])
            for table in schema['international_tables']:
                prompt_parts.append(f"- {table}")
        
        if schema.get('views'):
            prompt_parts.extend([
                "",
                "AVAILABLE VIEWS:"
            ])
            for view in schema['views']:
                prompt_parts.append(f"- {view}")
        
        prompt_parts.extend([
            "",
            f"COMPANY SPECIALTIES: {', '.join(schema['specialties'])}",
            ""
        ])
        
        return "\n".join(prompt_parts)
    
    async def generate(self, question: str) -> Optional[str]:
        """Generate SQL - Ollama first, pattern as emergency fallback only"""
        
        # 1. Try Ollama first (main method)
        ollama_sql = await self._ollama_generate_sql(question)
        if ollama_sql:
            logger.info(f"Generated SQL via Ollama for {self.tenant_id}: {ollama_sql}")
            return ollama_sql
        
        # 2. Emergency fallback to pattern matching only for very basic queries
        pattern_sql = self._emergency_pattern_fallback(question)
        if pattern_sql:
            logger.info(f"Generated SQL via emergency pattern for {self.tenant_id}: {pattern_sql}")
            return pattern_sql
        
        logger.warning(f"No SQL generated for question: {question}")
        return None
    
    def _emergency_pattern_fallback(self, question: str) -> Optional[str]:
        """Emergency patterns - now includes complex queries"""
        q_lower = question.lower()
        
        # Basic queries
        if q_lower in ['มีพนักงานกี่คน', 'จำนวนพนักงาน']:
            return "SELECT COUNT(*) FROM employees;"
        elif q_lower in ['เงินเดือนเฉลี่ย']:
            return "SELECT AVG(salary) FROM employees;"
        elif q_lower in ['มีโปรเจคกี่โปรเจค', 'จำนวนโปรเจค']:
            return "SELECT COUNT(*) FROM projects;"
        
        # Complex patterns for working hours and revenue
        elif 'ทำงาน' in q_lower and 'ชั่วโมง' in q_lower and 'รายได้' in q_lower:
            return """SELECT e.name, 
                            COALESCE(SUM(t.hours_worked), 0) as ชั่วโมงทำงาน,
                            COALESCE(SUM(t.hours_worked * t.hourly_rate), 0) as รายได้
                     FROM employees e 
                     LEFT JOIN timesheets t ON e.id = t.employee_id 
                     WHERE t.status = 'approved' OR t.status IS NULL
                     GROUP BY e.id, e.name 
                     ORDER BY รายได้ DESC;"""
        
        elif 'ทำงาน' in q_lower and 'ชั่วโมง' in q_lower:
            return """SELECT e.name, COALESCE(SUM(t.hours_worked), 0) as ชั่วโมงทำงาน
                     FROM employees e 
                     LEFT JOIN timesheets t ON e.id = t.employee_id 
                     GROUP BY e.id, e.name 
                     ORDER BY ชั่วโมงทำงาน DESC;"""
        
        elif 'รายได้' in q_lower and 'พนักงาน' in q_lower:
            return """SELECT e.name, COALESCE(SUM(t.hours_worked * t.hourly_rate), 0) as รายได้
                     FROM employees e 
                     LEFT JOIN timesheets t ON e.id = t.employee_id 
                     GROUP BY e.id, e.name 
                     ORDER BY รายได้ DESC;"""
        
        return None
    
    def _pattern_match_sql(self, question: str) -> Optional[str]:
        """Enhanced pattern matching for complex Thai business queries"""
        q_lower = question.lower()
        
        # Employee count queries
        if any(phrase in q_lower for phrase in ['พนักงานกี่คน', 'จำนวนพนักงาน', 'มีพนักงาน', 'how many employees']):
            if 'แผนก' in q_lower or 'department' in q_lower:
                return "SELECT department, COUNT(*) as จำนวน FROM employees GROUP BY department ORDER BY จำนวน DESC;"
            else:
                return "SELECT COUNT(*) as จำนวนพนักงาน FROM employees;"
        
        # Salary queries
        if any(phrase in q_lower for phrase in ['เงินเดือนเฉลี่ย', 'เงินเดือน', 'salary', 'เฉลี่ย']):
            if 'แผนก' in q_lower:
                return "SELECT department, AVG(salary) as เงินเดือนเฉลี่ย FROM employees GROUP BY department ORDER BY เงินเดือนเฉลี่ย DESC;"
            elif 'สูงสุด' in q_lower or 'เก่ง' in q_lower:
                return "SELECT department, MAX(salary) as เงินเดือนสูงสุด, COUNT(*) as จำนวนคน FROM employees GROUP BY department ORDER BY เงินเดือนสูงสุด DESC;"
            else:
                return "SELECT AVG(salary) as เงินเดือนเฉลี่ย FROM employees;"
        
        # Work hours and revenue (complex query)
        if any(phrase in q_lower for phrase in ['ทำงาน', 'ชั่วโมง', 'รายได้']) and any(phrase in q_lower for phrase in ['คนไหน', 'พนักงาน']):
            return """SELECT e.name, 
                            COALESCE(SUM(t.hours_worked), 0) as ชั่วโมงทำงาน,
                            COALESCE(SUM(t.hours_worked * t.hourly_rate), 0) as รายได้
                     FROM employees e 
                     LEFT JOIN timesheets t ON e.id = t.employee_id 
                     WHERE t.status = 'approved' OR t.status IS NULL
                     GROUP BY e.id, e.name 
                     ORDER BY รายได้ DESC;"""
        
        # Project count
        if any(phrase in q_lower for phrase in ['โปรเจคกี่', 'จำนวนโปรเจค', 'มีโปรเจค', 'how many projects']):
            return "SELECT COUNT(*) as จำนวนโปรเจค FROM projects;"
        
        # Active projects
        if any(phrase in q_lower for phrase in ['โปรเจคที่กำลัง', 'active', 'กำลังทำ']):
            return "SELECT name, client, budget FROM projects WHERE status = 'active' ORDER BY budget DESC;"
        
        # Complex project analysis
        if 'โปรเจค' in q_lower and any(phrase in q_lower for phrase in ['ทีมงาน', 'งบประมาณ', 'มาก']):
            return """SELECT p.name, p.client, p.budget, COUNT(ep.employee_id) as ขนาดทีม
                     FROM projects p 
                     LEFT JOIN employee_projects ep ON p.id = ep.project_id
                     GROUP BY p.id, p.name, p.client, p.budget
                     ORDER BY ขนาดทีม DESC, p.budget DESC;"""
        
        # Department percentage
        if 'แผนก' in q_lower and ('เปอร์เซ็นต์' in q_lower or 'percent' in q_lower):
            return """SELECT department, 
                            COUNT(*) as จำนวน,
                            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM employees), 1) as เปอร์เซ็นต์
                     FROM employees 
                     GROUP BY department 
                     ORDER BY จำนวน DESC;"""
        
        # Client budget analysis
        if 'ลูกค้า' in q_lower and 'งบประมาณ' in q_lower:
            return """SELECT p.client, 
                            SUM(p.budget) as งบประมาณรวม,
                            COUNT(DISTINCT ep.employee_id) as จำนวนพนักงาน
                     FROM projects p
                     LEFT JOIN employee_projects ep ON p.id = ep.project_id
                     GROUP BY p.client
                     ORDER BY งบประมาณรวม DESC;"""
        
        # Multiple projects per employee
        if 'พนักงาน' in q_lower and ('2 โปรเจค' in q_lower or 'หลายโปรเจค' in q_lower):
            return """SELECT e.name, 
                            COUNT(ep.project_id) as จำนวนโปรเจค,
                            STRING_AGG(DISTINCT ep.role, ', ') as บทบาท
                     FROM employees e
                     JOIN employee_projects ep ON e.id = ep.employee_id
                     GROUP BY e.id, e.name
                     HAVING COUNT(ep.project_id) > 1
                     ORDER BY จำนวนโปรเจค DESC;"""
        
        # Department info
        if 'แผนก' in q_lower and any(phrase in q_lower for phrase in ['กี่แผนก', 'จำนวนแผนก']):
            return "SELECT COUNT(DISTINCT department) as จำนวนแผนก FROM employees;"
        
        # Budget queries
        if any(phrase in q_lower for phrase in ['งบประมาณ', 'budget', 'ค่าใช้จ่าย']):
            if 'รวม' in q_lower or 'total' in q_lower:
                return "SELECT SUM(budget) as งบประมาณรวม FROM projects WHERE status = 'active';"
            else:
                return "SELECT name, client, budget FROM projects WHERE status = 'active' ORDER BY budget DESC;"
        
        # Client queries
        if any(phrase in q_lower for phrase in ['ลูกค้า', 'client', 'กี่ลูกค้า']):
            return "SELECT COUNT(DISTINCT client) as จำนวนลูกค้า FROM projects;"
        
        return None
    
    async def _ollama_generate_sql(self, question: str) -> Optional[str]:
        """Simplified SQL generation - shorter, clearer prompt"""
        
        prompt = f"""Generate PostgreSQL query for: {question}

Tables for {self.schema_info['name']}:
- employees (id, name, department, position, salary, hire_date, email)
- projects (id, name, client, budget, status, start_date, end_date)
- employee_projects (employee_id, project_id, role, allocation)
- timesheets (employee_id, project_id, work_date, hours_worked, hourly_rate, status)

GROUP BY Rule: If SELECT has both aggregate (SUM, COUNT) and regular columns, put ALL regular columns in GROUP BY

Examples:
Q: พนักงานทำงานกี่ชั่วโมง
A: SELECT e.name, SUM(t.hours_worked) FROM employees e LEFT JOIN timesheets t ON e.id = t.employee_id GROUP BY e.id, e.name;

Q: พนักงานได้รายได้เท่าไหร่  
A: SELECT e.name, SUM(t.hours_worked * t.hourly_rate) FROM employees e LEFT JOIN timesheets t ON e.id = t.employee_id GROUP BY e.id, e.name;

SQL only:"""

        try:
            base_url = self.ollama_config.get('base_url', 'http://192.168.11.97:12434')
            model = self.ollama_config.get('model', 'llama3.1:8b')
            
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 150,  # Shorter
                    "top_k": 10,
                    "top_p": 0.3,
                    "repeat_penalty": 1.1
                }
            }
            
            logger.info(f"Generating simplified SQL for: {question}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        sql = result.get('response', '').strip()
                        
                        logger.info(f"Raw Ollama response: {sql}")
                        
                        # Clean and validate SQL
                        sql = self._clean_sql(sql)
                        
                        if sql and self._validate_sql(sql):
                            logger.info(f"Generated valid SQL: {sql}")
                            return sql
                        else:
                            logger.warning(f"SQL validation failed: {sql}")
                            return None
                    else:
                        logger.error(f"Ollama HTTP error: {response.status}")
                        return None
            
        except Exception as e:
            logger.error(f"Simplified SQL generation failed: {e}")
            return None
    
    def _get_tenant_examples(self) -> str:
        """Get tenant-specific SQL examples with correct GROUP BY"""
        examples = {
            'company-a': [
                '"มีพนักงานกี่คน" → SELECT COUNT(*) FROM employees;',
                '"เงินเดือนเฉลี่ย" → SELECT AVG(salary) FROM employees;',
                '"พนักงานและชั่วโมงทำงาน" → SELECT e.name, COALESCE(SUM(t.hours_worked), 0) FROM employees e LEFT JOIN timesheets t ON e.id = t.employee_id GROUP BY e.id, e.name;',
                '"อุปกรณ์ที่มอบหมาย" → SELECT e.name, eq.name FROM employees e LEFT JOIN equipment eq ON e.id = eq.assigned_to;',
                '"โปรเจคต่อแผนก" → SELECT e.department, COUNT(DISTINCT ep.project_id) FROM employees e JOIN employee_projects ep ON e.id = ep.employee_id GROUP BY e.department;'
            ],
            'company-b': [
                '"มีพนักงานกี่คน" → SELECT COUNT(*) FROM employees;',
                '"ลูกค้าภาคเหนือ" → SELECT name, industry FROM clients WHERE region = \'Northern Thailand\';',
                '"ทักษะของพนักงาน" → SELECT e.name, STRING_AGG(s.name, \', \') FROM employees e JOIN employee_skills es ON e.id = es.employee_id JOIN skills s ON es.skill_id = s.id GROUP BY e.id, e.name;',
                '"การอบรมและต้นทุน" → SELECT category, SUM(cost) FROM training GROUP BY category;',
                '"พนักงานและรายได้" → SELECT e.name, COALESCE(SUM(t.hours_worked * t.hourly_rate), 0) FROM employees e LEFT JOIN timesheets t ON e.id = t.employee_id GROUP BY e.id, e.name;'
            ],
            'company-c': [
                '"มีพนักงานกี่คน" → SELECT COUNT(*) FROM employees;',
                '"รายได้ต่างประเทศ" → SELECT SUM(total_value_usd) FROM international_contracts WHERE status = \'active\';',
                '"ลูกค้าต่อประเทศ" → SELECT country, COUNT(*) FROM clients GROUP BY country;',
                '"การชำระเงินต่อสกุลเงิน" → SELECT currency, SUM(amount_usd) FROM international_payments WHERE status = \'received\' GROUP BY currency;',
                '"พนักงานและทักษะระดับโลก" → SELECT e.name, COUNT(DISTINCT es.skill_id) FROM employees e JOIN employee_skills es ON e.id = es.employee_id JOIN skills s ON es.skill_id = s.id WHERE s.global_demand_level = \'critical\' GROUP BY e.id, e.name;'
            ]
        }
        
        tenant_examples = examples.get(self.tenant_id, examples['company-a'])
        return '\n'.join(tenant_examples)
    
    def _fallback_sql(self, question: str) -> Optional[str]:
        """Final fallback for basic queries"""
        q_lower = question.lower()
        
        if 'พนักงาน' in q_lower:
            return "SELECT COUNT(*) as จำนวนพนักงาน FROM employees;"
        elif 'โปรเจค' in q_lower:
            return "SELECT COUNT(*) as จำนวนโปรเจค FROM projects;"
        
        return None
    
    def _clean_sql(self, sql: str) -> str:
        """Clean and extract SQL from response"""
        if not sql:
            return ""
        
        # Remove common prefixes/suffixes
        sql = sql.strip()
        
        # Remove markdown code blocks
        if '```' in sql:
            lines = sql.split('\n')
            sql_lines = []
            in_code = False
            for line in lines:
                if line.strip().startswith('```'):
                    in_code = not in_code
                    continue
                if in_code:
                    sql_lines.append(line)
            if sql_lines:
                sql = '\n'.join(sql_lines).strip()
        
        # Remove common prefixes
        prefixes_to_remove = [
            'SQL:', 'sql:', 'Query:', 'query:', 
            'SELECT query:', 'PostgreSQL:', 'Here is the SQL:',
            'The SQL query is:', 'SQL query:'
        ]
        
        for prefix in prefixes_to_remove:
            if sql.startswith(prefix):
                sql = sql[len(prefix):].strip()
        
        # Extract only the SQL part (first complete SELECT statement)
        sql = sql.strip()
        if sql.upper().startswith('SELECT'):
            # Find the end of the SQL statement
            if ';' in sql:
                sql = sql.split(';')[0] + ';'
            elif sql.count('\n') > 0:
                # Take everything up to double newline or explanatory text
                lines = sql.split('\n')
                sql_lines = []
                for line in lines:
                    line = line.strip()
                    if not line:
                        break
                    if any(word in line.lower() for word in ['explanation', 'this query', 'note:', 'คำอธิบาย']):
                        break
                    sql_lines.append(line)
                sql = ' '.join(sql_lines)
            
            # Ensure it ends with semicolon
            if not sql.endswith(';'):
                sql += ';'
        
        return sql.strip()
    
    def _validate_sql(self, sql: str) -> bool:
        """Enhanced SQL validation with PostgreSQL syntax checking"""
        if not sql or len(sql) < 10:
            return False
        
        sql_upper = sql.upper().strip()
        
        # Must start with SELECT
        if not sql_upper.startswith('SELECT'):
            return False
        
        # Must contain FROM
        if 'FROM' not in sql_upper:
            return False
        
        # Check for dangerous operations
        dangerous = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(word in sql_upper for word in dangerous):
            return False
        
        # Basic PostgreSQL syntax checks
        
        # Check GROUP BY rules - if GROUP BY exists, validate it
        if 'GROUP BY' in sql_upper:
            # This is a simplified check - in production you'd want more sophisticated parsing
            select_part = sql_upper.split('FROM')[0].replace('SELECT', '').strip()
            group_by_part = sql_upper.split('GROUP BY')[1].split('ORDER BY')[0].split('HAVING')[0].strip()
            
            # Look for common GROUP BY violations (simplified check)
            if ',' in select_part and 'COUNT(' not in select_part and 'SUM(' not in select_part and 'AVG(' not in select_part:
                # Has multiple SELECT columns but might not have proper GROUP BY
                logger.warning(f"Potential GROUP BY issue in SQL: {sql}")
        
        # Check for balanced parentheses
        if sql.count('(') != sql.count(')'):
            logger.warning(f"Unbalanced parentheses in SQL: {sql}")
            return False
        
        # Check for basic table names (they should exist in our schema)
        valid_tables = ['employees', 'projects', 'employee_projects', 'timesheets', 'expenses', 'clients']
        has_valid_table = any(table in sql.lower() for table in valid_tables)
        if not has_valid_table:
            logger.warning(f"No valid table names found in SQL: {sql}")
            return False
        
        return True

# ============================================================================
# Enhanced Response Formatter
# ============================================================================

class SmartResponseFormatter:
    """AI-powered response formatter - Let Ollama think and create answers"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.config = get_tenant_config(tenant_id)
        
        # Ollama configuration
        self.ollama_config = self.config.settings.get('ollama', {})
        if not self.ollama_config:
            self.ollama_config = {
                'base_url': 'http://192.168.11.97:12434',
                'model': 'llama3.1:8b'
            }
    
    async def format_results(self, results: Dict[str, Any], question: str) -> str:
        """Let Ollama AI create natural responses from database results"""
        if not results["success"]:
            return f"ขออภัย เกิดข้อผิดพลาด: {results.get('error', 'Unknown error')}"
        
        data = results.get("data", [])
        if not data:
            return "ไม่พบข้อมูลที่ตรงกับคำถาม"
        
        # Let Ollama think and create the answer
        return await self._ollama_create_answer(data, question)
    
    async def _ollama_create_answer(self, data: List[Dict], question: str) -> str:
        """Let Ollama AI think and create natural answers"""
        
        # Determine response language
        language = self.config.settings.get('response_language', 'th')
        
        if language == 'th':
            prompt = f"""คุณเป็น AI Assistant ของ {self.config.name} ที่เก่งในการอธิบายข้อมูลบริษัท

คำถามจากผู้ใช้: {question}

ข้อมูลจากฐานข้อมูลบริษัท:
{json.dumps(data, ensure_ascii=False, default=str, indent=2)}

สิ่งที่คุณต้องทำ:
1. วิเคราะห์ข้อมูลที่ได้รับ
2. ตอบคำถามด้วยภาษาที่เป็นธรรมชาติและน่าสนใจ
3. ใช้ตัวเลขจากข้อมูลจริง แต่อธิบายในบริบทที่เข้าใจง่าย
4. สามารถใช้ emoji เพื่อความน่าสนใจ
5. ตอบให้ครบถ้วนและมีประโยชน์

ตัวอย่าง:
- ถ้าข้อมูลเป็นจำนวนพนักงาน ให้อธิบายว่าเป็นทีมขนาดไหน
- ถ้าเป็นเงินเดือน ให้อธิบายในบริบทของตลาดงาน
- ถ้าเป็นโปรเจค ให้อธิบายถึงความหลากหลาย

คำตอบ:"""
        else:
            prompt = f"""You are an AI Assistant for {self.config.name} who excels at explaining company data

User Question: {question}

Company Database Information:
{json.dumps(data, ensure_ascii=False, default=str, indent=2)}

Your tasks:
1. Analyze the received data
2. Answer with natural and engaging language
3. Use real numbers from data but explain in an understandable context
4. You can use emojis for engagement
5. Provide complete and useful answers

Examples:
- If data shows employee count, explain what size team this represents
- If salary data, explain in job market context
- If project data, explain about diversity and scope

Answer:"""
        
        try:
            base_url = self.ollama_config.get('base_url', 'http://192.168.11.97:12434')
            model = self.ollama_config.get('model', 'llama3.1:8b')
            
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.8,      # More creative
                    "num_predict": 400,      # More space for detailed answers
                    "top_k": 50,
                    "top_p": 0.9,
                    "repeat_penalty": 1.1,
                    "stop": ["คำถาม:", "Question:", "ข้อมูล:", "Database:"]
                }
            }
            
            logger.info(f"Sending prompt to Ollama for {self.tenant_id}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30)  # More time
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        ai_answer = result.get('response', '').strip()
                        
                        logger.info(f"Ollama response length: {len(ai_answer)} chars")
                        
                        if ai_answer and len(ai_answer) > 15:  # Lower threshold
                            cleaned_answer = self._post_process_answer(ai_answer)
                            logger.info(f"Returning Ollama answer: {cleaned_answer[:100]}...")
                            return cleaned_answer
                        else:
                            logger.warning(f"Ollama answer too short: '{ai_answer}', using fallback")
                            return self._emergency_fallback(data, question)
                    else:
                        logger.error(f"Ollama HTTP error: {response.status}")
                        return self._emergency_fallback(data, question)
            
        except Exception as e:
            logger.error(f"Ollama response generation failed for {self.tenant_id}: {e}")
            return self._emergency_fallback(data, question)
    
    def _post_process_answer(self, answer: str) -> str:
        """Minimal post-processing - let AI answer stand as-is"""
        return answer.strip()
    
    def _emergency_fallback(self, data: List[Dict], question: str) -> str:
        """Raw data only - let Ollama handle all formatting"""
        if not data:
            return "ไม่พบข้อมูล"
        
        # Return raw data as simple text for Ollama to process
        if len(data) == 1 and len(data[0]) == 1:
            key, value = next(iter(data[0].items()))
            return str(value)
        
        # For multiple records, return simple list
        result = ""
        for row in data:
            row_text = ", ".join([f"{k}: {v}" for k, v in row.items()])
            result += f"{row_text}\n"
        
        return result.strip()

# ============================================================================
# Database Manager (same as before)
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
# Enhanced PostgreSQL Agent
# ============================================================================

class PostgreSQLAgent:
    """Enhanced PostgreSQL agent with smart SQL generation"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.sql_generator = SmartSQLGenerator(tenant_id)
        self.db_manager = DatabaseManager(tenant_id)
        self.formatter = SmartResponseFormatter(tenant_id)
    
    async def async_query(self, question: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Enhanced async query with better error handling"""
        tenant_id = tenant_id or self.tenant_id
        
        try:
            # 1. Generate SQL
            sql = await self.sql_generator.generate(question)
            if not sql:
                return {
                    "success": False,
                    "answer": "ไม่สามารถสร้าง SQL query สำหรับคำถามนี้ได้ กรุณาลองถามในรูปแบบอื่น",
                    "tenant_id": tenant_id
                }
            
            # 2. Execute query
            results = self.db_manager.execute_query(sql)
            
            # 3. Format response
            if results["success"]:
                answer = await self.formatter.format_results(results, question)
                return {
                    "success": True,
                    "answer": answer,
                    "sql": sql,
                    "data": results["data"],
                    "tenant_id": tenant_id,
                    "generation_method": "smart_sql"
                }
            else:
                return {
                    "success": False,
                    "answer": f"เกิดข้อผิดพลาดในการดึงข้อมูล: {results.get('error')}",
                    "sql": sql,
                    "tenant_id": tenant_id
                }
                
        except Exception as e:
            logger.error(f"Enhanced PostgreSQL Agent error for {tenant_id}: {e}")
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

# ============================================================================
# Test Functions
# ============================================================================

async def test_enhanced_postgres():
    """Test the enhanced PostgreSQL agent"""
    test_questions = [
        "มีพนักงานกี่คน",
        "เงินเดือนเฉลี่ยเท่าไหร่",
        "มีโปรเจคกี่โปรเจค",
        "พนักงานในแผนก IT กี่คน",
        "โปรเจคที่กำลังทำอยู่",
        "งบประมาณรวม"
    ]
    
    agent = PostgreSQLAgent('company-a')
    
    print("🧪 Testing Enhanced PostgreSQL Agent")
    print("=" * 60)
    
    for question in test_questions:
        print(f"\n❓ คำถาม: {question}")
        try:
            result = await agent.async_query(question)
            status = '✅' if result['success'] else '❌'
            print(f"{status} {result['answer']}")
            if 'sql' in result:
                print(f"🔧 SQL: {result['sql']}")
            if 'generation_method' in result:
                print(f"🎯 Method: {result['generation_method']}")
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_enhanced_postgres())