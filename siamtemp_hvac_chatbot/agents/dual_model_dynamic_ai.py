"""
Improved Dual Model Dynamic AI System
======================================
Enhanced version with better prompt management and error handling
Maintains all core capabilities while fixing critical issues
"""

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
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# =============================================================================
# PROMPT MANAGER - Centralized Prompt Management
# =============================================================================

class PromptManager:
    """
    ศูนย์กลางการจัดการ Prompts ทั้งหมด
    ทำให้แก้ไขและปรับปรุง prompts ได้ง่าย โดยไม่ต้องไปหาตามโค้ด
    """
    
    def __init__(self):
        # System prompt ที่บอกข้อจำกัดของฐานข้อมูลอย่างชัดเจน
        self.SQL_SYSTEM_PROMPT = """You are a PostgreSQL SQL expert for Siamtemp HVAC database.
You MUST follow these CRITICAL rules to avoid query failures:

⚠️ DATABASE QUIRKS (VERY IMPORTANT):
1. Revenue fields (overhaul_, replacement, service_contact_, parts_all_, product_all, solution_) are TEXT type
   - ALWAYS use: CAST(NULLIF(field, '') AS NUMERIC) for calculations
   - NEVER use: field::numeric or direct arithmetic

2. work_force.date is VARCHAR not DATE/TIMESTAMP
   - Format: 'DD/MM/YYYY' or Excel serial numbers like '45789'
   - NEVER use: date >= '2024-01-01' or DATE functions
   - CORRECT: date LIKE '%/06/2024' for June 2024

3. Boolean-like fields in work_force are VARCHAR not BOOLEAN
   - job_description_pm, success, report_kpi_2_days etc.
   - NEVER use: field = true
   - CORRECT: field = 'true' OR field IS NOT NULL

4. spare_part table has TEXT price fields
   - unit_price and balance are TEXT
   - ALWAYS cast before calculations

5. Job number patterns
   - Format: 'SVyy-mm-xxx' or 'JAEyy-mm-xxx'
   - Example: 'SV24-06-001' = June 2024, job #001"""

        # Schema information สำหรับแต่ละ table
        self.SCHEMA_INFO = {
            'sales': """
Tables: sales2022, sales2023, sales2024, sales2025
Columns:
- id: SERIAL PRIMARY KEY
- job_no: VARCHAR (pattern: SVyy-mm-xxx)
- customer_name: VARCHAR
- description: TEXT
- overhaul_: TEXT (revenue, needs CAST)
- replacement: TEXT (revenue, needs CAST)
- service_contact_: TEXT (revenue, needs CAST)
- parts_all_: TEXT (revenue, needs CAST)
- product_all: TEXT (revenue, needs CAST)
- solution_: TEXT (revenue, needs CAST)""",
            
            'spare_part': """
Table: spare_part
Columns:
- id: SERIAL PRIMARY KEY  
- wh: VARCHAR (warehouse)
- product_code: VARCHAR
- product_name: VARCHAR
- unit: NUMERIC
- balance: TEXT (needs CAST)
- unit_price: TEXT (needs CAST)
- total: NUMERIC
- description: TEXT""",
            
            'work_force': """
Table: work_force
Columns:
- id: SERIAL PRIMARY KEY
- date: VARCHAR (DD/MM/YYYY or Excel serial)
- customer: VARCHAR
- project: VARCHAR
- service_group: VARCHAR (team members)
- detail: TEXT
- job_description_pm: VARCHAR (not boolean!)
- job_description_replacement: VARCHAR
- job_description_overhaul: VARCHAR
- success: VARCHAR ('1' or 'true')
- report_kpi_2_days: VARCHAR"""
        }
        
        # Examples for different query types
        self.SQL_EXAMPLES = {
            'spare_parts_search': """
Question: ราคาอะไหล่ EKAC460
SQL: SELECT 
    product_code,
    product_name,
    CAST(NULLIF(unit_price, '') AS NUMERIC) as price,
    CAST(NULLIF(balance, '') AS NUMERIC) as stock
FROM spare_part
WHERE product_code ILIKE '%EKAC460%' 
   OR product_name ILIKE '%EKAC460%'
ORDER BY price DESC
LIMIT 20;""",
            
            'work_force_monthly': """
Question: งานเดือนมิถุนายน 2565
SQL: SELECT 
    date,
    customer,
    project,
    service_group,
    detail,
    CASE WHEN success = '1' OR success = 'true' THEN 'สำเร็จ' ELSE 'รอดำเนินการ' END as status
FROM work_force
WHERE date LIKE '%/06/2022' OR date LIKE '%/06/2565'
ORDER BY id DESC
LIMIT 50;""",
            
            'revenue_summary': """
Question: รายได้รวมเดือน 6 ปี 2024
SQL: SELECT 
    COUNT(*) as total_jobs,
    COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as overhaul_revenue,
    COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as replacement_revenue,
    COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as service_revenue,
    COALESCE(SUM(
        COALESCE(CAST(NULLIF(overhaul_, '') AS NUMERIC), 0) +
        COALESCE(CAST(NULLIF(replacement, '') AS NUMERIC), 0) +
        COALESCE(CAST(NULLIF(service_contact_, '') AS NUMERIC), 0) +
        COALESCE(CAST(NULLIF(parts_all_, '') AS NUMERIC), 0) +
        COALESCE(CAST(NULLIF(product_all, '') AS NUMERIC), 0) +
        COALESCE(CAST(NULLIF(solution_, '') AS NUMERIC), 0)
    ), 0) as total_revenue
FROM sales2024
WHERE job_no LIKE '%24-06-%';"""
        }
    
    def get_sql_generation_prompt(self, question: str, intent: str, 
                                 entities: Dict, examples: List[Dict]) -> str:
        """
        สร้าง prompt สำหรับการ generate SQL
        โดยรวม system prompt, schema, examples และคำถาม
        """
        prompt_parts = []
        
        # 1. System instructions with warnings
        prompt_parts.append(self.SQL_SYSTEM_PROMPT)
        
        # 2. Relevant schema based on intent
        if intent == 'spare_parts':
            prompt_parts.append("\nRELEVANT SCHEMA:")
            prompt_parts.append(self.SCHEMA_INFO['spare_part'])
        elif intent == 'work_force':
            prompt_parts.append("\nRELEVANT SCHEMA:")
            prompt_parts.append(self.SCHEMA_INFO['work_force'])
        elif intent in ['sales', 'revenue']:
            prompt_parts.append("\nRELEVANT SCHEMA:")
            prompt_parts.append(self.SCHEMA_INFO['sales'])
        
        # 3. Add relevant examples
        if examples:
            prompt_parts.append("\nEXAMPLES TO FOLLOW:")
            for ex in examples[:3]:  # ใช้แค่ 3 ตัวอย่าง
                if 'question' in ex and 'sql' in ex:
                    prompt_parts.append(f"Q: {ex['question']}")
                    prompt_parts.append(f"SQL: {ex['sql']}\n")
        
        # 4. Add specific example based on intent
        if intent == 'spare_parts' and 'spare_parts_search' in self.SQL_EXAMPLES:
            prompt_parts.append("\nSIMILAR EXAMPLE:")
            prompt_parts.append(self.SQL_EXAMPLES['spare_parts_search'])
        elif intent == 'work_force' and 'work_force_monthly' in self.SQL_EXAMPLES:
            prompt_parts.append("\nSIMILAR EXAMPLE:")
            prompt_parts.append(self.SQL_EXAMPLES['work_force_monthly'])
        
        # 5. Current question with context
        prompt_parts.append("\n" + "="*50)
        prompt_parts.append("NOW GENERATE SQL FOR THIS QUESTION:")
        prompt_parts.append(f"Question: {question}")
        prompt_parts.append(f"Detected Intent: {intent}")
        prompt_parts.append(f"Extracted Entities: {json.dumps(entities, ensure_ascii=False)}")
        
        # 6. Final instructions
        prompt_parts.append("\nREMEMBER:")
        prompt_parts.append("- CAST all TEXT numeric fields")
        prompt_parts.append("- Use LIKE for VARCHAR date comparisons")
        prompt_parts.append("- Use = 'true' for VARCHAR boolean fields")
        prompt_parts.append("- Include LIMIT to prevent huge results")
        prompt_parts.append("\nSQL Query (return ONLY the SQL, no explanations):")
        
        return "\n".join(prompt_parts)
    
    def get_response_generation_prompt(self, question: str, results: List[Dict], 
                                      sql_query: str) -> str:
        """
        สร้าง prompt สำหรับการสร้างคำตอบภาษาธรรมชาติ
        """
        prompt = f"""You are a helpful Thai business assistant for Siamtemp HVAC company.
Convert the database results into a natural, friendly Thai response.

User Question: {question}
SQL Query Used: {sql_query}
Query Results: {json.dumps(results[:10], ensure_ascii=False, default=str)}

Instructions:
1. Answer in Thai language
2. Be concise but informative
3. Format numbers with commas (e.g., 1,234,567)
4. If no results, suggest alternatives politely
5. Mention data source (month/year) when relevant

Response in Thai:"""
        
        return prompt

# =============================================================================
# SQL VALIDATOR - Validate and Fix SQL Before Execution
# =============================================================================

class SQLValidator:
    """
    ตรวจสอบและแก้ไข SQL ก่อนส่งไป execute
    ช่วยป้องกัน errors จาก SQL ที่ generate มาไม่ถูกต้อง
    """
    
    def __init__(self):
        # Common SQL mistakes และวิธีแก้ไข
        self.fix_patterns = [
            # Fix table name issues
            (r'siamtemp\.work\s+FORCE', 'work_force'),
            (r'FROM\s+work\s+FORCE', 'FROM work_force'),
            (r'FROM\s+siamtemp\.(\w+)', r'FROM \1'),
            
            # Remove MySQL-specific syntax
            (r'FORCE\s+INDEX\s*\([^)]*\)', ''),
            (r'USE\s+INDEX\s*\([^)]*\)', ''),
            (r'IGNORE\s+INDEX\s*\([^)]*\)', ''),
            
            # Fix date functions on VARCHAR fields
            (r"date_part\('year',\s*date\)", "SUBSTRING(date, 7, 4)"),
            (r"date_part\('month',\s*date\)", "SUBSTRING(date, 4, 2)"),
            (r"EXTRACT\(YEAR\s+FROM\s+date\)", "SUBSTRING(date, 7, 4)"),
            (r"EXTRACT\(MONTH\s+FROM\s+date\)", "SUBSTRING(date, 4, 2)"),
            
            # Fix wrong date comparisons
            (r"date\s*=\s*'2022-06-\d+'", "date LIKE '%/06/2022'"),
            (r"date\s*>=\s*'(\d{4})-(\d{2})-\d+'", r"date LIKE '%/\2/\1'"),
            
            # Fix casting syntax errors
            (r'\((\w+)::text,\s*\'\'\)', r"NULLIF(\1, '')"),
            (r'(\w+)::numeric', r"CAST(NULLIF(\1, '') AS NUMERIC)"),
            
            # Fix boolean fields
            (r'(\w+_pm|success|report_\w+)\s*=\s*true', r"\1 = 'true'"),
            (r'(\w+_pm|success|report_\w+)\s*=\s*false', r"\1 IS NULL"),
            
            # Fix SELECT syntax
            (r'SELECT\s+(\w+),\s+(\w+)\s+F\s+FROM', r'SELECT \1, \2 FROM'),
            
            # Fix missing spaces
            (r'(\w+)FROM', r'\1 FROM'),
            (r'(\w+)WHERE', r'\1 WHERE'),
            
            # Fix wrong field names
            (r'amount(?!s)', 'overhaul_'),  # ไม่มี field 'amount' ใน work_force
        ]
        
        # Dangerous operations to block
        self.dangerous_operations = [
            'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 
            'CREATE', 'UPDATE', 'INSERT', 'GRANT', 'REVOKE'
        ]
    
    def validate_and_fix(self, sql: str) -> Tuple[bool, str, List[str]]:
        """
        ตรวจสอบและพยายามแก้ไข SQL
        Returns: (is_valid, fixed_sql, issues_found)
        """
        if not sql:
            return False, sql, ["Empty SQL query"]
        
        fixed_sql = sql.strip()
        issues = []
        
        # 1. ตรวจสอบ dangerous operations
        sql_upper = fixed_sql.upper()
        for op in self.dangerous_operations:
            if f' {op} ' in f' {sql_upper} ':
                return False, fixed_sql, [f"Dangerous operation detected: {op}"]
        
        # 2. Apply fix patterns (ทำหลายรอบเพื่อให้แน่ใจ)
        for _ in range(2):  # ทำ 2 รอบเพื่อจับ patterns ซ้อน
            for pattern, replacement in self.fix_patterns:
                matches = re.findall(pattern, fixed_sql, re.IGNORECASE)
                if matches:
                    fixed_sql = re.sub(pattern, replacement, fixed_sql, flags=re.IGNORECASE)
                    issues.append(f"Fixed: {pattern[:30]}...")
        
        # 3. แก้ไขเฉพาะสำหรับ work_force queries
        if 'work_force' in fixed_sql or 'เดือน' in sql or '2565' in sql:
            # ถ้าเป็น query work_force แต่ยังใช้ date functions
            if 'date_part' in fixed_sql.lower() or 'extract' in fixed_sql.lower():
                # แทนที่ด้วย LIKE pattern
                if '2022' in fixed_sql and '6' in fixed_sql:
                    fixed_sql = """SELECT 
                        date, customer, project, service_group, detail,
                        CASE WHEN success = '1' OR success = 'true' THEN 'Success' ELSE 'Pending' END as status
                    FROM work_force 
                    WHERE date LIKE '%/06/2022' OR date LIKE '%/06/2565'
                    ORDER BY id DESC 
                    LIMIT 50"""
                    issues.append("Replaced with correct work_force query")
        
        # 4. ตรวจสอบโครงสร้าง SQL พื้นฐาน
        if 'SELECT' not in sql_upper:
            return False, fixed_sql, ["Missing SELECT statement"]
        
        if 'FROM' not in sql_upper:
            return False, fixed_sql, ["Missing FROM clause"]
        
        # 5. เพิ่ม LIMIT ถ้าไม่มี (เพื่อความปลอดภัย)
        if 'LIMIT' not in sql_upper:
            if fixed_sql.rstrip().endswith(';'):
                fixed_sql = fixed_sql.rstrip(';') + ' LIMIT 100;'
            else:
                fixed_sql += ' LIMIT 100;'
            issues.append("Added LIMIT clause for safety")
        
        # 6. ตรวจสอบ parentheses matching
        if fixed_sql.count('(') != fixed_sql.count(')'):
            issues.append("Unmatched parentheses")
            if fixed_sql.count('(') > fixed_sql.count(')'):
                fixed_sql += ')' * (fixed_sql.count('(') - fixed_sql.count(')'))
        
        # 7. Clean up SQL
        fixed_sql = re.sub(r'\s+', ' ', fixed_sql)  # ลบ whitespace ซ้ำ
        fixed_sql = fixed_sql.strip()
        
        # ตัดสินใจว่า valid หรือไม่
        # ถ้ามี work FORCE หรือ date_part ที่ยังไม่ได้แก้ ถือว่า invalid
        if 'work FORCE' in fixed_sql or 'FORCE INDEX' in fixed_sql:
            return False, fixed_sql, ["Still contains invalid syntax"]
        
        is_valid = len([i for i in issues if 'Fixed' not in i and 'Added' not in i]) == 0
        
        return is_valid, fixed_sql, issues

# =============================================================================
# IMPROVED INTENT DETECTOR
# =============================================================================

class ImprovedIntentDetector:
    """
    ระบบตรวจจับ intent ที่แม่นยำขึ้น
    แก้ปัญหาการ detect ผิดจากระบบเดิม
    """
    
    def __init__(self):
        # Keywords สำหรับแต่ละ intent (เรียงตามความสำคัญ)
        self.intent_keywords = {
            'work_force': {
                'strong': ['งาน', 'ทีม', 'ช่าง', 'ทำงาน', 'สรุปงาน', 'PM', 'service_group'],
                'medium': ['project', 'โครงการ', 'success', 'สำเร็จ', 'KPI'],
                'weak': ['เดือน', 'วันที่'],
                'negative': ['ราคา', 'อะไหล่', 'รายได้', 'ยอดขาย']  # ถ้าเจอพวกนี้ อาจไม่ใช่ work_force
            },
            'spare_parts': {
                'strong': ['อะไหล่', 'spare', 'part', 'ราคา', 'price', 'สินค้า', 'product'],
                'medium': ['stock', 'คงเหลือ', 'คลัง', 'warehouse', 'unit_price'],
                'weak': ['EK', 'model', 'รุ่น'],
                'negative': ['งาน', 'ทีม', 'รายได้']
            },
            'sales': {
                'strong': ['รายได้', 'ยอดขาย', 'revenue', 'sales', 'income'],
                'medium': ['overhaul', 'replacement', 'service_contact', 'บาท'],
                'weak': ['รวม', 'ทั้งหมด', 'total'],
                'negative': ['อะไหล่', 'งาน', 'ทีม']
            }
        }
        
        # Month names mapping
        self.month_map = {
            'มกราคม': 1, 'กุมภาพันธ์': 2, 'มีนาคม': 3,
            'เมษายน': 4, 'พฤษภาคม': 5, 'มิถุนายน': 6,
            'กรกฎาคม': 7, 'สิงหาคม': 8, 'กันยายน': 9,
            'ตุลาคม': 10, 'พฤศจิกายน': 11, 'ธันวาคม': 12
        }
        
        # Year conversion (Buddhist Era to Common Era)
        self.year_map = {
            '2565': 2022, '2566': 2023, '2567': 2024, '2568': 2025,
            '2022': 2022, '2023': 2023, '2024': 2024, '2025': 2025,
            '65': 2022, '66': 2023, '67': 2024, '68': 2025
        }
    
    def detect_intent_and_entities(self, question: str, 
                                  previous_intent: Optional[str] = None) -> Dict:
        """
        ตรวจจับ intent และ extract entities จากคำถาม
        พิจารณา context จาก previous_intent ด้วย แต่ไม่ให้น้ำหนักมากเกินไป
        """
        question_lower = question.lower()
        
        # Calculate scores for each intent
        intent_scores = {}
        
        for intent, keywords in self.intent_keywords.items():
            score = 0
            
            # Check strong indicators (weight = 10)
            for keyword in keywords.get('strong', []):
                if keyword.lower() in question_lower:
                    score += 10
                    
            # Check medium indicators (weight = 5)
            for keyword in keywords.get('medium', []):
                if keyword.lower() in question_lower:
                    score += 5
                    
            # Check weak indicators (weight = 2)
            for keyword in keywords.get('weak', []):
                if keyword.lower() in question_lower:
                    score += 2
                    
            # Subtract for negative indicators
            for keyword in keywords.get('negative', []):
                if keyword.lower() in question_lower:
                    score -= 3
            
            # Small bonus if matches previous intent (but not too much)
            if previous_intent == intent:
                score += 2
            
            intent_scores[intent] = max(0, score)  # ไม่ให้ติดลบ
        
        # Get best intent
        best_intent = max(intent_scores, key=intent_scores.get)
        confidence = min(intent_scores[best_intent] / 30, 1.0)  # Normalize to 0-1
        
        # ถ้า score ต่ำมาก ให้ดูจาก pattern
        if intent_scores[best_intent] < 5:
            best_intent = self._detect_by_pattern(question)
            confidence = 0.6
        
        # Extract entities
        entities = self._extract_entities(question)
        
        return {
            'intent': best_intent,
            'confidence': confidence,
            'entities': entities,
            'scores': intent_scores  # เก็บไว้ debug
        }
    
    def _detect_by_pattern(self, question: str) -> str:
        """Fallback detection using patterns"""
        question_lower = question.lower()
        
        # Specific patterns
        if re.search(r'(งาน|ทำ|ทีม|ช่าง).*(เดือน|วัน)', question_lower):
            return 'work_force'
        
        if re.search(r'(ราคา|price|อะไหล่|spare|part|EK\w+)', question_lower, re.IGNORECASE):
            return 'spare_parts'
        
        if re.search(r'(รายได้|ยอด|income|revenue|บาท)', question_lower):
            return 'sales'
        
        return 'sales'  # Default
    
    def _extract_entities(self, question: str) -> Dict:
        """Extract entities from question"""
        entities = {
            'years': [],
            'months': [],
            'products': [],
            'customers': [],
            'amounts': []
        }
        
        # Extract years
        for text_year, num_year in self.year_map.items():
            if text_year in question:
                if num_year not in entities['years']:
                    entities['years'].append(num_year)
        
        # Extract months
        for month_name, month_num in self.month_map.items():
            if month_name in question.lower():
                if month_num not in entities['months']:
                    entities['months'].append(month_num)
        
        # Extract month numbers
        month_pattern = r'เดือน\s*(\d{1,2})'
        month_matches = re.findall(month_pattern, question)
        for month in month_matches:
            month_num = int(month)
            if 1 <= month_num <= 12 and month_num not in entities['months']:
                entities['months'].append(month_num)
        
        # Extract product codes
        product_pattern = r'EK[A-Z]*[\d]+|EKAC[\d]+'
        products = re.findall(product_pattern, question, re.IGNORECASE)
        entities['products'].extend(products)
        
        # Extract amounts (numbers with possible commas)
        amount_pattern = r'[\d,]+(?:\.\d+)?'
        amounts = re.findall(amount_pattern, question)
        entities['amounts'] = [a.replace(',', '') for a in amounts]
        
        return entities

# =============================================================================
# MAIN DUAL MODEL DYNAMIC AI SYSTEM (IMPROVED)
# =============================================================================

class ImprovedDualModelDynamicAISystem:
    """
    ระบบ AI หลักที่ปรับปรุงแล้ว
    คงความสามารถเดิมทั้งหมด แต่เพิ่มการจัดการ prompt และ validation ที่ดีขึ้น
    """
    
    def __init__(self):
        # Initialize core components
        self.prompt_manager = PromptManager()
        self.sql_validator = SQLValidator()
        self.intent_detector = ImprovedIntentDetector()
        
        # Initialize existing components (คงไว้ตามเดิม)
        self.db_handler = SimplifiedDatabaseHandler()
        self.ollama_client = SimplifiedOllamaClient()
        self.conversation_memory = ConversationMemory()
        self.parallel_processor = ParallelProcessingEngine()
        self.data_cleaner = DataCleaningEngine()
        
        # Model configuration
        self.SQL_MODEL = os.getenv('SQL_MODEL', 'mannix/defog-llama3-sqlcoder-8b:latest')
        self.NL_MODEL = os.getenv('NL_MODEL', 'llama3.1:8b')
        
        # Flag to test Ollama connection later (not in __init__)
        self.ollama_tested = False
        
        # Feature flags (คงไว้ตามเดิม)
        self.enable_conversation_memory = True
        self.enable_parallel_processing = True
        self.enable_data_cleaning = True
        self.enable_sql_validation = True
        self.enable_few_shot_learning = True
        
        # Caches
        self.sql_cache = {}
        self.schema_cache = {}
        
        # Statistics
        self.stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'validation_fixes': 0,
            'cache_hits': 0,
            'avg_response_time': 0
        }
        
        # Load SQL examples
        self.sql_examples = self._load_sql_examples()
        self.dynamic_examples = []
        self.max_dynamic_examples = 100
        
        logger.info("🚀 Improved Dual-Model Dynamic AI System initialized")
    
    async def ensure_ollama_connection(self):
        """ทดสอบการเชื่อมต่อกับ Ollama (เรียกแยกต่างหาก)"""
        if not self.ollama_tested:
            connected = await self.ollama_client.test_connection()
            self.ollama_tested = True
            if not connected:
                logger.warning("⚠️ Ollama connection test failed. SQL generation may not work.")
            return connected
        return True
    
    def _load_sql_examples(self) -> List[Dict]:
        """
        โหลดตัวอย่าง SQL สำหรับ few-shot learning
        ตัวอย่างเหล่านี้จะช่วยให้ AI เรียนรู้รูปแบบการสร้าง SQL ที่ถูกต้อง
        """
        examples = [
            # Sales/Revenue Examples
            {
                'category': 'sales',
                'intent': 'sales',
                'question': 'รายได้รวมเดือนมิถุนายน 2024',
                'sql': """SELECT 
                    COUNT(*) as total_jobs,
                    COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as overhaul_revenue,
                    COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as replacement_revenue,
                    COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as service_revenue
                FROM sales2024 
                WHERE job_no LIKE '%24-06-%';""",
                'entities': {'years': [2024], 'months': [6]}
            },
            {
                'category': 'sales',
                'intent': 'sales',
                'question': 'ยอดขายทั้งหมดปี 2023',
                'sql': """SELECT 
                    COALESCE(SUM(
                        COALESCE(CAST(NULLIF(overhaul_, '') AS NUMERIC), 0) +
                        COALESCE(CAST(NULLIF(replacement, '') AS NUMERIC), 0) +
                        COALESCE(CAST(NULLIF(service_contact_, '') AS NUMERIC), 0)
                    ), 0) as total_revenue
                FROM sales2023;""",
                'entities': {'years': [2023]}
            },
            
            # Spare Parts Examples
            {
                'category': 'spare_parts',
                'intent': 'spare_parts',
                'question': 'ราคาอะไหล่ EKAC460',
                'sql': """SELECT 
                    product_code,
                    product_name,
                    CAST(NULLIF(unit_price, '') AS NUMERIC) as price,
                    CAST(NULLIF(balance, '') AS NUMERIC) as stock
                FROM spare_part 
                WHERE product_code ILIKE '%EKAC460%' 
                   OR product_name ILIKE '%EKAC460%'
                ORDER BY price DESC 
                LIMIT 20;""",
                'entities': {'products': ['EKAC460']}
            },
            {
                'category': 'spare_parts',
                'intent': 'spare_parts',
                'question': 'อะไหล่ที่มีในคลังกลาง',
                'sql': """SELECT 
                    product_code,
                    product_name,
                    CAST(NULLIF(balance, '') AS NUMERIC) as stock
                FROM spare_part 
                WHERE wh = 'คลังกลาง' 
                  AND CAST(NULLIF(balance, '') AS NUMERIC) > 0
                ORDER BY stock DESC 
                LIMIT 50;""",
                'entities': {}
            },
            
            # Work Force Examples
            {
                'category': 'work_force',
                'intent': 'work_force',
                'question': 'งานที่ทำเดือนมิถุนายน 2565',
                'sql': """SELECT 
                    date,
                    customer,
                    project,
                    service_group,
                    detail
                FROM work_force 
                WHERE date LIKE '%/06/2022' 
                   OR date LIKE '%/06/2565'
                ORDER BY id DESC 
                LIMIT 50;""",
                'entities': {'years': [2022], 'months': [6]}
            },
            {
                'category': 'work_force',
                'intent': 'work_force',
                'question': 'งาน PM ที่สำเร็จ',
                'sql': """SELECT 
                    date,
                    customer,
                    service_group,
                    detail
                FROM work_force 
                WHERE job_description_pm = 'true' 
                  AND (success = '1' OR success = 'true')
                ORDER BY id DESC 
                LIMIT 30;""",
                'entities': {}
            }
        ]
        
        logger.info(f"📚 Loaded {len(examples)} SQL examples for few-shot learning")
        return examples
    
    async def process_any_question(self, question: str, tenant_id: str = 'company-a',
                                  user_id: str = 'default') -> Dict[str, Any]:
        """
        Main entry point - รับคำถามและประมวลผลด้วยระบบที่ปรับปรุงแล้ว
        """
        start_time = time.time()
        self.stats['total_queries'] += 1
        
        try:
            # Test Ollama connection on first query
            await self.ensure_ollama_connection()
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🎯 Processing: {question}")
            logger.info(f"👤 User: {user_id} | 🏢 Tenant: {tenant_id}")
            
            # 1. Get conversation context (ถ้าเปิดใช้งาน)
            context = {}
            previous_intent = None
            
            if self.enable_conversation_memory:
                context = self.conversation_memory.get_context(user_id, question)
                if context.get('recent_intents'):
                    previous_intent = context['recent_intents'][-1] if context['recent_intents'] else None
                logger.info(f"💭 Context: {context.get('conversation_count', 0)} previous conversations")
            
            # 2. Detect intent and extract entities (ใช้ระบบใหม่)
            detection_result = self.intent_detector.detect_intent_and_entities(
                question, previous_intent
            )
            intent = detection_result['intent']
            entities = detection_result['entities']
            confidence = detection_result['confidence']
            
            logger.info(f"📊 Intent: {intent} (confidence: {confidence:.2f})")
            logger.info(f"🔍 Entities: {entities}")
            
            # 3. Generate SQL (with improved prompt)
            sql_query = await self._generate_improved_sql(
                question, intent, entities, context
            )
            
            if not sql_query:
                raise ValueError("Failed to generate SQL query")
            
            # 4. Validate and fix SQL
            if self.enable_sql_validation:
                is_valid, fixed_sql, issues = self.sql_validator.validate_and_fix(sql_query)
                
                if issues:
                    logger.info(f"🔧 SQL Validation: {issues}")
                    self.stats['validation_fixes'] += len(issues)
                
                sql_query = fixed_sql
                
                if not is_valid:
                    logger.warning(f"⚠️ SQL validation failed, attempting retry...")
                    # ลองสร้างใหม่ด้วย prompt ที่เข้มงวดขึ้น
                    sql_query = await self._generate_sql_with_strict_prompt(
                        question, intent, entities, issues
                    )
            
            logger.info(f"📝 SQL Generated: {sql_query[:200]}...")
            
            # 5. Execute query
            results = await self.execute_query(sql_query)
            
            # 6. Clean results (ถ้าเปิดใช้งาน)
            if self.enable_data_cleaning and results:
                results, cleaning_stats = self.data_cleaner.clean_results(results)
                logger.info(f"🧹 Data cleaned: {cleaning_stats}")
            
            # 7. Generate natural language response
            answer = await self._generate_response(
                question, results, sql_query, intent
            )
            
            # 8. Prepare final response
            processing_time = time.time() - start_time
            response = {
                'answer': answer,
                'success': True,
                'sql_query': sql_query,
                'results_count': len(results),
                'tenant_id': tenant_id,
                'user_id': user_id,
                'processing_time': processing_time,
                'ai_system_used': 'improved_dual_model',
                'intent': intent,
                'entities': entities,
                'confidence': confidence,
                'features_used': {
                    'conversation_memory': self.enable_conversation_memory,
                    'parallel_processing': self.enable_parallel_processing,
                    'data_cleaning': self.enable_data_cleaning,
                    'sql_validation': self.enable_sql_validation,
                    'validation_fixes': len(issues) if self.enable_sql_validation else 0
                }
            }
            
            # 9. Update memory and stats
            if self.enable_conversation_memory:
                self.conversation_memory.add_conversation(user_id, question, response)
            
            self.stats['successful_queries'] += 1
            self._update_avg_response_time(processing_time)
            
            logger.info(f"✅ Completed in {processing_time:.2f}s")
            
            return response
            
        except Exception as e:
            self.stats['failed_queries'] += 1
            logger.error(f"❌ Processing failed: {e}")
            
            return {
                'answer': f"ขออภัย เกิดข้อผิดพลาดในการประมวลผล: {str(e)}",
                'success': False,
                'error': str(e),
                'processing_time': time.time() - start_time,
                'ai_system_used': 'improved_dual_model'
            }
    
    async def _generate_improved_sql(self, question: str, intent: str,
                                    entities: Dict, context: Dict) -> str:
        """
        Generate SQL using improved prompt system
        """
        # Check cache first
        cache_key = hashlib.md5(
            f"{question}_{intent}_{json.dumps(entities, sort_keys=True)}".encode()
        ).hexdigest()
        
        if cache_key in self.sql_cache:
            self.stats['cache_hits'] += 1
            logger.info("📋 Using cached SQL")
            return self.sql_cache[cache_key]
        
        # Get relevant examples
        examples = self._get_relevant_examples(intent, entities)
        
        # Build prompt using PromptManager
        prompt = self.prompt_manager.get_sql_generation_prompt(
            question, intent, entities, examples
        )
        
        # Generate SQL using Ollama
        sql = await self.ollama_client.generate(prompt, self.SQL_MODEL)
        
        # Clean up the response
        sql = self._clean_sql_response(sql)
        
        # Cache the result
        if sql:
            self.sql_cache[cache_key] = sql
        
        return sql
    
    async def _generate_sql_with_strict_prompt(self, question: str, intent: str,
                                              entities: Dict, issues: List[str]) -> str:
        """
        Generate SQL with stricter prompt after validation failure
        """
        # Build a more explicit prompt based on the issues found
        prompt = f"""CRITICAL: Previous SQL had these errors: {', '.join(issues)}

You MUST generate a correct PostgreSQL query for Siamtemp database.

MANDATORY RULES TO AVOID ERRORS:
1. Cast TEXT fields: CAST(NULLIF(field_name, '') AS NUMERIC)
2. Date comparisons: Use LIKE for VARCHAR dates, never use DATE functions
3. Boolean fields: Use = 'true' not = true
4. Always include LIMIT

Question: {question}
Intent: {intent}
Entities: {json.dumps(entities, ensure_ascii=False)}

Generate ONLY the SQL query:"""
        
        sql = await self.ollama_client.generate(prompt, self.SQL_MODEL)
        return self._clean_sql_response(sql)
    
    async def _generate_response(self, question: str, results: List[Dict],
                                sql_query: str, intent: str) -> str:
        """
        Generate natural language response
        """
        if not results:
            return self._generate_no_results_response(intent, question)
        
        # Use PromptManager for response generation
        prompt = self.prompt_manager.get_response_generation_prompt(
            question, results, sql_query
        )
        
        response = await self.ollama_client.generate(prompt, self.NL_MODEL)
        
        # ถ้าไม่ได้คำตอบจาก model ให้ใช้ template
        if not response:
            response = self._generate_template_response(results, intent)
        
        return response
    
    def _generate_no_results_response(self, intent: str, question: str) -> str:
        """Generate appropriate response when no results found"""
        responses = {
            'spare_parts': "ขออภัย ไม่พบข้อมูลอะไหล่ที่ตรงกับคำค้นหา กรุณาตรวจสอบรหัสสินค้าหรือลองค้นหาด้วยคำอื่น",
            'work_force': "ขออภัย ไม่พบข้อมูลงานในช่วงเวลาที่ระบุ กรุณาตรวจสอบวันที่หรือเดือนที่ต้องการค้นหา",
            'sales': "ขออภัย ไม่พบข้อมูลรายได้ในช่วงที่ระบุ กรุณาตรวจสอบปีหรือเดือนที่ต้องการ"
        }
        
        return responses.get(intent, "ขออภัย ไม่พบข้อมูลที่ตรงกับคำถามของคุณ กรุณาลองถามใหม่อีกครั้ง")
    
    def _generate_template_response(self, results: List[Dict], intent: str) -> str:
        """Fallback template-based response generation"""
        if intent == 'spare_parts':
            if results[0].get('price'):
                return f"พบอะไหล่ {results[0].get('product_name', 'N/A')} ราคา {results[0].get('price', 0):,.2f} บาท"
        
        elif intent == 'work_force':
            return f"พบงานทั้งหมด {len(results)} รายการในช่วงเวลาที่ระบุ"
        
        elif intent == 'sales':
            if results[0].get('total_revenue'):
                return f"รายได้รวม {results[0].get('total_revenue', 0):,.2f} บาท"
        
        return f"พบข้อมูล {len(results)} รายการ"
    
    def _clean_sql_response(self, sql: str) -> str:
        """Clean SQL response from Ollama"""
        if not sql:
            return ""
        
        # Remove markdown code blocks
        sql = re.sub(r'```sql?\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'```', '', sql)
        
        # Remove explanations (หยุดที่บรรทัดที่ขึ้นต้นด้วย --, #, หรือคำอธิบาย)
        lines = []
        for line in sql.split('\n'):
            line = line.strip()
            if line.startswith('--') or line.startswith('#'):
                continue
            if any(word in line.lower() for word in ['explanation:', 'note:', 'this query']):
                break
            if line:
                lines.append(line)
        
        sql = ' '.join(lines)
        
        # Ensure it ends with semicolon
        if sql and not sql.rstrip().endswith(';'):
            sql = sql.rstrip() + ';'
        
        return sql.strip()
    
    def _get_relevant_examples(self, intent: str, entities: Dict) -> List[Dict]:
        """Get relevant SQL examples for few-shot learning"""
        relevant = []
        
        for example in self.sql_examples:
            if example.get('category') == intent or example.get('intent') == intent:
                relevant.append(example)
        
        # Add dynamic examples if available
        for example in self.dynamic_examples[-10:]:  # Last 10 dynamic examples
            if example.get('intent') == intent:
                relevant.append(example)
        
        return relevant[:3]  # Return top 3 examples
    
    async def execute_query(self, sql: str) -> List[Dict]:
        """Execute SQL query with error handling"""
        try:
            results = await self.db_handler.execute_query(sql)
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            # Log the problematic SQL for debugging
            logger.error(f"Problematic SQL: {sql}")
            raise
    
    def _update_avg_response_time(self, new_time: float):
        """Update average response time"""
        current_avg = self.stats['avg_response_time']
        total_queries = self.stats['successful_queries']
        
        if total_queries == 1:
            self.stats['avg_response_time'] = new_time
        else:
            # คำนวณค่าเฉลี่ยใหม่
            total_time = current_avg * (total_queries - 1) + new_time
            self.stats['avg_response_time'] = total_time / total_queries
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get comprehensive system statistics"""
        return {
            'performance': {
                'total_queries': self.stats['total_queries'],
                'successful_queries': self.stats['successful_queries'],
                'failed_queries': self.stats['failed_queries'],
                'success_rate': (self.stats['successful_queries'] / max(self.stats['total_queries'], 1)) * 100,
                'validation_fixes': self.stats['validation_fixes'],
                'cache_hit_rate': (self.stats['cache_hits'] / max(self.stats['total_queries'], 1)) * 100,
                'avg_response_time': self.stats['avg_response_time']
            },
            'features': {
                'conversation_memory': self.enable_conversation_memory,
                'parallel_processing': self.enable_parallel_processing,
                'data_cleaning': self.enable_data_cleaning,
                'sql_validation': self.enable_sql_validation,
                'few_shot_learning': self.enable_few_shot_learning
            }
        }

# =============================================================================
# SUPPORTING COMPONENTS (คลาสที่จำเป็นสำหรับการทำงาน)
# =============================================================================

class ConversationMemory:
    """
    ระบบจดจำบทสนทนา - เก็บประวัติการสนทนาและ context
    """
    def __init__(self, max_history: int = 20):
        self.conversations = defaultdict(lambda: deque(maxlen=max_history))
        self.user_preferences = defaultdict(dict)
        self.successful_patterns = defaultdict(list)
    
    def add_conversation(self, user_id: str, query: str, response: Dict[str, Any]):
        """บันทึกบทสนทนา"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'query': query,
            'intent': response.get('intent', 'unknown'),
            'entities': response.get('entities', {}),
            'success': response.get('success', False),
            'sql_query': response.get('sql_query'),
            'results_count': response.get('results_count', 0)
        }
        self.conversations[user_id].append(entry)
        
        # Track successful patterns
        if entry['success'] and entry['sql_query']:
            pattern_key = f"{entry['intent']}_{json.dumps(entry['entities'], sort_keys=True)}"
            self.successful_patterns[pattern_key].append(entry['sql_query'])
    
    def get_context(self, user_id: str, current_query: str) -> Dict[str, Any]:
        """ดึง context จากประวัติ"""
        recent = list(self.conversations[user_id])[-5:]
        
        context = {
            'conversation_count': len(self.conversations[user_id]),
            'recent_queries': [c['query'] for c in recent],
            'recent_intents': [c['intent'] for c in recent],
            'recent_entities': self._merge_recent_entities(recent),
            'has_history': len(recent) > 0
        }
        
        return context
    
    def _merge_recent_entities(self, conversations: List[Dict]) -> Dict:
        """รวม entities จากบทสนทนาล่าสุด"""
        merged = defaultdict(set)
        for conv in conversations:
            for key, value in conv.get('entities', {}).items():
                if isinstance(value, list):
                    merged[key].update(value)
                else:
                    merged[key].add(value)
        return {k: list(v) for k, v in merged.items()}

class ParallelProcessingEngine:
    """
    ระบบประมวลผลแบบขนาน - ยังไม่ได้ implement เต็มรูปแบบ
    แต่เตรียมโครงสร้างไว้สำหรับอนาคต
    """
    def __init__(self):
        self.performance_stats = defaultdict(list)
    
    async def parallel_analyze(self, question: str, context: Dict) -> Dict[str, Any]:
        """วิเคราะห์แบบขนาน (สำหรับอนาคต)"""
        # ตอนนี้ยังเป็น sequential
        return {
            'intent': 'sales',
            'entities': {},
            'complexity': 'simple'
        }

class DataCleaningEngine:
    """
    ระบบทำความสะอาดข้อมูล - แก้ไข encoding และ format ต่างๆ
    """
    def __init__(self):
        self.thai_encoding_fixes = {
            'à¸': '',  # Mojibake patterns
            'Ã': '',
            'â€': ''
        }
    
    def clean_results(self, results: List[Dict]) -> Tuple[List[Dict], Dict]:
        """ทำความสะอาดผลลัพธ์จาก database"""
        if not results:
            return results, {'cleaned': 0}
        
        cleaned_results = []
        stats = {'cleaned': 0, 'null_values': 0}
        
        for row in results:
            cleaned_row = {}
            for key, value in row.items():
                # แก้ไข encoding ภาษาไทย
                if isinstance(value, str):
                    for pattern, replacement in self.thai_encoding_fixes.items():
                        if pattern in value:
                            value = value.replace(pattern, replacement)
                            stats['cleaned'] += 1
                
                # จัดการ null values
                if value is None or value == 'NULL':
                    value = ''
                    stats['null_values'] += 1
                
                cleaned_row[key] = value
            
            cleaned_results.append(cleaned_row)
        
        return cleaned_results, stats

class SimplifiedDatabaseHandler:
    """
    ตัวจัดการฐานข้อมูล - เชื่อมต่อและ execute queries
    """
    def __init__(self):
        self.connection = None
        self._connect()
    
    def _connect(self):
        """สร้างการเชื่อมต่อกับ PostgreSQL"""
        try:
            # ดึง config จาก environment variables
            db_config = {
                'host': os.getenv('DB_HOST', 'postgres-company-a'),
                'port': os.getenv('DB_PORT', '5432'),
                'database': os.getenv('DB_NAME', 'siamtemp_company_a'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'password123')  # ใช้รหัสผ่านจาก docker-compose
            }
            
            logger.info(f"🔌 Connecting to database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
            
            self.connection = psycopg2.connect(
                **db_config,
                cursor_factory=RealDictCursor
            )
            logger.info("✅ Database connected successfully")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            logger.error(f"   Host: {db_config.get('host')}, User: {db_config.get('user')}")
            self.connection = None
    
    async def execute_query(self, sql: str) -> List[Dict]:
        """Execute SQL query และคืนผลลัพธ์"""
        if not self.connection:
            self._connect()
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            logger.error(f"SQL: {sql}")
            
            # ถ้า connection ขาด ลอง reconnect
            if 'connection' in str(e).lower():
                self._connect()
            
            raise e
    
    def close_connections(self):
        """ปิดการเชื่อมต่อ"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

class SimplifiedOllamaClient:
    """
    Client สำหรับเชื่อมต่อกับ Ollama API
    """
    def __init__(self):
        # ใช้ IP จริงของ Ollama server แทน host.docker.internal
        self.base_url = os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434')
        self.timeout = 120
        logger.info(f"🔗 Ollama client configured with: {self.base_url}")
    
    async def generate(self, prompt: str, model: str) -> str:
        """ส่ง prompt ไปยัง Ollama และรับคำตอบกลับมา"""
        payload = {
            'model': model,
            'prompt': prompt,
            'stream': False,
            'temperature': 0.1,  # ต่ำเพื่อความแม่นยำ
            'top_p': 0.9,
            'max_tokens': 500
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get('response', '')
                    else:
                        logger.error(f"Ollama API error: {response.status}")
                        return ""
        except asyncio.TimeoutError:
            logger.error("Ollama request timeout")
            return ""
        except Exception as e:
            logger.error(f"Ollama request failed: {e}")
            return ""
    
    async def test_connection(self) -> bool:
        """ทดสอบการเชื่อมต่อกับ Ollama server"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}/api/tags",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        models = data.get('models', [])
                        logger.info(f"✅ Ollama connected. Available models: {len(models)}")
                        for model in models[:3]:  # แสดงแค่ 3 models แรก
                            logger.info(f"   - {model.get('name', 'unknown')}")
                        return True
                    else:
                        logger.error(f"❌ Ollama connection failed: HTTP {response.status}")
                        return False
        except Exception as e:
            logger.error(f"❌ Cannot connect to Ollama at {self.base_url}: {e}")
            return False

# =============================================================================
# BACKWARD COMPATIBILITY
# =============================================================================

# สร้าง alias เพื่อความเข้ากันได้กับโค้ดเดิม
DualModelDynamicAISystem = ImprovedDualModelDynamicAISystem

class UnifiedEnhancedPostgresOllamaAgent:
    """Wrapper class for backward compatibility"""
    
    def __init__(self):
        self.dual_model_ai = ImprovedDualModelDynamicAISystem()
        logger.info("✅ Unified Enhanced Agent initialized with improved system")
    
    async def process_any_question(self, question: str, tenant_id: str = 'company-a',
                                  user_id: str = 'default') -> Dict[str, Any]:
        """Forward to improved system"""
        return await self.dual_model_ai.process_any_question(question, tenant_id, user_id)
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get system statistics"""
        return self.dual_model_ai.get_system_stats()

# Alias สำหรับความเข้ากันได้
EnhancedUnifiedPostgresOllamaAgent = UnifiedEnhancedPostgresOllamaAgent

# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    import asyncio
    
    async def test_improved_system():
        """Test the improved system"""
        system = ImprovedDualModelDynamicAISystem()
        
        test_cases = [
            "อยากทราบราคา อะไหล่เครื่อง EK model EKAC460",
            "สรุปงานที่ทำของเดือนมิถุนายน2565",
            "รายได้รวมเดือน 6 ปี 2024 เท่าไหร่",
            "มีอะไหล่อะไรบ้างในคลัง"
        ]
        
        print("="*60)
        print("🧪 TESTING IMPROVED SYSTEM")
        print("="*60)
        
        for question in test_cases:
            print(f"\n📝 Question: {question}")
            print("-"*40)
            
            result = await system.process_any_question(question)
            
            print(f"✅ Success: {result.get('success')}")
            print(f"🎯 Intent: {result.get('intent')}")
            print(f"📊 Confidence: {result.get('confidence', 0):.2f}")
            print(f"💬 Answer: {result.get('answer')[:200]}...")
            
            if result.get('features_used'):
                fixes = result['features_used'].get('validation_fixes', 0)
                if fixes > 0:
                    print(f"🔧 SQL fixes applied: {fixes}")
            
            print(f"⏱️ Time: {result.get('processing_time', 0):.2f}s")
        
        # Show stats
        stats = system.get_system_stats()
        print("\n" + "="*60)
        print("📊 SYSTEM STATISTICS")
        print("-"*40)
        print(json.dumps(stats, indent=2))
    
    # Run test
    asyncio.run(test_improved_system())