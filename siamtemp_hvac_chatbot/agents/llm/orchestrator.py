import json
import logging
import asyncio
import httpx
import re
import os
import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class LLMResponse:
    success: bool
    content: str
    error: Optional[str] = None

# =============================================================================
# DATABASE WRAPPER สำหรับแก้ปัญหา ASYNC/SYNC
# =============================================================================

class DatabaseWrapper:
    """Simple wrapper to handle async database calls"""
    
    def __init__(self, db_handler):
        self.db_handler = db_handler
        self.sync_connection = None
        self._create_sync_connection()
    
    def _create_sync_connection(self):
        """สร้าง sync connection สำรอง"""
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
            logger.info("Database wrapper sync connection created")
            
        except Exception as e:
            logger.error(f"Sync connection failed: {e}")
            self.sync_connection = None
    
    def execute_query(self, sql: str) -> List[Dict]:
        """Execute query synchronously"""
        try:
            # ลองใช้ sync connection ตรงๆ
            if self.sync_connection:
                with self.sync_connection.cursor() as cursor:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    return [dict(row) for row in rows]
            else:
                self._create_sync_connection()
                if self.sync_connection:
                    with self.sync_connection.cursor() as cursor:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        return [dict(row) for row in rows]
                        
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            # Try to reconnect
            self._create_sync_connection()
            raise
        
        return []
    
    def close(self):
        """Close connection"""
        if self.sync_connection:
            self.sync_connection.close()

# =============================================================================
# ULTRA-HIGH PERFORMANCE SCHEMA MANAGER
# =============================================================================

class SchemaManager:
    """จัดการข้อมูล schema ของฐานข้อมูล - Ultra Performance Version"""
    
    @staticmethod
    def get_schema_description() -> str:
        return """
=== SIAMTEMP HVAC DATABASE SCHEMA ===

**DATABASE OVERVIEW:**
ระบบ HVAC Siamtemp มี 3 ตารางหลัก สำหรับข้อมูลการขาย อะไหล่ และงานบริการ

**TABLE 1: v_sales (ข้อมูลการขายและรายได้)**
COLUMNS:
- id (integer): Primary key
- year (varchar(4)): ปี เช่น '2022','2023','2024','2025'
- job_no (varchar(100)): เลขที่งาน เช่น 'JAE22-03-004-OV', 'SV.64-11-143 S'
- customer_name (varchar(255)): ชื่อลูกค้า
- description (varchar(255)): รายละเอียดงาน
- overhaul_num, replacement_num, service_num, parts_num, product_num, solution_num: ยอดเงินแต่ละประเภท
- total_revenue (numeric): รายได้รวมทั้งหมด

ตัวอย่างข้อมูล:
- CLARION ASIA (THAILAND) CO.,LTD.: ลูกค้าประจำ มีงาน overhaul และ service
- บ. ชินอิทซึ แม็คเนติคส์ ฯ จก.: มีงานบำรุงรักษาเครื่องทำน้ำเย็น Hitachi
- สำนักปลัดกระทรวงกลาโหม: งานบำรุงรักษา VRF และซ่อมอุปกรณ์

USE FOR: การขาย, รายได้, ลูกค้า, ยอดขาย, การเงิน, ประเภทงาน, สถิติธุรกิจ

**TABLE 2: v_spare_part (อะไหล่และสินค้าคงคลัง)**  
COLUMNS:
- id (integer): Primary key
- wh (varchar(100)): คลัง เช่น '00 คลังกลาง', '07 บริการ'
- product_code (varchar(100)): รหัสสินค้า เช่น '17B27237A', 'G7A00032A'
- product_name (varchar(100)): ชื่อสินค้า เช่น 'PRINTED CIRCUIT BOARD', 'FAN MOTOR'
- unit (varchar(100)): หน่วย เช่น 'EA', 'PCS'
- balance_num (numeric): จำนวนคงเหลือ
- unit_price_num (numeric): ราคาต่อหน่วย
- total_num (numeric): มูลค่ารวม
- description (varchar(255)): รายละเอียดการใช้งาน
- received (varchar(100)): วันที่รับเข้า

ตัวอย่างข้อมูล:
- PCB, MOTOR, SENSOR, EXPANSION VALVE
- อะไหล่สำหรับ Chiller: RCU, RCUG, RCUF models
- คลังมี 2 แบบ: คลังกลาง และคลังบริการ

USE FOR: อะไหล่, สต็อก, ราคา, คลัง, สินค้าคงคลัง, inventory

**TABLE 3: v_work_force (งานบริการและทีมงาน)**
COLUMNS:  
- id (integer): Primary key
- date (date): วันที่ทำงาน เช่น '2025-04-04', '2025-07-12'
- customer (varchar(1000)): ชื่อลูกค้า
- project (varchar(100)): ชื่อโครงการ
- detail (varchar(255)): รายละเอียดงาน
- job_description_pm (boolean): งาน PM หรือไม่
- job_description_replacement (boolean): งานเปลี่ยนอุปกรณ์
- job_description_overhaul: งาน overhaul
- job_description_start_up: งาน start up
- service_group (varchar(255)): ทีมบริการ เช่น 'สุพรรณ วโรทร อำนาจ'
- success, unsuccessful: สถานะความสำเร็จ
- duration (varchar(255)): ระยะเวลา เช่น '6 ชั่วโมง', '2 วัน'
- report_kpi_2_days, report_over_kpi_2_days: KPI การส่งรายงาน

ตัวอย่างข้อมูล:
- STANLEY, ฮอนด้า: งานบำรุงรักษารายปี
- บริษัทเมนลี่ ซิลเวอร์: งานถ่ายน้ำมัน Chiller
- SADESA: งานล้าง CH-01

USE FOR: งานบริการ, ทีมงาน, แผนงาน, PM, การซ่อม, ประสิทธิภาพ

=== SQL GENERATION RULES ===

**1. CUSTOMER NAME FILTERING:**
- Extract key company names from question
- Use: customer_name ILIKE '%keyword%' 
- Remove prefixes: บ., บริษัท, จก., จำกัด, Co., Ltd.
- Examples:
  * "บ. ชินอิทซึ แม็คเนติคส์ ฯ จก." → ILIKE '%ชินอิทซึ%'
  * "Stanley Electric" → ILIKE '%stanley%'  
  * "Toyota Motor" → ILIKE '%toyota%'

**CRITICAL SEARCH TERM RULES:**
- Company names → customer_name ILIKE '%company%'
- Technical terms → description ILIKE '%term%'  
- Service types → description ILIKE '%service%'

Examples:
- "ชินอิทซึ แม็คเนติคส์" → customer_name ILIKE '%ชินอิทซึ%' (company name)
- "overhaul compressor" → description ILIKE '%overhaul%' OR description ILIKE '%compressor%' (technical terms)
- "งาน PM" → description ILIKE '%PM%' OR description ILIKE '%บำรุงรักษา%' (service type)
- "chiller repair" → description ILIKE '%chiller%' OR description ILIKE '%repair%' (technical work)

NEVER use customer_name for technical terms like overhaul, compressor, PM, service, etc.

**2. YEAR/TIME HANDLING:**
- Current year: 2025 (October)
- Thai Buddhist Era (พ.ศ.) conversion: พ.ศ. - 543 = ค.ศ.
  * ปี 2567 (พ.ศ.) = 2024 (ค.ศ.)
  * ปี 2568 (พ.ศ.) = 2025 (ค.ศ.)
- "3 ปีย้อนหลัง" = year IN ('2022','2023','2024')
- "ปีล่าสุด" = year = '2024'
- "ปีนี้" = year = '2025'
- "ปี 2567" = year = '2024'
- "ปี 2568" = year = '2025'
- Always use string format: year = '2024' NOT year = 2024

**CRITICAL YEAR CONVERSION EXAMPLES:**
- "ปี 2567-2568" → WHERE year IN ('2024','2025')
- "เดือนมิถุนายน 2568" → WHERE date::date BETWEEN '2025-06-01' AND '2025-06-30'
- "สิงหาคม-กันยายน 2568" → WHERE date::date BETWEEN '2025-08-01' AND '2025-09-30'

**3. TABLE SELECTION LOGIC:**
- Revenue/Sales data → v_sales ONLY
- Customer transactions → v_sales  
- Spare parts/inventory → v_spare_part ONLY
- Work plans/service → v_work_force ONLY
- Service types analysis → v_sales (use overhaul_num, service_num, etc.)

**4. COMMON QUERY PATTERNS:**

A) Customer History:
```sql
SELECT year, job_no, description, total_revenue 
FROM v_sales 
WHERE customer_name ILIKE '%customer%' AND year IN ('2022','2023','2024')
ORDER BY year DESC
```

B) Revenue Analysis: 
```sql
SELECT customer_name, SUM(total_revenue) as total
FROM v_sales 
WHERE year = '2024'
GROUP BY customer_name ORDER BY total DESC
```

C) Service Type Popularity:
```sql
SELECT 
  'Overhaul' as service_type, COUNT(*) as jobs, SUM(overhaul_num) as revenue
FROM v_sales WHERE overhaul_num > 0
UNION ALL
SELECT 'Service', COUNT(*), SUM(service_num) FROM v_sales WHERE service_num > 0
UNION ALL  
SELECT 'Parts', COUNT(*), SUM(parts_num) FROM v_sales WHERE parts_num > 0
ORDER BY revenue DESC
```

D) Inventory Status:
```sql
SELECT product_code, product_name, balance_num, unit_price_num, total_num
FROM v_spare_part 
WHERE balance_num > 0 
ORDER BY total_num DESC
```

E) Work Schedule:
```sql
SELECT date, customer, detail, service_group
FROM v_work_force 
WHERE date::date BETWEEN '2024-09-01' AND '2024-09-30'
ORDER BY date
```

**5. ADVANCED QUERY TECHNIQUES:**

- Top N queries: ORDER BY ... DESC LIMIT N
- Time ranges: date::date BETWEEN 'start' AND 'end'  
- Multiple conditions: WHERE condition1 AND condition2
- Aggregations: GROUP BY with SUM(), COUNT(), AVG()
- Text search: ILIKE '%keyword%' for partial matching
- Null handling: WHERE column IS NOT NULL
- Boolean checks: WHERE column = true/false

**6. QUERY OPTIMIZATION:**
- Always add LIMIT 1000 for safety
- Use specific columns instead of SELECT *
- Add ORDER BY for consistent results
- Use appropriate WHERE conditions
- Consider using indexes (year, customer_name)

**7. FORBIDDEN OPERATIONS:**
- NO: DELETE, UPDATE, INSERT, DROP, ALTER
- NO: Multiple statements (semicolons)
- NO: Dangerous functions or system commands
- YES: Only SELECT queries allowed

=== RESPONSE FORMAT ===
Always return valid JSON only:
{
  "sql": "SELECT ... LIMIT 1000;",
  "explanation": "Brief explanation in Thai",
  "confidence": "high/medium/low"
}
"""

# =============================================================================
# LLM CLIENT
# =============================================================================

class LLMClient:
    """Multi-model client สำหรับ SQL generation และ natural language response"""
    
    def __init__(self, base_url: str = None, sql_model: str = "llama3.1:8b", nl_model: str = "qwen2.5:7b-instruct"):
        # ใช้ URL จาก environment หรือ default จากโปรเจ็กต์
        self.base_url = base_url or os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434')
        self.sql_model = sql_model  # สำหรับสร้าง SQL
        self.nl_model = nl_model    # สำหรับตอบคำถามเป็นภาษาธรรมชาติ
        self.timeout = 60.0
        logger.info(f"Multi-model LLM Client: SQL={sql_model}, NL={nl_model}")
    
    async def generate_sql(self, prompt: str, system_prompt: str = None, max_tokens: int = 1500) -> LLMResponse:
        """Generate SQL using specialized SQL model"""
        return await self._generate(prompt, system_prompt, max_tokens, self.sql_model)
    
    async def generate_response(self, prompt: str, system_prompt: str = None, max_tokens: int = 2000) -> LLMResponse:
        """Generate natural language response using NL model"""
        return await self._generate(prompt, system_prompt, max_tokens, self.nl_model)
    
    async def _generate(self, prompt: str, system_prompt: str, max_tokens: int, model: str) -> LLMResponse:
        """Internal method for generating responses"""
        
        try:
            messages = []
            
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            
            messages.append({"role": "user", "content": prompt})
            
            # Model-specific optimization
            if model == self.sql_model:
                # Optimize for SQL generation
                temperature = 0.05  # Very low for consistent SQL
                top_p = 0.8
            else:
                # Optimize for natural language
                temperature = 0.2   # Low but allows some creativity
                top_p = 0.9
            
            payload = {
                "model": model,
                "messages": messages,
                "options": {
                    "temperature": temperature,
                    "top_p": top_p,
                    "num_predict": max_tokens
                },
                "stream": False
            }
            
            logger.info(f"Using model: {model} for generation")
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/api/chat",
                    json=payload
                )
                
                if response.status_code == 200:
                    result = response.json()
                    content = result.get("message", {}).get("content", "")
                    return LLMResponse(success=True, content=content)
                else:
                    return LLMResponse(
                        success=False, 
                        content="", 
                        error=f"HTTP {response.status_code}: {response.text}"
                    )
                    
        except Exception as e:
            logger.error(f"LLM generation failed with {model}: {e}")
            return LLMResponse(success=False, content="", error=str(e))

# =============================================================================
# SQL VALIDATOR
# =============================================================================

class SQLValidator:
    """ตรวจสอบ SQL ว่าปลอดภัยหรือไม่"""
    
    @staticmethod
    def validate_sql(sql: str) -> tuple[bool, str]:
        """ตรวจสอบ SQL พื้นฐาน"""
        if not sql or not sql.strip():
            return False, "SQL ว่างเปล่า"
        
        sql_lower = sql.lower().strip()
        
        # ต้องเริ่มด้วย SELECT
        if not sql_lower.startswith('select'):
            return False, "อนุญาตเฉพาะ SELECT queries เท่านั้น"
        
        # ไม่มี dangerous operations
        dangerous = ['drop', 'delete', 'truncate', 'alter', 'create', 'insert', 'update', 'grant', 'revoke']
        for word in dangerous:
            if re.search(rf'\b{word}\b', sql_lower):
                return False, f"ไม่อนุญาตให้ใช้คำสั่ง '{word.upper()}'"
        
        # ต้องมี FROM
        if 'from' not in sql_lower:
            return False, "SQL ต้องมี FROM clause"
        
        # ตรวจสอบว่ามี semicolon หลายตัวหรือไม่ (multiple statements)
        if sql.count(';') > 1:
            return False, "ไม่อนุญาตให้รัน SQL หลายคำสั่งพร้อมกัน"
        
        return True, "SQL ปลอดภัย"

# =============================================================================
# ULTRA-HIGH PERFORMANCE LLM ORCHESTRATOR
# =============================================================================

class LLMOrchestrator:
    """Ultra-High Performance LLM-First Orchestrator สำหรับ Siamtemp HVAC Chatbot"""
    
    def __init__(self, db_handler, llm_base_url: str = None, sql_model: str = None, nl_model: str = None):
        # ใช้ URL จาก environment หรือ default จากโปรเจ็กต์
        base_url = llm_base_url or os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434')
        
        # Model configuration from environment or defaults
        sql_model = sql_model or os.getenv('SQL_MODEL', 'llama3.1:8b')
        nl_model = nl_model or os.getenv('NL_MODEL', 'qwen2.5:7b-instruct')
        
        self.llm = LLMClient(base_url=base_url, sql_model=sql_model, nl_model=nl_model)
        self.schema = SchemaManager()
        self.validator = SQLValidator()
        
        # ใช้ Database Wrapper เพื่อแก้ปัญหา async/sync
        self.db = DatabaseWrapper(db_handler)
        
        # เพิ่ม SQL pattern cache เพื่อเพิ่มประสิทธิภาพและความแม่นยำ
        self.sql_patterns = self._initialize_sql_patterns()
        
        # เพิ่ม query cache เพื่อลด response time
        self.query_cache = {}
        self.cache_ttl = 300  # 5 minutes cache
        
        logger.info(f"LLM Orchestrator initialized with SQL model: {sql_model}, NL model: {nl_model}")
        
        # Specialized SQL System Prompt for SQL Model (llama3.1:8b)
        self.sql_system_prompt = """
You are a specialized PostgreSQL Expert for Siamtemp HVAC Business Intelligence System.
Your ONLY job is to generate precise, safe SQL queries.

=== CORE PRINCIPLES ===
1. ALWAYS return ONLY valid JSON format
2. ONLY SELECT queries allowed (no mutations)  
3. ALWAYS add LIMIT 1000 for safety
4. Use year as STRING: '2024' not 2024
5. Use date::date for date comparisons
6. DO NOT add filters that are not mentioned in the question

=== SPECIALIZED SQL INTELLIGENCE ===

**TABLE EXPERTISE:**
- v_sales: customer_name, year, total_revenue, overhaul_num, service_num, parts_num, description
- v_spare_part: product_code, product_name, balance_num, unit_price_num, total_num
- v_work_force: date, customer, detail, service_group, job_description_pm

**SEARCH PATTERN INTELLIGENCE:**
Company Names (→ customer_name ILIKE):
- "ชินอิทซึ แม็คเนติคส์" → customer_name ILIKE '%ชินอิทซึ%'
- "Stanley Electric" → customer_name ILIKE '%stanley%'

Technical Terms (→ description ILIKE):
- "overhaul compressor" → description ILIKE '%overhaul%' OR description ILIKE '%compressor%'
- "งาน PM" → description ILIKE '%PM%' OR description ILIKE '%บำรุงรักษา%'

**YEAR CONVERSION EXPERT:**
Thai Buddhist (พ.ศ.) → Gregorian (ค.ศ.): พ.ศ. - 543
- ปี 2567 → year = '2024'
- ปี 2568 → year = '2025'
- เดือนมิถุนายน 2568 → date::date BETWEEN '2025-06-01' AND '2025-06-30'

**SQL OPTIMIZATION:**
- Use UNION ALL for service type analysis
- Use GROUP BY for aggregations
- Use ORDER BY for meaningful sorting
- Always include WHERE conditions ONLY when mentioned in the question

=== OUTPUT FORMAT ===
Return ONLY this JSON (no markdown, no explanations):
{
  "sql": "SELECT ... FROM ... WHERE ... LIMIT 1000;",
  "explanation": "Thai explanation of query purpose",
  "confidence": "high|medium|low"
}
"""
        
        # Enhanced Natural Language System Prompt for NL Model (qwen2.5:7b-instruct)
        self.response_system_prompt = """
คุณเป็น AI Business Analyst ที่เชี่ยวชาญด้าน HVAC และการวิเคราะห์ธุรกิจของ Siamtemp

ความเชี่ยวชาญของคุณ:
1. วิเคราะห์ข้อมูลทางธุรกิจ HVAC (การขาย, ลูกค้า, บริการ)
2. สร้างรายงานที่เข้าใจง่ายและมีประโยชน์
3. ระบุ insights และแนวโน้มสำคัญ
4. ให้คำแนะนำเชิงธุรกิจ

หลักการในการตอบ:
1. **ความถูกต้อง**: ใช้เฉพาะข้อมูลที่ให้มา ห้ามแต่งเพิ่ม
2. **ความชัดเจน**: แสดงตัวเลขรูปแบบ 1,234,567 บาท
3. **การวิเคราะห์**: ระบุ patterns, trends, และข้อสังเกต
4. **ความครบถ้วน**: สรุปข้อมูลหลักและรายละเอียดสำคัญ
5. **ภาษาธุรกิจ**: ใช้ศัพท์ HVAC และธุรกิจที่เหมาะสม

รูปแบบการนำเสนอ:
- เริ่มด้วยสรุปหลัก (Executive Summary)
- แสดงข้อมูลสำคัญด้วยตัวเลข
- วิเคราะห์แนวโน้มและ insights
- ข้อเสนอแนะ (ถ้าเหมาะสม)

ตัวอย่างคำตอบที่ดี:
"จากการวิเคราะห์ข้อมูลการขาย พบว่า..."
"ลูกค้า [ชื่อ] มีการใช้บริการ..."
"แนวโน้มยอดขายแสดงให้เห็นว่า..."
"""
    
    def _initialize_sql_patterns(self) -> Dict[str, Dict]:
        """สร้าง pattern matching สำหรับคำถามที่พบบ่อย เพื่อให้ได้ SQL ที่ถูกต้องแม่นยำ"""
        return {
            # รายได้รวม
            'รายได้รวมทั้งหมด': {
                'sql': "SELECT SUM(total_revenue) as total_revenue FROM v_sales",
                'description': 'คำนวณรายได้รวมทั้งหมดจากทุกปี'
            },
            'รายได้ปี {year}': {
                'sql': "SELECT SUM(total_revenue) as total_revenue FROM v_sales WHERE year = '{year}'",
                'description': 'คำนวณรายได้รวมของปีที่ระบุ'
            },
            'รายได้แต่ละปี': {
                'sql': "SELECT year, SUM(total_revenue) as total_revenue FROM v_sales GROUP BY year ORDER BY year",
                'description': 'แสดงรายได้รวมแยกตามแต่ละปี'
            },
            
            # ยอดขายตามประเภท
            'ยอดขาย overhaul': {
                'sql': "SELECT SUM(overhaul_num) as total_overhaul FROM v_sales WHERE overhaul_num > 0",
                'description': 'คำนวณยอดขาย overhaul ทั้งหมด'
            },
            'ยอดขาย service': {
                'sql': "SELECT SUM(service_num) as total_service FROM v_sales WHERE service_num > 0",
                'description': 'คำนวณยอดขาย service ทั้งหมด'
            },
            'ยอดขาย parts|อะไหล่': {
                'sql': "SELECT SUM(parts_num) as total_parts FROM v_sales WHERE parts_num > 0",
                'description': 'คำนวณยอดขายอะไหล่ทั้งหมด'
            },
            'ยอดขาย.*replacement|เปลี่ยนอุปกรณ์': {
                'sql': "SELECT SUM(replacement_num) as total_replacement FROM v_sales WHERE replacement_num > 0",
                'description': 'คำนวณยอดขาย replacement ทั้งหมด'
            },
            'ยอดขายแยกตามประเภท': {
                'sql': """SELECT 
                    SUM(overhaul_num) as overhaul,
                    SUM(service_num) as service,
                    SUM(parts_num) as parts,
                    SUM(replacement_num) as replacement,
                    SUM(product_num) as product,
                    SUM(solution_num) as solution
                FROM v_sales""",
                'description': 'แสดงยอดขายแยกตามประเภทงานทั้งหมด'
            },
            
            # การวิเคราะห์
            'รายได้เฉลี่ยต่อปี': {
                'sql': """SELECT AVG(yearly_revenue) as avg_revenue
                FROM (
                    SELECT year, SUM(total_revenue) as yearly_revenue 
                    FROM v_sales 
                    GROUP BY year
                ) as yearly_totals""",
                'description': 'คำนวณรายได้เฉลี่ยต่อปี'
            },
            'ปี.*รายได้สูงสุด': {
                'sql': "SELECT year, SUM(total_revenue) as total_revenue FROM v_sales GROUP BY year ORDER BY total_revenue DESC LIMIT 1",
                'description': 'หาปีที่มีรายได้สูงสุด'
            },
            'ปี.*รายได้ต่ำสุด': {
                'sql': "SELECT year, SUM(total_revenue) as total_revenue FROM v_sales GROUP BY year ORDER BY total_revenue ASC LIMIT 1",
                'description': 'หาปีที่มีรายได้ต่ำสุด'
            },
            
            # งานสูงสุด/ต่ำสุด
            'งาน.*มูลค่าสูงสุด|งาน.*รายได้สูงสุด': {
                'sql': """SELECT job_no, customer_name, description, total_revenue 
                         FROM v_sales 
                         WHERE total_revenue > 0 
                         ORDER BY total_revenue DESC 
                         LIMIT 1""",
                'description': 'หางานที่มีมูลค่าสูงสุด'
            },
            'งาน.*มูลค่าต่ำสุด|งาน.*รายได้ต่ำสุด': {
                'sql': """SELECT job_no, customer_name, description, total_revenue 
                         FROM v_sales 
                         WHERE total_revenue > 0 
                         ORDER BY total_revenue ASC 
                         LIMIT 1""",
                'description': 'หางานที่มีมูลค่าต่ำสุด (ที่ไม่ใช่ศูนย์)'
            },
            
            # สินค้าคงคลัง
            'มูลค่า.*สินค้าคงคลัง|สต็อก': {
                'sql': "SELECT SUM(total_num) as total_inventory_value FROM v_spare_part WHERE total_num > 0",
                'description': 'คำนวณมูลค่ารวมของสินค้าคงคลัง'
            }
        }
    
    def _extract_customer_name(self, question: str) -> Optional[str]:
        """
        ดึงชื่อลูกค้าจากคำถามอย่างชาญฉลาด
        ตัวอย่าง: 'คลีนิคประกอบโรคศิลป์ฯ' -> 'คลีนิคประกอบโรคศิลป'
        """
        # รายการ patterns สำหรับจับชื่อองค์กรไทย
        thai_org_patterns = [
            r'([\u0E00-\u0E7F]+(?:คลีนิค|โรงพยาบาล|ศูนย์|สำนัก|กรม|กระทรวง|การไฟฟ้า|มหาวิทยาลัย)[\u0E00-\u0E7F\s]*)',
            r'บริษัท\s*([\u0E00-\u0E7Fa-zA-Z0-9\s\(\)\.]+?)(?:\s*จำกัด|\s*จก\.?|\s*ฯ)',
            r'บ\.\s*([\u0E00-\u0E7Fa-zA-Z0-9\s\(\)]+?)(?:\s*จำกัด|\s*จก\.?|\s*ฯ)',
            r'หจก\.\s*([\u0E00-\u0E7Fa-zA-Z0-9\s]+)',
            r'([\u0E00-\u0E7F\s]+(?:เซ็นเตอร์|พลาซ่า|มาร์ท|สโตร์))',
        ]
        
        # patterns สำหรับชื่อบริษัทภาษาอังกฤษ
        english_org_patterns = [
            r'([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)\s+(?:CO\.|LTD\.|LIMITED|COMPANY|CORPORATION)',
            r'([A-Z][A-Z0-9\s\-]+)\s+(?:THAILAND|ASIA|INTERNATIONAL)',
            r'(STANLEY|CLARION|DENSO|TOYOTA|HITACHI|HONDA|SEIKO|IRPC|MASTER\s+GLOVE)',
        ]
        
        # ลองจับชื่อไทยก่อน
        for pattern in thai_org_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                customer = match.group(1).strip()
                # ทำความสะอาดชื่อ
                customer = re.sub(r'\s+', ' ', customer)
                # ตัด 'ฯ' ออกถ้าอยู่ท้ายสุด
                customer = customer.rstrip('ฯ').strip()
                return customer
        
        # ลองจับชื่ออังกฤษ
        for pattern in english_org_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        # ตรวจหาชื่อลูกค้าที่รู้จักแบบ keyword
        known_customers = {
            'stanley': 'STANLEY',
            'clarion': 'CLARION',
            'ชินอิทซึ': 'ชินอิทซึ',
            'คลีนิค': 'คลีนิค',
            'denso': 'DENSO',
            'toyota': 'Toyota',
            'hitachi': 'Hitachi',
            'irpc': 'IRPC',
            'กลาโหม': 'กลาโหม',
            'กรมบังคับคดี': 'กรมบังคับคดี'
        }
        
        question_lower = question.lower()
        for key, value in known_customers.items():
            if key in question_lower:
                return value
        
        return None
    
    def _extract_time_period(self, question: str) -> Dict[str, Any]:
        """
        วิเคราะห์ช่วงเวลาจากคำถาม
        return: {'type': 'specific'|'range'|'relative', 'years': [...]}
        """
        current_year = 2025
        result = {'type': None, 'years': []}
        
        # ตรวจหาปี ค.ศ. แบบเฉพาะเจาะจง
        gregorian_years = re.findall(r'20\d{2}', question)
        if gregorian_years:
            result['type'] = 'specific' if len(gregorian_years) == 1 else 'range'
            result['years'] = gregorian_years
            return result
        
        # ตรวจหาปี พ.ศ.
        thai_years = re.findall(r'25\d{2}', question)
        if thai_years:
            result['type'] = 'specific' if len(thai_years) == 1 else 'range'
            result['years'] = [str(int(year) - 543) for year in thai_years]
            return result
        
        # ตรวจหาคำพิเศษ
        if 'ย้อนหลัง' in question:
            match = re.search(r'(\d+)\s*ปี\s*ย้อนหลัง', question)
            if match:
                num_years = int(match.group(1))
                result['type'] = 'relative'
                result['years'] = [str(current_year - i) for i in range(1, num_years + 1)]
                return result
        
        if 'ปีนี้' in question:
            result['type'] = 'specific'
            result['years'] = ['2025']
        elif 'ปีล่าสุด' in question or 'ปีที่แล้ว' in question:
            result['type'] = 'specific'
            result['years'] = ['2024']
        elif 'ทุกปี' in question or 'แต่ละปี' in question:
            result['type'] = 'range'
            result['years'] = ['2022', '2023', '2024', '2025']
        
        return result
        
        # Specialized SQL System Prompt for SQL Model (llama3.1:8b)
        self.sql_system_prompt = """
You are a specialized PostgreSQL Expert for Siamtemp HVAC Business Intelligence System.
Your ONLY job is to generate precise, safe SQL queries.

=== CORE PRINCIPLES ===
1. ALWAYS return ONLY valid JSON format
2. ONLY SELECT queries allowed (no mutations)  
3. ALWAYS add LIMIT 1000 for safety
4. Use year as STRING: '2024' not 2024
5. Use date::date for date comparisons

=== SPECIALIZED SQL INTELLIGENCE ===

**TABLE EXPERTISE:**
- v_sales: customer_name, year, total_revenue, overhaul_num, service_num, parts_num, description
- v_spare_part: product_code, product_name, balance_num, unit_price_num, total_num
- v_work_force: date, customer, detail, service_group, job_description_pm

**SEARCH PATTERN INTELLIGENCE:**
Company Names (→ customer_name ILIKE):
- "ชินอิทซึ แม็คเนติคส์" → customer_name ILIKE '%ชินอิทซึ%'
- "Stanley Electric" → customer_name ILIKE '%stanley%'

Technical Terms (→ description ILIKE):
- "overhaul compressor" → description ILIKE '%overhaul%' OR description ILIKE '%compressor%'
- "งาน PM" → description ILIKE '%PM%' OR description ILIKE '%บำรุงรักษา%'

**YEAR CONVERSION EXPERT:**
Thai Buddhist (พ.ศ.) → Gregorian (ค.ศ.): พ.ศ. - 543
- ปี 2567 → year = '2024'
- ปี 2568 → year = '2025'
- เดือนมิถุนายน 2568 → date::date BETWEEN '2025-06-01' AND '2025-06-30'

**SQL OPTIMIZATION:**
- Use UNION ALL for service type analysis
- Use GROUP BY for aggregations
- Use ORDER BY for meaningful sorting
- Always include WHERE conditions for filtering

=== OUTPUT FORMAT ===
Return ONLY this JSON (no markdown, no explanations):
{
  "sql": "SELECT ... FROM ... WHERE ... LIMIT 1000;",
  "explanation": "Thai explanation of query purpose",
  "confidence": "high|medium|low"
}
"""
        
        # Enhanced Natural Language System Prompt for NL Model (qwen2.5:7b-instruct)
        self.response_system_prompt = """
คุณเป็น AI Business Analyst ที่เชี่ยวชาญด้าน HVAC และการวิเคราะห์ธุรกิจของ Siamtemp

ความเชี่ยวชาญของคุณ:
1. วิเคราะห์ข้อมูลทางธุรกิจ HVAC (การขาย, ลูกค้า, บริการ)
2. สร้างรายงานที่เข้าใจง่ายและมีประโยชน์
3. ระบุ insights และแนวโน้มสำคัญ
4. ให้คำแนะนำเชิงธุรกิจ

หลักการในการตอบ:
1. **ความถูกต้อง**: ใช้เฉพาะข้อมูลที่ให้มา ห้ามแต่งเพิ่ม
2. **ความชัดเจน**: แสดงตัวเลขรูปแบบ 1,234,567 บาท
3. **การวิเคราะห์**: ระบุ patterns, trends, และข้อสังเกต
4. **ความครบถ้วน**: สรุปข้อมูลหลักและรายละเอียดสำคัญ
5. **ภาษาธุรกิจ**: ใช้ศัพท์ HVAC และธุรกิจที่เหมาะสม

รูปแบบการนำเสนอ:
- เริ่มด้วยสรุปหลัก (Executive Summary)
- แสดงข้อมูลสำคัญด้วยตัวเลข
- วิเคราะห์แนวโน้มและ insights
- ข้อเสนอแนะ (ถ้าเหมาะสม)

ตัวอย่างคำตอบที่ดี:
"จากการวิเคราะห์ข้อมูลการขาย พบว่า..."
"ลูกค้า [ชื่อ] มีการใช้บริการ..."
"แนวโน้มยอดขายแสดงให้เห็นว่า..."
"""
    
    async def process_question(self, question: str) -> Dict[str, Any]:
        """ประมวลผลคำถามด้วย Ultra-High Performance LLM-First approach"""
        
        logger.info(f"Processing question: {question}")
        
        # ตรวจสอบ cache ก่อน
        cache_key = self._generate_cache_key(question)
        if cache_key in self.query_cache:
            cached_result = self.query_cache[cache_key]
            if self._is_cache_valid(cached_result):
                logger.info(f"Returning cached result for: {question}")
                cached_result['from_cache'] = True
                return cached_result
        
        try:
            # Step 1: ลองใช้ pattern matching ก่อนเพื่อความแม่นยำและเร็ว
            pattern_sql = self._match_sql_pattern(question)
            
            if pattern_sql:
                sql_result = {
                    "success": True,
                    "sql": pattern_sql['sql'],
                    "explanation": pattern_sql.get('description', ''),
                    "confidence": "high"
                }
                logger.info(f"Using pattern-matched SQL: {sql_result['sql']}")
            else:
                # Step 2: ถ้าไม่มี pattern ให้ใช้ LLM สร้าง SQL
                sql_result = await self._generate_sql_with_llm(question)
                
                if not sql_result["success"]:
                    return self._create_error_response(f"ไม่สามารถสร้าง SQL ได้: {sql_result['error']}")
            
            # Step 3: Validate SQL
            is_valid, validation_msg = self.validator.validate_sql(sql_result["sql"])
            if not is_valid:
                logger.warning(f"SQL validation failed: {validation_msg}")
                # ลอง fallback
                fallback_result = await self._generate_fallback_sql(question, validation_msg)
                if fallback_result["success"]:
                    sql_result = fallback_result
                else:
                    return self._create_error_response(f"SQL ไม่ปลอดภัย: {validation_msg}")
            
            # Step 4: Execute SQL
            try:
                logger.info(f"Executing SQL: {sql_result['sql']}")
                query_results = self.db.execute_query(sql_result["sql"])
                logger.info(f"Query executed successfully, {len(query_results)} results")
            except Exception as e:
                logger.error(f"SQL execution failed: {e}")
                # ลอง fallback SQL
                fallback_result = await self._generate_fallback_sql(question, f"SQL Error: {str(e)}")
                if fallback_result["success"]:
                    try:
                        query_results = self.db.execute_query(fallback_result["sql"])
                        sql_result = fallback_result
                    except:
                        return self._create_error_response("ไม่สามารถค้นหาข้อมูลได้ กรุณาลองใหม่")
                else:
                    return self._create_error_response("ไม่สามารถค้นหาข้อมูลได้ กรุณาลองใหม่")
            
            # Step 5: Generate response with LLM
            response_text = await self._generate_response_with_llm(question, query_results, sql_result["sql"])
            
            result = {
                "success": True,
                "question": question,
                "sql": sql_result["sql"],
                "sql_explanation": sql_result.get("explanation", ""),
                "results_count": len(query_results),
                "response": response_text,
                "confidence": sql_result.get("confidence", "medium"),
                "from_cache": False
            }
            
            # เก็บลง cache
            self.query_cache[cache_key] = {
                **result,
                'cached_at': asyncio.get_event_loop().time()
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Orchestrator error: {e}")
            return self._create_error_response(f"เกิดข้อผิดพลาด: {str(e)}")
    
    def _match_sql_pattern(self, question: str) -> Optional[Dict]:
        """จับคู่คำถามกับ SQL pattern ที่กำหนดไว้ล่วงหน้า"""
        question_lower = question.lower()
        
        # ตรวจสอบปีจากคำถาม
        year = None
        if '2022' in question:
            year = '2022'
        elif '2023' in question or '2566' in question:
            year = '2023'
        elif '2024' in question or '2567' in question:
            year = '2024'
        elif '2025' in question or '2568' in question:
            year = '2025'
        elif 'ปีนี้' in question:
            year = '2025'  # Current year
        elif 'ปีล่าสุด' in question:
            year = '2024'  # Latest complete year
        
        # ตรวจสอบ pattern
        for pattern_key, pattern_value in self.sql_patterns.items():
            if '|' in pattern_key:
                # Handle multiple keywords with OR
                keywords = pattern_key.split('|')
                if any(keyword in question_lower for keyword in keywords):
                    sql = pattern_value['sql']
                    if '{year}' in sql and year:
                        sql = sql.format(year=year)
                    elif '{year}' not in pattern_key and year and 'WHERE' in sql:
                        # เพิ่ม year filter ถ้าระบุปี
                        sql = sql.replace('WHERE', f"WHERE year = '{year}' AND")
                    elif '{year}' not in pattern_key and year:
                        # เพิ่ม WHERE clause ถ้ายังไม่มี
                        sql = sql.replace('FROM v_sales', f"FROM v_sales WHERE year = '{year}'")
                    return {'sql': sql, 'description': pattern_value['description']}
            else:
                # Use regex for flexible matching
                if re.search(pattern_key, question_lower):
                    sql = pattern_value['sql']
                    if '{year}' in sql and year:
                        sql = sql.format(year=year)
                    elif year and 'WHERE' not in sql and 'v_sales' in sql:
                        sql = sql.replace('FROM v_sales', f"FROM v_sales WHERE year = '{year}'")
                    elif year and 'WHERE' in sql and 'year' not in sql:
                        sql = sql.replace('WHERE', f"WHERE year = '{year}' AND")
                    return {'sql': sql, 'description': pattern_value['description']}
        
        return None
    
    def _generate_cache_key(self, question: str) -> str:
        """สร้าง cache key จากคำถาม"""
        return hashlib.md5(question.lower().encode()).hexdigest()
    
    def _is_cache_valid(self, cached_item: Dict) -> bool:
        """ตรวจสอบว่า cache ยังใช้ได้หรือไม่"""
        if 'cached_at' not in cached_item:
            return False
        current_time = asyncio.get_event_loop().time()
        return (current_time - cached_item['cached_at']) < self.cache_ttl
    
    async def _generate_sql_with_llm(self, question: str) -> Dict[str, Any]:
        """ให้ LLM สร้าง SQL โดยมีการวิเคราะห์ context อย่างละเอียด"""
        
        # วิเคราะห์ entities จากคำถามก่อน
        customer_name = self._extract_customer_name(question)
        time_info = self._extract_time_period(question)
        
        # สร้าง prompt ที่ชัดเจนและมี context ครบถ้วน
        prompt = f"""
IMPORTANT: Analyze ONLY this specific question. Do NOT add filters that are not mentioned.

CURRENT QUESTION: "{question}"

EXTRACTED ENTITIES (pre-analyzed for you):
• Customer: {f"'{customer_name}'" if customer_name else "NO SPECIFIC CUSTOMER - query ALL customers"}
• Time Period: {f"Years {time_info['years']}" if time_info['years'] else "NO SPECIFIC TIME - query all years"}
• Query Type: {self._identify_query_type(question)}

SQL GENERATION RULES:
1. Customer Filter:
   - If customer detected: MUST add "WHERE customer_name ILIKE '%{customer_name}%'"
   - If NO customer: DO NOT add any customer filter
   
2. Time Filter:
   - If years detected: Add "WHERE year IN ({', '.join([f"'{y}'" for y in time_info['years']])})"
   - If NO years specified: No year filter unless logic requires it

3. Current Context:
   - Current year is 2025
   - Latest complete year is 2024
   - Database has years 2022, 2023, 2024, 2025

{self.schema.get_schema_description()}

CRITICAL: Generate SQL that EXACTLY matches the extracted entities above.
- Customer filter: {"YES - customer_name ILIKE '%" + customer_name + "%'" if customer_name else "NO - do not add customer filter"}
- Year filter: {"YES - year IN " + str(tuple(time_info['years'])) if time_info['years'] else "NO - do not add year filter"}

Return ONLY this JSON format:
{{
  "sql": "YOUR_SQL_HERE",
  "explanation": "Brief explanation in Thai",
  "confidence": "high|medium|low"
}}
"""
        
        llm_response = await self.llm.generate_sql(prompt, self.sql_system_prompt)
        
        if not llm_response.success:
            return {"success": False, "error": llm_response.error}
        
        try:
            # Parse และทำความสะอาด response
            content = llm_response.content.strip()
            if content.startswith('```json'):
                content = content.replace('```json', '').replace('```', '').strip()
            elif content.startswith('```'):
                content = content.replace('```', '').strip()
            
            content = content.replace('\n', ' ')
            content = re.sub(r'\s+', ' ', content)
            
            result = json.loads(content)
            sql = result.get("sql", "")
            
            # ขั้นสุดท้าย: Validate และแก้ไข SQL ให้ตรงกับ intent
            sql = self._validate_and_fix_sql(question, sql, customer_name, time_info)
            
            # Add LIMIT if not present
            if sql and "limit" not in sql.lower():
                sql = sql.rstrip(';') + " LIMIT 1000;"
            
            result["sql"] = sql
            
            logger.info(f"LLM generated SQL: {sql}")
            logger.info(f"SQL explanation: {result.get('explanation', 'No explanation')}")
            
            return {
                "success": True,
                "sql": result.get("sql", ""),
                "explanation": result.get("explanation", ""),
                "confidence": result.get("confidence", "medium")
            }
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM JSON response: {e}")
            logger.error(f"Raw response: {llm_response.content}")
            
            # Try to extract SQL from response if JSON parsing fails
            sql_match = re.search(r'SELECT.*?(?:;|$)', llm_response.content, re.IGNORECASE | re.DOTALL)
            if sql_match:
                sql = sql_match.group(0).strip()
                sql = self._validate_and_fix_sql(question, sql, customer_name, time_info)
                if "limit" not in sql.lower():
                    sql = sql.rstrip(';') + " LIMIT 1000;"
                return {
                    "success": True,
                    "sql": sql,
                    "explanation": "SQL extracted from response",
                    "confidence": "low"
                }
            
            return {"success": False, "error": "ไม่สามารถสร้าง SQL ได้"}
    
    def _identify_query_type(self, question: str) -> str:
        """ระบุประเภทของคำถามเพื่อช่วย LLM เข้าใจ context"""
        question_lower = question.lower()
        
        if 'รายได้' in question_lower or 'ยอดขาย' in question_lower:
            return 'Revenue/Sales Analysis'
        elif 'ลูกค้า' in question_lower or 'ประวัติ' in question_lower:
            return 'Customer History'
        elif 'อะไหล่' in question_lower or 'สต็อก' in question_lower:
            return 'Inventory/Parts'
        elif 'งาน' in question_lower and ('สูงสุด' in question_lower or 'ต่ำสุด' in question_lower):
            return 'Job Ranking'
        elif 'เปรียบเทียบ' in question_lower:
            return 'Comparison'
        else:
            return 'General Query'
    
    def _validate_and_fix_sql(self, question: str, sql: str, customer_name: Optional[str], time_info: Dict) -> str:
        """
        ตรวจสอบและแก้ไข SQL ให้ตรงกับ intent ของคำถาม
        ฟังก์ชันนี้เป็น safety net สุดท้ายเพื่อให้แน่ใจว่า SQL ถูกต้อง
        """
        if not sql:
            return sql
            
        sql_lower = sql.lower()
        
        # กฎที่ 1: ตรวจสอบ customer filter
        if customer_name:
            # ต้องมี customer filter
            if 'customer_name' not in sql_lower:
                logger.warning(f"Adding missing customer filter for: {customer_name}")
                if 'where' in sql_lower:
                    # มี WHERE อยู่แล้ว
                    sql = re.sub(
                        r'WHERE\s+',
                        f"WHERE customer_name ILIKE '%{customer_name}%' AND ",
                        sql,
                        count=1,
                        flags=re.IGNORECASE
                    )
                else:
                    # ไม่มี WHERE
                    sql = re.sub(
                        r'FROM\s+v_sales',
                        f"FROM v_sales WHERE customer_name ILIKE '%{customer_name}%'",
                        sql,
                        flags=re.IGNORECASE
                    )
        else:
            # ไม่ควรมี customer filter ถ้าไม่ได้ระบุ
            # แต่ต้องระวังไม่ให้ลบ filter ที่จำเป็นออก
            pass
        
        # กฎที่ 2: ตรวจสอบ year filter
        if time_info['years'] and 'v_sales' in sql_lower:
            if 'year' not in sql_lower:
                logger.warning(f"Adding missing year filter for: {time_info['years']}")
                years_str = ', '.join([f"'{y}'" for y in time_info['years']])
                if 'where' in sql_lower:
                    sql = re.sub(
                        r'WHERE\s+',
                        f"WHERE year IN ({years_str}) AND ",
                        sql,
                        count=1,
                        flags=re.IGNORECASE
                    )
                else:
                    sql = re.sub(
                        r'FROM\s+v_sales',
                        f"FROM v_sales WHERE year IN ({years_str})",
                        sql,
                        flags=re.IGNORECASE
                    )
        
        # กฎที่ 3: ตรวจสอบการใช้ aggregate function
        question_lower = question.lower()
        if ('รายได้รวม' in question_lower or 'ยอดขายรวม' in question_lower) and 'sum(' not in sql_lower:
            logger.warning("Question asks for total but SQL missing SUM()")
            # พยายามแก้ไขโดยเพิ่ม SUM
            sql = re.sub(
                r'SELECT\s+total_revenue',
                'SELECT SUM(total_revenue) as total_revenue',
                sql,
                flags=re.IGNORECASE
            )
        
        return sql
    
    async def _generate_fallback_sql(self, question: str, error_msg: str) -> Dict[str, Any]:
        """สร้าง fallback SQL เมื่อ SQL แรกมีปัญหา"""
        
        prompt = f"""
SQL เดิมมีปัญหา: {error_msg}

คำถาม: {question}

{self.schema.get_schema_description()}

กรุณาสร้าง SQL ใหม่ที่ง่ายและปลอดภัยกว่า:
- ใช้ SELECT * แทน complex operations
- เพิ่ม WHERE clause ที่จำเป็นเท่านั้น
- เพิ่ม LIMIT 100 เพื่อความปลอดภัย
- หลีกเลี่ยงการใช้ฟังก์ชันซับซ้อน

ตอบในรูปแบบ JSON:
"""
        
        llm_response = await self.llm.generate_sql(prompt, self.sql_system_prompt)
        
        if not llm_response.success:
            return {"success": False, "error": llm_response.error}
        
        try:
            # Clean response
            content = llm_response.content.strip()
            if content.startswith('```json'):
                content = content.replace('```json', '').replace('```', '').strip()
            elif content.startswith('```'):
                content = content.replace('```', '').strip()
                
            result = json.loads(content)
            return {
                "success": True,
                "sql": result.get("sql", ""),
                "explanation": f"Fallback SQL: {result.get('explanation', '')}",
                "confidence": "low"
            }
        except:
            return {"success": False, "error": "ไม่สามารถสร้าง fallback SQL ได้"}
    
    async def _generate_response_with_llm(self, question: str, results: List[Dict], sql: str) -> str:
        """ให้ LLM สร้างคำตอบจากผลลัพธ์"""
        
        # จำกัดข้อมูลที่ส่งให้ LLM
        if len(results) > 20:
            sample_results = results[:20]
            has_more = True
        else:
            sample_results = results
            has_more = False
        
        results_json = json.dumps(sample_results, ensure_ascii=False, default=str, indent=2)
        
        prompt = f"""
คำถาม: {question}
SQL ที่ใช้: {sql}
จำนวนผลลัพธ์ทั้งหมด: {len(results)} รายการ
{"(แสดงเฉพาะ 20 รายการแรก)" if has_more else ""}

ข้อมูลผลลัพธ์:
{results_json}

กรุณาสร้างคำตอบที่:
1. ตอบคำถามตรงประเด็น
2. สรุปข้อมูลสำคัญอย่างชัดเจน  
3. แสดงตัวเลขในรูปแบบที่อ่านง่าย
4. หากมีข้อมูลหลายรายการ ให้เน้น highlights สำคัญ
5. ใช้ภาษาไทยที่เป็นมิตร

ห้าม:
- แต่งข้อมูลที่ไม่มี
- ใช้ข้อมูลนอกเหนือจากที่ให้มา
- ตอบคำถามที่ไม่ได้ถาม

คำตอบ:
"""
        
        llm_response = await self.llm.generate_response(prompt, self.response_system_prompt)
        
        if llm_response.success:
            return llm_response.content.strip()
        else:
            # Fallback response
            if len(results) == 0:
                return "ไม่พบข้อมูลที่ตรงกับคำถาม"
            elif len(results) == 1:
                return f"พบข้อมูล 1 รายการ: {json.dumps(results[0], ensure_ascii=False)}"
            else:
                return f"พบข้อมูล {len(results)} รายการ ตัวอย่างรายการแรก: {json.dumps(results[0], ensure_ascii=False)}"
    
    def _create_error_response(self, error_message: str) -> Dict[str, Any]:
        """สร้าง error response"""
        return {
            "success": False,
            "error": error_message,
            "response": f"ขออภัย {error_message} กรุณาลองใหม่หรือปรับคำถามให้ชัดเจนขึ้น",
            "sql": None,
            "results_count": 0
        }

# =============================================================================
# TESTING FUNCTION
# =============================================================================

async def test_llm_orchestrator():
    """ฟังก์ชันทดสอบ Ultra-High Performance LLM Orchestrator"""
    
    # Mock database handler สำหรับทดสอบ
    class MockDBHandler:
        def execute_query(self, sql: str):
            # Return mock data
            if "total_revenue" in sql.lower():
                return [{"total_revenue": 1500000}]
            elif "customer_name" in sql.lower():
                return [
                    {"customer_name": "Stanley", "total_revenue": 500000},
                    {"customer_name": "Toyota", "total_revenue": 400000}
                ]
            else:
                return []
    
    orchestrator = LLMOrchestrator(MockDBHandler())
    
    test_questions = [
        "รายได้รวมทั้งหมดเท่าไหร่",
        "ประเภทงานที่ลูกค้านิยมใช้บริการมากที่สุด", 
        "บ. ชินอิทซึ แม็คเนติคส์ ฯ จก. มีการซื้อขายย้อนหลัง 3 ปี มีอะไรบ้าง มูลค่าเท่าไหร",
        "ลูกค้าที่มียอดสูงสุด 5 ราย",
        "อะไหล่ที่ใกล้หมดสต็อก"
    ]
    
    for question in test_questions:
        print(f"\n{'='*60}")
        print(f"คำถาม: {question}")
        print('='*60)
        
        result = await orchestrator.process_question(question)
        
        if result["success"]:
            print(f"SQL: {result['sql']}")
            print(f"ผลลัพธ์: {result['results_count']} รายการ")
            print(f"คำตอบ: {result['response']}")
        else:
            print(f"Error: {result['error']}")

if __name__ == "__main__":
    # ทดสอบ Ultra-High Performance LLM Orchestrator
    asyncio.run(test_llm_orchestrator())