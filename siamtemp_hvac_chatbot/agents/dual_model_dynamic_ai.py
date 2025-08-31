# =============================================================================
# Self-Contained Dual Model Dynamic AI Agent
# Version: 5.0-Simplified
# No external dependencies, fully self-contained
# =============================================================================

import os
import re
import json
import asyncio
import time
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date
from decimal import Decimal

logger = logging.getLogger(__name__)

class SimplifiedDatabaseHandler:
    """Simplified database handler for HVAC data"""
    
    def __init__(self):
        self.connection_cache = {}
    
    def get_database_connection(self, tenant_id: str):
        """Get database connection for tenant"""
        try:
            import psycopg2
            
            # Database configuration mapping
            db_configs = {
                'company-a': {
                    'host': os.getenv('POSTGRES_HOST_COMPANY_A', 'postgres-company-a'),
                    'port': int(os.getenv('POSTGRES_PORT_COMPANY_A', '5432')),
                    'database': os.getenv('POSTGRES_DB_COMPANY_A', 'siamtemp_company_a'),
                    'user': os.getenv('POSTGRES_USER_COMPANY_A', 'postgres'),
                    'password': os.getenv('POSTGRES_PASSWORD_COMPANY_A', 'password123')
                },
                'company-b': {
                    'host': os.getenv('POSTGRES_HOST_COMPANY_B', 'postgres-company-b'),
                    'port': int(os.getenv('POSTGRES_PORT_COMPANY_B', '5432')),
                    'database': os.getenv('POSTGRES_DB_COMPANY_B', 'siamtemp_company_b'),
                    'user': os.getenv('POSTGRES_USER_COMPANY_B', 'postgres'),
                    'password': os.getenv('POSTGRES_PASSWORD_COMPANY_B', 'password123')
                },
                'company-c': {
                    'host': os.getenv('POSTGRES_HOST_COMPANY_C', 'postgres-company-c'),
                    'port': int(os.getenv('POSTGRES_PORT_COMPANY_C', '5432')),
                    'database': os.getenv('POSTGRES_DB_COMPANY_C', 'siamtemp_company_c'),
                    'user': os.getenv('POSTGRES_USER_COMPANY_C', 'postgres'),
                    'password': os.getenv('POSTGRES_PASSWORD_COMPANY_C', 'password123')
                }
            }
            
            config = db_configs.get(tenant_id, db_configs['company-a'])
            
            connection = psycopg2.connect(**config)
            return connection
            
        except Exception as e:
            logger.error(f"Database connection failed for {tenant_id}: {e}")
            return None
    
    async def execute_query(self, sql: str, tenant_id: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results"""
        try:
            conn = self.get_database_connection(tenant_id)
            if not conn:
                return []
            
            with conn.cursor() as cursor:
                cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                results = []
                for row in rows:
                    result = {}
                    for i, value in enumerate(row):
                        # Handle different data types
                        if isinstance(value, Decimal):
                            result[columns[i]] = float(value)
                        elif isinstance(value, (date, datetime)):
                            result[columns[i]] = str(value)
                        else:
                            result[columns[i]] = value
                    results.append(result)
                
                conn.close()
                return results
                
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []

class SimplifiedOllamaClient:
    """Simplified Ollama client for AI requests"""
    
    def __init__(self):
        self.base_url = os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434')
        self.timeout = int(os.getenv('OLLAMA_TIMEOUT', '60'))
    
    async def generate_response(self, model: str, prompt: str, temperature: float = 0.7) -> str:
        """Generate response from Ollama model"""
        try:
            import aiohttp
            
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": 2000
                }
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.post(f"{self.base_url}/api/generate", json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get('response', '')
                    else:
                        logger.error(f"Ollama API error: {response.status}")
                        return ""
                        
        except Exception as e:
            logger.error(f"Ollama request failed: {e}")
            return ""

class DualModelDynamicAISystem:
    """Core Dual-Model AI System for HVAC chatbot with Performance Optimization"""
    
    def __init__(self):
        # Initialize components
        self.db_handler = SimplifiedDatabaseHandler()
        self.ollama_client = SimplifiedOllamaClient()
        
        # Model configuration
        self.SQL_MODEL = "mannix/defog-llama3-sqlcoder-8b:latest"
        self.NL_MODEL = "llama3.1:8b"
        
        # Performance optimization settings
        self.sql_cache = {}  # Cache SQL patterns ที่ใช้บ่อย
        self.schema_cache = {}  # Cache database schema
        self.enable_parallel = True
        self.timeout_sql = 12  # ลด timeout SQL generation เหลือ 12 วินาที
        self.timeout_nl = 8   # ลด timeout NL generation เหลือ 8 วินาที
        
        # HVAC Business Knowledge
        self.hvac_context = {
            'tables': {
                'sales2024': 'ข้อมูลงานบริการและการขายปี 2024',
                'sales2023': 'ข้อมูลงานบริการและการขายปี 2023',
                'sales2022': 'ข้อมูลงานบริการและการขายปี 2022',
                'sales2025': 'ข้อมูลงานบริการและการขายปี 2025',
                'spare_part': 'คลังอะไหล่หลัก',
                'spare_part2': 'คลังอะไหล่สำรอง',
                'work_force': 'การจัดทีมงานและแผนการทำงาน'
            },
            'keywords': {
                'job_quotation': ['เสนอราคา', 'งาน', 'standard', 'มาตรฐาน', 'quotation', 'job_no'],
                'spare_parts': ['อะไหล่', 'spare', 'parts', 'motor', 'chiller', 'compressor', 'product_name'],
                'sales_analysis': ['การขาย', 'ยอดขาย', 'รายได้', 'วิเคราะห์', 'service_contact_'],
                'work_summary': ['งาน', 'work', 'สรุป', 'summary', 'ทีม', 'team'],
                'brands': ['hitachi', 'daikin', 'mitsubishi', 'carrier', 'trane', 'york', 'lg']
            },
            'year_mapping': {
                '2565': '2022', '2566': '2023', '2567': '2024', '2568': '2025',
                'ปี2565': '2022', 'ปี2566': '2023', 'ปี2567': '2024', 'ปี2568': '2025'
            }
        }
        
        # Performance tracking
        self.stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'sql_generated': 0,
            'nl_responses': 0,
            'cache_hits': 0,
            'avg_response_time': 0.0
        }
        
        logger.info("Dual-Model Dynamic AI System with Performance Optimization initialized successfully")
    
    def _classify_question_intent(self, question: str) -> str:
        """Enhanced question intent classification for comprehensive HVAC business queries"""
        question_lower = question.lower()
        
        # Customer-specific queries
        customer_keywords = ['บริษัท', 'ลูกค้า', 'customer', 'จำนวนลูกค้า', 'ประวัติ', 'ซื้อขาย', 'การซ่อม']
        if any(keyword in question_lower for keyword in customer_keywords):
            return 'customer_analysis'
        
        # Work schedule and planning queries  
        schedule_keywords = ['แผนงาน', 'วางแผน', 'schedule', 'วันที่', 'เดือน', 'work_force', 
                            'มกราคม', 'กุมภาพันธ์', 'มีนาคม', 'เมษายน', 'พฤษภาคม', 'มิถุนายน',
                            'กรกฎาคม', 'สิงหาคม', 'กันยายน', 'ตุลาคม', 'พฤศจิกายน', 'ธันวาคม']
        if any(keyword in question_lower for keyword in schedule_keywords):
            return 'work_schedule'
            
        # Specific model/part number queries
        model_keywords = ['model', 'rcug', 'ekac', 'ek258', 'ราคาอะไหล่', 'air cooled', 'water cooled']
        if any(keyword in question_lower for keyword in model_keywords):
            return 'specific_parts'
        
        # Enhanced spare parts detection
        spare_parts_keywords = ['อะไหล่', 'spare', 'parts', 'motor', 'chiller', 'compressor', 
                               'hitachi', 'daikin', 'mitsubishi', 'carrier', 'trane', 'york', 
                               'fan', 'circuit', 'board', 'transformer', 'valve']
        if any(keyword in question_lower for keyword in spare_parts_keywords):
            return 'spare_parts'
        
        # Overhaul-specific analysis
        overhaul_keywords = ['overhaul', 'ยกเครื่อง', 'ซ่อมใหญ่', 'compressor']
        if any(keyword in question_lower for keyword in overhaul_keywords):
            return 'overhaul_analysis'
            
        # Standard job quotations  
        job_keywords = ['เสนอราคา', 'งาน', 'quotation', 'standard', 'สรุปงาน']
        if any(keyword in question_lower for keyword in job_keywords):
            return 'job_quotation'
            
        # Sales analysis
        sales_keywords = ['วิเคราะห์', 'การขาย', 'ยอดขาย', 'รายงาน', 'analysis']
        if any(keyword in question_lower for keyword in sales_keywords):
            return 'sales_analysis'
            
        return 'general'
    
    def _convert_thai_years_and_months(self, question: str) -> Tuple[str, Dict[str, Any]]:
        """Convert Thai years to international years and extract month info"""
        converted = question
        month_info = {}
        
        # Year mappings
        year_mappings = {
            '2565': '2022', '2566': '2023', '2567': '2024', '2568': '2025',
            'ปี2565': '2022', 'ปี2566': '2023', 'ปี2567': '2024', 'ปี2568': '2025',
            'ปี 2565': '2022', 'ปี 2566': '2023', 'ปี 2567': '2024', 'ปี 2568': '2025'
        }
        
        # Month mappings for job_no pattern matching
        month_mappings = {
            'มกราคม': '01', 'กุมภาพันธ์': '02', 'มีนาคม': '03',
            'เมษายน': '04', 'พฤษภาคม': '05', 'มิถุนายน': '06',
            'กรกฎาคม': '07', 'สิงหาคม': '08', 'กันยายน': '09', 
            'ตุลาคม': '10', 'พฤศจิกายน': '11', 'ธันวาคม': '12'
        }
        
        # Apply year conversions
        for thai_year, intl_year in year_mappings.items():
            if thai_year in question:
                converted = converted.replace(thai_year, intl_year)
                # Extract the last 2 digits for job_no pattern
                month_info['year_short'] = intl_year[-2:]  # '2022' -> '22'
                month_info['year_full'] = intl_year
        
        # Extract month information
        for thai_month, month_code in month_mappings.items():
            if thai_month in question:
                month_info['month'] = month_code
                month_info['month_thai'] = thai_month
        
        # Log conversion if any changes were made
        if converted != question or month_info:
            logger.info(f"Thai conversion: '{question}' → '{converted}', month_info: {month_info}")
        
        return converted, month_info
    
    async def _generate_sql_query(self, question: str, intent: str, tenant_id: str) -> str:
        """Generate SQL query using specialized SQL model with enhanced Thai support"""
        try:
            self.stats['sql_generated'] += 1
            
            # Check cache first
            cache_key = f"{intent}_{hash(question.lower().strip())}"
            if cache_key in self.sql_cache:
                logger.info("Using cached SQL pattern")
                self.stats['cache_hits'] += 1
                return self.sql_cache[cache_key]
            
            # Convert Thai years and extract month info
            converted_question, month_info = self._convert_thai_years_and_months(question)
            
            # Build enhanced context for job_no pattern matching
            job_pattern_help = ""
            if month_info:
                if 'year_short' in month_info and 'month' in month_info:
                    job_pattern_help = f"\nSPECIAL: For {month_info.get('month_thai', 'unknown month')} {month_info.get('year_full', 'unknown year')}, search job_no pattern like '%{month_info['year_short']}-{month_info['month']}-%'"
            
            # Enhanced prompt with comprehensive HVAC business examples
            sql_prompt = f"""Generate PostgreSQL query for HVAC business.

DATABASE SCHEMA:
- sales2022,2023,2024,2025: id, job_no, customer_name, description, overhaul_, replacement, service_contact_
- spare_part, spare_part2: id, wh, product_code, product_name, unit, balance, unit_price, description
- work_force: id, date, customer, project, detail, service_group

JOB_NO PATTERN: JAE[year]-[month]-[sequence]-[type]
DATA TYPES: service_contact_, unit_price are TEXT - use CAST(field AS NUMERIC)

BUSINESS EXAMPLES WITH SQL PATTERNS:{job_pattern_help}

1. Customer Analysis:
"จำนวนลูกค้าทั้งหมด"
```sql
SELECT COUNT(DISTINCT customer_name) as total_customers FROM sales2024 WHERE customer_name IS NOT NULL;
```

2. Customer Purchase History:
"บริษัท ABC มีการซื้อขายย้อนหลัง 3 ปี มีอะไรบ้าง มูลค่าเท่าไหร่"
```sql
SELECT job_no, description, CAST(service_contact_ AS NUMERIC) as value FROM sales2022 WHERE customer_name ILIKE '%ABC%'
UNION ALL
SELECT job_no, description, CAST(service_contact_ AS NUMERIC) as value FROM sales2023 WHERE customer_name ILIKE '%ABC%'
UNION ALL  
SELECT job_no, description, CAST(service_contact_ AS NUMERIC) as value FROM sales2024 WHERE customer_name ILIKE '%ABC%'
ORDER BY job_no;
```

3. Customer Service History:
"บริษัท ABC มีประวัติการซ่อมอะไรบ้าง เมื่อไหร่"
```sql
SELECT job_no, customer_name, description, CAST(overhaul_ AS NUMERIC) as overhaul, CAST(replacement AS NUMERIC) as replacement
FROM sales2024 WHERE customer_name ILIKE '%ABC%' AND (overhaul_ IS NOT NULL OR replacement IS NOT NULL);
```

4. Work Schedule:
"แผนงานวันที่... มีงานอะไรบ้าง"
```sql
SELECT date, customer, project, detail, service_group FROM work_force WHERE date = '2024-08-31';
```

5. Specific Spare Parts Search:
"ราคาอะไหล่เครื่องทำน้ำเย็น Hitachi air cooled chiller model RCUG120"
```sql
SELECT product_code, product_name, CAST(unit_price AS NUMERIC) as price, balance, description FROM spare_part
WHERE (LOWER(product_name) LIKE '%hitachi%' OR LOWER(description) LIKE '%hitachi%')
AND (LOWER(product_name) LIKE '%rcug120%' OR LOWER(description) LIKE '%rcug120%')
UNION ALL
SELECT product_code, product_name, CAST(unit_price AS NUMERIC) as price, balance, description FROM spare_part2
WHERE (LOWER(product_name) LIKE '%hitachi%' OR LOWER(description) LIKE '%hitachi%')
AND (LOWER(product_name) LIKE '%rcug120%' OR LOWER(description) LIKE '%rcug120%');
```

6. Standard Job Summary:
"สรุปเสนอราคางาน Standard ทั้งหมด"
```sql
SELECT job_no, customer_name, description, CAST(service_contact_ AS NUMERIC) as price FROM sales2024
WHERE LOWER(description) LIKE '%standard%' ORDER BY CAST(service_contact_ AS NUMERIC) DESC NULLS LAST;
```

7. Overhaul Analysis (IMPROVED):
"รายงานยอดขาย overhaul compressor ปี 2567-2568"
```sql
SELECT '2024' as year, COUNT(*) as jobs, 
       SUM(CAST(overhaul_ AS NUMERIC)) as overhaul_revenue,
       SUM(CAST(service_contact_ AS NUMERIC)) as service_revenue,
       SUM(COALESCE(CAST(overhaul_ AS NUMERIC), 0) + COALESCE(CAST(service_contact_ AS NUMERIC), 0)) as total_revenue
FROM sales2024 WHERE LOWER(description) LIKE '%overhaul%' AND LOWER(description) LIKE '%compressor%'
UNION ALL
SELECT '2025' as year, COUNT(*) as jobs,
       SUM(CAST(overhaul_ AS NUMERIC)) as overhaul_revenue,
       SUM(CAST(service_contact_ AS NUMERIC)) as service_revenue, 
       SUM(COALESCE(CAST(overhaul_ AS NUMERIC), 0) + COALESCE(CAST(service_contact_ AS NUMERIC), 0)) as total_revenue
FROM sales2025 WHERE LOWER(description) LIKE '%overhaul%' AND LOWER(description) LIKE '%compressor%';
```

"แสดงรายละเอียดงาน overhaul ปี 2568"
```sql
SELECT job_no, customer_name, description, 
       CAST(overhaul_ AS NUMERIC) as overhaul_cost,
       CAST(service_contact_ AS NUMERIC) as service_cost,
       COALESCE(CAST(overhaul_ AS NUMERIC), 0) + COALESCE(CAST(service_contact_ AS NUMERIC), 0) as total_cost
FROM sales2025 WHERE LOWER(description) LIKE '%overhaul%' 
ORDER BY total_cost DESC NULLS LAST;
```

8. Monthly Work Schedule:
"งานที่วางแผนของเดือน สิงหาคม - กันยายน 2568 มีกี่งานอะไรบ้าง"
```sql
SELECT COUNT(*) as total_jobs, customer, project FROM work_force 
WHERE date >= '2025-08-01' AND date <= '2025-09-30' GROUP BY customer, project;
```

Question: {converted_question}
Original: {question}  
Intent: {intent}

Generate appropriate SQL based on similar patterns above:"""
            
            # Use timeout for faster response
            sql_response = await asyncio.wait_for(
                self.ollama_client.generate_response(
                    model=self.SQL_MODEL,
                    prompt=sql_prompt,
                    temperature=0.1  # Low temperature for precise SQL
                ),
                timeout=self.timeout_sql
            )
            
            # Clean up the SQL response
            sql_query = self._clean_sql_response(sql_response)
            
            # Basic SQL validation
            if not sql_query.upper().startswith(('SELECT', 'WITH')):
                logger.warning(f"Generated SQL doesn't start with SELECT: {sql_query}")
                return self._get_fallback_sql_with_context(intent, month_info)
            
            # Cache for future use
            self.sql_cache[cache_key] = sql_query
            
            return sql_query
            
        except asyncio.TimeoutError:
            logger.warning("SQL generation timeout, using contextual fallback")
            return self._get_fallback_sql_with_context(intent, {})
        except Exception as e:
            logger.error(f"SQL generation failed: {e}")
            return self._get_fallback_sql(intent)
    
    def _clean_sql_response(self, sql_response: str) -> str:
        """Clean up SQL response from AI model"""
        sql_query = sql_response.strip()
        
        # Remove markdown code blocks
        if sql_query.startswith('```sql'):
            sql_query = sql_query[6:]
        if sql_query.startswith('```'):
            sql_query = sql_query[3:]
        if sql_query.endswith('```'):
            sql_query = sql_query[:-3]
        
        return sql_query.strip()
    
    def _get_fallback_sql(self, intent: str) -> str:
        """Get fallback SQL queries based on real schema structure"""
        fallback_queries = {
            'job_quotation': """SELECT job_no, customer_name, description, 
                               CAST(service_contact_ AS NUMERIC) as service_price,
                               CAST(overhaul_ AS NUMERIC) as overhaul,
                               CAST(replacement AS NUMERIC) as replacement
                               FROM sales2024 
                               WHERE description IS NOT NULL 
                               ORDER BY CAST(service_contact_ AS NUMERIC) DESC NULLS LAST 
                               LIMIT 10""",
            
            'spare_parts': """SELECT product_code, product_name, wh,
                             CAST(unit_price AS NUMERIC) as price, balance, description
                             FROM spare_part 
                             WHERE product_name IS NOT NULL 
                             UNION ALL
                             SELECT product_code, product_name, wh,
                             CAST(unit_price AS NUMERIC) as price, balance, description  
                             FROM spare_part2
                             WHERE product_name IS NOT NULL
                             ORDER BY price DESC NULLS LAST
                             LIMIT 15""",
            
            'sales_analysis': """SELECT '2022' as year, COUNT(*) as jobs, SUM(CAST(service_contact_ AS NUMERIC)) as revenue FROM sales2022 WHERE service_contact_ IS NOT NULL
                                UNION ALL
                                SELECT '2023' as year, COUNT(*) as jobs, SUM(CAST(service_contact_ AS NUMERIC)) as revenue FROM sales2023 WHERE service_contact_ IS NOT NULL  
                                UNION ALL
                                SELECT '2024' as year, COUNT(*) as jobs, SUM(CAST(service_contact_ AS NUMERIC)) as revenue FROM sales2024 WHERE service_contact_ IS NOT NULL
                                UNION ALL  
                                SELECT '2025' as year, COUNT(*) as jobs, SUM(CAST(service_contact_ AS NUMERIC)) as revenue FROM sales2025 WHERE service_contact_ IS NOT NULL
                                ORDER BY year""",
            
            'work_summary': """SELECT date, customer, detail, service_group
                              FROM work_force 
                              WHERE detail IS NOT NULL 
                              ORDER BY date DESC NULLS LAST 
                              LIMIT 10""",
            
            'general': "SELECT COUNT(*) as total_records FROM sales2024"
        }
        
    def _get_fallback_sql_with_context(self, intent: str, month_info: Dict[str, Any]) -> str:
        """Get contextual fallback SQL queries"""
        
        # If we have month and year info, use job_no pattern matching
        if month_info.get('year_short') and month_info.get('month'):
            year_short = month_info['year_short']
            month = month_info['month']
            table = f"sales20{year_short}"
            
            return f"""SELECT job_no, customer_name, description, 
                      CAST(service_contact_ AS NUMERIC) as revenue,
                      CAST(overhaul_ AS NUMERIC) as overhaul,
                      CAST(replacement AS NUMERIC) as replacement
                      FROM {table}
                      WHERE job_no LIKE '%{year_short}-{month}-%'
                      AND (service_contact_ IS NOT NULL OR overhaul_ IS NOT NULL OR replacement IS NOT NULL)
                      ORDER BY job_no"""
        
        # Otherwise use standard fallback
        return self._get_fallback_sql(intent)
    
    async def _generate_natural_response(self, question: str, sql_results: List[Dict], sql_query: str, tenant_id: str) -> str:
        """Generate natural language response using NL model with optimization"""
        try:
            self.stats['nl_responses'] += 1
            
            if not sql_results:
                return f"ขออภัยครับ ไม่พบข้อมูลที่เกี่ยวข้องกับคำถาม: {question}"
            
            # Create concise data summary for faster processing
            data_summary = self._create_concise_summary(sql_results)
            
            # Use shorter, focused prompt for faster NL generation
            nl_prompt = f"""คำถาม: {question}
ข้อมูลจากฐานข้อมูล HVAC: {data_summary}

ตอบคำถามในภาษาไทยแบบกระชับและเป็นมิตร สำหรับธุรกิจ HVAC:"""
            
            # Use timeout for faster response
            nl_response = await asyncio.wait_for(
                self.ollama_client.generate_response(
                    model=self.NL_MODEL,
                    prompt=nl_prompt,
                    temperature=0.7  # Higher temperature for natural language
                ),
                timeout=self.timeout_nl
            )
            
            return nl_response.strip() if nl_response else self._get_structured_response(question, sql_results)
            
        except asyncio.TimeoutError:
            logger.warning("NL generation timeout, using structured response")
            return self._get_structured_response(question, sql_results)
        except Exception as e:
            logger.error(f"NL response generation failed: {e}")
            return self._get_fallback_response(question, sql_results)
    
    async def _generate_natural_response(self, question: str, sql_results: List[Dict], sql_query: str, tenant_id: str) -> str:
        """Generate natural language response using NL model with smart optimization"""
        try:
            self.stats['nl_responses'] += 1
            
            if not sql_results:
                return f"ไม่พบข้อมูลสำหรับคำถาม: {question}"
            
            # Smart response strategy based on data size
            if len(sql_results) > 20:
                # For large datasets, use structured response immediately to save time
                logger.info("Large dataset detected, using structured response for speed")
                return self._get_structured_response(question, sql_results)
            
            # Create concise data summary for faster processing
            data_summary = self._create_smart_summary(sql_results, question)
            
            # Use ultra-short prompt for speed
            nl_prompt = f"""คำถาม: {question}
ข้อมูล: {data_summary}

ตอบสั้นๆ ในภาษาไทย:"""
            
            # Use shorter timeout for faster response
            nl_response = await asyncio.wait_for(
                self.ollama_client.generate_response(
                    model=self.NL_MODEL,
                    prompt=nl_prompt,
                    temperature=0.5  # Lower temperature for faster, more focused response
                ),
                timeout=6  # Very short timeout
            )
            
            return nl_response.strip() if nl_response else self._get_structured_response(question, sql_results)
            
        except asyncio.TimeoutError:
            logger.info("NL generation timeout (6s), using optimized structured response")
            return self._get_optimized_structured_response(question, sql_results)
        except Exception as e:
            logger.error(f"NL response generation failed: {e}")
            return self._get_optimized_structured_response(question, sql_results)
    
    def _create_smart_summary(self, results: List[Dict], question: str) -> str:
        """Create smart data summary based on question type"""
        if not results:
            return "ไม่พบข้อมูล"
        
        # For sales analysis - focus on numbers
        if any(word in question.lower() for word in ['ยอดขาย', 'เปรียบเทียบ', 'วิเคราะห์']):
            if len(results) <= 5:
                return f"Sales data: {results}"
            else:
                return f"Sales summary: {len(results)} records, sample: {results[:2]}"
        
        # For spare parts - focus on product info  
        elif any(word in question.lower() for word in ['อะไหล่', 'spare', 'parts']):
            if len(results) <= 10:
                return f"Parts found: {results[:5]}"
            else:
                return f"Found {len(results)} parts, top items: {results[:3]}"
        
        # Generic summary
        else:
            return f"{len(results)} records: {results[:2] if len(results) > 2 else results}"
    
    def _get_optimized_structured_response(self, question: str, results: List[Dict]) -> str:
        """Enhanced structured response with complete business data"""
        if not results:
            return f"ไม่พบข้อมูลสำหรับคำถาม: {question}"
        
        # Overhaul analysis response
        if any(word in question.lower() for word in ['overhaul', 'ยกเครื่อง', 'compressor']):
            response = f"รายละเอียดงาน Overhaul: {question}\n\nพบ {len(results)} รายการ\n\n"
            
            total_value = 0
            for i, result in enumerate(results, 1):
                job_no = result.get('job_no', 'N/A')
                customer = result.get('customer_name', 'N/A')
                description = result.get('description', 'N/A')
                
                # Try different price fields
                price = (result.get('total_overhaul') or 
                        result.get('overhaul_cost') or 
                        result.get('overhaul_') or 
                        result.get('service_contact_') or 0)
                
                if price:
                    total_value += float(price)
                    response += f"{i}. {job_no}\n   ลูกค้า: {customer}\n   งาน: {description}\n   มูลค่า: {price:,.0f} บาท\n\n"
                else:
                    response += f"{i}. {job_no}\n   ลูกค้า: {customer}\n   งาน: {description}\n   มูลค่า: ไม่ระบุ\n\n"
            
            if total_value > 0:
                response += f"รวมมูลค่าทั้งหมด: {total_value:,.0f} บาท"
            else:
                response += "หมายเหตุ: ไม่พบข้อมูลมูลค่างานในระบบ อาจต้องตรวจสอบข้อมูลเพิ่มเติม"
            
            return response
        
        # Sales analysis response
        elif any(word in question.lower() for word in ['ยอดขาย', 'เปรียบเทียบ', 'วิเคราะห์']):
            response = f"การวิเคราะห์ยอดขาย: {question}\n\n"
            
            total_jobs = 0
            total_revenue = 0
            
            for result in results:
                year = result.get('year', 'N/A')
                jobs = result.get('jobs', 0) 
                revenue = result.get('revenue') or result.get('total_overhaul') or 0
                avg_per_job = revenue / jobs if jobs > 0 else 0
                
                total_jobs += jobs
                total_revenue += revenue
                
                if revenue > 0:
                    response += f"ปี {year}: {jobs:,} งาน, รายได้ {revenue:,.0f} บาท (เฉลี่ย {avg_per_job:,.0f} บาท/งาน)\n"
                else:
                    response += f"ปี {year}: {jobs:,} งาน, ไม่พบข้อมูลรายได้\n"
            
            if total_revenue > 0:
                response += f"\nสรุปรวม: {total_jobs:,} งาน, รายได้รวม {total_revenue:,.0f} บาท"
            else:
                response += f"\nสรุปรวม: {total_jobs:,} งาน, ข้อมูลรายได้ไม่สมบูรณ์"
            
            return response
        
        # Spare parts response  
        elif any(word in question.lower() for word in ['อะไหล่', 'spare', 'parts']):
            response = f"ผลการค้นหาอะไหล่: {question}\n\nพบ {len(results)} รายการ\n\n"
            
            for i, result in enumerate(results[:5], 1):
                name = result.get('product_name', 'N/A')
                code = result.get('product_code', 'N/A')
                price = result.get('price') or result.get('unit_price', 0)
                balance = result.get('balance', 0)
                description = result.get('description', '')
                
                response += f"{i}. {name} ({code})\n"
                if price:
                    response += f"   ราคา: {price:,.0f} บาท"
                if balance:
                    response += f" | คงเหลือ: {balance} ชิ้น"
                if description:
                    response += f"\n   รายละเอียด: {description}"
                response += "\n\n"
            
            if len(results) > 5:
                response += f"... และอีก {len(results) - 5} รายการ"
            
            return response
        
        # Customer analysis response
        elif any(word in question.lower() for word in ['ลูกค้า', 'customer', 'บริษัท']):
            if len(results) == 1 and 'total_customers' in results[0]:
                # Customer count query
                count = results[0]['total_customers']
                return f"จำนวนลูกค้าทั้งหมด: {count:,} ราย"
            else:
                # Customer details
                response = f"ข้อมูลลูกค้า: {question}\n\nพบ {len(results)} รายการ\n\n"
                
                total_value = 0
                for i, result in enumerate(results[:10], 1):
                    job_no = result.get('job_no', 'N/A')
                    customer = result.get('customer_name', 'N/A')
                    description = result.get('description', 'N/A')
                    value = result.get('value') or result.get('service_contact_') or 0
                    
                    response += f"{i}. {job_no} | {customer}\n   งาน: {description}"
                    if value:
                        total_value += float(value)
                        response += f"\n   มูลค่า: {value:,.0f} บาท"
                    response += "\n\n"
                
                if total_value > 0:
                    response += f"รวมมูลค่าทั้งหมด: {total_value:,.0f} บาท"
                
                return response
        
        # Generic response
        else:
            response = f"ผลการค้นหา: {question}\n\nพบ {len(results)} รายการ\n\n"
            for i, result in enumerate(results[:5], 1):
                response += f"{i}. "
                key_values = list(result.items())[:4]  # Show first 4 fields
                response += ", ".join(f"{k}: {v}" for k, v in key_values)
                response += "\n"
            
            if len(results) > 5:
                response += f"\n... และอีก {len(results) - 5} รายการ"
            
            return response
    
    def _get_fallback_response(self, question: str, sql_results: List[Dict]) -> str:
        """Generate fallback response when NL model fails"""
        if not sql_results:
            return f"ขออภัยครับ ไม่พบข้อมูลที่เกี่ยวข้องกับคำถาม: {question}"
        
        # Simple structured response
        response = f"ผลลัพธ์สำหรับคำถาม: {question}\n\n"
        response += f"พบข้อมูล {len(sql_results)} รายการ\n\n"
        
        # Show first 3 results
        for i, result in enumerate(sql_results[:3], 1):
            response += f"รายการที่ {i}:\n"
            for key, value in result.items():
                response += f"  • {key}: {value}\n"
            response += "\n"
        
        if len(sql_results) > 3:
            response += f"... และอีก {len(sql_results) - 3} รายการ"
        
        return response
    
    async def process_any_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Main entry point for processing questions with optimized dual-model approach"""
        start_time = time.time()
        self.stats['total_queries'] += 1
        
        try:
            logger.info(f"Processing question with Optimized Dual-Model AI: {question[:50]}...")
            
            # Step 1: Quick intent classification
            intent = self._classify_question_intent(question)
            logger.info(f"Question intent classified as: {intent}")
            
            # Step 2: Generate SQL query using specialized model (with caching)
            sql_query = await self._generate_sql_query(question, intent, tenant_id)
            logger.info(f"Generated SQL: {sql_query}")
            
            # Step 3: Execute SQL query
            sql_results = await self.db_handler.execute_query(sql_query, tenant_id)
            logger.info(f"SQL execution returned {len(sql_results)} results")
            
            # Step 4: Generate natural language response (optimized)
            if self.enable_parallel and len(sql_results) > 0:
                # Use optimized NL generation
                nl_response = await self._generate_natural_response(question, sql_results, sql_query, tenant_id)
            else:
                # Use structured response for empty results or when parallel disabled
                nl_response = self._get_structured_response(question, sql_results)
            
            # Step 5: Prepare final response
            processing_time = time.time() - start_time
            success = len(sql_results) > 0
            
            if success:
                self.stats['successful_queries'] += 1
            
            # Update average response time
            total_time = self.stats['avg_response_time'] * (self.stats['total_queries'] - 1) + processing_time
            self.stats['avg_response_time'] = total_time / self.stats['total_queries']
            
            return {
                'answer': nl_response,
                'success': success,
                'sql_query': sql_query,
                'results_count': len(sql_results),
                'processing_time': processing_time,
                'ai_system_used': 'dual_model_optimized',
                'question_analysis': {
                    'intent': intent,
                    'tenant_id': tenant_id,
                    'optimization': 'enabled',
                    'cache_used': self.stats['cache_hits'] > 0,
                    'models_used': {
                        'sql_generation': self.SQL_MODEL,
                        'nl_generation': self.NL_MODEL
                    }
                },
                'models_used': {
                    'sql_model': self.SQL_MODEL,
                    'nl_model': self.NL_MODEL
                },
                'performance_stats': {
                    'avg_response_time': self.stats['avg_response_time'],
                    'cache_hit_rate': self.stats['cache_hits'] / max(self.stats['sql_generated'], 1) * 100,
                    'success_rate': self.stats['successful_queries'] / max(self.stats['total_queries'], 1) * 100
                }
            }
            
        except Exception as e:
            logger.error(f"Optimized dual-model processing failed: {e}")
            processing_time = time.time() - start_time
            
            return {
                'answer': f"เกิดข้อผิดพลาดในการประมวลผล: {str(e)}\n\nกรุณาลองใหม่อีกครั้งหรือติดต่อผู้ดูแลระบบ",
                'success': False,
                'sql_query': None,
                'results_count': 0,
                'processing_time': processing_time,
                'ai_system_used': 'dual_model_error',
                'error': str(e)
            }

class EnhancedUnifiedPostgresOllamaAgent:
    """Main agent class that provides the interface for the chatbot system"""
    
    def __init__(self):
        try:
            # Initialize the dual-model system
            self.dual_model_ai = DualModelDynamicAISystem()
            
            # Initialize basic stats
            self.stats = {
                'total_queries': 0,
                'successful_queries': 0,
                'failed_queries': 0,
                'avg_response_time': 0.0
            }
            
            logger.info("Enhanced Unified PostgreSQL Ollama Agent initialized successfully")
            logger.info("✅ Dual-Model Dynamic AI System ready")
            
        except Exception as e:
            logger.error(f"Failed to initialize Enhanced Agent: {e}")
            raise
    
    async def process_any_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Main entry point for question processing"""
        try:
            # Update stats
            self.stats['total_queries'] += 1
            
            # Process using dual-model system
            result = await self.dual_model_ai.process_any_question(question, tenant_id)
            
            # Update stats based on result
            if result.get('success'):
                self.stats['successful_queries'] += 1
            else:
                self.stats['failed_queries'] += 1
            
            # Update average response time
            if 'processing_time' in result:
                total_time = self.stats['avg_response_time'] * (self.stats['total_queries'] - 1) + result['processing_time']
                self.stats['avg_response_time'] = total_time / self.stats['total_queries']
            
            return result
            
        except Exception as e:
            self.stats['failed_queries'] += 1
            logger.error(f"Question processing failed: {e}")
            
            return {
                'answer': f"เกิดข้อผิดพลาดในการประมวลผลคำถาม: {str(e)}\n\nกรุณาลองใหม่อีกครั้ง",
                'success': False,
                'sql_query': None,
                'results_count': 0,
                'ai_system_used': 'error_handler',
                'error': str(e)
            }
    
    async def process_enhanced_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Compatibility method - redirects to process_any_question"""
        return await self.process_any_question(question, tenant_id)
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get comprehensive system statistics with performance metrics"""
        return {
            'agent_stats': self.stats,
            'dual_model_stats': self.dual_model_ai.stats,
            'performance_metrics': {
                'avg_response_time': self.dual_model_ai.stats.get('avg_response_time', 0),
                'cache_hit_rate': self.dual_model_ai.stats.get('cache_hits', 0) / max(self.dual_model_ai.stats.get('sql_generated', 1), 1) * 100,
                'success_rate': self.dual_model_ai.stats.get('successful_queries', 0) / max(self.dual_model_ai.stats.get('total_queries', 1), 1) * 100,
                'sql_cache_size': len(self.dual_model_ai.sql_cache),
                'optimization_enabled': self.dual_model_ai.enable_parallel
            },
            'system_info': {
                'sql_model': self.dual_model_ai.SQL_MODEL,
                'nl_model': self.dual_model_ai.NL_MODEL,
                'ollama_url': self.dual_model_ai.ollama_client.base_url,
                'timeout_settings': {
                    'sql_timeout': self.dual_model_ai.timeout_sql,
                    'nl_timeout': self.dual_model_ai.timeout_nl
                }
            }
        }
    
    def clear_cache(self):
        """Clear SQL and schema cache for fresh start"""
        self.dual_model_ai.sql_cache.clear()
        self.dual_model_ai.schema_cache.clear()
        logger.info("Dual-Model caches cleared")

# =============================================================================
# EXPORT FOR BACKWARDS COMPATIBILITY
# =============================================================================

# These aliases ensure compatibility with different import patterns
UnifiedEnhancedPostgresOllamaAgent = EnhancedUnifiedPostgresOllamaAgent
DualModelDynamicAgent = EnhancedUnifiedPostgresOllamaAgent