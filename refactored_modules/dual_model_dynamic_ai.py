import os
import re
import json
import asyncio
import aiohttp
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date
import logging
from decimal import Decimal

logger = logging.getLogger(__name__)

class DualModelDynamicAISystem:
    """🧠 ระบบ AI 2 โมเดล: SQL Generation + Natural Language Response"""
    
    def __init__(self, database_handler, original_ollama_client):
        self.db_handler = database_handler
        self.original_ollama_client = original_ollama_client
        
        # Model Configuration
        self.SQL_MODEL = "mannix/defog-llama3-sqlcoder-8b:latest"
        self.NL_MODEL = "llama3.2:3b"
        
        # Ollama Configuration
        self.ollama_base_url = os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434')
        self.request_timeout = 60
        
        # Caching System
        self.schema_cache = {}
        self.sql_cache = {}
        self.cache_ttl = 3600
        
        logger.info(f"🚀 Dual-Model Dynamic AI initialized:")
        logger.info(f"   📝 SQL Generation: {self.SQL_MODEL}")
        logger.info(f"   💬 Response Generation: {self.NL_MODEL}")
    
    # =========================================================================
    # 🎯 MAIN PROCESSING PIPELINE
    # =========================================================================
    
    async def process_any_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """🎯 แก้ไข main processing ให้มี error recovery"""
        
        try:
            start_time = datetime.now()
            logger.info(f"🚀 [DUAL-MODEL] Processing: {question}")
            
            # Step 1: Schema Discovery with error handling
            try:
                actual_schema = await self._discover_complete_schema(tenant_id)
                if not actual_schema:
                    raise Exception("No schema discovered")
            except Exception as schema_error:
                logger.error(f"❌ Schema discovery failed: {schema_error}")
                # ใช้ fallback schema
                actual_schema = self._get_fallback_schema()
            
            # Step 2: SQL Generation with multiple attempts
            sql_attempts = 0
            max_attempts = 2
            
            while sql_attempts < max_attempts:
                sql_attempts += 1
                
                try:
                    sql_result = await self._generate_sql_with_specialist(question, actual_schema, tenant_id)
                    
                    if sql_result["success"]:
                        break
                    else:
                        logger.warning(f"⚠️ SQL generation attempt {sql_attempts} failed")
                        if sql_attempts == max_attempts:
                            return await self._create_fallback_response(question, tenant_id, actual_schema)
                        
                except Exception as sql_error:
                    logger.error(f"❌ SQL generation attempt {sql_attempts} error: {sql_error}")
                    if sql_attempts == max_attempts:
                        return await self._create_fallback_response(question, tenant_id, actual_schema)
            
            # Step 3: Execute SQL
            try:
                results = await self._execute_sql_safely(sql_result["sql_query"], tenant_id)
            except Exception as exec_error:
                logger.error(f"❌ SQL execution failed: {exec_error}")
                return await self._create_fallback_response(question, tenant_id, actual_schema)
            
            # Step 4: Generate Natural Response
            try:
                if results:
                    natural_response = await self._generate_natural_response(
                        question, results, sql_result["sql_query"], tenant_id
                    )
                else:
                    natural_response = f"ไม่พบข้อมูลสำหรับคำถาม: {question}\n\nอาจเป็นเพราะ:\n• ไม่มีข้อมูลในช่วงเวลาที่ระบุ\n• คำค้นหาไม่ตรงกับข้อมูลที่มี\n\nลองปรับคำถามใหม่หรือสอบถามข้อมูลที่มีในระบบได้ครับ"
                    
            except Exception as response_error:
                logger.error(f"❌ NL generation failed: {response_error}")
                natural_response = self._create_simple_formatted_response(question, results)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return {
                "answer": natural_response,
                "success": len(results) > 0,
                "sql_query": sql_result["sql_query"],
                "results_count": len(results),
                "question_analysis": sql_result["analysis"],
                "data_source_used": "dual_model_dynamic_ai_fixed",
                "system_used": "sql_specialist_plus_nl_generator", 
                "processing_time": processing_time,
                "models_used": {
                    "sql_generation": self.SQL_MODEL,
                    "response_generation": self.NL_MODEL
                }
            }
            
        except Exception as e:
            logger.error(f"❌ [DUAL-MODEL] Complete failure: {e}")
            return self._create_error_response(str(e), tenant_id)     
        
    def _get_fallback_schema(self) -> Dict[str, Any]:
        """🆘 สร้าง fallback schema เมื่อไม่สามารถค้นพบได้"""
        
        return {
            "sales2023": {
                "columns": [
                    {"name": "id", "type": "integer"},
                    {"name": "customer_name", "type": "varchar"},
                    {"name": "service_contact_", "type": "integer"},
                    {"name": "job_no", "type": "varchar"},
                    {"name": "description", "type": "varchar"}
                ]
            },
            "sales2024": {
                "columns": [
                    {"name": "id", "type": "integer"},
                    {"name": "customer_name", "type": "varchar"},
                    {"name": "service_contact_", "type": "integer"},
                    {"name": "job_no", "type": "varchar"},
                    {"name": "description", "type": "varchar"}
                ]
            },
            "spare_part": {
                "columns": [
                    {"name": "id", "type": "integer"},
                    {"name": "product_name", "type": "varchar"},
                    {"name": "unit_price", "type": "varchar"},
                    {"name": "balance", "type": "integer"}
                ]
            }
        }

    async def _create_no_results_response(self, question: str, tenant_id: str) -> str:
        """📭 สร้างคำตอบเมื่อไม่พบข้อมูล"""
        
        suggestions = [
            "ลองเปลี่ยนช่วงปีที่ค้นหา",
            "ตรวจสอบการสะกดชื่อลูกค้าหรือแบรนด์",
            "ใช้คำค้นหาที่ทั่วไปขึ้น"
        ]
        
        nl_prompt = f"""ผู้ใช้ถามเกี่ยวกับ: {question}

    แต่ไม่พบข้อมูลในฐานข้อมูล กรุณาสร้างคำตอบที่:
    1. อธิบายว่าไม่พบข้อมูล
    2. เสนอแนะวิธีการค้นหาที่อาจได้ผล
    3. แสดงตัวอย่างคำถามที่ทำได้

    ใช้ภาษาไทยที่เป็นมิตร:"""

        try:
            response = await self._call_ollama_model(self.NL_MODEL, nl_prompt)
            return self._clean_nl_response(response)
        except:
            return f"ไม่พบข้อมูลสำหรับคำถาม: {question}\n\nลองปรับคำถามใหม่หรือสอบถามข้อมูลที่มีในระบบได้ครับ"

    # =============================================================================
    # 🧪 สำหรับ DEBUGGING - เพิ่ม Sync Version Functions
    # =============================================================================

    def _get_database_connection_sync(self, tenant_id: str):
        """🔗 Sync version ของ database connection"""
        
        try:
            if hasattr(self.db_handler, 'get_database_connection'):
                # เรียก sync function
                return self.db_handler.get_database_connection(tenant_id)
            else:
                # Manual connection
                from .tenant_config import TenantConfigManager
                import psycopg2
                
                config_manager = TenantConfigManager()
                tenant_config = config_manager.tenant_configs[tenant_id]
                
                return psycopg2.connect(
                    host=tenant_config.db_host,
                    port=tenant_config.db_port,
                    database=tenant_config.db_name,
                    user=tenant_config.db_user,
                    password=tenant_config.db_password
                )
        except Exception as e:
            logger.error(f"❌ Sync database connection failed: {e}")
            raise

    # =========================================================================
    # 🔍 SCHEMA DISCOVERY
    # =========================================================================
    
    async def _discover_complete_schema(self, tenant_id: str) -> Dict[str, Any]:
        """🔍 แก้ไขการค้นพบ schema - รองรับทั้ง async และ sync"""
        
        cache_key = f"schema_{tenant_id}"
        if cache_key in self.schema_cache:
            cache_time = self.schema_cache[cache_key]["timestamp"]
            if (datetime.now() - cache_time).total_seconds() < self.cache_ttl:
                return self.schema_cache[cache_key]["data"]
        
        try:
            # ✅ แก้ไข: ตรวจสอบว่า database connection เป็น async หรือ sync
            try:
                # ลองใช้ async version ก่อน
                if hasattr(self.db_handler, 'get_database_connection'):
                    # ตรวจสอบว่าเป็น coroutine หรือไม่
                    conn_result = self.db_handler.get_database_connection(tenant_id)
                    
                    # ถ้าเป็น coroutine ให้ await
                    if hasattr(conn_result, '__await__'):
                        conn = await conn_result
                    else:
                        conn = conn_result
                else:
                    conn = await self._create_manual_connection(tenant_id)
                    
            except Exception as db_error:
                logger.warning(f"⚠️ Primary DB connection failed: {db_error}")
                # Fallback to manual sync connection
                conn = self._create_sync_connection(tenant_id)
            
            cursor = conn.cursor()
            
            # Discover all tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            schema = {}
            for table in tables:
                try:
                    # Get column information
                    cursor.execute(f"""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns 
                        WHERE table_name = '{table}' 
                        AND table_schema = 'public'
                        ORDER BY ordinal_position
                    """)
                    
                    columns = []
                    for col_row in cursor.fetchall():
                        col_name, data_type, nullable, default = col_row
                        columns.append({
                            "name": col_name,
                            "type": data_type,
                            "nullable": nullable == "YES",
                            "default": default
                        })
                    
                    # Get sample data (ลดเป็น 2 rows เพื่อความเร็ว)
                    cursor.execute(f"SELECT * FROM {table} LIMIT 2")
                    sample_rows = cursor.fetchall()
                    
                    sample_data = []
                    if sample_rows and cursor.description:
                        sample_data = [dict(zip([desc[0] for desc in cursor.description], row)) 
                                    for row in sample_rows]
                    
                    schema[table] = {
                        "columns": columns,
                        "sample_data": sample_data,
                        "row_count": len(sample_data)
                    }
                    
                except Exception as table_error:
                    logger.warning(f"⚠️ Failed to analyze table {table}: {table_error}")
                    # เก็บข้อมูลขั้นต่ำ
                    schema[table] = {
                        "columns": [{"name": "id", "type": "integer"}],
                        "sample_data": [],
                        "row_count": 0
                    }
            
            cursor.close()
            conn.close()
            
            # Cache the result
            self.schema_cache[cache_key] = {
                "data": schema,
                "timestamp": datetime.now()
            }
            
            logger.info(f"✅ Schema discovered: {len(tables)} tables")
            return schema
            
        except Exception as e:
            logger.error(f"❌ Schema discovery failed: {e}")
            # Return minimal fallback schema
            return {
                "sales2023": {"columns": [{"name": "service_contact_", "type": "integer"}]},
                "sales2024": {"columns": [{"name": "service_contact_", "type": "integer"}]},
                "spare_part": {"columns": [{"name": "product_name", "type": "varchar"}]}
            }

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

    async def _create_manual_connection(self, tenant_id: str):
        """🔗 สร้าง manual async connection"""
        
        try:
            import asyncpg
            
            connection_string = f"postgresql://postgres:password123@postgres-{tenant_id}:5432/siamtemp_{tenant_id.replace('-', '_')}"
            
            return await asyncpg.connect(connection_string)
            
        except ImportError:
            # asyncpg ไม่มี ให้ใช้ sync connection
            return self._create_sync_connection(tenant_id)
    
    # =========================================================================
    # 📝 SQL GENERATION WITH SPECIALIST MODEL
    # =========================================================================
    
    async def _generate_sql_with_specialist(self, question: str, schema: Dict[str, Any], tenant_id: str) -> Dict[str, Any]:
        """แก้ไข error handling"""
        
        try:
            # เพิ่ม logging เพื่อ debug
            logger.info(f"🔍 Creating prompt for: {question}")
            sql_prompt = self._create_business_aware_prompt(question, schema, tenant_id)
            logger.info(f"📝 Prompt length: {len(sql_prompt)}")
            
            # Call SQL Specialist
            logger.info(f"⏳ Calling {self.SQL_MODEL}...")
            sql_response = await self._call_ollama_model(self.SQL_MODEL, sql_prompt)
            logger.info(f"✅ Got response: {len(sql_response)} chars")
            
            if not sql_response:
                logger.error("❌ Empty response from SQL model")
                return {"success": False, "error": "Empty SQL response"}
            
            # Extract SQL
            sql_query = self._extract_clean_sql(sql_response)
            logger.info(f"🔧 Extracted SQL: {sql_query}")
            
            if not sql_query:
                logger.error(f"❌ Failed to extract SQL from: {sql_response[:200]}")
                return {"success": False, "error": "SQL extraction failed"}
                
            return {
                "success": True,
                "sql_query": sql_query,
                "analysis": {"model_used": self.SQL_MODEL}
            }
            
        except Exception as e:
            logger.error(f"❌ SQL generation error: {str(e)}")
            logger.error(f"❌ Question was: {question}")
            return {"success": False, "error": str(e)}

    def _extract_and_validate_sql(self, response: str, schema: Dict[str, Any]) -> Optional[str]:
        """🔧 ดึงและตรวจสอบ SQL อย่างละเอียด"""
        
        # ลบ markdown และ formatting
        cleaned_response = re.sub(r'```sql\s*', '', response)
        cleaned_response = re.sub(r'```\s*', '', cleaned_response)
        cleaned_response = cleaned_response.strip()
        
        # หา SELECT statement
        sql_patterns = [
            r'(SELECT.*?;)',                                    # Simple SELECT with semicolon
            r'(SELECT.*?FROM\s+\w+.*?(?:ORDER BY.*?)?;?)',      # SELECT with FROM
            r'(SELECT.*?UNION.*?;?)',                           # SELECT with UNION
        ]
        
        for pattern in sql_patterns:
            matches = re.findall(pattern, cleaned_response, re.DOTALL | re.IGNORECASE)
            for match in matches:
                sql = match.strip()
                
                # ทำความสะอาด
                sql = re.sub(r'\s+', ' ', sql)
                if not sql.endswith(';'):
                    sql += ';'
                
                # ตรวจสอบความสมบูรณ์
                if self._is_sql_complete(sql, schema):
                    return sql
        
        # ถ้าไม่เจอ ลองหาแค่ SELECT ธรรมดา
        select_match = re.search(r'SELECT\s+.*?FROM\s+(\w+)', cleaned_response, re.IGNORECASE | re.DOTALL)
        if select_match:
            table_name = select_match.group(1)
            if table_name in schema:
                # พบ table ที่ถูกต้อง ลองสร้าง SQL ใหม่
                return self._fix_incomplete_sql(cleaned_response, schema)
        
        logger.warning(f"❌ No valid SQL found in: {response[:300]}")
        return None

    def _is_sql_complete(self, sql: str, schema: Dict[str, Any]) -> bool:
        """✅ ตรวจสอบ SQL ว่าสมบูรณ์"""
        
        sql_upper = sql.upper()
        
        # ต้องมี SELECT และ FROM
        if 'SELECT' not in sql_upper or 'FROM' not in sql_upper:
            return False
        
        # ต้องไม่จบด้วย FROM; เปล่า ๆ
        if re.search(r'FROM\s*;', sql, re.IGNORECASE):
            return False
        
        # ต้องมี table name ที่ถูกต้องหลัง FROM
        from_tables = re.findall(r'FROM\s+(\w+)', sql, re.IGNORECASE)
        if not from_tables:
            return False
        
        # ตรวจสอบว่า table มีจริงใน schema
        for table in from_tables:
            if table.lower() not in [t.lower() for t in schema.keys()]:
                logger.warning(f"❌ Table {table} not found in schema")
                return False
        
        return True

    def _fix_incomplete_sql(self, sql_response: str, schema: Dict[str, Any]) -> Optional[str]:
        """🔧 แก้ไข SQL ที่ไม่สมบูรณ์"""
        
        # หาคำสั่ง SELECT
        select_match = re.search(r'(SELECT.*?)(?:FROM|$)', sql_response, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return None
        
        select_part = select_match.group(1).strip()
        
        # หา table ที่เหมาะสม
        if 'ปี' in sql_response or 'year' in sql_response.lower():
            # เป็นคำถามเกี่ยวกับปี ใช้ sales tables
            sales_tables = [t for t in schema.keys() if 'sales' in t.lower()]
            if sales_tables:
                # ใช้ table ล่าสุด
                main_table = sorted(sales_tables, reverse=True)[0]
                
                fixed_sql = f"""
                {select_part}
                FROM {main_table} 
                WHERE service_contact_ IS NOT NULL 
                AND service_contact_ > 0
                ORDER BY id DESC
                LIMIT 100;
                """
                
                return re.sub(r'\s+', ' ', fixed_sql).strip()
        
        return None

    def _create_business_aware_prompt(self, question: str, schema: Dict[str, Any], tenant_id: str) -> str:
        """สร้าง prompt ตามประเภทคำถาม"""
        
        question_lower = question.lower()
        
        # ลูกค้า questions
        if any(word in question_lower for word in ['ลูกค้า', 'customer']):
            return f"""Generate PostgreSQL to count unique customers.
            
    Tables: sales2024, sales2022 (customer_name column)
    Question: {question}

    Example: SELECT COUNT(DISTINCT customer_name) AS customers FROM sales2024;

    Generate query:"""

        # อะไหล่ questions  
        elif any(word in question_lower for word in ['อะไหล่', 'motor', 'ราคา']):
            return f"""Generate PostgreSQL for spare parts search.
            
    Tables: spare_part, spare_part2
    Columns: product_name, unit_price, description
    Question: {question}

    Example: SELECT product_name, unit_price FROM spare_part WHERE product_name ILIKE '%motor%';

    Generate query:"""

        # งาน PM questions
        elif any(word in question_lower for word in ['pm', 'งาน pm']):
            return f"""Generate PostgreSQL for PM jobs.
            
    Tables: work_force 
    Columns: job_description_pm (boolean), customer, detail
    Question: {question}

    Example: SELECT COUNT(*) FROM work_force WHERE job_description_pm = true;

    Generate query:"""
        
        elif any(word in question_lower for word in ['เดือน', 'วันที่']):
            # ดึงเดือนและปีจากคำถาม
            month_map = {
                'มกราคม': '01', 'กุมภาพันธ์': '02', 'มีนาคม': '03', 'เมษายน': '04',
                'พฤษภาคม': '05', 'มิถุนายน': '06', 'กรกฎาคม': '07', 'สิงหาคม': '08',
                'กันยายน': '09', 'ตุลาคม': '10', 'พฤศจิกายน': '11', 'ธันวาคม': '12'
            }
            
            month_code = None
            for month_name, code in month_map.items():
                if month_name in question:
                    month_code = code
                    break
            
            if not month_code:
                month_code = 'XX'  # wildcard
            
            return f"""Generate PostgreSQL for work_force date queries.
            
        Table: work_force
        Date formats: "1-3/06/2025", "26/05/2025 – 02/06/2025", "45751"
        Question: {question}

        For month {month_code}: WHERE date LIKE '%/{month_code}/%'
        For any month: WHERE date LIKE '%/MM/%' (replace MM with month number)
        Show work details: SELECT date, customer, detail FROM work_force WHERE condition;

        Generate query:"""
        
        # การเปรียบเทียบ questions
        elif any(word in question_lower for word in ['เปรียบเทียบ', 'วิเคราะห์']):
            return self._create_comparison_prompt(question, schema)
        
        # Default
        else:
            return f"""Generate simple PostgreSQL query.
    Question: {question}
    Use appropriate table based on question context.
    Generate query:"""

    def _create_schema_prompt_for_sql_coder(self, schema: Dict[str, Any]) -> str:
        """📋 สร้าง schema prompt ที่เหมาะกับ SQL Coder"""
        
        prompt = "TABLES AND COLUMNS:\n\n"
        
        for table_name, table_info in schema.items():
            prompt += f"Table: {table_name}\n"
            
            # Show columns with types
            for col in table_info["columns"]:
                prompt += f"  - {col['name']} ({col['type']})\n"
            
            # Show sample data patterns
            if table_info["sample_data"]:
                prompt += "  Sample values:\n"
                sample_row = table_info["sample_data"][0]
                for col_name, value in list(sample_row.items())[:3]:  # First 3 columns
                    if value is not None:
                        prompt += f"    {col_name}: {value}\n"
            
            prompt += "\n"
        
        return prompt
    
    def _extract_clean_sql(self, response: str) -> Optional[str]:
        """🔧 แก้ไขการดึง SQL ให้สมบูรณ์"""
        
        # ลบ prefix ที่ไม่จำเป็น
        response = response.strip()
        lines = response.split('\n')
        
        sql_lines = []
        found_select = False
        
        for line in lines:
            line = line.strip()
            
            # ข้าม comments และบรรทัดว่าง
            if not line or line.startswith(('--', '#', '/*')):
                continue
                
            # เริ่มเก็บจาก SELECT
            if line.upper().startswith('SELECT'):
                found_select = True
                sql_lines.append(line)
            elif found_select:
                # เก็บบรรทัดต่อเนื่อง
                if any(keyword in line.upper() for keyword in ['FROM', 'WHERE', 'GROUP', 'ORDER', 'UNION', 'LIMIT', ';']):
                    sql_lines.append(line)
                else:
                    # หยุดถ้าเจอบรรทัดที่ไม่เกี่ยวข้อง
                    break
        
        if sql_lines:
            sql = ' '.join(sql_lines)
            
            # ทำความสะอาด
            sql = re.sub(r'\s+', ' ', sql)  # Multiple spaces to single
            sql = sql.replace(' ;', ';')    # Fix spacing before semicolon
            
            # เพิ่ม semicolon ถ้าไม่มี
            if not sql.rstrip().endswith(';'):
                sql += ';'
            
            # ตรวจสอบความสมบูรณ์
            if self._validate_sql_completeness(sql):
                return sql
        
        logger.warning(f"❌ Could not extract valid SQL from: {response[:200]}...")
        return None

    def _create_year_comparison_prompt(self, question: str, schema: Dict[str, Any]) -> str:
        """🎯 สร้าง prompt เฉพาะสำหรับการเปรียบเทียบยอดขายระหว่างปี"""
        
        # ตรวจสอบตารางที่มี
        available_sales_tables = [table for table in schema.keys() if 'sales' in table.lower()]
        
        prompt = f"""Generate PostgreSQL query for Thai HVAC sales comparison.

    AVAILABLE SALES TABLES: {', '.join(available_sales_tables)}

    THAI YEAR MAPPING:
    - ปี 2566 → sales2023 table
    - ปี 2567 → sales2024 table
    - ปี 2568 → sales2025 table

    REVENUE COLUMN: service_contact_ (contains revenue amounts)

    QUESTION: {question}

    Required SQL structure for year comparison:
    ```sql
    SELECT 
        '2566' as year,
        COUNT(*) as total_jobs,
        SUM(CAST(service_contact_ AS NUMERIC)) as total_revenue,
        AVG(CAST(service_contact_ AS NUMERIC)) as avg_revenue
    FROM sales2023 
    WHERE service_contact_ IS NOT NULL AND service_contact_ > 0

    UNION ALL

    SELECT 
        '2567' as year,
        COUNT(*) as total_jobs,
        SUM(CAST(service_contact_ AS NUMERIC)) as total_revenue,
        AVG(CAST(service_contact_ AS NUMERIC)) as avg_revenue  
    FROM sales2024
    WHERE service_contact_ IS NOT NULL AND service_contact_ > 0

    ORDER BY year;
    ```

    Generate similar SQL for the question above:"""

        return prompt

    def _validate_sql_completeness(self, sql: str) -> bool:
        """✅ ตรวจสอบความสมบูรณ์ของ SQL"""
        
        sql_upper = sql.upper()
        
        # ต้องมี SELECT และ FROM
        if 'SELECT' not in sql_upper or 'FROM' not in sql_upper:
            return False
        
        # ต้องมี table name หลัง FROM
        from_match = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
        if not from_match:
            return False
        
        # ไม่ควรลงท้ายด้วย FROM; เปล่า ๆ
        if sql.strip().endswith('FROM;'):
            return False
        
        # ต้องมีความยาวที่สมเหตุสมผล
        if len(sql.strip()) < 20:
            return False
        
        return True

    def _validate_sql_security(self, sql: str) -> bool:
        """🔒 ตรวจสอบความปลอดภัยของ SQL"""
        
        sql_upper = sql.upper()
        
        # Allow only SELECT statements
        if not sql_upper.strip().startswith('SELECT'):
            return False
        
        # Block dangerous operations
        dangerous_keywords = [
            'DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 
            'TRUNCATE', 'EXEC', 'EXECUTE', 'SP_', 'XP_'
        ]
        
        if any(keyword in sql_upper for keyword in dangerous_keywords):
            return False
        
        return True
    
    def _is_valid_sql_structure(self, sql: str) -> bool:
        """✅ ตรวจสอบโครงสร้าง SQL"""
        
        sql_upper = sql.upper()
        
        # Must have SELECT and FROM
        if 'SELECT' not in sql_upper:
            return False
        
        # Should be reasonable length
        if len(sql.strip()) < 10:
            return False
        
        return True
    
    # =========================================================================
    # 💬 NATURAL LANGUAGE RESPONSE GENERATION
    # =========================================================================
    
    async def _generate_natural_response(self, question: str, results: List[Dict], 
                                    sql_query: str, tenant_id: str) -> str:
        """💬 แก้ไขการสร้างคำตอบภาษาธรรมชาติให้ใช้ข้อมูลจริงเท่านั้น"""
        
        if not results:
            return "ไม่พบข้อมูลในฐานข้อมูลสำหรับคำถามนี้"
        
        # สร้าง context ที่บังคับให้ใช้ข้อมูลจริงเท่านั้น
        results_data = self._format_results_for_nl_strict(results)
        
        nl_prompt = f"""คุณเป็น AI Assistant สำหรับบริษัท HVAC (ซ่อมแซมเครื่องปรับอากาศ)

    คำถาม: {question}

    ข้อมูลจากฐานข้อมูล (ใช้เฉพาะข้อมูลนี้เท่านั้น):
    {results_data}

    SQL ที่ใช้: {sql_query}

    กรุณาสร้างคำตอบโดย:
    1. ใช้เฉพาะตัวเลขจากข้อมูลที่ให้มา - ห้ามสร้างตัวเลขใหม่
    2. ตอบเป็นภาษาไทยที่เข้าใจง่าย
    3. แสดงตัวเลขที่สำคัญให้ชัดเจน
    4. ไม่ต้องเดาหรือสร้างรายละเอียดที่ไม่มีในข้อมูล
    5. หากเป็นการเปรียบเทียบ ให้คำนวณเปอร์เซ็นต์ที่ถูกต้อง

    ตอบ:"""

        try:
            response = await self._call_ollama_model(self.NL_MODEL, nl_prompt)
            cleaned_response = self._clean_and_validate_response(response, results)
            return cleaned_response
            
        except Exception as e:
            logger.error(f"NL generation failed: {e}")
            # Fallback: ใช้การ format แบบ hardcode
            return self._create_hardcoded_response(question, results)

    def _format_results_for_nl_strict(self, results: List[Dict]) -> str:
        """📊 Format ข้อมูลให้ NL model อย่างเข้มงวด"""
        
        if not results:
            return "ไม่มีข้อมูล"
        
        formatted = "ข้อมูลที่พบ:\n"
        
        for i, row in enumerate(results, 1):
            formatted += f"รายการ {i}: "
            
            # แสดงข้อมูลทุกคอลัมน์ที่มีค่า
            data_parts = []
            for key, value in row.items():
                if value is not None and str(value).strip():
                    # จัดรูปแบบตัวเลขที่เป็นเงิน
                    if key in ['revenue', 'service_contact_', 'total', 'unit_price'] and isinstance(value, (int, float)):
                        formatted_value = f"{value:,.0f} บาท" if value >= 1000 else f"{value} บาท"
                        data_parts.append(f"{key}: {formatted_value}")
                    else:
                        data_parts.append(f"{key}: {value}")
            
            formatted += " | ".join(data_parts[:5])  # จำกัดไม่เกิน 5 fields
            formatted += "\n"
        
        # เพิ่มสรุปสำคัญหากเป็นข้อมูลตัวเลข
        if results and any(isinstance(list(row.values())[0], (int, float)) for row in results):
            formatted += "\n** ใช้เฉพาะตัวเลขข้างต้นในการตอบ **\n"
        
        return formatted

    def _clean_and_validate_response(self, response: str, original_results: List[Dict]) -> str:
        """🧹 ทำความสะอาดและตรวจสอบคำตอบจาก NL model"""
        
        # ลบส่วนที่ไม่จำเป็น
        response = response.strip()
        response = re.sub(r'^(ตอบ:|คำตอบ:|ผลลัพธ์:)', '', response)
        
        # ตรวจสอบว่ามีการใช้ตัวเลขจริงหรือไม่
        original_numbers = self._extract_numbers_from_results(original_results)
        response_numbers = self._extract_numbers_from_text(response)
        
        # หากพบตัวเลขที่ไม่ตรงกับข้อมูลต้นฉบับ ให้เตือน
        suspicious_numbers = [n for n in response_numbers if not self._is_number_reasonable(n, original_numbers)]
        
        if suspicious_numbers and len(suspicious_numbers) > len(original_numbers):
            logger.warning(f"Suspicious numbers in response: {suspicious_numbers}")
            # ใช้ fallback response แทน
            return self._create_hardcoded_response("", original_results)
        
        return response

    def _extract_numbers_from_results(self, results: List[Dict]) -> List[float]:
        """🔢 ดึงตัวเลขจากผลลัพธ์จริง"""
        
        numbers = []
        for row in results:
            for key, value in row.items():
                if isinstance(value, (int, float)) and value > 0:
                    numbers.append(float(value))
        return sorted(numbers, reverse=True)  # เรียงจากมากไปน้อย

    def _extract_numbers_from_text(self, text: str) -> List[float]:
        """🔢 ดึงตัวเลขจากข้อความ"""
        
        # หาตัวเลขรูปแบบต่างๆ (รวมถึงมีคอมม่า)
        number_patterns = [
            r'(\d{1,3}(?:,\d{3})+)',      # ตัวเลขที่มีคอมม่า เช่น 17,604,462
            r'(\d+\.\d+)',                # ทศนิยม เช่น 25.71
            r'(\d+)'                      # ตัวเลขธรรมดา
        ]
        
        numbers = []
        for pattern in number_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                try:
                    # ลบคอมม่าแล้วแปลงเป็นตัวเลข
                    clean_number = match.replace(',', '')
                    numbers.append(float(clean_number))
                except:
                    continue
        
        return numbers

    def _is_number_reasonable(self, response_number: float, original_numbers: List[float]) -> bool:
        """✅ ตรวจสอบว่าตัวเลขในคำตอบสมเหตุสมผลหรือไม่"""
        
        if not original_numbers:
            return True
        
        # ตัวเลขควรอยู่ในช่วงที่สมเหตุสมผลเมื่อเทียบกับข้อมูลต้นฉบับ
        max_original = max(original_numbers)
        min_original = min(original_numbers)
        
        # อนุญาตให้มีการคำนวณ (เช่น เปอร์เซ็นต์, ค่าเฉลี่ย)
        if response_number <= 100 and len(str(int(response_number))) <= 3:
            return True  # น่าจะเป็นเปอร์เซ็นต์
        
        # ตัวเลขใหญ่ต้องใกล้เคียงกับข้อมูลต้นฉบับ
        if response_number > 1000:
            return min_original * 0.1 <= response_number <= max_original * 2
        
        return True

    def _create_hardcoded_response(self, question: str, results: List[Dict]) -> str:
        """🔧 สร้างคำตอบแบบ hardcode เมื่อ NL model ไม่เชื่อถือได้"""
        
        if not results:
            return "ไม่พบข้อมูลสำหรับคำถามนี้"
        
        # ตรวจสอบรูปแบบข้อมูล
        if len(results) == 2 and all('period' in row for row in results):
            # เป็นการเปรียบเทียบระหว่าง 2 ช่วงเวลา
            return self._create_comparison_response(results)
        elif len(results) == 1:
            # เป็นข้อมูลเดี่ยว
            return self._create_single_result_response(results[0])
        else:
            # ข้อมูลหลายรายการ
            return self._create_multiple_results_response(results)

    def _create_comparison_response(self, results: List[Dict]) -> str:
        """📊 สร้างคำตอบสำหรับการเปรียบเทียบ"""
        
        # แยกข้อมูล old และ new
        old_data = next((r for r in results if r.get('period') == 'old'), None)
        new_data = next((r for r in results if r.get('period') == 'new'), None)
        
        if not old_data or not new_data:
            return "ข้อมูลการเปรียบเทียบไม่สมบูรณ์"
        
        old_jobs = old_data.get('jobs', 0)
        old_revenue = old_data.get('revenue', 0)
        new_jobs = new_data.get('jobs', 0)
        new_revenue = new_data.get('revenue', 0)
        
        # คำนวณการเปลี่ยนแปลง
        job_change = ((new_jobs - old_jobs) / old_jobs * 100) if old_jobs > 0 else 0
        revenue_change = ((new_revenue - old_revenue) / old_revenue * 100) if old_revenue > 0 else 0
        
        # คำนวณรายได้เฉลี่ย
        old_avg = (old_revenue / old_jobs) if old_jobs > 0 else 0
        new_avg = (new_revenue / new_jobs) if new_jobs > 0 else 0
        
        response = f"""การวิเคราะห์ยอดขายระหว่าง 2 ช่วงเวลา:

    ช่วงเก่า (old):
    - จำนวนงาน: {old_jobs:,} งาน
    - รายได้รวม: {old_revenue:,.0f} บาท
    - รายได้เฉลี่ย: {old_avg:,.0f} บาท/งาน

    ช่วงใหม่ (new):  
    - จำนวนงาน: {new_jobs:,} งาน
    - รายได้รวม: {new_revenue:,.0f} บาท
    - รายได้เฉลี่ย: {new_avg:,.0f} บาท/งาน

    การเปลี่ยนแปลง:
    - จำนวนงาน: {job_change:+.1f}%
    - รายได้รวม: {revenue_change:+.1f}%"""

        # เพิ่มการวิเคราะห์
        if revenue_change < 0 and job_change > 0:
            response += "\n\nสังเกต: จำนวนงานเพิ่มขึ้นแต่รายได้ลดลง อาจเป็นเพราะราคาเฉลี่ยต่องานลดลง"
        elif revenue_change > job_change:
            response += "\n\nสังเกต: รายได้เติบโตมากกว่าจำนวนงาน แสดงว่าราคาเฉลี่ยต่อหน่วยเพิ่มขึ้น"
        
        return response

    def _create_single_result_response(self, result: Dict) -> str:
        """📋 สร้างคำตอบสำหรับข้อมูลเดี่ยว"""
        
        response_parts = ["ผลการค้นหา:"]
        
        for key, value in result.items():
            if value is not None and str(value).strip():
                if key in ['revenue', 'service_contact_', 'total'] and isinstance(value, (int, float)):
                    response_parts.append(f"- {key}: {value:,.0f} บาท")
                elif key == 'jobs' and isinstance(value, (int, float)):
                    response_parts.append(f"- จำนวนงาน: {value:,} งาน")
                else:
                    response_parts.append(f"- {key}: {value}")
        
        return "\n".join(response_parts)

    def _create_multiple_results_response(self, results: List[Dict]) -> str:
        """📋 สร้างคำตอบสำหรับข้อมูลหลายรายการ"""
        
        response = f"พบข้อมูล {len(results)} รายการ:\n\n"
        
        for i, row in enumerate(results[:10], 1):  # แสดงสูงสุด 10 รายการ
            response += f"{i}. "
            important_fields = []
            
            for key, value in row.items():
                if value is not None and str(value).strip():
                    if key in ['revenue', 'service_contact_', 'total', 'unit_price'] and isinstance(value, (int, float)):
                        important_fields.append(f"{key}: {value:,.0f} บาท")
                    else:
                        important_fields.append(f"{key}: {value}")
            
            response += " | ".join(important_fields[:4])  # แสดง 4 fields แรก
            response += "\n"
        
        if len(results) > 10:
            response += f"\n... และอีก {len(results) - 10} รายการ"
        
        return response

    
    def _prepare_results_for_nl_model(self, results: List[Dict]) -> str:
        """📊 เตรียมผลลัพธ์สำหรับ NL Model"""
        
        if not results:
            return "ไม่พบข้อมูลที่ตรงกับคำถาม"
        
        # Limit data to prevent token overflow
        limited_results = results[:10]  
        
        summary = f"จำนวนรายการทั้งหมด: {len(results)}\n\n"
        summary += "ข้อมูลตัวอย่าง:\n"
        
        for i, row in enumerate(limited_results, 1):
            summary += f"{i}. "
            # Show important fields only
            important_fields = []
            for key, value in row.items():
                if value is not None and str(value).strip():
                    # Prioritize important business fields
                    if key in ['customer_name', 'product_name', 'service_contact_', 'unit_price', 'total', 'job_no', 'description']:
                        important_fields.append(f"{key}: {value}")
            
            summary += " | ".join(important_fields[:4])  # Max 4 fields per row
            summary += "\n"
        
        if len(results) > 10:
            summary += f"\n... และอีก {len(results) - 10} รายการ"
        
        return summary
    
    def _clean_nl_response(self, response: str) -> str:
        """🧹 ทำความสะอาดคำตอบจาก NL Model"""
        
        # Remove common AI prefixes
        response = re.sub(r'^(ตอบ:|คำตอบ:|ผลลัพธ์:)', '', response.strip())
        
        # Remove code blocks if any
        response = re.sub(r'```.*?```', '', response, flags=re.DOTALL)
        
        # Clean up formatting
        response = re.sub(r'\n\s*\n\s*\n', '\n\n', response)  # Multiple newlines to double
        
        return response.strip()
    
    # =========================================================================
    # 🔧 OLLAMA API INTEGRATION  
    # =========================================================================
    
    async def _call_ollama_model(self, model_name: str, prompt: str) -> str:
        """🔧 เรียกใช้ Ollama Model"""
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.request_timeout)) as session:
                payload = {
                    "model": model_name,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1 if "sqlcoder" in model_name.lower() else 0.7,
                        "top_p": 0.9,
                        "top_k": 40
                    }
                }
                
                async with session.post(f"{self.ollama_base_url}/api/generate", json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get("response", "")
                    else:
                        error_text = await response.text()
                        raise Exception(f"Ollama API error: {response.status} - {error_text}")
                        
        except asyncio.TimeoutError:
            raise Exception(f"Timeout calling {model_name}")
        except Exception as e:
            raise Exception(f"Failed to call {model_name}: {e}")
    
    # =========================================================================
    # 🗃️ DATABASE OPERATIONS
    # =========================================================================

    def _create_enhanced_sqlcoder_prompt(self, question: str, schema: Dict[str, Any], tenant_id: str) -> str:
        """🎯 สร้าง prompt ที่ดีกว่าสำหรับ SQL Coder พร้อมแก้ปัญหาปี 2566/2567"""
        
        # Business Year Mapping (สำคัญมาก!)
        year_mapping = """
    YEAR MAPPING FOR THAI BUSINESS:
    - ปี 2566 = year 2023 = use sales2023 table
    - ปี 2567 = year 2024 = use sales2024 table  
    - ปี 2568 = year 2025 = use sales2025 table
    """
        
        # สร้าง schema ที่เรียบง่าย
        schema_text = "DATABASE TABLES:\n"
        for table_name, table_info in schema.items():
            schema_text += f"\nTable: {table_name}\n"
            # แสดงเฉพาะคอลัมน์สำคัญ
            important_columns = []
            for col in table_info["columns"]:
                if col["name"] in ["id", "customer_name", "service_contact_", "job_no", 
                                "product_name", "unit_price", "total", "date", "description"]:
                    important_columns.append(f"  {col['name']} ({col['type']})")
            
            if important_columns:
                schema_text += "\n".join(important_columns)
            schema_text += "\n"
        
        # เพิ่ม business rules ที่ชัดเจน
        business_rules = """
    BUSINESS RULES:
    - service_contact_ column = revenue amount (use for sales analysis)
    - For sales comparison: use UNION to combine multiple years
    - For spare parts: search in spare_part and spare_part2 tables
    - Always include WHERE service_contact_ > 0 for revenue calculations
    - Use CAST(service_contact_ AS NUMERIC) for calculations
    """
        
        prompt = f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>

    You are an expert PostgreSQL query generator for Thai HVAC business.

    {year_mapping}

    {schema_text}

    {business_rules}

    IMPORTANT:
    - Generate complete SELECT statements with proper table names
    - For year comparisons, use UNION to combine data from different tables
    - Include proper WHERE clauses
    - Use aggregation functions (SUM, COUNT, AVG) for analysis
    - Always end with semicolon

    <|eot_id|><|start_header_id|>user<|end_header_id|>

    Generate SQL for: {question}

    Requirements:
    - Must be complete valid SQL
    - Include table names in FROM clause
    - Use proper Thai year mapping above

    <|eot_id|><|start_header_id|>assistant<|end_header_id|>

    """
        
        return prompt

    async def _execute_sql_safely(self, sql_query: str, tenant_id: str) -> List[Dict]:
        """🗃️ แก้ไขการรัน SQL - รองรับทั้ง async และ sync"""
        
        try:
            # Import datetime และ Decimal ที่จำเป็น
            from datetime import datetime, date
            from decimal import Decimal
            
            # ใช้ sync connection เพื่อหลีกเลี่ยงปัญหา async/sync
            conn = self._create_sync_connection(tenant_id)
            cursor = conn.cursor()
            
            logger.info(f"📝 Executing SQL: {sql_query}")
            
            # Execute query with error handling
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            
            # Convert to dict format
            results = []
            if cursor.description:
                column_names = [desc[0] for desc in cursor.description]
                
                for row in rows:
                    row_dict = {}
                    for i, value in enumerate(row):
                        col_name = column_names[i]
                        # Handle special data types
                        if isinstance(value, (datetime, date)):
                            row_dict[col_name] = value.isoformat()
                        elif isinstance(value, Decimal):  # Handle Decimal type
                            row_dict[col_name] = float(value)
                        elif hasattr(value, '__float__'):  # Handle other numeric types
                            try:
                                row_dict[col_name] = float(value)
                            except:
                                row_dict[col_name] = str(value)
                        else:
                            row_dict[col_name] = value
                    results.append(row_dict)
            
            cursor.close() 
            conn.close()
            
            logger.info(f"✅ SQL executed successfully: {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"❌ SQL execution failed: {e}")
            logger.error(f"❌ Failed SQL: {sql_query}")
            return []
        
    async def _get_database_connection(self, tenant_id: str):
        """🔗 แก้ไขการเชื่อมต่อฐานข้อมูล - ลบ await ออก"""
        
        try:
            # แก้ปัญหา: psycopg2 connection ไม่ใช่ async
            if hasattr(self.db_handler, 'get_database_connection'):
                # ถ้าเป็น sync function ให้เรียกตรง ๆ (ไม่ใช่ await)
                return self.db_handler.get_database_connection(tenant_id)
            else:
                # Fallback implementation - ใช้ sync psycopg2
                from .tenant_config import TenantConfigManager
                import psycopg2
                
                config_manager = TenantConfigManager()
                tenant_config = config_manager.tenant_configs[tenant_id]
                
                return psycopg2.connect(
                    host=tenant_config.db_host,
                    port=tenant_config.db_port,
                    database=tenant_config.db_name,
                    user=tenant_config.db_user,
                    password=tenant_config.db_password
                )
                
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    
    # =========================================================================
    # 🔄 FALLBACK AND ERROR HANDLING
    # =========================================================================
    
    async def _create_fallback_response(self, question: str, tenant_id: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """🔄 สร้างคำตอบ fallback เมื่อไม่สามารถสร้าง SQL ได้"""
        
        # Analyze available data
        table_summary = self._create_schema_summary(schema)
        
        fallback_answer = f"""🤔 ไม่สามารถประมวลผลคำถาม "{question}" ได้โดยตรง

{table_summary}

💡 คำแนะนำ:
• ลองถามคำถามที่เฉพาะเจาะจงมากขึ้น
• ระบุชื่อลูกค้า แบรนด์ หรือประเภทงานที่ต้องการ
• ใช้คำศัพท์ที่เกี่ยวข้องกับธุรกิจ HVAC

🎯 ตัวอย่างคำถามที่ทำได้:
• "จำนวนลูกค้าทั้งหมด"
• "ราคาอะไหล่ MOTOR"
• "งานบำรุงรักษาเดือนนี้"

ลองถามใหม่ได้ครับ!"""

        return {
            "answer": fallback_answer,
            "success": False,
            "sql_query": None,
            "results_count": 0,
            "system_used": "fallback_response",
            "processing_time": 0.5
        }
    
    def _create_schema_summary(self, schema: Dict[str, Any]) -> str:
        """📋 สร้างสรุปข้อมูลในระบบ"""
        
        summary = "📊 ข้อมูลที่มีในระบบ:\n"
        
        for table_name, table_info in schema.items():
            row_count = table_info.get("row_count", 0)
            col_count = len(table_info.get("columns", []))
            
            # Table description based on name
            if "sales" in table_name:
                description = "ข้อมูลการขายและบริการ"
            elif "spare" in table_name:
                description = "ข้อมูลอะไหล่และสต็อก" 
            elif "work" in table_name:
                description = "ข้อมูลการทำงานและทีม"
            else:
                description = "ข้อมูลทั่วไป"
            
            summary += f"• {table_name}: {description} ({col_count} คอลัมน์)\n"
        
        return summary
    
    def _create_simple_formatted_response(self, question: str, results: List[Dict]) -> str:
        """📋 สร้างคำตอบแบบง่าย เมื่อ NL Model ล้มเหลว"""
        
        if not results:
            return f"ไม่พบข้อมูลที่เกี่ยวข้องกับคำถาม: {question}"
        
        response = f"📊 ผลการค้นหา: {question}\n\n"
        response += f"พบข้อมูล {len(results)} รายการ:\n\n"
        
        for i, row in enumerate(results[:5], 1):  # Show first 5 results
            response += f"{i}. "
            # Show important fields
            important_data = []
            for key, value in row.items():
                if value is not None and str(value).strip():
                    important_data.append(f"{key}: {value}")
            
            response += " | ".join(important_data[:3])  # Show first 3 fields
            response += "\n"
        
        if len(results) > 5:
            response += f"\n... และอีก {len(results) - 5} รายการ"
        
        return response
    
    def _create_error_response(self, error_message: str, tenant_id: str) -> Dict[str, Any]:
        """❌ สร้างคำตอบเมื่อเกิดข้อผิดพลาด"""
        
        return {
            "answer": f"เกิดข้อผิดพลาดในระบบ: {error_message}",
            "success": False,
            "sql_query": None,
            "results_count": 0,
            "system_used": "error_handler",
            "processing_time": 0.1
        }

# =============================================================================
# 🔧 ENHANCED AGENT WITH DUAL-MODEL INTEGRATION
# =============================================================================

class EnhancedUnifiedPostgresOllamaAgent:
    """🚀 Enhanced Agent ที่ใช้ Dual-Model Strategy"""
    
    def __init__(self):
        try:
            # Import original agent
            from .enhanced_postgres_agent_unified import UnifiedEnhancedPostgresOllamaAgent as OriginalAgent
            
            # Initialize original agent
            original_agent = OriginalAgent()
            
            # Copy all attributes and methods
            for attr_name in dir(original_agent):
                if not attr_name.startswith('__'):
                    setattr(self, attr_name, getattr(original_agent, attr_name))
            
            # Add Dual-Model AI System
            self.dual_model_ai = DualModelDynamicAISystem(self, self)
            self.STRICT_MODE = True  # บังคับให้ใช้เฉพาะข้อมูลจริง
            self.VALIDATION_ENABLED = True 

            logger.info("🚀 Enhanced Agent with Dual-Model AI initialized")
            logger.info(f"   📝 SQL Model: {self.dual_model_ai.SQL_MODEL}")
            logger.info(f"   💬 NL Model: {self.dual_model_ai.NL_MODEL}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Enhanced Agent with Dual-Model: {e}")
            raise
    
    async def process_any_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """🎯 ประมวลผลคำถามด้วย Dual-Model Strategy"""
        return await self.dual_model_ai.process_any_question(question, tenant_id)
    
    async def process_enhanced_question_with_dual_model(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """🔄 ประมวลผลคำถามด้วย Dual-Model พร้อม fallback"""
        
        try:
            # Try Dual-Model approach first
            result = await self.dual_model_ai.process_any_question(question, tenant_id)
            
            if result.get("success") and result.get("results_count", 0) > 0:
                return result
            
            # Fallback to original method
            logger.info("🔄 Dual-Model failed, using original method")
            return await self.process_enhanced_question(question, tenant_id)
            
        except Exception as e:
            logger.error(f"❌ All methods failed: {e}")
            return self._create_error_response(str(e), tenant_id)