# refactored_modules/advanced_dynamic_ai_system.py
# 🔧 แก้ไข Dynamic AI System ให้ใช้ Schema จริง

import re
import json
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class AdvancedDynamicAISystem:
    """🚀 ระบบ AI แบบ Dynamic ที่ตอบได้ทุกคำถาม - แก้ไขแล้ว"""
    
    def __init__(self, database_handler, ollama_client):
        self.db_handler = database_handler
        self.ollama_client = ollama_client
        
        # Cache สำหรับเก็บข้อมูล schema และตัวอย่าง
        self.schema_cache = {}
        self.sample_data_cache = {}
        self.relationship_cache = {}
        self.actual_columns_cache = {}  # 🆕 Cache คอลัมน์จริง
        
        # Advanced Question Pattern Recognition
        self.question_patterns = {
            'counting': {
                'patterns': [
                    r'(?:มี|จำนวน|กี่|count|how many).*?(?:ลูกค้า|customer|งาน|job|อะไหล่|part)',
                    r'(?:ทั้งหมด|total|รวม|all).*?(?:กี่|how many|count)',
                ],
                'response_format': 'summary_with_count'
            },
            'spare_parts_search': {
                'patterns': [
                    r'(?:ราคา|price|อะไหล่|spare.*part).*?(?:hitachi|daikin|euroklimat)',
                    r'(?:หา|find|ค้นหา).*?(?:อะไหล่|spare|part)',
                ],
                'preferred_tables': ['spare_part', 'spare_part2'],
                'response_format': 'spare_parts_list'
            },
            'customer_analysis': {
                'patterns': [
                    r'(?:ลูกค้า|customer|บริษัท|company).*?(?:มากที่สุด|most|ทั้งหมด|all)',
                    r'(?:วิเคราะห์|analysis).*?(?:ลูกค้า|customer)',
                ],
                'preferred_tables': ['sales2024', 'sales2023', 'sales2022'],
                'response_format': 'customer_analysis'
            },
            'sales_analysis': {
                'patterns': [
                    r'(?:วิเคราะห์|analysis|สรุป|summary).*?(?:ขาย|sales|ปี|year)',
                    r'(?:ยอดขาย|sales).*?(?:ปี|year|\d{4})',
                ],
                'preferred_tables': ['sales2024', 'sales2023', 'sales2022'],
                'response_format': 'sales_summary'
            }
        }
        
        logger.info("🚀 Fixed Advanced Dynamic AI System initialized")
    
    async def _generate_natural_ai_response(self, question: str, results: List[Dict], 
                                        analysis: Dict[str, Any], tenant_id: str) -> str:
        """🤖 ใช้ AI สร้างคำตอบที่เป็นธรรมชาติ"""
        
        if not results:
            return self._create_natural_no_results_response(question, analysis)
        
        # สร้าง prompt สำหรับ AI ให้ตอบแบบธรรมชาติ
        context_prompt = self._build_natural_response_prompt(question, results, analysis)
        
        try:
            # เรียกใช้ AI
            ai_response = await self.ollama_client._call_ollama_api(context_prompt, tenant_id)
            
            # ตรวจสอบและปรับปรุงคำตอบ
            cleaned_response = self._clean_and_enhance_response(ai_response, question, len(results))
            
            return cleaned_response
            
        except Exception as e:
            logger.error(f"❌ Natural AI response generation failed: {e}")
            # Fallback ไปใช้การจัดรูปแบบแบบเดิม
            return self._format_results_intelligently(question, results, analysis)

    def _build_natural_response_prompt(self, question: str, results: List[Dict], 
                                    analysis: Dict[str, Any]) -> str:
        """📝 สร้าง prompt สำหรับคำตอบที่เป็นธรรมชาติ"""
        
        question_type = analysis.get('question_type', 'unknown')
        
        # ข้อมูลตัวอย่างสำหรับ AI (จำกัด 3 รายการแรก)
        sample_data = json.dumps(results[:3], ensure_ascii=False, indent=2, default=str)
        
        prompt = f"""คุณคือผู้ช่วย AI ที่เชี่ยวชาญด้านธุรกิจ HVAC (เครื่องปรับอากาศและระบบทำความเย็น)

    🎯 งานของคุณคือตอบคำถามให้ฟังดูเป็นธรรมชาติ เหมือนพนักงานจริงๆ ที่กำลังช่วยลูกค้า

    คำถาม: {question}
    ประเภทคำถาม: {question_type}
    จำนวนข้อมูลที่พบ: {len(results)} รายการ

    ข้อมูลที่พบ (ตัวอย่าง):
    {sample_data}

    📋 คำแนะนำในการตอบ:
    1. ใช้ภาษาไทยธรรมชาติ ไม่เป็นทางการจนเกินไป
    2. ใช้คำว่า "ครับ" "ค่ะ" ให้เหมาะสม
    3. ไม่ต้องใส่ emoji มากเกินไป
    4. หากเป็นข้อมูลเงิน ให้แสดงหน่วย "บาท" และใส่ comma
    5. หากเป็นข้อมูลจำนวน ให้แสดงเป็นตัวเลขที่อ่านง่าย
    6. ตอบให้กระชับ ไม่ยืดยาว
    7. หากมีข้อมูลเยอะ แสดงแค่ส่วนสำคัญ แล้วบอกว่า "มีอีก"

    เริ่มตอบเลยครับ (ไม่ต้องบอกว่า "ตามข้อมูลที่ได้รับ" หรือ "จากการค้นหา"):"""

        return prompt

    def _clean_and_enhance_response(self, ai_response: str, question: str, result_count: int) -> str:
        """✨ ปรับปรุงคำตอบให้ดียิ่งขึ้น"""
        
        # ลบส่วนที่ไม่ต้องการ
        unwanted_phrases = [
            "ตามข้อมูลที่ได้รับ",
            "จากการค้นหา",
            "จากฐานข้อมูล",
            "ตามที่คุณถาม",
            "สรุปแล้ว"
        ]
        
        cleaned_response = ai_response.strip()
        
        for phrase in unwanted_phrases:
            cleaned_response = cleaned_response.replace(phrase, "").strip()
        
        # ลบ space หรือ newline ที่เกิน
        lines = [line.strip() for line in cleaned_response.split('\n') if line.strip()]
        cleaned_response = '\n'.join(lines)
        
        # ตรวจสอบความยาว หากยาวเกินไป ให้ตัดให้เหลือส่วนสำคัญ
        if len(cleaned_response) > 800:
            # หา paragraph แรก หรือ 2-3 บรรทัดแรก
            sentences = cleaned_response.split('.')
            important_sentences = sentences[:3]  # เอา 3 ประโยคแรก
            cleaned_response = '.'.join(important_sentences)
            
            if result_count > 5:
                cleaned_response += f"\n\nมีข้อมูลอีก {result_count - 3} รายการ หากต้องการดูเพิ่มเติมแจ้งได้ครับ"
        
        # เพิ่มข้อมูลเสริมหากจำเป็น
        if not cleaned_response.endswith(('ครับ', 'ค่ะ', 'คะ')):
            cleaned_response += " ครับ"
        
        return cleaned_response

    # อัปเดต method หลัก
    async def process_any_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """🎯 ประมวลผลคำถามใดๆ ด้วย Natural AI Response"""
        
        try:
            logger.info(f"🚀 Processing dynamic question: {question}")
            
            # Step 1: ค้นพบ schema จริงจากฐานข้อมูล
            actual_schema = await self._get_actual_database_schema(tenant_id)
            
            # Step 2: วิเคราะห์คำถาม
            question_analysis = self._analyze_question_with_schema(question, actual_schema)
            
            # Step 3: สร้าง SQL ที่ถูกต้อง
            sql_query = await self._generate_accurate_sql(question, question_analysis, actual_schema, tenant_id)
            
            # Step 4: Execute และ process results
            if sql_query:
                results = await self._execute_sql_safely(sql_query, tenant_id)
                
                # 🆕 ใช้ AI สร้างคำตอบที่เป็นธรรมชาติ
                answer = await self._generate_natural_ai_response(question, results, question_analysis, tenant_id)
                
                return {
                    "answer": answer,
                    "success": True,
                    "sql_query": sql_query,
                    "results_count": len(results),
                    "question_analysis": question_analysis,
                    "data_source_used": "natural_ai_response",
                    "system_used": "advanced_dynamic_ai_natural"
                }
            else:
                return await self._create_helpful_response(question, actual_schema, tenant_id)
                
        except Exception as e:
            logger.error(f"❌ Natural dynamic processing failed: {e}")
            return {
                "answer": f"ขออภัยครับ ไม่สามารถประมวลผลคำถามนี้ได้ในขณะนี้: {str(e)}",
                "success": False,
                "error": str(e),
                "data_source_used": "natural_ai_error"
            }
    
    async def _get_actual_database_schema(self, tenant_id: str) -> Dict[str, Any]:
        """🔍 ดึง schema จริงจากฐานข้อมูล - แม่นยำ 100%"""
        
        cache_key = f"{tenant_id}_actual_schema"
        if cache_key in self.schema_cache:
            cache_age = (datetime.now() - self.schema_cache[cache_key]['timestamp']).seconds
            if cache_age < 1800:  # 30 นาที
                return self.schema_cache[cache_key]['data']
        
        try:
            conn = self.db_handler.get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            actual_schema = {}
            
            # ดึงรายชื่อตารางทั้งหมด
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            table_names = [row[0] for row in cursor.fetchall()]
            
            # วิเคราะห์แต่ละตาราง
            for table_name in table_names:
                actual_schema[table_name] = await self._get_table_actual_structure(cursor, table_name)
            
            cursor.close()
            conn.close()
            
            # Cache ผลลัพธ์
            self.schema_cache[cache_key] = {
                'data': actual_schema,
                'timestamp': datetime.now()
            }
            
            logger.info(f"✅ Actual schema loaded: {len(actual_schema)} tables")
            for table_name, info in actual_schema.items():
                column_names = [col['name'] for col in info['columns']]
                logger.info(f"  📋 {table_name}: {', '.join(column_names[:5])}...")
            
            return actual_schema
            
        except Exception as e:
            logger.error(f"❌ Actual schema discovery failed: {e}")
            return {}
    
    async def _get_table_actual_structure(self, cursor, table_name: str) -> Dict[str, Any]:
        """📋 ดึงโครงสร้างตารางจริง"""
        
        try:
            # ดึงข้อมูลคอลัมน์จริง
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES',
                    'default': row[3]
                })
            
            # ดึงข้อมูลตัวอย่าง
            try:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                sample_rows = cursor.fetchall()
                if sample_rows and cursor.description:
                    sample_columns = [desc[0] for desc in cursor.description]
                    samples = []
                    for row in sample_rows:
                        sample_dict = dict(zip(sample_columns, row))
                        # แปลงเป็น string เพื่อความปลอดภัย
                        for key, value in sample_dict.items():
                            if value is not None:
                                sample_dict[key] = str(value)
                        samples.append(sample_dict)
                else:
                    samples = []
            except Exception as e:
                logger.warning(f"Could not get samples from {table_name}: {e}")
                samples = []
            
            # นับจำนวนแถว
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
            except:
                row_count = 0
            
            return {
                'columns': columns,
                'samples': samples,
                'row_count': row_count,
                'purpose': self._infer_table_purpose_from_actual_data(table_name, columns, samples)
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to analyze table {table_name}: {e}")
            return {'columns': [], 'samples': [], 'row_count': 0, 'purpose': 'unknown'}
    
    def _infer_table_purpose_from_actual_data(self, table_name: str, columns: List[Dict], samples: List[Dict]) -> str:
        """🎯 ประเมินวัตถุประสงค์ของตารางจากข้อมูลจริง"""
        
        column_names = [col['name'].lower() for col in columns]
        
        # ตรวจสอบตามชื่อตาราง
        if 'sales' in table_name.lower():
            return 'sales_transaction_data'
        elif 'spare' in table_name.lower() or 'part' in table_name.lower():
            return 'spare_parts_inventory'
        elif 'work' in table_name.lower() or 'force' in table_name.lower():
            return 'workforce_scheduling'
        
        # ตรวจสอบตามคอลัมน์จริง
        if 'customer_name' in column_names:
            return 'customer_transaction_data'
        elif 'product_name' in column_names and 'unit_price' in column_names:
            return 'product_inventory_data'
        elif 'service_group' in column_names and 'date' in column_names:
            return 'work_scheduling_data'
        
        return 'general_business_data'
    
    def _analyze_question_with_schema(self, question: str, actual_schema: Dict[str, Any]) -> Dict[str, Any]:
        """🧠 วิเคราะห์คำถามโดยใช้ schema จริง"""
        
        question_lower = question.lower()
        
        analysis = {
            'original_question': question,
            'question_type': 'unknown',
            'target_tables': [],
            'available_columns': {},
            'search_keywords': [],
            'filters': [],
            'confidence': 0.0
        }
        
        # หาประเภทคำถาม
        max_confidence = 0
        best_type = 'unknown'
        
        for q_type, pattern_info in self.question_patterns.items():
            confidence = 0
            for pattern in pattern_info['patterns']:
                if re.search(pattern, question_lower):
                    confidence += 1
            
            if confidence > max_confidence:
                max_confidence = confidence
                best_type = q_type
        
        analysis['question_type'] = best_type
        analysis['confidence'] = max_confidence
        
        # เลือกตารางที่เหมาะสม
        if best_type in self.question_patterns and 'preferred_tables' in self.question_patterns[best_type]:
            preferred_tables = self.question_patterns[best_type]['preferred_tables']
            analysis['target_tables'] = [t for t in preferred_tables if t in actual_schema]
        
        # หากไม่พบตารางที่เฉพาะเจาะจง ให้เลือกตามคำถาม
        if not analysis['target_tables']:
            analysis['target_tables'] = self._select_tables_by_content(question, actual_schema)
        
        # ดึงคอลัมน์ที่มีอยู่จริงในตารางที่เลือก
        for table_name in analysis['target_tables']:
            if table_name in actual_schema:
                analysis['available_columns'][table_name] = [
                    col['name'] for col in actual_schema[table_name]['columns']
                ]
        
        # หาคำสำคัญสำหรับการค้นหา
        analysis['search_keywords'] = self._extract_search_keywords(question)
        
        # สร้าง filters
        analysis['filters'] = self._create_filters_from_question(question, analysis['available_columns'])
        
        return analysis
    
    def _select_tables_by_content(self, question: str, actual_schema: Dict[str, Any]) -> List[str]:
        """📋 เลือกตารางตามเนื้อหาคำถาม"""
        
        question_lower = question.lower()
        selected_tables = []
        
        # คำสำคัญสำหรับแต่ละประเภทตาราง
        table_keywords = {
            'spare_parts': ['อะไหล่', 'spare', 'part', 'ราคา', 'price', 'สต็อก'],
            'sales': ['ลูกค้า', 'customer', 'บริการ', 'service', 'ขาย', 'sales', 'บริษัท'],
            'workforce': ['ทีม', 'team', 'ช่าง', 'แผนงาน', 'schedule', 'งาน', 'work']
        }
        
        for category, keywords in table_keywords.items():
            if any(keyword in question_lower for keyword in keywords):
                # หาตารางที่เข้ากับ category
                for table_name in actual_schema.keys():
                    table_lower = table_name.lower()
                    if category == 'spare_parts' and ('spare' in table_lower or 'part' in table_lower):
                        selected_tables.append(table_name)
                    elif category == 'sales' and 'sales' in table_lower:
                        selected_tables.append(table_name)
                    elif category == 'workforce' and ('work' in table_lower or 'force' in table_lower):
                        selected_tables.append(table_name)
        
        # หากไม่พบ ให้เลือกตารางหลัก
        if not selected_tables:
            main_tables = ['sales2024', 'spare_part', 'work_force']
            selected_tables = [t for t in main_tables if t in actual_schema]
        
        return selected_tables[:3]  # จำกัด 3 ตารางแรก
    
    def _extract_search_keywords(self, question: str) -> List[str]:
        """🔍 สกัดคำสำคัญสำหรับการค้นหา"""
        
        keywords = []
        
        # หาชื่อแบรนด์
        brands = ['hitachi', 'daikin', 'euroklimat', 'toyota', 'mitsubishi', 'york', 'carrier']
        for brand in brands:
            if brand in question.lower():
                keywords.append(brand)
        
        # หาชื่อบริษัท
        company_pattern = r'บริษัท\s*([^ม\s]+)'
        company_match = re.search(company_pattern, question)
        if company_match:
            company_name = company_match.group(1).strip()
            keywords.append(company_name)
        
        # หาคำเทคนิค
        technical_terms = ['pm', 'overhaul', 'maintenance', 'repair', 'chiller', 'compressor']
        for term in technical_terms:
            if term in question.lower():
                keywords.append(term)
        
        return keywords
    
    def _create_filters_from_question(self, question: str, available_columns: Dict[str, List[str]]) -> List[Dict]:
        """🎯 สร้างฟิลเตอร์จากคำถาม"""
        
        filters = []
        keywords = self._extract_search_keywords(question)
        
        for table_name, columns in available_columns.items():
            for keyword in keywords:
                # หาคอลัมน์ที่เหมาะสำหรับค้นหา
                text_columns = []
                
                for col in columns:
                    col_lower = col.lower()
                    if any(text_type in col_lower for text_type in ['name', 'description', 'detail']):
                        text_columns.append(col)
                
                if text_columns:
                    filters.append({
                        'table': table_name,
                        'columns': text_columns,
                        'keyword': keyword,
                        'operator': 'ILIKE',
                        'value': f'%{keyword}%'
                    })
        
        return filters
    
    async def _generate_accurate_sql(self, question: str, analysis: Dict[str, Any], 
                                   actual_schema: Dict[str, Any], tenant_id: str) -> str:
        """🔧 สร้าง SQL ที่แม่นยำตาม schema จริง"""
        
        question_type = analysis['question_type']
        target_tables = analysis['target_tables']
        available_columns = analysis['available_columns']
        
        if not target_tables:
            logger.warning("No target tables found")
            return None
        
        main_table = target_tables[0]
        
        try:
            if question_type == 'counting':
                return self._generate_counting_sql(main_table, available_columns, analysis)
            
            elif question_type == 'spare_parts_search':
                return self._generate_spare_parts_sql(main_table, available_columns, analysis)
            
            elif question_type == 'customer_analysis':
                return self._generate_customer_sql(main_table, available_columns, analysis)
            
            elif question_type == 'sales_analysis':
                return self._generate_sales_analysis_sql(target_tables, available_columns, analysis)
            
            else:
                return self._generate_general_sql(main_table, available_columns, analysis)
                
        except Exception as e:
            logger.error(f"❌ SQL generation failed: {e}")
            return None
    
    def _generate_counting_sql(self, table_name: str, available_columns: Dict[str, List[str]], 
                             analysis: Dict[str, Any]) -> str:
        """📊 สร้าง SQL สำหรับการนับ"""
        
        columns = available_columns.get(table_name, [])
        
        # หาคอลัมน์ที่เหมาะสำหรับนับ
        if 'customer_name' in columns:
            count_column = 'DISTINCT customer_name'
            alias = 'total_customers'
        elif 'product_name' in columns:
            count_column = 'DISTINCT product_name'
            alias = 'total_products'
        else:
            count_column = '*'
            alias = 'total_records'
        
        sql = f"SELECT COUNT({count_column}) as {alias} FROM {table_name}"
        
        # เพิ่มเงื่อนไข
        where_conditions = self._build_where_conditions(table_name, columns, analysis)
        if where_conditions:
            sql += f" WHERE {where_conditions}"
        
        return sql
    
    def _generate_spare_parts_sql(self, table_name: str, available_columns: Dict[str, List[str]], 
                                analysis: Dict[str, Any]) -> str:
        """🔧 สร้าง SQL สำหรับค้นหาอะไหล่"""
        
        columns = available_columns.get(table_name, [])
        
        # เลือกคอลัมน์ที่สำคัญ
        select_columns = []
        for col in ['product_code', 'product_name', 'unit_price', 'balance', 'description']:
            if col in columns:
                select_columns.append(col)
        
        if not select_columns:
            select_columns = columns[:5]  # เลือก 5 คอลัมน์แรก
        
        sql = f"SELECT {', '.join(select_columns)} FROM {table_name}"
        
        # เพิ่มเงื่อนไข
        where_conditions = self._build_where_conditions(table_name, columns, analysis)
        if where_conditions:
            sql += f" WHERE {where_conditions}"
        
        sql += " ORDER BY product_code LIMIT 20"
        
        return sql
    
    def _generate_customer_sql(self, table_name: str, available_columns: Dict[str, List[str]], 
                             analysis: Dict[str, Any]) -> str:
        """👥 สร้าง SQL สำหรับวิเคราะห์ลูกค้า"""
        
        columns = available_columns.get(table_name, [])
        
        # เลือกคอลัมน์ที่เกี่ยวกับลูกค้า
        select_columns = []
        for col in ['customer_name', 'job_no', 'description', 'service_contact_']:
            if col in columns:
                select_columns.append(col)
        
        if not select_columns:
            select_columns = columns[:4]
        
        sql = f"SELECT {', '.join(select_columns)} FROM {table_name}"
        
        # เพิ่มเงื่อนไข
        where_conditions = self._build_where_conditions(table_name, columns, analysis)
        if where_conditions:
            sql += f" WHERE {where_conditions}"
        
        sql += " ORDER BY customer_name LIMIT 20"
        
        return sql
    
    def _generate_sales_analysis_sql(self, target_tables: List[str], available_columns: Dict[str, List[str]], 
                                   analysis: Dict[str, Any]) -> str:
        """📈 สร้าง SQL สำหรับวิเคราะห์ยอดขาย"""
        
        # ใช้ตารางแรกเป็นหลัก
        main_table = target_tables[0]
        columns = available_columns.get(main_table, [])
        
        # หาคอลัมน์ที่เกี่ยวกับการขาย
        value_column = None
        for col in ['service_contact_', 'amount', 'value', 'price']:
            if col in columns:
                value_column = col
                break
        
        if value_column:
            sql = f"""SELECT 
                COUNT(*) as total_jobs,
                SUM(CAST({value_column} as NUMERIC)) as total_revenue,
                AVG(CAST({value_column} as NUMERIC)) as avg_job_value
            FROM {main_table}"""
        else:
            sql = f"SELECT COUNT(*) as total_jobs FROM {main_table}"
        
        # เพิ่มเงื่อนไข
        where_conditions = self._build_where_conditions(main_table, columns, analysis)
        if where_conditions:
            sql += f" WHERE {where_conditions}"
        
        return sql
    
    def _generate_general_sql(self, table_name: str, available_columns: Dict[str, List[str]], 
                            analysis: Dict[str, Any]) -> str:
        """🔍 สร้าง SQL ทั่วไป"""
        
        columns = available_columns.get(table_name, [])
        
        # เลือกคอลัมน์ที่สำคัญ
        important_columns = []
        priority_columns = ['id', 'name', 'customer_name', 'product_name', 'description', 'date']
        
        for col in priority_columns:
            if col in columns:
                important_columns.append(col)
        
        # เติมคอลัมน์อื่นจนครบ 5 คอลัมน์
        for col in columns:
            if col not in important_columns and len(important_columns) < 5:
                important_columns.append(col)
        
        if not important_columns:
            important_columns = ['*']
        
        sql = f"SELECT {', '.join(important_columns)} FROM {table_name}"
        
        # เพิ่มเงื่อนไข
        where_conditions = self._build_where_conditions(table_name, columns, analysis)
        if where_conditions:
            sql += f" WHERE {where_conditions}"
        
        sql += " LIMIT 20"
        
        return sql
    
    def _build_where_conditions(self, table_name: str, columns: List[str], 
                              analysis: Dict[str, Any]) -> str:
        """🎯 สร้างเงื่อนไข WHERE"""
        
        conditions = []
        keywords = analysis.get('search_keywords', [])
        
        for keyword in keywords:
            keyword_conditions = []
            
            # หาคอลัมน์ที่เหมาะสำหรับค้นหาคำนี้
            for col in columns:
                col_lower = col.lower()
                if any(text_type in col_lower for text_type in ['name', 'description', 'detail']):
                    keyword_conditions.append(f"{col} ILIKE '%{keyword}%'")
            
            if keyword_conditions:
                conditions.append(f"({' OR '.join(keyword_conditions)})")
        
        # เพิ่มเงื่อนไขพิเศษ
        if 'service_contact_' in columns:
            conditions.append("service_contact_ IS NOT NULL")
        
        return ' AND '.join(conditions)
    
    async def _execute_sql_safely(self, sql_query: str, tenant_id: str) -> List[Dict[str, Any]]:
        """🛡️ Execute SQL อย่างปลอดภัย"""
        
        try:
            results = await self.db_handler._execute_sql_unified(sql_query, tenant_id)
            return results if results else []
            
        except Exception as e:
            logger.error(f"❌ Safe SQL execution failed: {e}")
            logger.error(f"❌ Failed SQL: {sql_query}")
            
            # ลอง fallback query อย่างง่าย
            try:
                # หาตารางจาก SQL
                table_match = re.search(r'FROM\s+(\w+)', sql_query, re.IGNORECASE)
                if table_match:
                    table_name = table_match.group(1)
                    simple_query = f"SELECT * FROM {table_name} LIMIT 5"
                    logger.info(f"🔄 Trying fallback query: {simple_query}")
                    
                    fallback_results = await self.db_handler._execute_sql_unified(simple_query, tenant_id)
                    return fallback_results if fallback_results else []
            except:
                pass
            
            return []
    

    def _format_results_intelligently(self, question: str, results: List[Dict], 
                                    analysis: Dict[str, Any]) -> str:
        """🎨 จัดรูปแบบผลลัพธ์แบบธรรมชาติ"""
        
        if not results:
            return self._create_natural_no_results_response(question, analysis)
        
        question_type = analysis.get('question_type', 'unknown')
        
        # เลือกรูปแบบตามประเภทคำถาม
        if question_type == 'counting':
            return self._format_counting_natural(question, results)
        elif question_type == 'spare_parts_search':
            return self._format_spare_parts_natural(question, results)
        elif question_type == 'customer_analysis':
            return self._format_customer_natural(question, results)
        elif question_type == 'sales_analysis':
            return self._format_sales_natural(question, results)
        else:
            return self._format_general_natural(question, results)

    def _format_counting_natural(self, question: str, results: List[Dict]) -> str:
        """📊 คำตอบการนับแบบธรรมชาติ"""
        
        if len(results) == 1:
            count_result = results[0]
            # หาค่าที่เป็นตัวเลข
            count_value = None
            
            for key, value in count_result.items():
                if isinstance(value, (int, float)) and value > 0:
                    count_value = int(value)
                    break
            
            if count_value is not None:
                if 'customer' in question.lower() or 'ลูกค้า' in question:
                    return f"ตอนนี้เรามีลูกค้าทั้งหมด {count_value:,} ราย ครับ"
                elif 'อะไหล่' in question or 'spare' in question.lower():
                    return f"ในคลังมีอะไหล่ทั้งหมด {count_value:,} รายการ ครับ"
                elif 'งาน' in question or 'job' in question.lower():
                    return f"มีงานทั้งหมด {count_value:,} งาน ครับ"
                else:
                    return f"พบข้อมูลทั้งหมด {count_value:,} รายการ ครับ"
        
        return f"พบข้อมูลทั้งหมด {len(results)} รายการ ครับ"

    def _format_spare_parts_natural(self, question: str, results: List[Dict]) -> str:
        """🔧 คำตอบอะไหล่แบบธรรมชาติ"""
        
        if not results:
            return "ขออภัยครับ ไม่พบอะไหล่ที่ตรงกับที่ค้นหา"
        
        # เริ่มต้นแบบธรรมชาติ
        if len(results) == 1:
            response = "เจออะไหล่ที่คุณต้องการแล้วครับ:\n\n"
        else:
            response = f"เจออะไหล่ที่เกี่ยวข้อง {len(results)} รายการครับ:\n\n"
        
        for i, result in enumerate(results[:5], 1):  # แสดงแค่ 5 รายการแรก
            code = result.get('product_code', '')
            name = result.get('product_name', '')
            price = result.get('unit_price', '')
            balance = result.get('balance', '')
            description = result.get('description', '')
            
            # ข้อมูลหลัก
            if code and name:
                response += f"{i}. {code} - {name}\n"
            elif name:
                response += f"{i}. {name}\n"
            elif code:
                response += f"{i}. รหัส {code}\n"
            else:
                response += f"{i}. รายการที่ {i}\n"
            
            # ราคาและสต็อก
            price_info = []
            if price and str(price).replace('.', '').replace(',', '').isdigit():
                try:
                    price_num = float(str(price).replace(',', ''))
                    if price_num > 0:
                        price_info.append(f"ราคา {price_num:,.0f} บาท")
                except:
                    pass
            
            if balance and str(balance).isdigit():
                balance_num = int(balance)
                if balance_num > 0:
                    price_info.append(f"เหลือ {balance_num} ชิ้น")
                else:
                    price_info.append("สินค้าหมด")
            
            if price_info:
                response += f"   {' | '.join(price_info)}\n"
            
            # รายละเอียดเพิ่มเติม
            if description and len(description) > 10:
                desc_short = description[:60] + "..." if len(description) > 60 else description
                response += f"   {desc_short}\n"
            
            response += "\n"
        
        # ข้อมูลเพิ่มเติม
        if len(results) > 5:
            response += f"และอีก {len(results) - 5} รายการ หากต้องการดูเพิ่มเติมแจ้งได้ครับ"
        
        return response

    def _format_customer_natural(self, question: str, results: List[Dict]) -> str:
        """👥 คำตอบลูกค้าแบบธรรมชาติ"""
        
        if not results:
            return "ไม่พบข้อมูลลูกค้าที่ตรงกับที่ค้นหาครับ"
        
        if len(results) == 1:
            response = "พบข้อมูลลูกค้าครับ:\n\n"
        else:
            response = f"พบข้อมูลลูกค้า {len(results)} รายการครับ:\n\n"
        
        for i, result in enumerate(results[:8], 1):
            customer = result.get('customer_name', result.get('customer', ''))
            job_no = result.get('job_no', '')
            description = result.get('description', result.get('detail', ''))
            value = result.get('service_contact_', result.get('value', ''))
            
            # ชื่อลูกค้า
            if customer:
                response += f"{i}. {customer}\n"
            else:
                response += f"{i}. ลูกค้ารายที่ {i}\n"
            
            # รายละเอียดงาน
            job_details = []
            if job_no:
                job_details.append(f"งาน {job_no}")
            
            if description:
                # ตัดให้สั้นลงเพื่อให้อ่านง่าย
                desc_short = description[:80] + "..." if len(description) > 80 else description
                if job_details:
                    job_details.append(desc_short)
                else:
                    job_details.append(f"บริการ: {desc_short}")
            
            if job_details:
                response += f"   {' - '.join(job_details)}\n"
            
            # มูลค่า
            if value and str(value).replace('.', '').replace(',', '').isdigit():
                try:
                    value_num = float(str(value).replace(',', ''))
                    if value_num > 0:
                        response += f"   มูลค่า {value_num:,.0f} บาท\n"
                except:
                    pass
            
            response += "\n"
        
        if len(results) > 8:
            response += f"และอีก {len(results) - 8} รายการ"
        
        return response

    def _format_sales_natural(self, question: str, results: List[Dict]) -> str:
        """📊 คำตอบยอดขายแบบธรรมชาติ"""
        
        if len(results) == 1:
            result = results[0]
            
            total_jobs = result.get('total_jobs', 0)
            total_revenue = result.get('total_revenue', 0)
            avg_job_value = result.get('avg_job_value', 0)
            
            # เริ่มต้นการตอบ
            if 'ปี' in question or 'year' in question.lower():
                response = "จากการวิเคราะห์ยอดขายแล้วครับ:\n\n"
            else:
                response = "สรุปข้อมูลการขายครับ:\n\n"
            
            # จำนวนงาน
            if total_jobs:
                if total_jobs == 1:
                    response += "มีงานเพียง 1 งาน"
                elif total_jobs < 10:
                    response += f"มีงานทั้งหมด {total_jobs} งาน"
                elif total_jobs < 100:
                    response += f"ทำงานไปแล้ว {total_jobs} งาน"
                else:
                    response += f"ทำงานไปแล้วถึง {total_jobs:,} งาน!"
            
            # รายได้รวม
            if total_revenue:
                try:
                    revenue_num = float(total_revenue)
                    if revenue_num >= 1_000_000:
                        revenue_million = revenue_num / 1_000_000
                        response += f" รายได้รวม {revenue_million:.1f} ล้านบาท"
                    else:
                        response += f" รายได้รวม {revenue_num:,.0f} บาท"
                except:
                    pass
            
            # มูลค่าเฉลี่ย
            if avg_job_value:
                try:
                    avg_num = float(avg_job_value)
                    response += f"\nมูลค่าเฉลี่ยต่องาน {avg_num:,.0f} บาท"
                    
                    # เพิ่มความคิดเห็น
                    if avg_num > 200_000:
                        response += " (งานใหญ่เลยทีเดียว)"
                    elif avg_num > 100_000:
                        response += " (งานขนาดกลาง)"
                    elif avg_num > 50_000:
                        response += " (งานทั่วไป)"
                    else:
                        response += " (งานเล็ก)"
                except:
                    pass
            
            return response
        else:
            # หลายแถว
            response = "ข้อมูลการขายแยกตามช่วงเวลาครับ:\n\n"
            
            for i, result in enumerate(results[:5], 1):
                response += f"{i}. "
                
                # แสดงข้อมูลแต่ละแถว
                for key, value in result.items():
                    if value is not None and isinstance(value, (int, float)):
                        if 'revenue' in key.lower() or 'value' in key.lower():
                            response += f"รายได้ {value:,.0f} บาท | "
                        elif 'job' in key.lower() or 'count' in key.lower():
                            response += f"{value:,} งาน | "
                        else:
                            response += f"{key}: {value:,} | "
                    elif value is not None:
                        response += f"{value} | "
                
                response = response.rstrip(" | ") + "\n"
            
            return response

    def _format_general_natural(self, question: str, results: List[Dict]) -> str:
        """📋 คำตอบทั่วไปแบบธรรมชาติ"""
        
        if not results:
            return "ขออภัยครับ ไม่พบข้อมูลที่ตรงกับที่ค้นหา"
        
        # เลือกคำเริ่มต้นตามคำถาม
        if 'หา' in question or 'ค้นหา' in question:
            response = f"ค้นหาแล้วพบข้อมูล {len(results)} รายการครับ:\n\n"
        elif 'แสดง' in question or 'ดู' in question:
            response = f"ข้อมูลที่ต้องการครับ:\n\n"
        else:
            response = f"ข้อมูลที่เกี่ยวข้อง {len(results)} รายการครับ:\n\n"
        
        for i, result in enumerate(results[:6], 1):
            response += f"{i}. "
            
            # เลือกข้อมูลสำคัญมาแสดง
            important_info = []
            
            for key, value in result.items():
                if value and str(value).strip():
                    # ข้อมูลหลัก
                    if key.lower() in ['name', 'customer', 'customer_name', 'product_name']:
                        important_info.insert(0, str(value))  # ใส่ไว้หน้าสุด
                    # ราคา/เงิน
                    elif key.lower() in ['price', 'cost', 'service_contact_', 'unit_price']:
                        if str(value).replace('.', '').replace(',', '').isdigit():
                            try:
                                price_num = float(str(value).replace(',', ''))
                                important_info.append(f"{price_num:,.0f} บาท")
                            except:
                                important_info.append(str(value))
                    # ข้อมูลเพิ่มเติม
                    elif len(important_info) < 3:
                        info_str = str(value)
                        if len(info_str) > 50:
                            info_str = info_str[:47] + "..."
                        important_info.append(info_str)
            
            if important_info:
                response += " - ".join(important_info[:3])
            else:
                response += "ข้อมูลไม่สมบูรณ์"
            
            response += "\n"
        
        if len(results) > 6:
            response += f"\nและอีก {len(results) - 6} รายการ หากต้องการดูเพิ่มเติม บอกได้ครับ"
        
        return response

    def _create_natural_no_results_response(self, question: str, analysis: Dict[str, Any]) -> str:
        """📭 คำตอบเมื่อไม่พบข้อมูล - แบบธรรมชาติ"""
        
        question_type = analysis.get('question_type', 'unknown')
        keywords = analysis.get('search_keywords', [])
        
        # เลือกคำตอบตามประเภทคำถาม
        if question_type == 'spare_parts_search':
            response = "หาอะไหล่ที่ต้องการไม่เจอเลยครับ"
            
            if keywords:
                response += f" (ค้นหาด้วยคำว่า: {', '.join(keywords)})"
            
            response += "\n\nลองเปลี่ยนคำค้นหาดูครับ เช่น:\n"
            response += "• ใช้ชื่อภาษาอังกฤษ เช่น 'Motor', 'Compressor'\n"
            response += "• ใช้รหัสสินค้า หรือ\n"
            response += "• ถาม 'มีอะไหล่อะไรบ้าง' เพื่อดูรายการทั้งหมด"
        
        elif question_type == 'customer_analysis':
            response = "ไม่เจอข้อมูลลูกค้าที่ต้องการครับ"
            
            if keywords:
                response += f" (ค้นหาด้วย: {', '.join(keywords)})"
            
            response += "\n\nลองเปลี่ยนวิธีค้นหาดูครับ:\n"
            response += "• ใช้ชื่อบริษัทแบบย่อๆ\n"
            response += "• ถาม 'มีลูกค้าอะไรบ้าง' เพื่อดูรายชื่อ\n"
            response += "• หรือค้นหาด้วยประเภทบริการ เช่น 'PM', 'ซ่อม'"
        
        elif question_type == 'sales_analysis':
            response = "ไม่พบข้อมูลการขายที่ต้องการครับ"
            
            response += "\n\nลองปรับคำถามดูครับ:\n"
            response += "• ระบุปีที่ชัดเจน เช่น '2024'\n"
            response += "• ถาม 'ยอดขายทั้งหมด' เพื่อดูภาพรวม\n"
            response += "• หรือแยกตามประเภทบริการ"
        
        else:
            response = "ขออภัยครับ ไม่เจอข้อมูลที่ตรงกับที่ต้องการ"
            
            if keywords:
                response += f" (ค้นหาด้วย: {', '.join(keywords)})"
            
            response += "\n\nลองเปลี่ยนคำถามหรือใช้คำค้นหาอื่นดูครับ"
        
        return response
    
    def _format_spare_parts_results(self, question: str, results: List[Dict]) -> str:
        """🔧 จัดรูปแบบผลลัพธ์อะไหล่"""
        
        response = f"🔧 รายการอะไหล่: {question}\n\n"
        
        for i, result in enumerate(results[:10], 1):
            response += f"{i}. "
            
            # ข้อมูลสำคัญของอะไหล่
            code = result.get('product_code', '')
            name = result.get('product_name', '')
            price = result.get('unit_price', '')
            balance = result.get('balance', '')
            description = result.get('description', '')
            
            if code:
                response += f"{code}"
            if name:
                response += f" - {name}"
            response += "\n"
            
            if price and str(price).replace('.', '').replace(',', '').isdigit():
                try:
                    price_num = float(str(price).replace(',', ''))
                    response += f"   💰 ราคา: {price_num:,.0f} บาท"
                    if balance:
                        response += f" | 📦 คงเหลือ: {balance} ชิ้น"
                    response += "\n"
                except:
                    pass
            
            if description:
                desc_short = description[:80] + "..." if len(description) > 80 else description
                response += f"   📝 รายละเอียด: {desc_short}\n"
            
            response += "\n"
        
        response += f"📈 สรุป: แสดงผลลัพธ์ {min(len(results), 10)} รายการจากทั้งหมด {len(results)} รายการ"
        
        return response
    
    def _format_customer_results(self, question: str, results: List[Dict]) -> str:
        """👥 จัดรูปแบบผลลัพธ์ลูกค้า"""
        
        response = f"👥 ข้อมูลลูกค้า: {question}\n\n"
        
        for i, result in enumerate(results[:10], 1):
            customer = result.get('customer_name', result.get('customer', ''))
            job_no = result.get('job_no', '')
            description = result.get('description', result.get('detail', ''))
            value = result.get('service_contact_', result.get('value', ''))
            
            response += f"{i}. {customer}\n"
            
            if job_no:
                response += f"   📋 งาน: {job_no}\n"
            
            if description:
                desc_short = description[:100] + "..." if len(description) > 100 else description
                response += f"   🛠️ บริการ: {desc_short}\n"
            
            if value and str(value).replace('.', '').replace(',', '').isdigit():
                try:
                    value_num = float(str(value).replace(',', ''))
                    if value_num > 0:
                        response += f"   💰 มูลค่า: {value_num:,.0f} บาท\n"
                except:
                    pass
            
            response += "\n"
        
        response += f"📈 สรุป: แสดงผลลัพธ์ {min(len(results), 10)} รายการจากทั้งหมด {len(results)} รายการ"
        
        return response
    
    def _format_sales_results(self, question: str, results: List[Dict]) -> str:
        """📊 จัดรูปแบบผลลัพธ์การขาย"""
        
        response = f"📊 วิเคราะห์การขาย: {question}\n\n"
        
        if len(results) == 1:
            result = results[0]
            
            total_jobs = result.get('total_jobs', 0)
            total_revenue = result.get('total_revenue', 0)
            avg_job_value = result.get('avg_job_value', 0)
            
            response += f"📋 จำนวนงานทั้งหมด: {total_jobs:,} งาน\n"
            
            if total_revenue:
                try:
                    revenue_num = float(total_revenue)
                    response += f"💰 รายได้รวม: {revenue_num:,.0f} บาท\n"
                    
                    if avg_job_value:
                        avg_num = float(avg_job_value)
                        response += f"📈 มูลค่าเฉลี่ยต่องาน: {avg_num:,.0f} บาท\n"
                except:
                    pass
        else:
            # แสดงผลหลายแถว
            for i, result in enumerate(results[:5], 1):
                response += f"{i}. "
                for key, value in result.items():
                    if value is not None:
                        if isinstance(value, (int, float)):
                            if 'revenue' in key.lower() or 'value' in key.lower():
                                response += f"{key}: {value:,.0f} บาท | "
                            else:
                                response += f"{key}: {value:,} | "
                        else:
                            response += f"{key}: {value} | "
                response = response.rstrip(" | ") + "\n"
        
        return response
    
    def _format_general_results(self, question: str, results: List[Dict]) -> str:
        """📋 จัดรูปแบบผลลัพธ์ทั่วไป"""
        
        response = f"🔍 ผลการค้นหา: {question}\n\n"
        
        for i, result in enumerate(results[:10], 1):
            response += f"{i}. "
            
            # เลือกข้อมูลสำคัญมาแสดง
            important_data = []
            
            for key, value in result.items():
                if value and str(value).strip():
                    # จัดรูปแบบตามประเภทข้อมูล
                    if key.lower() in ['name', 'customer', 'customer_name', 'product_name']:
                        important_data.append(str(value))
                    elif key.lower() in ['price', 'cost', 'service_contact_', 'unit_price'] and str(value).replace('.', '').replace(',', '').isdigit():
                        try:
                            price_num = float(str(value).replace(',', ''))
                            important_data.append(f"{price_num:,.0f} บาท")
                        except:
                            important_data.append(str(value))
                    elif len(important_data) < 3:  # จำกัดข้อมูลที่แสดง
                        important_data.append(str(value))
            
            response += " | ".join(important_data[:3])
            response += "\n"
        
        response += f"\n📈 สรุป: แสดงผลลัพธ์ {min(len(results), 10)} รายการจากทั้งหมด {len(results)} รายการ"
        
        return response
    
    # เพิ่มใน advanced_dynamic_ai_system.py

    async def _create_smart_spare_parts_suggestions(self, question: str, target_tables: List[str], 
                                                tenant_id: str) -> str:
        """💡 แนะนำคำค้นหาอะไหล่อัจฉริยะจากข้อมูลจริง"""
        
        try:
            # ดึงข้อมูลจริงจากฐานข้อมูล
            main_table = target_tables[0] if target_tables else 'spare_part'
            
            # ดึงตัวอย่าง product_name ที่ไม่ซ้ำ
            sample_query = f"""
                SELECT DISTINCT product_name, unit_price, description 
                FROM {main_table} 
                WHERE product_name IS NOT NULL 
                ORDER BY product_name 
                LIMIT 15
            """
            
            results = await self.db_handler._execute_sql_unified(sample_query, tenant_id)
            
            if not results:
                return "🔍 ไม่พบข้อมูลอะไหล่ในระบบ"
            
            response = f"🔍 ไม่พบ 'Hitachi' ในระบบอะไหล่\n\n"
            response += "🔧 **อะไหล่ที่มีจริงในสต็อก:**\n\n"
            
            # จัดกลุ่มอะไหล่ตามประเภท
            categories = {
                'MOTOR': [],
                'CIRCUIT BOARD': [],
                'SENSOR': [],
                'TRANSFORMER': [],
                'OTHER': []
            }
            
            for item in results:
                product_name = item.get('product_name', '')
                unit_price = item.get('unit_price', '')
                description = item.get('description', '')
                
                # จัดกลุ่ม
                if 'MOTOR' in product_name.upper():
                    categories['MOTOR'].append({
                        'name': product_name,
                        'price': unit_price,
                        'desc': description
                    })
                elif 'CIRCUIT BOARD' in product_name.upper() or 'BOARD' in product_name.upper():
                    categories['CIRCUIT BOARD'].append({
                        'name': product_name,
                        'price': unit_price,
                        'desc': description
                    })
                elif 'SENSOR' in product_name.upper():
                    categories['SENSOR'].append({
                        'name': product_name,
                        'price': unit_price,
                        'desc': description
                    })
                elif 'TRANSFORMER' in product_name.upper():
                    categories['TRANSFORMER'].append({
                        'name': product_name,
                        'price': unit_price,
                        'desc': description
                    })
                else:
                    categories['OTHER'].append({
                        'name': product_name,
                        'price': unit_price,
                        'desc': description
                    })
            
            # แสดงผลแต่ละประเภท
            for category, items in categories.items():
                if items:
                    response += f"**{category}:**\n"
                    for item in items[:3]:  # แสดงแค่ 3 รายการแรก
                        name = item['name']
                        price = item['price']
                        desc = item['desc'][:50] + "..." if len(item['desc']) > 50 else item['desc']
                        
                        response += f"• {name}"
                        if price and str(price).replace('.', '').replace(',', '').isdigit():
                            try:
                                price_num = float(str(price).replace(',', ''))
                                response += f" - {price_num:,.0f} บาท"
                            except:
                                pass
                        response += f"\n  {desc}\n"
                    
                    response += "\n"
            
            # แนะนำคำค้นหาที่น่าจะได้ผล
            response += "💡 **ลองค้นหาด้วยคำเหล่านี้:**\n"
            
            suggestions = []
            if categories['MOTOR']:
                suggestions.append('"ราคาอะไหล่ MOTOR"')
            if categories['CIRCUIT BOARD']:
                suggestions.append('"ราคาอะไหล่ Circuit Board"')
            if categories['SENSOR']:
                suggestions.append('"ราคาอะไหล่ Sensor"')
            if categories['TRANSFORMER']:
                suggestions.append('"ราคาอะไหล่ Transformer"')
            
            # เพิ่มคำค้นหาจากรุ่นเครื่อง
            models = set()
            for item in results:
                desc = item.get('description', '')
                if 'SET FREE' in desc:
                    models.add('SET FREE')
                if 'RAS-24' in desc:
                    models.add('RAS-24')
            
            for model in models:
                suggestions.append(f'"อะไหล่ {model}"')
            
            for suggestion in suggestions[:5]:
                response += f"• {suggestion}\n"
            
            response += f"\n📋 รวม {len(results)} รายการอะไหล่ในระบบ"
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to create smart suggestions: {e}")
            return "🔍 ไม่พบข้อมูลที่ตรงกับคำถาม แต่ลองค้นหาด้วยคำอื่นดู"

    # อัปเดต method หลักที่เรียกใช้
    def _create_no_results_response(self, question: str, analysis: Dict[str, Any]) -> str:
        """📭 สร้างคำตอบเมื่อไม่พบข้อมูล - ใช้ข้อมูลจริง"""
        
        question_type = analysis.get('question_type', 'unknown')
        target_tables = analysis.get('target_tables', [])
        keywords = analysis.get('search_keywords', [])
        
        # สำหรับการค้นหาอะไหล่ ให้แนะนำจากข้อมูลจริง
        if question_type == 'spare_parts_search' and 'hitachi' in [k.lower() for k in keywords]:
            # ใช้ async function (ถ้าเป็นไปได้)
            return f"""🔍 ไม่พบ 'Hitachi' ในระบบอะไหล่

    💡 **คำแนะนำ:**
    • ลองค้นหา "ราคาอะไหล่ MOTOR" 
    • ลองค้นหา "ราคาอะไหล่ Circuit Board"
    • ลองค้นหา "อะไหล่ SET FREE"
    • ลองค้นหา "ราคาอะไหล่ 17B29401A"

    🔧 หรือลองถาม "อะไหล่ทั้งหมดที่มีในสต็อก" เพื่อดูรายการทั้งหมด

    📋 ตารางที่ค้นหา: {', '.join(target_tables)}"""
        
        # กรณีอื่นๆ ใช้ response เดิม
        response = f"🔍 ไม่พบข้อมูลที่ตรงกับคำถาม: {question}\n\n"
        
        if keywords:
            response += f"🔍 คำที่ใช้ค้นหา: {', '.join(keywords)}\n\n"
        
        response += "💡 คำแนะนำทั่วไป:\n"
        response += "• ลองใช้คำถามที่เฉพาะเจาะจงมากขึ้น\n"
        response += "• ลองถามข้อมูลทั่วไปก่อน เช่น 'มีข้อมูลอะไรบ้าง'\n"
        
        return response
    
    async def _create_helpful_response(self, question: str, actual_schema: Dict[str, Any], 
                                     tenant_id: str) -> Dict[str, Any]:
        """💡 สร้างคำตอบที่เป็นประโยชน์เมื่อไม่สามารถสร้าง SQL ได้"""
        
        # แสดงข้อมูลที่มีในระบบ
        available_tables = list(actual_schema.keys())
        
        answer = f"""🤔 ไม่สามารถประมวลผลคำถามนี้ได้: {question}

📊 ข้อมูลที่มีในระบบ:
"""
        
        for table_name, table_info in actual_schema.items():
            purpose = table_info.get('purpose', 'ไม่ทราบ')
            row_count = table_info.get('row_count', 0)
            column_names = [col['name'] for col in table_info.get('columns', [])]
            
            answer += f"• {table_name} ({row_count:,} รายการ)\n"
            answer += f"  วัตถุประสงค์: {purpose}\n"
            answer += f"  คอลัมน์หลัก: {', '.join(column_names[:5])}\n\n"
        
        answer += """💡 ลองถามคำถามเหล่านี้:
• "จำนวนลูกค้าทั้งหมด"
• "รายการอะไหล่ทั้งหมด" 
• "งานที่ทำล่าสุด"
• "ข้อมูลในตาราง [ชื่อตาราง]"
• "มีข้อมูลอะไรบ้างในระบบ"
"""
        
        return {
            "answer": answer,
            "success": False,
            "data_source_used": "schema_exploration",
            "system_used": "helpful_response_generator",
            "available_tables": available_tables
        }


# เพิ่มเมธอดใหม่ให้กับ existing agent
class EnhancedUnifiedPostgresOllamaAgent:
    """🚀 ขยาย agent เดิมด้วยความสามารถ Dynamic AI ที่แก้ไขแล้ว"""
    
    def __init__(self):
        # Import และ initialize ตัวเดิม
        from .enhanced_postgres_agent_unified import UnifiedEnhancedPostgresOllamaAgent as OriginalAgent
        
        # เก็บ properties ของ agent เดิม
        original_agent = OriginalAgent()
        for attr_name in dir(original_agent):
            if not attr_name.startswith('_') or attr_name in ['_call_ollama_api', '_execute_sql_unified', 'get_database_connection']:
                setattr(self, attr_name, getattr(original_agent, attr_name))
        
        # เพิ่ม Dynamic AI system ที่แก้ไขแล้ว
        self.dynamic_ai_system = AdvancedDynamicAISystem(self, self)
        logger.info("🚀 Enhanced agent with FIXED Dynamic AI capabilities initialized")
    
    async def process_any_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """🎯 ประมวลผลคำถามใดๆ ด้วย FIXED Dynamic AI"""
        return await self.dynamic_ai_system.process_any_question(question, tenant_id)
    
    async def process_enhanced_question_with_fallback(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """🔄 ประมวลผลคำถามด้วย fallback เป็น FIXED Dynamic AI"""
        
        # ลองใช้วิธีเดิมก่อน
        try:
            result = await self.process_enhanced_question(question, tenant_id)
            
            # หากผลลัพธ์ไม่ดีพอ ใช้ Dynamic AI
            if (not result.get('success') or 
                not result.get('answer') or 
                'ไม่สามารถ' in result.get('answer', '') or
                'ขออภัย' in result.get('answer', '')):
                
                logger.info("🔄 Falling back to FIXED Dynamic AI system")
                return await self.dynamic_ai_system.process_any_question(question, tenant_id)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Standard processing failed, using FIXED Dynamic AI: {e}")
            return await self.dynamic_ai_system.process_any_question(question, tenant_id)