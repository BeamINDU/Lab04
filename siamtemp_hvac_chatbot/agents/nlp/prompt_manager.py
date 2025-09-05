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
from textwrap import dedent
from psycopg2.extras import RealDictCursor
from collections import Counter, defaultdict
logger = logging.getLogger(__name__)

class PromptManager:
    """
    Enhanced PromptManager - แก้ไขปัญหาทั้งหมด
    - Date handling ที่ถูกต้อง (ใช้ BETWEEN แทน LIKE)
    - CASE statement logic ที่ถูกต้อง
    - Column validation ครบถ้วน
    - SQL examples ที่มีประโยชน์
    - Response generation ที่ดีขึ้น
    """
    
    def __init__(self):
        # =================================================================
        # CORE SCHEMA INFORMATION (แก้ไขแล้ว)
        # =================================================================
        self.VIEW_COLUMNS = {
            'v_sales2022': [
                'id', 'job_no', 'customer_name', 'description',
                'overhaul_text', 'replacement_text', 'service_text', 'parts_text', 'product_text', 'solution_text',
                'overhaul_num', 'replacement_num', 'service_num', 'parts_num', 'product_num', 'solution_num',
                'total_revenue'
            ],
            'v_sales2023': [
                'id', 'job_no', 'customer_name', 'description',
                'overhaul_text', 'replacement_text', 'service_text', 'parts_text', 'product_text', 'solution_text',
                'overhaul_num', 'replacement_num', 'service_num', 'parts_num', 'product_num', 'solution_num',
                'total_revenue'
            ],
            'v_sales2024': [
                'id', 'job_no', 'customer_name', 'description',
                'overhaul_text', 'replacement_text', 'service_text', 'parts_text', 'product_text', 'solution_text',
                'overhaul_num', 'replacement_num', 'service_num', 'parts_num', 'product_num', 'solution_num',
                'total_revenue'
            ],
            'v_sales2025': [
                'id', 'job_no', 'customer_name', 'description',
                'overhaul_text', 'replacement_text', 'service_text', 'parts_text', 'product_text', 'solution_text',
                'overhaul_num', 'replacement_num', 'service_num', 'parts_num', 'product_num', 'solution_num',
                'total_revenue'
            ],
            'v_work_force': [
                'id', 'date', 'customer', 'project', 'detail', 'duration', 'service_group',
                'job_description_pm', 'job_description_replacement', 'job_description_cpa',  # BOOLEAN
                'job_description_overhaul', 'job_description_start_up', 'job_description_support_all',  # VARCHAR
                'success', 'unsuccessful', 'failure_reason',
                'report_kpi_2_days', 'report_over_kpi_2_days'
            ],
            'v_spare_part': [
                'id', 'wh', 'product_code', 'product_name', 'unit', 'description', 'received',
                'balance_text', 'unit_price_text', 'total_text',  # INTEGER
                'balance_num', 'unit_price_num', 'total_num'  # NUMERIC
            ],
            'v_spare_part2': [
                'id', 'wh', 'product_code', 'product_name', 'unit', 'description', 'received',
                'balance_text', 'unit_price_text', 'total_text',  # INTEGER  
                'balance_num', 'unit_price_num', 'total_num'  # NUMERIC
            ],
            'v_revenue_summary': ['year', 'total']
        }

        # =================================================================
        # CORRECTED SQL SYSTEM PROMPT
        # =================================================================
        self.SQL_SYSTEM_PROMPT = dedent("""
        You are a PostgreSQL SQL expert for the Siamtemp HVAC database.

        CRITICAL SCHEMA FACTS (CORRECTED):
        
        1. SALES VIEWS (v_sales2022-2025):
           • *_text fields are INTEGER type (original raw values) - NEVER use string operations
           • *_num fields are NUMERIC type (cleaned for calculations) - USE THESE ONLY
           • total_revenue = pre-calculated sum of all *_num fields
           • Available columns: overhaul_num, replacement_num, service_num, parts_num, product_num, solution_num
           
        2. WORK FORCE VIEW (v_work_force) - CRITICAL RESTRICTIONS:
            ❌ NO service_num, overhaul_num, replacement_num columns
            ❌ NO amount, revenue, price, value columns  
            ✅ Use ONLY: date, customer, project, detail, service_group
            ✅ job_description_pm, job_description_replacement, job_description_cpa (boolean)
            ✅ job_description_overhaul, job_description_start_up, job_description_support_all (varchar)
            ✅ success, unsuccessful, failure_reason
            ✅ For aggregations use: COUNT(*), COUNT(CASE WHEN conditions)
            
            • date field format: YYYY-MM-DD (use date::date BETWEEN 'start' AND 'end')
            • NO 'job_type' column exists - derive using CASE statement
            • For pending work: success IS NULL AND unsuccessful IS NULL
                                        
            STATUS FIELDS (CRITICAL - VARCHAR not BOOLEAN):
            - success: VARCHAR(255) - Contains completion details or NULL
            - unsuccessful: VARCHAR(255) - Contains failure details or NULL  
            - failure_reason: VARCHAR(255) - Contains reason or NULL

            STATUS LOGIC:
            ✅ For completed work: success IS NOT NULL
            ✅ For failed work: unsuccessful IS NOT NULL  
            ✅ For pending work: success IS NULL AND unsuccessful IS NULL

            ❌ NEVER use: success = true (it's VARCHAR!)
            ❌ NEVER use: CASE WHEN success THEN (wrong type!)
            ✅ ALWAYS use: CASE WHEN success IS NOT NULL THEN
                                                    
        3. SPARE PARTS VIEWS (v_spare_part, v_spare_part2):
           • *_text fields are INTEGER type (original values)
           • *_num fields are NUMERIC type (use for calculations)
           • Available numeric columns: balance_num, unit_price_num, total_num
           
        MANDATORY RULES:
        1. Use ONLY columns that exist (see whitelist above)
        2. For dates: use date::date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
        3. Convert Thai years: 2567→2024, 2568→2025
        4. Always include LIMIT clause (max 1000)
        5. Use *_num fields for ALL calculations
        6. Generate detailed SELECT (not just COUNT) for better insights

        Return ONLY valid PostgreSQL SQL without explanations.
        """).strip()

        # =================================================================
        # ENHANCED SCHEMA INFO
        # =================================================================
        self.SCHEMA_INFO = {
            'sales': dedent("""
            SALES VIEWS: v_sales2022, v_sales2023, v_sales2024, v_sales2025
            
            STRUCTURE (identical across all years):
            - Identifiers: id, job_no, customer_name, description
            
            VALUE FIELDS (use ONLY *_num for calculations):
            ✅ NUMERIC fields: overhaul_num, replacement_num, service_num, parts_num, product_num, solution_num
            ⚠️ INTEGER fields: overhaul_text, replacement_text, service_text, parts_text, product_text, solution_text
            ✅ CALCULATED: total_revenue (sum of all *_num fields)
            
            YEAR MAPPING: 2567→v_sales2024, 2568→v_sales2025
            """).strip(),
            
            'work_force': dedent("""
            WORK FORCE VIEW: v_work_force
            
            COLUMNS:
            - Basic: id, date, customer, project, detail, service_group
            - Status: success, unsuccessful, failure_reason
            
            JOB DESCRIPTION (mixed types - CRITICAL):
            ✅ Boolean (use = true): job_description_pm, job_description_replacement, job_description_cpa
            ✅ Varchar (use IS NOT NULL): job_description_overhaul, job_description_start_up, job_description_support_all
            
            DATE HANDLING:
            ✅ Format: YYYY-MM-DD (e.g., 2025-04-04)
            ✅ Query: date::date BETWEEN '2025-08-01' AND '2025-09-30'
            ❌ NEVER use LIKE patterns for dates
            
            JOB TYPE DERIVATION:
            ⚠️ NO 'job_type' column - use CASE statement with correct logic
            """).strip(),
            
            'spare_parts': dedent("""
            SPARE PARTS VIEWS: v_spare_part, v_spare_part2
            
            COLUMNS:
            - Basic: id, wh, product_code, product_name, unit, description, received
            ⚠️ INTEGER fields: balance_text, unit_price_text, total_text
            ✅ NUMERIC fields: balance_num, unit_price_num, total_num (use for calculations)
            
            COMMON PRODUCTS: EKAC460, EKAC230, RCUG120AHYZ1, 17B27237A
            """).strip()
        }

        # =================================================================
        # ENHANCED SQL EXAMPLES (แก้ไขทั้งหมด)
        # =================================================================
        self.SQL_EXAMPLES = self._load_enhanced_sql_examples()

    def _load_enhanced_sql_examples(self) -> Dict[str, str]:
        """Load enhanced SQL examples with better quality and correct logic"""
        return {
            # =============================================================
            # WORK FORCE EXAMPLES (แก้ไขแล้ว)
            # =============================================================
            'work_plan_detailed': dedent("""
            -- แผนงาน ส.ค.-ก.ย. 2568 (แสดงรายละเอียด)
            SELECT 
                date AS วันที่,
                customer AS ลูกค้า,
                project AS โครงการ,
                detail AS รายละเอียด,
                CASE 
                    WHEN job_description_pm = true THEN 'PM'
                    WHEN job_description_replacement = true THEN 'Replacement'
                    WHEN job_description_overhaul IS NOT NULL THEN 'Overhaul'
                    WHEN job_description_start_up IS NOT NULL THEN 'Start Up'
                    WHEN job_description_support_all IS NOT NULL THEN 'Support'
                    WHEN job_description_cpa = true THEN 'CPA'
                    ELSE 'Other'
                END AS ประเภทงาน,
                service_group AS ทีมงาน
            FROM v_work_force
            WHERE success IS NULL 
              AND unsuccessful IS NULL
              AND date::date BETWEEN '2025-08-01' AND '2025-09-30'
            ORDER BY date, customer
            LIMIT 100;
            """).strip(),

            'work_summary_by_type': dedent("""
            -- สรุปงานแยกตามประเภท เดือนมิถุนายน 2568
            SELECT 
                CASE 
                    WHEN job_description_pm = true THEN 'PM'
                    WHEN job_description_replacement = true THEN 'Replacement'
                    WHEN job_description_overhaul IS NOT NULL THEN 'Overhaul'
                    WHEN job_description_start_up IS NOT NULL THEN 'Start Up'
                    WHEN job_description_support_all IS NOT NULL THEN 'Support'
                    WHEN job_description_cpa = true THEN 'CPA'
                    ELSE 'Other'
                END AS ประเภทงาน,
                COUNT(*) AS จำนวนงาน,
                COUNT(CASE WHEN success IS NOT NULL THEN 1 END) AS งานสำเร็จ,
                COUNT(CASE WHEN unsuccessful IS NOT NULL THEN 1 END) AS งานไม่สำเร็จ,
                COUNT(CASE WHEN success IS NULL AND unsuccessful IS NULL THEN 1 END) AS งานค้างอยู่
            FROM v_work_force
            WHERE date::date BETWEEN '2025-06-01' AND '2025-06-30'
            GROUP BY ประเภทงาน
            ORDER BY จำนวนงาน DESC
            LIMIT 50;
            """).strip(),

            'work_by_customer': dedent("""
            -- งานแยกตามลูกค้า พร้อมสถานะ
            SELECT 
                customer AS ลูกค้า,
                COUNT(*) AS งานทั้งหมด,
                COUNT(CASE WHEN success IS NOT NULL AND success != '' THEN 1 END) AS งานสำเร็จ,
                COUNT(CASE WHEN success IS NULL AND unsuccessful IS NULL THEN 1 END) AS งานค้างอยู่,
                ROUND(
                    COUNT(CASE WHEN success IS NOT NULL THEN 1 END)::NUMERIC * 100.0 / 
                    COUNT(*), 1
                ) AS อัตราสำเร็จ_percent
            FROM v_work_force
            WHERE date::date >= '2025-01-01'
            GROUP BY customer
            HAVING COUNT(*) >= 2
            ORDER BY งานทั้งหมด DESC, อัตราสำเร็จ_percent DESC
            LIMIT 20;
            """).strip(),

            # =============================================================
            # SALES EXAMPLES (แก้ไขแล้ว)  
            # =============================================================
            'sales_analysis_detailed': dedent("""
            -- วิเคราะห์การขาย 2567-2568 พร้อมรายละเอียด
            SELECT 
                year_label AS ปี,
                COUNT(*) AS จำนวนงาน,
                SUM(overhaul_num) AS ยอด_overhaul,
                SUM(replacement_num) AS ยอด_replacement,
                SUM(service_num) AS ยอด_service,
                SUM(parts_num) AS ยอด_parts,
                SUM(product_num) AS ยอด_product,
                SUM(solution_num) AS ยอด_solution,
                SUM(total_revenue) AS รายได้รวม,
                ROUND(AVG(total_revenue), 0) AS ค่าเฉลี่ยต่องาน
            FROM (
                SELECT '2567 (2024)' AS year_label, overhaul_num, replacement_num, 
                       service_num, parts_num, product_num, solution_num, total_revenue
                FROM v_sales2024
                
                UNION ALL
                
                SELECT '2568 (2025)', overhaul_num, replacement_num,
                       service_num, parts_num, product_num, solution_num, total_revenue
                FROM v_sales2025
            ) combined
            GROUP BY year_label
            ORDER BY year_label;
            """).strip(),

            'overhaul_compressor_report': dedent("""
            -- รายงาน overhaul compressor 2567-2568
            SELECT 
                year_th AS ปี,
                COUNT(*) AS จำนวนงาน,
                SUM(overhaul_num) AS ยอดรวม_overhaul,
                ROUND(AVG(overhaul_num), 0) AS ค่าเฉลี่ยต่องาน,
                MAX(overhaul_num) AS ยอดสูงสุด,
                COUNT(DISTINCT customer_name) AS จำนวนลูกค้า
            FROM (
                SELECT '2567' AS year_th, customer_name, overhaul_num
                FROM v_sales2024
                WHERE overhaul_num > 0
                  AND (description ILIKE '%overhaul%' OR description ILIKE '%compressor%')
                
                UNION ALL
                
                SELECT '2568', customer_name, overhaul_num
                FROM v_sales2025  
                WHERE overhaul_num > 0
                  AND (description ILIKE '%overhaul%' OR description ILIKE '%compressor%')
            ) overhaul_data
            GROUP BY year_th
            ORDER BY year_th;
            """).strip(),

            'top_customers_detailed': dedent("""
            -- ลูกค้า Top 10 ปี 2568 พร้อมรายละเอียด
            SELECT 
                customer_name AS ลูกค้า,
                COUNT(*) AS จำนวนงาน,
                SUM(overhaul_num) AS overhaul,
                SUM(replacement_num) AS replacement,
                SUM(service_num) AS service,
                SUM(total_revenue) AS รายได้รวม,
                ROUND(AVG(total_revenue), 0) AS ค่าเฉลี่ยต่องาน,
                ROUND(SUM(total_revenue) * 100.0 / SUM(SUM(total_revenue)) OVER(), 1) AS สัดส่วน_percent
            FROM v_sales2025
            WHERE total_revenue > 0
            GROUP BY customer_name
            ORDER BY รายได้รวม DESC
            LIMIT 10;
            """).strip(),

            # =============================================================
            # SPARE PARTS EXAMPLES (แก้ไขแล้ว)
            # =============================================================
            'spare_part_price_detailed': dedent("""
            -- ราคาอะไหล่ EK model EKAC460
            SELECT 
                product_code AS รหัสสินค้า,
                product_name AS ชื่อสินค้า,
                wh AS คลัง,
                balance_num AS จำนวนคงเหลือ,
                unit_price_num AS ราคาต่อหน่วย,
                unit AS หน่วย,
                total_num AS มูลค่ารวม,
                description AS รายละเอียด
            FROM v_spare_part
            WHERE product_code ILIKE '%EKAC460%'
               OR product_name ILIKE '%EKAC460%'
               OR description ILIKE '%EKAC460%'
            ORDER BY unit_price_num DESC, balance_num DESC
            LIMIT 20;
            """).strip(),

            'inventory_summary_detailed': dedent("""
            -- สรุปมูลค่าคงคลังแยกตามคลัง
            SELECT 
                wh AS คลัง,
                COUNT(*) AS จำนวนรายการ,
                SUM(balance_num) AS จำนวนชิ้นรวม,
                SUM(total_num) AS มูลค่ารวม,
                ROUND(AVG(unit_price_num), 0) AS ราคาเฉลี่ย,
                COUNT(CASE WHEN balance_num > 0 THEN 1 END) AS รายการมีสต็อก,
                COUNT(CASE WHEN balance_num = 0 THEN 1 END) AS รายการหมดสต็อก
            FROM v_spare_part
            GROUP BY wh
            ORDER BY มูลค่ารวม DESC
            LIMIT 20;
            """).strip(),

            # =============================================================
            # CUSTOMER ANALYSIS EXAMPLES
            # =============================================================
            'customer_history_detailed': dedent("""
            -- ประวัติลูกค้า CLARION ครบทุกปี
            SELECT 
                year_label AS ปี,
                customer_name AS ลูกค้า,
                COUNT(*) AS จำนวนงาน,
                SUM(total_revenue) AS ยอดซื้อรวม,
                ROUND(AVG(total_revenue), 0) AS ค่าเฉลี่ยต่องาน,
                SUM(overhaul_num) AS overhaul,
                SUM(replacement_num) AS replacement,
                SUM(service_num) AS service
            FROM (
                SELECT '2022' AS year_label, customer_name, total_revenue, overhaul_num, replacement_num, service_num
                FROM v_sales2022 WHERE customer_name ILIKE '%CLARION%'
                UNION ALL
                SELECT '2023', customer_name, total_revenue, overhaul_num, replacement_num, service_num  
                FROM v_sales2023 WHERE customer_name ILIKE '%CLARION%'
                UNION ALL
                SELECT '2024', customer_name, total_revenue, overhaul_num, replacement_num, service_num
                FROM v_sales2024 WHERE customer_name ILIKE '%CLARION%'
                UNION ALL
                SELECT '2025', customer_name, total_revenue, overhaul_num, replacement_num, service_num
                FROM v_sales2025 WHERE customer_name ILIKE '%CLARION%'
            ) all_years
            GROUP BY year_label, customer_name
            ORDER BY year_label, ยอดซื้อรวม DESC;
            """).strip()
        }

    # =================================================================
    # PROMPT GENERATION METHODS
    # =================================================================
    
    def build_sql_prompt(self, question: str, intent: str, entities: Dict, 
                        context: Dict = None, examples_override: List[str] = None) -> str:
        """
        สร้าง SQL generation prompt ที่สมบูรณ์และแก้ไขปัญหาการ query หลายปี
        """
        # =================================================================
        # 1. DEBUG LOGGING
        # =================================================================
        logging.info(f"🎯 Building SQL prompt for intent: {intent}")
        logging.info(f"📊 Entities: {entities}")
        
        # =================================================================
        # 2. SELECT AND VALIDATE EXAMPLES
        # =================================================================
        selected_examples = []
        
        if examples_override:
            # Use provided examples
            for name in examples_override:
                if name in self.SQL_EXAMPLES:
                    selected_examples.append(self.SQL_EXAMPLES[name])
                    logging.info(f"✅ Using override example: {name}")
                else:
                    logging.warning(f"⚠️ Example not found: {name}")
        else:
            # Auto-select based on intent and entities
            selected_examples = self._get_relevant_examples_auto(intent, entities)
        
        # Ensure we have at least one example
        if not selected_examples:
            logging.warning("⚠️ No examples selected, using default")
            # Use most relevant default based on intent
            default_examples = {
                'sales_analysis': 'sales_analysis_detailed',
                'work_force': 'work_plan_detailed',
                'spare_parts': 'spare_part_price_detailed',
                'customer_history': 'customer_history_detailed'
            }
            default_key = default_examples.get(intent, 'sales_analysis_detailed')
            if default_key in self.SQL_EXAMPLES:
                selected_examples.append(self.SQL_EXAMPLES[default_key])
        
        examples_text = self._format_examples_enhanced(selected_examples)
        logging.info(f"📚 Examples included: {len(examples_text)} chars")
        
        # =================================================================
        # 3. BUILD SCHEMA INFORMATION
        # =================================================================
        schema_text = self._get_targeted_schema(intent, entities)
        
        # =================================================================
        # 4. HANDLE MULTI-YEAR QUERIES EXPLICITLY
        # =================================================================
        multi_year_instruction = ""
        if entities.get('years') and len(entities['years']) > 1:
            years = sorted(entities['years'])
            views_needed = [f"v_sales{year}" for year in years]
            
            multi_year_instruction = dedent(f"""
            === CRITICAL MULTI-YEAR REQUIREMENT ===
            This query requires data from MULTIPLE years: {', '.join(map(str, years))}
            
            YOU MUST USE ALL THESE VIEWS:
            {chr(10).join(['- ' + v for v in views_needed])}
            
            CORRECT APPROACH:
            SELECT year_label, columns... FROM (
                SELECT '{years[0]}' AS year_label, * FROM v_sales{years[0]}
                {chr(10).join([f"    UNION ALL" + chr(10) + f"    SELECT '{y}' AS year_label, * FROM v_sales{y}" for y in years[1:]])}
            ) combined_data
            
            ❌ WRONG: SELECT * FROM v_sales{years[0]}  -- This only gets ONE year
            ✅ RIGHT: Use UNION ALL to combine all years
            """).strip()
        
        # =================================================================
        # 5. HANDLE MONTH RANGES
        # =================================================================
        date_instruction = ""
        if entities.get('months'):
            months = entities['months']
            year = entities.get('years', [2025])[0]  # Default to current year
            
            if len(months) == 1:
                month = months[0]
                date_instruction = f"""
                === DATE FILTER REQUIRED ===
                Filter for month {month}/{year}:
                WHERE date::date BETWEEN '{year}-{month:02d}-01' AND '{year}-{month:02d}-31'
                """
            elif len(months) > 1:
                start_month = min(months)
                end_month = max(months)
                date_instruction = f"""
                === DATE RANGE REQUIRED ===
                Filter for months {start_month}-{end_month}/{year}:
                WHERE date::date BETWEEN '{year}-{start_month:02d}-01' AND '{year}-{end_month:02d}-31'
                """
        
        # =================================================================
        # 6. ENTITY-SPECIFIC INSTRUCTIONS
        # =================================================================
        entity_instructions = []
        
        if entities.get('customers'):
            customers = entities['customers']
            entity_instructions.append(f"""
            Customer Filter: WHERE customer_name ILIKE '%{customers[0]}%'
            """)
        
        if entities.get('products'):
            products = entities['products']
            entity_instructions.append(f"""
            Product Filter: WHERE product_code ILIKE '%{products[0]}%' 
                            OR product_name ILIKE '%{products[0]}%'
            """)
        
        if entities.get('job_types'):
            job_types = entities['job_types']
            if 'overhaul' in [jt.lower() for jt in job_types]:
                entity_instructions.append("""
                Job Type: Focus on overhaul_num > 0 or description ILIKE '%overhaul%'
                """)
        
        entity_instructions_text = '\n'.join(entity_instructions)
        
        # =================================================================
        # 7. BUILD FINAL PROMPT
        # =================================================================
        prompt = dedent(f"""
        You are a PostgreSQL SQL expert for the Siamtemp HVAC database.
        
        === DATABASE SCHEMA ===
        {schema_text}
        
        === WORKING SQL EXAMPLES ===
        {examples_text}
        
        {multi_year_instruction}
        
        {date_instruction}
        
        === YOUR TASK ===
        Question: {question}
        Intent: {intent}
        Entities: {json.dumps(entities, ensure_ascii=False)}
        
        {entity_instructions_text}
        
        MANDATORY RULES:
        1. Use ONLY columns that exist in the schema above
        2. For multiple years, MUST use UNION ALL to combine views
        3. For dates: use date::date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'  
        4. Use *_num fields for ALL calculations, never *_text fields
        5. Always include LIMIT 1000 unless aggregating
        6. Return complete, working PostgreSQL SQL
        
        IMPORTANT CHECKS:
        - If years = [2024, 2025], you MUST query BOTH v_sales2024 AND v_sales2025
        - If analyzing sales trends, include year labels for clarity
        - For revenue calculations, use total_revenue or sum of *_num fields
        
        Generate ONLY the SQL query, no explanations:
        """).strip()
        
        # =================================================================
        # 8. VALIDATE PROMPT SIZE
        # =================================================================
        max_prompt_size = 8000  # Conservative limit for model context
        if len(prompt) > max_prompt_size:
            logging.warning(f"⚠️ Prompt too long ({len(prompt)} chars), trimming examples")
            # Reduce examples to fit
            examples_text = examples_text[:2000] + "\n... (examples trimmed for size)"
            prompt = self._rebuild_prompt_with_trimmed_examples(
                question, intent, entities, schema_text, 
                examples_text, multi_year_instruction, date_instruction
            )
        
        logging.info(f"📝 Final prompt size: {len(prompt)} chars")
        
        # Log first part of prompt for debugging
        logging.debug(f"Prompt preview:\n{prompt[:500]}...")
        
        return prompt

    def _get_relevant_examples_auto(self, intent: str, entities: Dict) -> List[str]:
        """
        Automatically select most relevant examples based on intent and entities
        """
        selected = []
        
        # Base selection by intent
        intent_examples = {
            'sales_analysis': ['sales_analysis_detailed'],
            'overhaul_report': ['overhaul_compressor_report'],
            'work_force': ['work_plan_detailed', 'work_summary_by_type'],
            'work_plan': ['work_plan_detailed'],
            'work_summary': ['work_summary_by_type', 'work_by_customer'],
            'spare_parts': ['spare_part_price_detailed'],
            'parts_price': ['spare_part_price_detailed'],
            'customer_history': ['customer_history_detailed'],
            'top_customers': ['top_customers_detailed']
        }
        
        base_keys = intent_examples.get(intent, ['sales_analysis_detailed'])
        
        # Add examples based on entities
        if entities.get('years') and len(entities['years']) > 1:
            # Multi-year query - prioritize examples with UNION
            if intent in ['sales', 'sales_analysis']:
                base_keys = ['sales_analysis_detailed', 'customer_history_detailed']
            elif intent == 'overhaul_report':
                base_keys = ['overhaul_compressor_report']
        
        if entities.get('months'):
            # Date-based query
            if intent in ['work_force', 'work_plan', 'work_summary']:
                base_keys.append('work_plan_detailed')
        
        # Get actual examples
        for key in base_keys[:3]:  # Max 3 examples
            if key in self.SQL_EXAMPLES:
                selected.append(self.SQL_EXAMPLES[key])
        
        return selected

    def _format_examples_enhanced(self, examples: List[str]) -> str:
        """
        Format examples with clear labels and structure
        """
        if not examples:
            return "-- No specific examples available"
        
        formatted = []
        for i, example in enumerate(examples, 1):
            # Clean and format each example
            example_clean = example.strip()
            formatted.append(f"""
    --- Example {i} ---
    {example_clean}
    --- End Example {i} ---
            """.strip())
        
        return "\n\n".join(formatted)

    def _get_targeted_schema(self, intent: str, entities: Dict) -> str:
        """
        Get only relevant schema information based on intent
        """
        sections = []
        
        # Determine which schemas are needed
        need_sales = intent in ['sales', 'sales_analysis', 'overhaul_report', 'customer_history', 'top_customers']
        need_work = intent in ['work_force', 'work_plan', 'work_summary']
        need_parts = intent in ['spare_parts', 'parts_price', 'inventory_value']
        
        # Add multi-year sales if needed
        if need_sales and entities.get('years'):
            years = entities['years']
            for year in years:
                view_name = f"v_sales{year}"
                if view_name in self.VIEW_COLUMNS:
                    sections.append(f"""
    === {view_name.upper()} ===
    Columns: {', '.join(self.VIEW_COLUMNS[view_name][:10])}
    Key fields: customer_name, total_revenue, overhaul_num, replacement_num, service_num
                    """.strip())
        elif need_sales:
            sections.append(self.SCHEMA_INFO['sales'])
        
        if need_work:
            sections.append(self.SCHEMA_INFO['work_force'])
        
        if need_parts:
            sections.append(self.SCHEMA_INFO['spare_parts'])
        
        return "\n\n".join(sections) if sections else self._get_all_schema_info()

    def _rebuild_prompt_with_trimmed_examples(self, question, intent, entities, 
                                            schema_text, examples_text, 
                                            multi_year_instruction, date_instruction):
        """
        Rebuild prompt when it's too long
        """
        return dedent(f"""
        PostgreSQL SQL expert for Siamtemp HVAC database.
        
        {schema_text[:1500]}
        
        === EXAMPLE (SHORTENED) ===
        {examples_text}
        
        {multi_year_instruction}
        {date_instruction}
        
        Question: {question}
        Intent: {intent}
        Entities: {json.dumps(entities, ensure_ascii=False)}
        
        CRITICAL: If years=[2024,2025], MUST use BOTH v_sales2024 AND v_sales2025 with UNION ALL
        
        SQL query only:
        """).strip()

    def build_response_prompt(self, question: str, results: List[Dict], 
                             sql_query: str, locale: str = "th") -> str:
        """
        สร้าง response generation prompt ที่ปรับปรุงแล้ว
        """
        def _safe_dump(obj, max_len: int = 12000) -> str:
            try:
                s = json.dumps(obj, ensure_ascii=False, default=str, indent=2)
            except Exception:
                s = str(obj)
            if len(s) > max_len:
                s = s[:max_len] + "\n...(truncated)"
            return s

        header = "ตอบเป็นภาษาไทย กระชับ ชัดเจน" if locale == "th" else "Respond in concise English"
        results_json = _safe_dump(results)

        return dedent(f"""
        {header}
        บทบาท: นักวิเคราะห์ข้อมูล Siamtemp HVAC

        กฎการตอบ (ENHANCED):
        1. อ้างอิงเฉพาะข้อมูลในผลลัพธ์
        2. ระบุจำนวนรายการที่พบชัดเจน
        3. จัดกลุ่มข้อมูลตามประเภท/ลูกค้า/เดือน (ถ้ามี)
        4. แสดงยอดรวม/เฉลี่ย พร้อมหน่วยบาท
        5. ระบุลูกค้าหลัก/รายการสำคัญ 3-5 ราย (ถ้ามี)
        6. วิเคราะห์ patterns หรือแนวโน้ม (ถ้าเห็น)
        7. ใช้ bullet points กระชับ 4-8 ข้อ
        8. ห้ามแต่งเพิ่มหรือเดาข้อมูล

        คำถาม: {question}
        SQL: {sql_query}
        
        ผลลัพธ์ ({len(results)} records):
        {results_json}

        สรุปผลการวิเคราะห์:
        """).strip()

    def build_clarification_prompt(self, question: str, missing_info: List[str]) -> str:
        """
        สร้าง prompt สำหรับคำถามที่ไม่ชัดเจน
        """
        return dedent(f"""
        คำถาม "{question}" ต้องการข้อมูลเพิ่มเติม:

        ข้อมูลที่ขาด:
        {chr(10).join([f"• {info}" for info in missing_info])}

        ตัวอย่างการถามที่ชัดเจน:
        • "แผนงานวันที่ 15 สิงหาคม 2568"
        • "รายได้ของบริษัท CLARION ปี 2567-2568"
        • "ราคาอะไหล่ EKAC460"

        กรุณาระบุข้อมูลเหล่านี้เพื่อให้สามารถตอบได้อย่างถูกต้อง
        """).strip()

    # =================================================================
    # HELPER METHODS
    # =================================================================
    
    def _get_relevant_examples(self, intent: str, entities: Dict, max_examples: int = 3) -> List[str]:
        """
        เลือก SQL examples ที่เหมาะสมตาม intent และ entities
        """
        relevant_examples = []
        
        # Intent-based selection
        intent_mapping = {
            'work_force': ['work_plan_detailed', 'work_summary_by_type', 'work_by_customer'],
            'work_plan': ['work_plan_detailed'],
            'work_summary': ['work_summary_by_type', 'work_by_customer'],
            'sales_analysis': ['sales_analysis_detailed', 'top_customers_detailed'],
            'overhaul_report': ['overhaul_compressor_report'],
            'spare_parts': ['spare_part_price_detailed', 'inventory_summary_detailed'],
            'parts_price': ['spare_part_price_detailed'],
            'inventory_value': ['inventory_summary_detailed'],
            'customer_history': ['customer_history_detailed'],
            'top_customers': ['top_customers_detailed']
        }
        
        examples_for_intent = intent_mapping.get(intent, ['sales_analysis_detailed'])
        
        # Add entity-based scoring
        for example_key in examples_for_intent:
            if example_key in self.SQL_EXAMPLES:
                relevant_examples.append(example_key)
        
        return relevant_examples[:max_examples]

    def _format_examples(self, example_keys: List[str]) -> str:
        """
        Format selected SQL examples
        """
        formatted_examples = []
        for i, key in enumerate(example_keys, 1):
            if key in self.SQL_EXAMPLES:
                formatted_examples.append(f"Example {i}:\n{self.SQL_EXAMPLES[key]}")
        
        return "\n\n".join(formatted_examples) if formatted_examples else "-- No specific examples available"

    def _format_schema_info(self, intent: str) -> str:
        """
        Format schema information based on intent
        """
        schema_sections = []
        
        if 'work' in intent.lower():
            schema_sections.append(f"=== WORK FORCE ===\n{self.SCHEMA_INFO['work_force']}")
        
        if 'sales' in intent.lower() or 'revenue' in intent.lower() or 'customer' in intent.lower():
            schema_sections.append(f"=== SALES ===\n{self.SCHEMA_INFO['sales']}")
        
        if 'spare' in intent.lower() or 'parts' in intent.lower() or 'price' in intent.lower():
            schema_sections.append(f"=== SPARE PARTS ===\n{self.SCHEMA_INFO['spare_parts']}")
        
        return "\n\n".join(schema_sections) if schema_sections else self._get_all_schema_info()

    def _get_all_schema_info(self) -> str:
        """
        Get all schema information
        """
        return "\n\n".join([
            f"=== {category.upper()} ===\n{info}"
            for category, info in self.SCHEMA_INFO.items()
        ])

    # =================================================================
    # VALIDATION METHODS
    # =================================================================
    
    def validate_column_usage(self, sql: str, view_name: str) -> tuple[bool, List[str]]:
        """
        ตรวจสอบการใช้ columns ที่ไม่มีจริง
        """
        issues = []
        
        if view_name not in self.VIEW_COLUMNS:
            return True, []
        
        valid_columns = self.VIEW_COLUMNS[view_name]
        
        # Extract column references
        column_patterns = [
            rf'{view_name[2:]}\.(\w+)',  # v_table -> table.column
            rf'[a-zA-Z_]\.(\w+)',        # alias.column
            rf'FROM\s+{view_name}\s+[a-zA-Z_]+\s+.*?(\w+)\s*[,\s]'  # columns in SELECT
        ]
        
        used_columns = []
        for pattern in column_patterns:
            matches = re.findall(pattern, sql, re.IGNORECASE)
            used_columns.extend(matches)
        
        # Check for invalid columns
        for col in set(used_columns):
            if col.lower() not in [c.lower() for c in valid_columns]:
                issues.append(f"Invalid column '{col}' in {view_name}")
        
        return len(issues) == 0, issues

    def suggest_column_fix(self, invalid_column: str, view_name: str) -> Optional[str]:
        """
        แนะนำ column ที่ถูกต้อง
        """
        if view_name not in self.VIEW_COLUMNS:
            return None
        
        valid_columns = self.VIEW_COLUMNS[view_name]
        
        # Common mistakes mapping
        fixes = {
            'product_num': 'product_name',
            'job_type': 'job_description_pm',  # + CASE statement
            'amount': 'total_num',
            'price': 'unit_price_num',
            'quantity': 'balance_num'
        }
        
        return fixes.get(invalid_column.lower()) or self._find_similar_column(invalid_column, valid_columns)

    def _find_similar_column(self, target: str, valid_columns: List[str]) -> Optional[str]:
        """
        หา column ที่คล้ายกัน
        """
        target_lower = target.lower()
        
        # Exact partial match
        for col in valid_columns:
            if target_lower in col.lower() or col.lower() in target_lower:
                return col
        
        return None

    # =================================================================
    # UTILITY METHODS
    # =================================================================
    
    def get_view_columns(self, view_name: str) -> List[str]:
        """
        Get valid columns for a view
        """
        return self.VIEW_COLUMNS.get(view_name, [])

    def get_available_examples(self) -> List[str]:
        """
        Get list of available example keys
        """
        return list(self.SQL_EXAMPLES.keys())

    def get_schema_summary(self) -> Dict[str, Any]:
        """
        Get summary of schema information
        """
        return {
            'views': list(self.VIEW_COLUMNS.keys()),
            'total_examples': len(self.SQL_EXAMPLES),
            'schema_categories': list(self.SCHEMA_INFO.keys())
        }
