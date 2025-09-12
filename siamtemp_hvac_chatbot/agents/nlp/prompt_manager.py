# PromptManager - Production Ready Version 5.0
# File: agents/nlp/prompt_manager.py
# Optimized for Siamtemp HVAC Chatbot with 3-table structure

import json
import logging
import re
from typing import Dict, List, Any, Optional
from textwrap import dedent
from datetime import datetime

logger = logging.getLogger(__name__)

class PromptManager:

    def __init__(self, db_handler=None):
        self.db_handler = db_handler
        
        # Schema definition for 3 tables
        self.VIEW_COLUMNS = {
            'v_sales': [
                'id', 'year', 'job_no', 'customer_name', 'description',
                'overhaul_text', 'replacement_text', 'service_text', 
                'parts_text', 'product_text', 'solution_text',
                'overhaul_num', 'replacement_num', 'service_num',
                'parts_num', 'product_num', 'solution_num', 'total_revenue'
            ],
            'v_spare_part': [
                'id', 'wh', 'product_code', 'product_name', 'unit',
                'balance_text', 'unit_price_text', 'total_text',
                'balance_num', 'unit_price_num', 'total_num',
                'description', 'received'
            ],
            'v_work_force': [
                'id', 'date', 'customer', 'project',
                'job_description_pm', 'job_description_replacement',
                'job_description_overhaul', 'job_description_start_up',
                'job_description_support_all', 'job_description_cpa',
                'detail', 'duration', 'service_group',
                'success', 'unsuccessful', 'failure_reason',
                'report_kpi_2_days', 'report_over_kpi_2_days'
            ]
        }
        
        # Load production SQL examples
        self.SQL_EXAMPLES = self._load_production_examples()
        
        # System prompt - simplified and clear
        self.SQL_SYSTEM_PROMPT = self._get_system_prompt()
        
        # Precompiled patterns for performance
        self._compile_patterns()
        
        # Days in month mapping
        self.DAYS_IN_MONTH = {
            1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
            7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
        }
    
    def _compile_patterns(self):
        """Pre-compile regex patterns for better performance"""
        self.compiled_patterns = {
            'sales_yoy_growth': re.compile(r'เติบโต|อัตราการเติบโต|growth|yoy|year\s+over\s+year', re.IGNORECASE),
            'sales_zero_value': re.compile(r'ศูนย์|zero|ไม่มียอด|no\s+revenue', re.IGNORECASE),
            'parts_price_range': re.compile(r'ช่วงราคา|price\s+range|ระหว่าง|between', re.IGNORECASE),
            'warehouse_value': re.compile(r'มูลค่าคลัง|warehouse\s+value|valuation', re.IGNORECASE),
            'work_summary': re.compile(r'สรุปงาน.*เดือน|งานที่ทำ.*เดือน', re.IGNORECASE),
            'work_plan': re.compile(r'งานที่วางแผน|แผนงาน.*เดือน', re.IGNORECASE),
            'overhaul': re.compile(r'overhaul|โอเวอร์ฮอล|compressor|คอมเพรสเซอร์', re.IGNORECASE),
            'repair_history': re.compile(r'ประวัติการซ่อม|repair\s+history', re.IGNORECASE),
            'customer_history': re.compile(r'ประวัติ.*ลูกค้า|การซื้อขาย.*ย้อนหลัง', re.IGNORECASE),
            'spare_parts': re.compile(r'อะไหล่|ราคาอะไหล่|spare|parts', re.IGNORECASE),
            'sales_analysis': re.compile(r'วิเคราะห์การขาย|วิเคราะห์.*ยอดขาย', re.IGNORECASE),
            'top_parts_customers': re.compile(r'ลูกค้า.*ซื้ออะไหล่|ลูกค้า.*parts.*สูง|top.*parts.*customer', re.IGNORECASE),
            'service_vs_replacement': re.compile(r'เปรียบเทียบ.*service.*replacement|service.*กับ.*replacement', re.IGNORECASE),
            'solution_sales': re.compile(r'ยอด.*solution|solution.*สูง|ลูกค้า.*solution', re.IGNORECASE),
            'quarterly_summary': re.compile(r'ไตรมาส|quarterly|รายไตรมาส|quarter', re.IGNORECASE),
            'highest_value_items': re.compile(r'สินค้า.*มูลค่าสูง|มูลค่าสูงสุด.*คลัง|highest.*value.*item', re.IGNORECASE),
            'warehouse_summary': re.compile(r'สรุป.*คลัง|มูลค่า.*แต่ละคลัง|warehouse.*summary', re.IGNORECASE),
            'low_stock_items': re.compile(r'ใกล้หมด|สต็อกน้อย|สินค้าเหลือน้อย|low.*stock', re.IGNORECASE),
            'high_unit_price': re.compile(r'ราคาต่อหน่วยสูง|ราคาแพง|expensive.*parts|high.*price', re.IGNORECASE),
            'successful_work_monthly': re.compile(r'งานสำเร็จ|งานเสร็จ|successful.*work|completed.*work', re.IGNORECASE),
            'pm_work_summary': re.compile(r'งาน\s*pm|preventive.*maintenance|บำรุงรักษาเชิงป้องกัน', re.IGNORECASE),
            'startup_works': re.compile(r'start.*up|สตาร์ทอัพ|เริ่มเครื่อง|งานติดตั้ง', re.IGNORECASE),
            'kpi_reported_works': re.compile(r'kpi|รายงาน.*kpi|งาน.*kpi', re.IGNORECASE),
            'team_specific_works': re.compile(r'งาน.*ทีม|งาน.*สุพรรณ|งาน.*ช่าง|team.*work', re.IGNORECASE),
            'replacement_monthly': re.compile(r'งาน.*replacement|งานเปลี่ยน|replacement.*เดือน', re.IGNORECASE),
            'long_duration_works': re.compile(r'ใช้เวลานาน|หลายวัน|งานนาน|long.*duration', re.IGNORECASE),
            'customer_years': re.compile(r'ซื้อขาย.*กี่ปี|มีการซื้อขาย.*ปี|years.*operation|how.*many.*years', re.IGNORECASE),
        }
    
    def _load_production_examples(self) -> Dict[str, str]:
        """Load actual production SQL examples"""
        return {
            # Customer History - ประวัติลูกค้า
            'customer_history': dedent("""
                SELECT year AS year_label,
                       customer_name,
                       SUM(overhaul_num) AS overhaul,
                       SUM(replacement_num) AS replacement,
                       SUM(service_num) AS service,
                       SUM(parts_num) AS parts,
                       SUM(product_num) AS product,
                       SUM(solution_num) AS solution,
                       SUM(total_revenue) AS total_revenue
                FROM v_sales
                WHERE customer_name LIKE '%STANLEY%'
                  AND year IN ('2023','2024','2025')
                GROUP BY year, customer_name
                ORDER BY year, total_revenue DESC
                LIMIT 100;
            """).strip(),

            # Repair History - ประวัติการซ่อม
            'repair_history': dedent("""
                SELECT customer, detail, service_group
                FROM v_work_force
                WHERE customer LIKE '%STANLEY%'
                ORDER BY date DESC
                LIMIT 100;
            """).strip(),

            # Work Plan Specific Date - แผนงานวันที่เฉพาะ
            'work_plan_date': dedent("""
                SELECT id, date, customer, project,
                       job_description_pm, job_description_replacement,
                       detail, service_group
                FROM v_work_force
                WHERE date = '2025-09-05'
                ORDER BY customer
                LIMIT 100;
            """).strip(),

            # Work Monthly - งานรายเดือน (Production format)
            'work_monthly': dedent("""
                SELECT date, customer, detail
                FROM v_work_force
                WHERE date::date BETWEEN '2025-08-01' AND '2025-09-30'
                ORDER BY date
                LIMIT 200;
            """).strip(),

            # Work Summary Monthly - สรุปงานเดือน
            'work_summary_monthly': dedent("""
                SELECT date, customer, detail
                FROM v_work_force
                WHERE date::date BETWEEN '2025-06-01' AND '2025-06-30'
                ORDER BY date
                LIMIT 200;
            """).strip(),

            # Spare Parts Price - ราคาอะไหล่
            'spare_parts_price': dedent("""
                SELECT * 
                FROM v_spare_part
                WHERE product_name LIKE '%EKAC460%'
                   OR product_name LIKE '%EKAC460%'
                ORDER BY total_num DESC
                LIMIT 50;
            """).strip(),

            # Parts Search Multiple - ค้นหาอะไหล่หลายคำ
            'parts_search_multi': dedent("""
                SELECT *
                FROM v_spare_part
                WHERE product_name LIKE '%EK%'
                   OR product_name LIKE '%model%'
                   OR product_name LIKE '%EKAC460%'
                ORDER BY total_num DESC
                LIMIT 100;
            """).strip(),

            # Sales Analysis Multi Year - วิเคราะห์การขายหลายปี
            'sales_analysis': dedent("""
                SELECT year AS year_label,
                       SUM(overhaul_num) AS overhaul,
                       SUM(replacement_num) AS replacement,
                       SUM(service_num) AS service,
                       SUM(parts_num) AS parts,
                       SUM(product_num) AS product,
                       SUM(solution_num) AS solution
                FROM v_sales
                WHERE year IN ('2024','2025')
                GROUP BY year
                ORDER BY year;
            """).strip(),

            # Overhaul Sales - ยอดขาย overhaul
            'overhaul_sales': dedent("""
                SELECT year AS year_label,
                       SUM(overhaul_num) AS overhaul
                FROM v_sales
                WHERE year IN ('2024','2025')
                GROUP BY year
                ORDER BY year;
            """).strip(),

            # Top Customers - ลูกค้าสูงสุด
            'top_customers': dedent("""
                SELECT customer_name,
                       COUNT(*) AS transaction_count,
                       SUM(total_revenue) AS total_revenue
                FROM v_sales
                WHERE year = '2024'
                  AND total_revenue > 0
                GROUP BY customer_name
                ORDER BY total_revenue DESC
                LIMIT 10;
            """).strip(),

            # Inventory Check - ตรวจสอบสินค้าคงคลัง
            'inventory_check': dedent("""
                SELECT product_code, product_name, wh AS warehouse,
                       balance_num AS stock_quantity,
                       unit_price_num AS unit_price,
                       (balance_num * unit_price_num) AS total_value
                FROM v_spare_part
                WHERE balance_num > 0
                ORDER BY total_value DESC
                LIMIT 100;
            """).strip(),

            # Year over Year Growth - อัตราการเติบโต
            'sales_yoy_growth': dedent("""
                WITH yearly AS (
                  SELECT year::int AS y, SUM(total_revenue) AS total
                  FROM v_sales
                  WHERE year IN ('2024','2025')
                  GROUP BY year
                )
                SELECT curr.y AS year,
                       curr.total AS total_revenue,
                       LAG(curr.total) OVER (ORDER BY curr.y) AS prev_total,
                       ROUND(((curr.total - LAG(curr.total) OVER (ORDER BY curr.y))
                             / LAG(curr.total) OVER (ORDER BY curr.y)) * 100, 2) AS yoy_percent
                FROM yearly curr
                ORDER BY year;
            """).strip(),
            'top_parts_customers': dedent("""
                SELECT customer_name,
                    SUM(parts_num) AS total_parts,
                    COUNT(*) AS order_count
                FROM v_sales
                WHERE year = '2024'
                AND parts_num > 0
                GROUP BY customer_name
                ORDER BY total_parts DESC
                LIMIT 10;
            """).strip(),

            # เปรียบเทียบยอดขาย service vs replacement
            'service_vs_replacement': dedent("""
                SELECT year,
                    SUM(service_num) AS total_service,
                    SUM(replacement_num) AS total_replacement,
                    SUM(service_num + replacement_num) AS combined_total
                FROM v_sales
                WHERE year IN ('2023','2024','2025')
                GROUP BY year
                ORDER BY year;
            """).strip(),

            # ลูกค้าที่มียอด solution สูงสุด
            'solution_customers': dedent("""
                SELECT customer_name,
                    job_no,
                    description,
                    solution_num
                FROM v_sales
                WHERE year = '2025'
                AND solution_num > 0
                ORDER BY solution_num DESC
                LIMIT 20;
            """).strip(),

            # สรุปยอดขายรายไตรมาส (จำลอง)
            'quarterly_summary': dedent("""
                SELECT year,
                    CASE 
                        WHEN job_no LIKE 'JAE%-01-%' OR job_no LIKE 'JAE%-02-%' OR job_no LIKE 'JAE%-03-%' THEN 'Q1'
                        WHEN job_no LIKE 'JAE%-04-%' OR job_no LIKE 'JAE%-05-%' OR job_no LIKE 'JAE%-06-%' THEN 'Q2'
                        WHEN job_no LIKE 'JAE%-07-%' OR job_no LIKE 'JAE%-08-%' OR job_no LIKE 'JAE%-09-%' THEN 'Q3'
                        ELSE 'Q4'
                    END AS quarter,
                    SUM(total_revenue) AS revenue
                FROM v_sales
                WHERE year = '2024'
                GROUP BY year, quarter
                ORDER BY quarter;
            """).strip(),

            # ========== v_spare_part Examples ==========

            # สินค้าที่มีมูลค่าสูงสุดในคลัง
            'highest_value_items': dedent("""
                SELECT product_code,
                    product_name,
                    balance_num AS quantity,
                    unit_price_num AS unit_price,
                    total_num AS total_value
                FROM v_spare_part
                WHERE total_num > 50000
                ORDER BY total_num DESC
                LIMIT 20;
            """).strip(),

            # สรุปมูลค่าสินค้าแต่ละคลัง
            'warehouse_summary': dedent("""
                SELECT wh AS warehouse,
                    COUNT(*) AS item_count,
                    SUM(balance_num) AS total_items,
                    SUM(total_num) AS total_value
                FROM v_spare_part
                WHERE balance_num > 0
                GROUP BY wh
                ORDER BY total_value DESC;
            """).strip(),

            # ค้นหาสินค้าที่ใกล้หมด (balance น้อย)
            'low_stock_items': dedent("""
                SELECT product_code,
                    product_name,
                    balance_num AS current_stock,
                    unit_price_num AS unit_price
                FROM v_spare_part
                WHERE balance_num > 0 AND balance_num <= 5
                ORDER BY balance_num, unit_price_num DESC
                LIMIT 50;
            """).strip(),

            # สินค้าที่มีราคาต่อหน่วยสูง
            'high_unit_price': dedent("""
                SELECT product_code,
                    product_name,
                    unit_price_num AS unit_price,
                    balance_num AS stock
                FROM v_spare_part
                WHERE unit_price_num > 10000
                ORDER BY unit_price_num DESC
                LIMIT 30;
            """).strip(),

            # ========== v_work_force Examples ==========

            # งานที่ทำสำเร็จในเดือนที่กำหนด
            'successful_work_monthly': dedent("""
                SELECT date,
                    customer,
                    detail,
                    service_group
                FROM v_work_force
                WHERE date::date BETWEEN '2025-07-01' AND '2025-07-31'
                AND success = '1'
                ORDER BY date;
            """).strip(),

            # สรุปงาน PM (Preventive Maintenance)
            'pm_work_summary': dedent("""
                SELECT date,
                    customer,
                    project,
                    detail
                FROM v_work_force
                WHERE date::date BETWEEN '2025-06-01' AND '2025-08-31'
                AND job_description_pm = true
                ORDER BY date DESC
                LIMIT 100;
            """).strip(),

            # งาน Start Up ในช่วงเวลา
            'startup_works': dedent("""
                SELECT date,
                    customer,
                    project,
                    detail,
                    duration
                FROM v_work_force
                WHERE date::date BETWEEN '2025-05-01' AND '2025-07-31'
                AND job_description_start_up = '1'
                ORDER BY date DESC;
            """).strip(),

            # งานที่มีการรายงาน KPI
            'kpi_reported_works': dedent("""
                SELECT date,
                    customer,
                    detail,
                    report_kpi_2_days,
                    report_over_kpi_2_days
                FROM v_work_force
                WHERE date::date BETWEEN '2025-05-01' AND '2025-05-31'
                AND (report_kpi_2_days = '1' OR report_over_kpi_2_days = '1')
                ORDER BY date;
            """).strip(),

            # งานของทีมบริการเฉพาะ
            'team_specific_works': dedent("""
                SELECT date,
                    customer,
                    detail,
                    service_group
                FROM v_work_force
                WHERE date::date BETWEEN '2025-07-01' AND '2025-07-31'
                AND service_group LIKE '%สุพรรณ%'
                ORDER BY date;
            """).strip(),

            # งาน Replacement ประจำเดือน
            'replacement_monthly': dedent("""
                SELECT date,
                    customer,
                    project,
                    detail
                FROM v_work_force
                WHERE date::date BETWEEN '2025-07-01' AND '2025-07-31'
                AND job_description_replacement = true
                ORDER BY date;
            """).strip(),

            # ========== Cross-table Examples ==========

            # ตรวจสอบลูกค้าที่มีทั้งยอดขายและงานบริการ
            'customer_sales_and_service': dedent("""
                WITH sales_customers AS (
                    SELECT DISTINCT customer_name
                    FROM v_sales
                    WHERE year = '2025'
                ),
                service_customers AS (
                    SELECT DISTINCT customer
                    FROM v_work_force
                    WHERE date::date >= '2025-01-01'
                )
                SELECT 
                    sc.customer_name,
                    EXISTS(SELECT 1 FROM service_customers wc 
                        WHERE wc.customer LIKE '%' || SPLIT_PART(sc.customer_name, ' ', 1) || '%') AS has_service
                FROM sales_customers sc
                LIMIT 50;
            """).strip(),

            # วิเคราะห์งานที่ใช้เวลานาน
            'long_duration_works': dedent("""
                SELECT date,
                    customer,
                    detail,
                    duration,
                    service_group
                FROM v_work_force
                WHERE date::date BETWEEN '2025-05-01' AND '2025-07-31'
                AND duration LIKE '%วัน%'
                ORDER BY date DESC;
            """).strip(),

            'customer_years_count': dedent("""
            SELECT year,
                COUNT(*) AS transaction_count,
                SUM(total_revenue) AS total_revenue
            FROM v_sales
            WHERE customer_name LIKE '%ABB%'
            GROUP BY year
            ORDER BY year;
        """).strip(),

        }
    
    def _get_system_prompt(self) -> str:
        """Enhanced system prompt - เน้นย้ำว่ามี table เดียว"""
        return dedent("""
        ⚠️ CRITICAL: You have EXACTLY 3 tables. NO OTHER TABLES EXIST!
        
        1. v_sales (ONE table for ALL years):
        - Contains data from 2022, 2023, 2024, 2025 in single table
        - Use WHERE year IN ('2023','2024','2025') to filter years
        - ❌ NEVER use v_sales2023, v_sales2024 - THEY DON'T EXIST!
        - ✅ ALWAYS use: FROM v_sales WHERE year = 'YYYY'
        
        2. v_work_force (work/repair records):  
        - Has: date, customer, detail, project, service_group
        - Use date::date for date comparisons
        
        3. v_spare_part (inventory):
        - Has: product_code, product_name, balance_num, unit_price_num
        
        ⚠️ FORBIDDEN - THESE TABLES DO NOT EXIST:
        - v_sales2022, v_sales2023, v_sales2024, v_sales2025
        - sales_2023, sales_2024, sales_2025
        - Any year-specific table variants
        
        ALWAYS use the EXACT structure from the provided example!
        """).strip()

    # ===== VALIDATION METHODS =====
    
    def validate_sql_safety(self, sql: str) -> tuple[bool, str]:
        """Validate SQL for dangerous operations"""
        if not sql:
            return False, "Empty SQL query"
        
        sql_lower = sql.lower()
        
        # Check for dangerous keywords
        dangerous_keywords = [
            'drop', 'delete', 'truncate', 'alter', 'create',
            'insert', 'update', 'grant', 'revoke', 'exec',
            'execute', 'shutdown', 'backup', 'restore'
        ]
        
        for keyword in dangerous_keywords:
            if re.search(rf'\b{keyword}\b', sql_lower):
                return False, f"Dangerous operation '{keyword}' not allowed"
        
        # Check for multiple statements
        if ';' in sql.strip()[:-1]:
            return False, "Multiple statements not allowed"
        
        # Basic structure check
        if not sql_lower.strip().startswith('select'):
            return False, "Only SELECT queries allowed"
        
        return True, "Query is safe"
    
    def validate_input(self, text: str, max_length: int = 500) -> tuple[bool, str]:
        """Validate user input"""
        if not text or not text.strip():
            return False, "Input cannot be empty"
        
        if len(text) > max_length:
            return False, f"Input too long (max {max_length} characters)"
        
        return True, "Input is valid"
    
    def validate_entities(self, entities: Dict) -> Dict:
        """Validate and sanitize entities - FIXED VERSION"""
        validated = {}
        
        # Validate years - แก้ไขการแปลงซ้ำซ้อน
        if entities.get('years'):
            valid_years = []
            for year in entities['years']:
                try:
                    # Debug logging
                    logger.debug(f"Processing year: {year}, type: {type(year)}")
                    
                    # ถ้าเป็น int
                    if isinstance(year, int):
                        # ถ้าอยู่ในช่วง AD ที่ถูกต้องแล้ว (2020-2030)
                        if 2020 <= year <= 2030:
                            valid_years.append(str(year))
                            logger.debug(f"Year {year} is valid AD year, using as-is")
                        # ถ้าเป็นปี พ.ศ. (> 2500)
                        elif year > 2500:
                            converted = str(year - 543)
                            valid_years.append(converted)
                            logger.debug(f"Converted Buddhist year {year} to AD {converted}")
                        else:
                            logger.warning(f"Year {year} out of valid range")
                    
                    # ถ้าเป็น string
                    elif isinstance(year, str):
                        year_int = int(year)
                        # ถ้าอยู่ในช่วง AD ที่ถูกต้องแล้ว
                        if 2020 <= year_int <= 2030:
                            valid_years.append(year)
                            logger.debug(f"String year {year} is valid AD year, using as-is")
                        # ถ้าเป็นปี พ.ศ.
                        elif year_int > 2500:
                            converted = str(year_int - 543)
                            valid_years.append(converted)
                            logger.debug(f"Converted Buddhist string year {year} to AD {converted}")
                        else:
                            logger.warning(f"String year {year} out of valid range")
                            
                except (ValueError, TypeError) as e:
                    logger.error(f"Error processing year {year}: {e}")
                    continue
            
            if valid_years:
                validated['years'] = valid_years
                logger.info(f"Final validated years: {valid_years}")
        
        # Validate months
        if entities.get('months'):
            valid_months = [m for m in entities['months'] if 1 <= m <= 12]
            if valid_months:
                validated['months'] = valid_months
        
        # Validate dates
        if entities.get('dates'):
            valid_dates = []
            for date in entities['dates']:
                if isinstance(date, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', date):
                    valid_dates.append(date)
            if valid_dates:
                validated['dates'] = valid_dates
        
        # Sanitize customer names
        if entities.get('customers'):
            sanitized = []
            for customer in entities['customers']:
                clean = re.sub(r'[;\'\"\\]', '', customer).strip()[:50]
                if clean:
                    sanitized.append(clean)
            if sanitized:
                validated['customers'] = sanitized
        
        # Sanitize product codes
        if entities.get('products'):
            sanitized = []
            for product in entities['products']:
                clean = re.sub(r'[^a-zA-Z0-9\-_]', '', product)[:30]
                if clean:
                    sanitized.append(clean)
            if sanitized:
                validated['products'] = sanitized
        
        return validated
    
    # ===== UTILITY METHODS =====
    
    def convert_thai_year(self, year_value) -> str:
        """Convert Thai Buddhist year to AD year"""
        try:
            year = int(year_value)
            if year > 2500:  # Thai Buddhist Era
                return str(year - 543)
            elif year < 100:  # 2-digit year
                if year >= 50:
                    return str(2500 + year - 543)
                else:
                    return str(2600 + year - 543)
            return str(year)  # Already AD
        except (ValueError, TypeError):
            return '2025'
    
    def _is_leap_year(self, year: int) -> bool:
        """Check if year is a leap year"""
        return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
    
    def _get_last_day_of_month(self, year: int, month: int) -> int:
        """Get the last day of a given month"""
        if month == 2 and self._is_leap_year(year):
            return 29
        return self.DAYS_IN_MONTH.get(month, 31)
    
    # ===== MAIN METHODS =====
    
    def build_sql_prompt(self, question: str, intent: str, entities: Dict,
                        context: Dict = None, examples_override: List[str] = None) -> str:
        """Build SQL generation prompt with ULTRA STRICT COPY enforcement"""
        
        try:
            # Validate input
            is_valid, msg = self.validate_input(question)
            if not is_valid:
                logger.error(f"Invalid input: {msg}")
                raise ValueError(f"Invalid input: {msg}")
            
            # Validate and convert entities
            entities = self.validate_entities(entities)
            question = re.sub(r'\b25[67]\d\b', lambda m: str(int(m.group())-543), question)
            # Use context if provided
            if context:
                logger.debug(f"Using context: {context}")
                for key, value in context.get('entities', {}).items():
                    if key not in entities or not entities[key]:
                        entities[key] = value
            
            # Select best matching example
            example = self._select_best_example(question, intent, entities)

            # Log selected example for debugging
            example_name = self._get_example_name(example)
            logger.info(f"Selected SQL example: {example_name}")
            
            # Build entity-specific hints
            hints = self._build_sql_hints(entities, intent)
            
            # ===== DETERMINE IF WE HAVE EXACT MATCH =====
            has_exact_example = self._has_exact_matching_example(question, example_name)
            
            if has_exact_example:
                # ===== ULTRA STRICT MODE FOR EXACT MATCHES =====
                prompt = dedent(f"""
                You are a SQL query generator. Output ONLY the SQL query with no explanation.
                
                EXAMPLE SQL TO COPY:
                ----------------------------------------
                {example}
                ----------------------------------------
                
                COPY RULES:
                1. COPY the SELECT clause structure EXACTLY
                2. COPY the FROM clause EXACTLY  
                3. COPY the WHERE clause pattern, only change the search value
                4. COPY the GROUP BY if present
                5. COPY the ORDER BY if present
                
                ONLY ALLOWED CHANGES:
                - Customer name in WHERE clause: Change to '{entities.get('customers', [''])[0] if entities.get('customers') else 'search_term'}'
                - Year values in WHERE clause: Change to {entities.get('years', [])}
                - Product code: Change to '{entities.get('products', [''])[0] if entities.get('products') else 'search_term'}'
                
                ⚠️ DO NOT:
                - Change column names
                - Add or remove columns
                - Change aggregation functions
                - Add WHERE clauses not in the example
                - Remove GROUP BY if example has it
                
                YOUR TASK: {question}
                
                FINAL INSTRUCTION: COPY THE EXAMPLE, change ONLY the search values!
                
                SQL:
                """).strip()
            
            else:
                # ===== STRICT SCHEMA MODE FOR NON-EXACT MATCHES =====
                schema_definition = self._get_strict_schema_for_intent(intent)
                column_rules = self._get_column_rules_for_intent(intent)
                error_examples = self._get_common_errors()
                
                prompt = dedent(f"""
                You are a SQL query generator. Output ONLY the SQL query with no explanation.
                ⚠️ STRICT MODE: Follow the example structure closely!
                
                {schema_definition}
                
                REFERENCE EXAMPLE:
                ----------------------------------------
                {example}
                ----------------------------------------
                
                IMPORTANT:
                - Use the SAME SELECT structure as the example
                - Use the SAME table as the example
                - Follow the WHERE clause pattern from the example
                
                {column_rules}
                
                {error_examples}
                
                YOUR TASK: {question}
                
                {hints}
                
                CHECKLIST:
                ✓ Did you use the same columns as the example?
                ✓ Did you copy the FROM clause?
                ✓ Did you follow the WHERE pattern?
                ✓ Did you keep GROUP BY if example has it?
                
                SQL:
                """).strip()
            
            return prompt
            
        except Exception as e:
            logger.error(f"Failed to build SQL prompt: {e}")
            return self._get_fallback_prompt(question)

    def _has_exact_matching_example(self, question: str, example_name: str) -> bool:
        """Check if we have an exact matching example for this question type"""
        question_lower = question.lower()
        
        # Define exact match patterns
        exact_matches = {
            'customer_years_count': ['ซื้อขายมากี่ปี', 'กี่ปีแล้ว', 'how many years'],
            'customer_history': ['ประวัติลูกค้า', 'ประวัติการซื้อขาย', 'customer history'],
            'work_monthly': ['งานที่วางแผน', 'แผนงาน', 'work plan'],
            'overhaul_sales': ['ยอดขาย overhaul', 'overhaul sales'],
            'sales_analysis': ['วิเคราะห์การขาย', 'sales analysis'],
            'spare_parts_price': ['ราคาอะไหล่', 'spare parts price'],
        }
        
        # Check if current example has exact match patterns
        if example_name in exact_matches:
            patterns = exact_matches[example_name]
            for pattern in patterns:
                if pattern in question_lower:
                    logger.info(f"Found exact match pattern '{pattern}' for example '{example_name}'")
                    return True
        
        return False

    def _get_strict_schema_for_intent(self, intent: str) -> str:
        """Get exact schema based on intent"""
        
        if intent in ['sales', 'sales_analysis', 'customer_history', 'overhaul_report', 
                    'top_customers', 'revenue', 'annual_revenue']:
            return dedent("""
            TABLE: v_sales
            EXACT COLUMNS (use ONLY these):
            - year (varchar) → filter: WHERE year = '2024'
            - customer_name (varchar) → filter: WHERE customer_name LIKE '%X%'
            - job_no (varchar)
            - description (varchar)
            - total_revenue (numeric) → FOR ALL REVENUE/INCOME QUERIES
            - overhaul_num (numeric) → overhaul amount
            - replacement_num (numeric) → replacement amount
            - service_num (numeric) → service amount
            - parts_num (numeric) → parts amount
            - product_num (numeric) → product amount
            - solution_num (numeric) → solution amount
            
            ⚠️ FORBIDDEN COLUMNS (DO NOT USE):
            - sales_num ❌ (does not exist)
            - revenue_num ❌ (use total_revenue)
            - sales_amount ❌ (use total_revenue)
            - total_sales ❌ (use total_revenue)
            """)
        
        elif intent in ['work_plan', 'work_force', 'work_summary', 'repair_history',
                        'pm_summary', 'startup_summary', 'successful_works']:
            return dedent("""
            TABLE: v_work_force
            EXACT COLUMNS (use ONLY these):
            - date (date) → filter: WHERE date::date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
            - customer (varchar) → filter: WHERE customer LIKE '%X%'
            - project (varchar)
            - detail (varchar)
            - duration (varchar)
            - service_group (varchar)
            - job_description_pm (boolean) → filter: WHERE job_description_pm = true
            - job_description_replacement (boolean)
            - success (varchar)
            
            ⚠️ FORBIDDEN COLUMNS (DO NOT USE):
            - work_date ❌ (use date)
            - customer_name ❌ (use customer)
            - job_detail ❌ (use detail)
            """)
        
        elif intent in ['spare_parts', 'parts_price', 'inventory_check', 
                        'inventory_value', 'warehouse_summary']:
            return dedent("""
            TABLE: v_spare_part
            EXACT COLUMNS (use ONLY these):
            - product_code (varchar)
            - product_name (varchar)
            - wh (varchar) → warehouse
            - balance_num (numeric) → stock quantity
            - unit_price_num (numeric) → price per unit
            - total_num (numeric) → total value
            - unit (varchar)
            - description (varchar)
            
            ⚠️ FORBIDDEN COLUMNS (DO NOT USE):
            - stock_num ❌ (use balance_num)
            - quantity ❌ (use balance_num)
            - price ❌ (use unit_price_num)
            """)
        
        else:
            # Default to v_sales for unknown intents
            return self._get_strict_schema_for_intent('sales')

    def _get_column_rules_for_intent(self, intent: str) -> str:
        """Get specific rules for common queries"""
        
        rules_map = {
            'sales': """
            COLUMN USAGE RULES:
            - Question about "รายได้" → USE: SUM(total_revenue)
            - Question about "ยอดขาย" → USE: SUM(total_revenue) 
            - Question about "overhaul" → USE: SUM(overhaul_num)
            - Question about "service" → USE: SUM(service_num)
            - Question about "parts/อะไหล่" → USE: SUM(parts_num)
            """,
            
            'work_plan': """
            COLUMN USAGE RULES:
            - Always SELECT: date, customer, detail
            - For date ranges: WHERE date::date BETWEEN 'start' AND 'end'
            - For PM jobs: WHERE job_description_pm = true
            - Never use COUNT(*) for "มีกี่งาน" - show the actual records
            """,
            
            'spare_parts': """
            COLUMN USAGE RULES:
            - Stock quantity → USE: balance_num
            - Unit price → USE: unit_price_num
            - Total value → USE: total_num
            - Search by name: WHERE product_name LIKE '%X%'
            """
        }
        
        return rules_map.get(intent, "")

    def _get_common_errors(self) -> str:
        """Show common mistakes to avoid"""
        return dedent("""
        ❌ COMMON ERRORS TO AVOID:
        
        WRONG: SELECT SUM(sales_num) FROM v_sales
        RIGHT: SELECT SUM(total_revenue) FROM v_sales
        
        WRONG: SELECT * FROM v_sales2024
        RIGHT: SELECT * FROM v_sales WHERE year = '2024'
        
        WRONG: SELECT COUNT(*) FROM v_work_force WHERE...
        RIGHT: SELECT date, customer, detail FROM v_work_force WHERE...
        
        WRONG: SELECT stock_num FROM v_spare_part
        RIGHT: SELECT balance_num FROM v_spare_part
        """)


    
    def _get_example_name(self, example: str) -> str:
        """Get example name for logging"""
        for name, sql in self.SQL_EXAMPLES.items():
            if sql == example:
                return name
        return 'custom'
    
    def _get_fallback_prompt(self, question: str) -> str:
        """Generate a safe fallback prompt"""
        return dedent(f"""
        {self.SQL_SYSTEM_PROMPT}
        
        Generate a simple SQL query for: {question}
        
        Use v_sales for sales data, v_work_force for work data, v_spare_part for inventory.
        """).strip()
    
    def _select_best_example(self, question: str, intent: str, entities: Dict) -> str:
        """Select most relevant example"""
        question_lower = question.lower()
        
        logger.debug(f"Selecting example for intent: {intent}, question: {question_lower[:50]}...")
        
        # PRIORITY 1: Work plan with months - ALWAYS use work_monthly
        if intent == 'work_plan' and entities.get('months'):
            logger.info("Selected: work_monthly (work_plan with months)")
            return self.SQL_EXAMPLES['work_monthly']
        
        # PRIORITY 2: Specific patterns
        if 'งานที่วางแผน' in question_lower or 'แผนงาน' in question_lower:
            if entities.get('months'):
                logger.info("Selected: work_monthly (งานที่วางแผน with months)")
                return self.SQL_EXAMPLES['work_monthly']
            elif entities.get('dates'):
                logger.info("Selected: work_plan_date (specific date)")
                return self.SQL_EXAMPLES['work_plan_date']
            else:
                logger.info("Selected: work_monthly (default for งานที่วางแผน)")
                return self.SQL_EXAMPLES['work_monthly']
        
        # PRIORITY 3: Work summaries
        if 'สรุปงาน' in question_lower and 'เดือน' in question_lower:
            logger.info("Selected: work_summary_monthly")
            return self.SQL_EXAMPLES['work_summary_monthly']
        
        # PRIORITY 4: Check compiled patterns
        for pattern_name, pattern in self.compiled_patterns.items():
            if pattern.search(question_lower):
                if pattern_name in self.SQL_EXAMPLES:
                    logger.info(f"Selected: {pattern_name} (pattern match)")
                    return self.SQL_EXAMPLES[pattern_name]
        
        # PRIORITY 5: Parts/spare parts
        if 'อะไหล่' in question_lower or 'ราคาอะไหล่' in question_lower:
            if any(word in question_lower for word in ['ek', 'model', 'ekac']):
                logger.info("Selected: parts_search_multi")
                return self.SQL_EXAMPLES['parts_search_multi']
            logger.info("Selected: spare_parts_price")
            return self.SQL_EXAMPLES['spare_parts_price']
        
        # PRIORITY 6: Sales analysis
        if 'วิเคราะห์' in question_lower or len(entities.get('years', [])) > 1:
            logger.info("Selected: sales_analysis")
            return self.SQL_EXAMPLES['sales_analysis']
        
        # PRIORITY 7: Customer history
        if 'ประวัติ' in question_lower or 'ย้อนหลัง' in question_lower:
            if 'ซ่อม' in question_lower:
                logger.info("Selected: repair_history")
                return self.SQL_EXAMPLES['repair_history']
            logger.info("Selected: customer_history")
            return self.SQL_EXAMPLES['customer_history']
        
        if ('กี่ปี' in question_lower or 'จำนวนปี' in question_lower) and \
            ('ซื้อขาย' in question_lower or 'การซื้อขาย' in question_lower):
            if entities.get('customers'):
                logger.info("Selected: customer_years_count (years of operation)")
                return self.SQL_EXAMPLES['customer_years_count']
        
        # PRIORITY 8: Default by intent
        intent_map = {
            'work_plan': self.SQL_EXAMPLES['work_monthly'],
            'work_force': self.SQL_EXAMPLES['work_monthly'],
            'work_summary': self.SQL_EXAMPLES['work_summary_monthly'],
            'repair_history': self.SQL_EXAMPLES['repair_history'],
            'customer_history': self.SQL_EXAMPLES['customer_history'],
            'spare_parts': self.SQL_EXAMPLES['spare_parts_price'],
            'parts_price': self.SQL_EXAMPLES['spare_parts_price'],
            'sales': self.SQL_EXAMPLES['sales_analysis'],
            'sales_analysis': self.SQL_EXAMPLES['sales_analysis'],
            'overhaul_report': self.SQL_EXAMPLES['overhaul_sales'],
            'top_customers': self.SQL_EXAMPLES['top_customers'],
            'inventory_check': self.SQL_EXAMPLES['inventory_check'],
            'top_parts_customers': 'top_parts_customers',
            'top_parts_customers': self.SQL_EXAMPLES['top_parts_customers'],
            'service_comparison': self.SQL_EXAMPLES['service_vs_replacement'], 
            'solution_sales': self.SQL_EXAMPLES['solution_customers'],
            'quarterly_sales': self.SQL_EXAMPLES['quarterly_summary'],
            'inventory_value': self.SQL_EXAMPLES['highest_value_items'],
            'warehouse_analysis': self.SQL_EXAMPLES['warehouse_summary'],
            'low_stock': self.SQL_EXAMPLES['low_stock_items'],
            'expensive_parts': self.SQL_EXAMPLES['high_unit_price'],
            'successful_works': self.SQL_EXAMPLES['successful_work_monthly'],
            'pm_summary': self.SQL_EXAMPLES['pm_work_summary'],
            'startup_summary': self.SQL_EXAMPLES['startup_works'],
            'kpi_works': self.SQL_EXAMPLES['kpi_reported_works'],
            'team_works': self.SQL_EXAMPLES['team_specific_works'],
            'replacement_works': self.SQL_EXAMPLES['replacement_monthly'],
            'customer_years': self.SQL_EXAMPLES['customer_years_count'],
            'years_of_operation': self.SQL_EXAMPLES['customer_years_count'],
            'long_works': self.SQL_EXAMPLES['long_duration_works']
        }
        
        if intent in intent_map:
            logger.info(f"Selected: {intent} (intent default)")
            return intent_map[intent]
        
        # Final default
        logger.info("Selected: sales_analysis (final default)")
        return self.SQL_EXAMPLES['sales_analysis']
    
    def _build_sql_hints(self, entities: Dict, intent: str) -> str:
        """Build SQL hints with table specification - FIXED VERSION"""
        hints = []
        
        # Table specification based on intent
        if intent in ['work_plan', 'work_force', 'work_summary', 'repair_history']:
            hints.append("USE TABLE: v_work_force")
            hints.append("Format: WHERE date::date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'")
            hints.append("SELECT: date, customer, detail (do NOT use COUNT)")
        elif intent in ['sales', 'sales_analysis', 'customer_history', 'top_customers', 'overhaul_report']:
            hints.append("USE TABLE: v_sales")
            hints.append("Filter by year column")
        elif intent in ['spare_parts', 'parts_price', 'inventory_value', 'inventory_check']:
            hints.append("USE TABLE: v_spare_part")
        
        # Year hints - ไม่แปลงซ้ำ! ใช้ค่าที่ validate แล้ว
        if entities.get('years'):
            years = entities['years']
            
            # Log for debugging
            logger.debug(f"Building SQL hints for years: {years}")
            
            # ใช้ค่าที่ผ่าน validate_entities มาแล้วโดยตรง
            # ไม่เรียก convert_thai_year อีก!
            year_list = []
            for year in years:
                # แค่แปลงเป็น string ถ้าจำเป็น
                year_str = str(year)
                year_list.append(year_str)
                logger.debug(f"Using year: {year_str}")
            
            year_str = "', '".join(year_list)
            
            if intent in ['sales', 'sales_analysis', 'customer_history', 'top_customers', 'overhaul_report']:
                hint = f"WHERE year IN ('{year_str}')"
                hints.append(hint)
                logger.info(f"Generated year hint: {hint}")
        
        # Date hints
        if entities.get('dates'):
            date = entities['dates'][0]
            hints.append(f"WHERE date = '{date}'")
        
        # Month range hints
        if entities.get('months'):
            months = entities['months']
            year = int(entities.get('years', ['2025'])[0])
            
            if len(months) == 1:
                month = months[0]
                last_day = self._get_last_day_of_month(year, month)
                hints.append(f"WHERE date::date BETWEEN '{year}-{month:02d}-01' "
                        f"AND '{year}-{month:02d}-{last_day:02d}'")
            else:
                min_month = min(months)
                max_month = max(months)
                last_day = self._get_last_day_of_month(year, max_month)
                hints.append(f"WHERE date::date BETWEEN '{year}-{min_month:02d}-01' "
                        f"AND '{year}-{max_month:02d}-{last_day:02d}'")
        
        # Customer hints
        if entities.get('customers'):
            customer = entities['customers'][0]
            if 'sales' in intent or 'customer_history' in intent:
                hints.append(f"WHERE customer_name LIKE '%{customer}%'")
            else:
                hints.append(f"WHERE customer LIKE '%{customer}%'")
        
        # Product hints
        if entities.get('products'):
            product = entities['products'][0]
            hints.append(f"WHERE product_name LIKE '%{product}%' OR product_name LIKE '%{product}%'")
        
        return '\n'.join(hints) if hints else ""
    
    def build_response_prompt(self, question: str, results: List[Dict],
                            sql_query: str, locale: str = "th") -> str:
        """Build response generation prompt - แก้ไขให้แสดงข้อมูลครบ"""
        if not results:
            return f"ไม่พบข้อมูลสำหรับคำถาม: {question}"
        
        stats = self._analyze_results(results)
        
        # ปรับจำนวน sample และไม่ตัด string
        if len(results) <= 20:
            # ถ้าข้อมูลไม่เกิน 20 รายการ ส่งทั้งหมด
            sample = results
            sample_json = json.dumps(sample, ensure_ascii=False, default=str)
        else:
            # ถ้าเกิน 20 รายการ ส่งแค่ 20 รายการแรก
            sample = results[:20]
            sample_json = json.dumps(sample, ensure_ascii=False, default=str)
            # ถ้า json ยาวเกินไป ค่อยตัด
            if len(sample_json) > 3000:
                sample_json = sample_json[:3000] + "..."
        
        prompt = dedent(f"""
        สรุปข้อมูลสำหรับคำถาม: {question}
        
        พบข้อมูลทั้งหมด: {len(results)} รายการ
        {stats}
        
        ข้อมูลทั้งหมด ({len(sample)} รายการ):
        {sample_json}
        
        กรุณาสรุปข้อมูลให้ครบถ้วน:
        1. จำนวนรายการทั้งหมดที่พบ
        2. รายละเอียดของแต่ละรายการ (แสดงให้ครบทุกรายการ)
        3. ข้อสังเกตหรือการวิเคราะห์
        
        ข้อกำหนดสำคัญ:
        ⚠️ ห้ามแต่งหรือเพิ่มข้อมูลที่ไม่มีอยู่ในข้อมูลต้นฉบับ
        ✓ ใช้ข้อมูลจากที่ให้มาเท่านั้น
        ✓ หากข้อมูลมีมากกว่าที่แสดง ให้ระบุไว้ชัดเจน


        ตอบภาษาไทยแบบกระชับ ตรงประเด็น อ่านง่าย:
        """)
        
        return prompt
    
    def _analyze_results(self, results: List[Dict]) -> str:
        """Analyze query results"""
        if not results:
            return ""
        
        stats = []
        
        try:
            # Check for revenue fields
            revenue_fields = ['total_revenue', 'total', 'overhaul', 'total_value']
            for field in revenue_fields:
                if field in results[0]:
                    total = 0
                    for r in results:
                        try:
                            value = r.get(field, 0) or 0
                            if isinstance(value, (int, float)):
                                total += value
                            elif isinstance(value, str):
                                cleaned = re.sub(r'[^0-9.-]', '', value)
                                if cleaned:
                                    total += float(cleaned)
                        except:
                            continue
                    
                    if total > 0:
                        stats.append(f"ยอดรวม ({field}): {total:,.0f} บาท")
                    break
            
            # Check for year
            if 'year' in results[0] or 'year_label' in results[0]:
                field = 'year' if 'year' in results[0] else 'year_label'
                years = set(r.get(field) for r in results if r.get(field))
                if years:
                    stats.append(f"ปีที่มีข้อมูล: {', '.join(sorted(str(y) for y in years))}")
            
            # Check for customer count
            if 'customer_name' in results[0] or 'customer' in results[0]:
                field = 'customer_name' if 'customer_name' in results[0] else 'customer'
                customers = set(r.get(field) for r in results if r.get(field))
                if customers:
                    stats.append(f"จำนวนลูกค้า: {len(customers)} ราย")
            
        except Exception as e:
            logger.error(f"Error analyzing results: {e}")
        
        return '\n'.join(stats)
    
    def build_clarification_prompt(self, question: str, missing_info: List[str]) -> str:
        """Build clarification request"""
        examples = {
            'ระบุปี': 'เช่น "ปี 2567" หรือ "ปี 2567-2568"',
            'ชื่อบริษัท': 'เช่น "STANLEY" หรือ "CLARION"',
            'รหัสสินค้า': 'เช่น "EKAC460" หรือ "RCUG120"',
            'ช่วงเวลา': 'เช่น "เดือนสิงหาคม-กันยายน 2568"',
            'ระบุเดือน/ปี ที่ต้องการค้นหา': 'เช่น "เดือนสิงหาคม 2568"',
            'ชื่อบริษัทที่ต้องการค้นหา': 'เช่น "STANLEY" หรือ "คลีนิค"',
            'รหัสสินค้าหรือ model': 'เช่น "EKAC460"'
        }
        
        hints = [examples.get(info, info) for info in missing_info]
        
        return dedent(f"""
        ต้องการข้อมูลเพิ่มเติม:
        {chr(10).join(['• ' + h for h in hints])}
        
        กรุณาระบุให้ชัดเจน
        """).strip()
    
    def get_view_columns(self, view_name: str) -> List[str]:
        """Get columns for a view"""
        return self.VIEW_COLUMNS.get(view_name, [])
    
    def get_available_examples(self) -> List[str]:
        """Get available example keys"""
        return list(self.SQL_EXAMPLES.keys())
    
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get comprehensive schema summary"""
        return {
            'tables': list(self.VIEW_COLUMNS.keys()),
            'table_count': len(self.VIEW_COLUMNS),
            'examples': len(self.SQL_EXAMPLES),
            'examples_list': list(self.SQL_EXAMPLES.keys()),
            'features': [
                'Simplified 3-table structure',
                'Thai year conversion',
                'SQL injection protection',
                'Production-tested examples',
                'Date validation with leap year support',
                'Precompiled regex patterns',
                'Context-aware prompt building',
                'Proper error handling with fallback',
                'Production SQL format (date::date, LIKE)',
                'Optimized prompt length for faster generation'
            ],
            'optimized': True,
            'version': '5.0-production'
        }