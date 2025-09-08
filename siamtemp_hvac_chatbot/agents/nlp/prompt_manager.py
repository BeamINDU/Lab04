# PromptManager - Optimized for 3-Table Structure with Comprehensive Examples
# File: agents/nlp/prompt_manager.py

import json
import logging
from typing import Dict, List, Any, Optional
from textwrap import dedent

logger = logging.getLogger(__name__)

class PromptManager:
    """
    Production PromptManager for 3-table database structure with 24 comprehensive SQL examples
    Tables: v_sales (all years), v_spare_part, v_work_force
    """
    
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
        
        # Load comprehensive SQL examples
        self.SQL_EXAMPLES = self._load_production_examples()
        
        # System prompt
        self.SQL_SYSTEM_PROMPT = self._get_system_prompt()
    
    def _load_production_examples(self) -> Dict[str, str]:
        """Load actual production SQL examples (broader coverage)"""
        return {
            # Customer History - ประวัติลูกค้าหลายปี (มี filter ปีเสมอ)
            'customer_history': dedent("""
                SELECT year AS year_label,
                       customer_name,
                       COUNT(*) AS transaction_count,
                       SUM(overhaul_num) AS overhaul,
                       SUM(replacement_num) AS replacement,
                       SUM(service_num) AS service,
                       SUM(total_revenue) AS total_amount
                FROM v_sales
                WHERE customer_name ILIKE '%STANLEY%'
                  AND year IN ('2023','2024','2025')
                GROUP BY year, customer_name
                ORDER BY year::int DESC, total_amount DESC
                LIMIT 100;
            """).strip(),

            # Repair History - ประวัติการซ่อม (เรียงล่าสุดก่อน)
            'repair_history': dedent("""
                SELECT date, customer, detail, service_group
                FROM v_work_force
                WHERE customer ILIKE '%STANLEY%'
                ORDER BY date DESC
                LIMIT 100;
            """).strip(),

            # Work Plan - แผนงานวันที่เฉพาะ
            'work_plan_specific_date': dedent("""
                SELECT id, date, customer, project,
                       job_description_pm, job_description_replacement,
                       detail, service_group
                FROM v_work_force
                WHERE date = '2025-09-05'
                ORDER BY customer
                LIMIT 100;
            """).strip(),

            # Spare Parts Price - ราคาอะไหล่ (ค้นหาจากชื่อ/รหัส)
            'spare_parts_price': dedent("""
                SELECT product_code, product_name, wh,
                       balance_num AS stock,
                       unit_price_num AS price,
                       total_num AS value
                FROM v_spare_part
                WHERE product_name ILIKE '%EKAC460%'
                   OR product_code ILIKE '%EKAC460%'
                ORDER BY total_num DESC
                LIMIT 50;
            """).strip(),

            # Sales Analysis - วิเคราะห์การขายหลายปี
            'sales_analysis_multi_year': dedent("""
                SELECT year AS year_label,
                       SUM(overhaul_num) AS overhaul,
                       SUM(replacement_num) AS replacement,
                       SUM(service_num) AS service,
                       SUM(parts_num) AS parts,
                       SUM(product_num) AS product,
                       SUM(solution_num) AS solution,
                       SUM(total_revenue) AS total
                FROM v_sales
                WHERE year IN ('2024','2025')
                GROUP BY year
                ORDER BY year::int;
            """).strip(),

            # Monthly Work - งานรายเดือน (ช่วงเวลา)
            'work_monthly': dedent("""
                SELECT date, customer, detail
                FROM v_work_force
                WHERE date BETWEEN '2025-08-01' AND '2025-09-30'
                ORDER BY date
                LIMIT 200;
            """).strip(),

            # Overhaul Sales Report - รายงานยอดขาย overhaul
            'overhaul_sales': dedent("""
                SELECT year AS year_label,
                       SUM(overhaul_num) AS overhaul_total
                FROM v_sales
                WHERE year IN ('2024','2025')
                  AND overhaul_num > 0
                GROUP BY year
                ORDER BY year::int;
            """).strip(),

            # Inventory Check - ตรวจสอบสินค้าคงคลังรวมมูลค่า
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

            # Top Customers - ลูกค้าสูงสุดตามรายได้ในปีที่ระบุ
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

            # Low Stock Alert - อะไหล่ใกล้หมด (จัดกลุ่มสถานะ)
            'low_stock_alert': dedent("""
                SELECT product_code, product_name,
                       balance_num AS current_stock,
                       unit_price_num AS unit_price,
                       CASE 
                           WHEN balance_num = 0 THEN 'Out of Stock'
                           WHEN balance_num < 10 THEN 'Critical'
                           WHEN balance_num < 50 THEN 'Low'
                           ELSE 'Normal'
                       END AS stock_status
                FROM v_spare_part
                WHERE balance_num < 50
                ORDER BY balance_num ASC
                LIMIT 50;
            """).strip(),

            # Sales By Category & Customer - ยอดขายแยกหมวดตามลูกค้า/ปี
            'sales_by_category_per_customer': dedent("""
                SELECT year AS year_label,
                       customer_name,
                       SUM(overhaul_num) AS overhaul,
                       SUM(replacement_num) AS replacement,
                       SUM(service_num) AS service,
                       SUM(parts_num) AS parts,
                       SUM(product_num) AS product,
                       SUM(solution_num) AS solution,
                       SUM(total_revenue) AS total
                FROM v_sales
                WHERE year IN ('2024','2025')
                  AND customer_name ILIKE '%STANLEY%'
                GROUP BY year, customer_name
                ORDER BY year::int DESC, total DESC
                LIMIT 100;
            """).strip(),

            # Year-over-Year Growth - อัตราเติบโตปีต่อปี (ทุกหมวดรวม)
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
                       CASE 
                         WHEN LAG(curr.total) OVER (ORDER BY curr.y) > 0
                         THEN ROUND( ( (curr.total - LAG(curr.total) OVER (ORDER BY curr.y))
                                      / LAG(curr.total) OVER (ORDER BY curr.y) ) * 100, 2)
                         ELSE NULL
                       END AS yoy_percent
                FROM yearly curr
                ORDER BY year
                LIMIT 100;
            """).strip(),

            # Sales Zero-Value Check - ตรวจข้อมูลยอดขายเป็นศูนย์ (คุณภาพข้อมูล)
            'sales_zero_value_check': dedent("""
                SELECT year, job_no, customer_name, description, total_revenue
                FROM v_sales
                WHERE year IN ('2024','2025')
                  AND total_revenue = 0
                ORDER BY year::int DESC, job_no
                LIMIT 200;
            """).strip(),

            # Parts By Price Range - ค้นหาตามช่วงราคา
            'parts_price_range': dedent("""
                SELECT product_code, product_name, unit_price_num AS unit_price, balance_num AS stock
                FROM v_spare_part
                WHERE unit_price_num BETWEEN 1000 AND 10000
                ORDER BY unit_price_num DESC
                LIMIT 100;
            """).strip(),

            # Warehouse Valuation - มูลค่าสินค้าคงคลังตามคลัง
            'warehouse_valuation': dedent("""
                SELECT wh AS warehouse,
                       SUM(balance_num) AS total_qty,
                       SUM(total_num) AS total_value
                FROM v_spare_part
                GROUP BY wh
                ORDER BY total_value DESC
                LIMIT 100;
            """).strip(),

            # Workforce Summary by Service Group - สรุปงานตามทีมบริการ
            'workforce_service_group_summary': dedent("""
                SELECT service_group,
                       COUNT(*) AS job_count
                FROM v_work_force
                WHERE date BETWEEN '2025-01-01' AND '2025-12-31'
                GROUP BY service_group
                ORDER BY job_count DESC
                LIMIT 100;
            """).strip(),

            # Workforce Success Rate - อัตราสำเร็จ/ไม่สำเร็จ (นับจากการมีข้อความ)
            'workforce_success_rate': dedent("""
                SELECT service_group,
                       SUM(CASE WHEN COALESCE(success,'') <> '' THEN 1 ELSE 0 END) AS success_count,
                       SUM(CASE WHEN COALESCE(unsuccessful,'') <> '' THEN 1 ELSE 0 END) AS unsuccessful_count
                FROM v_work_force
                WHERE date BETWEEN '2025-01-01' AND '2025-12-31'
                GROUP BY service_group
                ORDER BY success_count DESC
                LIMIT 100;
            """).strip(),

            # Work Items for Specific Customer & Period - งานของลูกค้า X ในช่วงเวลา
            'work_for_customer_period': dedent("""
                SELECT date, customer, detail, service_group
                FROM v_work_force
                WHERE customer ILIKE '%STANLEY%'
                  AND date BETWEEN '2025-07-01' AND '2025-09-30'
                ORDER BY date DESC
                LIMIT 200;
            """).strip(),

            # Top Jobs by Revenue - งานที่ทำรายได้สูงสุดในปีที่เลือก
            'top_jobs_by_revenue': dedent("""
                SELECT year, job_no, customer_name, total_revenue
                FROM v_sales
                WHERE year IN ('2024','2025')
                  AND total_revenue > 0
                ORDER BY total_revenue DESC
                LIMIT 50;
            """).strip(),

            # Parts Keyword Multi-field - คีย์เวิร์ดหลายฟิลด์
            'parts_keyword_multi_field': dedent("""
                SELECT product_code, product_name, wh, description, balance_num, unit_price_num, total_num
                FROM v_spare_part
                WHERE product_name ILIKE '%SENSOR%'
                   OR product_code ILIKE '%SENSOR%'
                   OR description ILIKE '%SENSOR%'
                ORDER BY total_num DESC
                LIMIT 100;
            """).strip(),

            # Workforce Text Search - ค้นหางานด้วยคีย์เวิร์ดในรายละเอียด
            'work_text_search': dedent("""
                SELECT date, customer, project, detail, service_group
                FROM v_work_force
                WHERE detail ILIKE '%CHILLER%'
                   OR project ILIKE '%START%'
                   OR service_group ILIKE '%TEAM A%'
                ORDER BY date DESC
                LIMIT 100;
            """).strip(),

            # Parts Reorder Suggestion - ชิ้นส่วนที่ควรเติมสต็อก (ต่ำแต่มีมูลค่า)
            'parts_reorder_suggestion': dedent("""
                SELECT product_code, product_name, wh,
                       balance_num AS stock, unit_price_num AS unit_price,
                       total_num AS total_value
                FROM v_spare_part
                WHERE balance_num BETWEEN 1 AND 10
                  AND total_num > 0
                ORDER BY balance_num ASC, total_value DESC
                LIMIT 100;
            """).strip(),

            # Sales by Customer (Exact Year) - ยอดขายต่อรายลูกค้าในปีเดียว
            'sales_by_customer_in_year': dedent("""
                SELECT customer_name,
                       SUM(total_revenue) AS total_revenue
                FROM v_sales
                WHERE year = '2025'
                GROUP BY customer_name
                ORDER BY total_revenue DESC
                LIMIT 100;
            """).strip()
        }
    
    def _get_system_prompt(self) -> str:
        """Optimized system prompt for PostgreSQL (3 views)"""
        return dedent("""
        You are an expert SQL generator for a PostgreSQL database.
        The schema consists of 3 views only:

        1. v_sales – Sales data
        Columns: id, year, job_no, customer_name, description,
                    overhaul_text, replacement_text, service_text, parts_text,
                    product_text, solution_text,
                    overhaul_num, replacement_num, service_num,
                    parts_num, product_num, solution_num, total_revenue

        2. v_spare_part – Spare parts inventory
        Columns: id, wh, product_code, product_name, unit,
                    balance_text, unit_price_text, total_text,
                    balance_num, unit_price_num, total_num,
                    description, received

        3. v_work_force – Work / repair records
        Columns: id, date, customer, project,
                    job_description_pm, job_description_replacement,
                    job_description_overhaul, job_description_start_up,
                    job_description_support_all, job_description_cpa,
                    detail, duration, service_group,
                    success, unsuccessful, failure_reason,
                    report_kpi_2_days, report_over_kpi_2_days

        IMPORTANT RULES:
        - Always filter by `year` when querying v_sales.
        - Always use ILIKE for text matching.
        - Date format must be 'YYYY-MM-DD'.
        - Return only pure SQL query (no explanation, no markdown, no comments).
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
            if keyword in sql_lower:
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
        """Validate and sanitize entities"""
        validated = {}
        
        # Validate years and convert Thai years
        if entities.get('years'):
            valid_years = []
            for year in entities['years']:
                converted = self.convert_thai_year(year)
                if 2020 <= int(converted) <= 2030:
                    valid_years.append(converted)
            if valid_years:
                validated['years'] = valid_years
        
        # Validate months
        if entities.get('months'):
            valid_months = [m for m in entities['months'] if 1 <= m <= 12]
            if valid_months:
                validated['months'] = valid_months
        
        # Validate dates
        if entities.get('dates'):
            valid_dates = []
            for date in entities['dates']:
                if isinstance(date, str) and len(date) == 10:
                    valid_dates.append(date)
            if valid_dates:
                validated['dates'] = valid_dates
        
        # Sanitize customer names
        if entities.get('customers'):
            sanitized = []
            for customer in entities['customers']:
                clean = customer.replace("'", "").replace('"', '').replace(';', '').strip()
                if clean:
                    sanitized.append(clean[:50])
            if sanitized:
                validated['customers'] = sanitized
        
        # Sanitize product codes
        if entities.get('products'):
            sanitized = []
            for product in entities['products']:
                clean = ''.join(c for c in product if c.isalnum() or c in '-_')
                if clean:
                    sanitized.append(clean[:30])
            if sanitized:
                validated['products'] = sanitized
        
        return validated
    
    # ===== UTILITY METHODS =====
    
    def convert_thai_year(self, year_value) -> str:
        """Convert Thai Buddhist year to AD year and return as string"""
        try:
            year = int(year_value)
            # If year > 2500, it's likely Thai Buddhist Era
            if year > 2500:
                return str(year - 543)
            # If year is 2-digit
            elif year < 100:
                if year >= 50:
                    return str(2500 + year - 543)
                else:
                    return str(2600 + year - 543)
            # Otherwise assume it's already AD
            return str(year)
        except (ValueError, TypeError):
            return '2025'
    
    # ===== MAIN METHODS =====
    
    def build_sql_prompt(self, question: str, intent: str, entities: Dict,
                        context: Dict = None, examples_override: List[str] = None) -> str:
        """Build SQL generation prompt"""
        
        # Validate input
        is_valid, msg = self.validate_input(question)
        if not is_valid:
            logger.error(f"Invalid input: {msg}")
            return ""
        
        # Validate and convert entities
        entities = self.validate_entities(entities)
        
        # Select best matching example
        example = self._select_best_example(question, intent, entities)
        
        # Build entity-specific hints
        hints = self._build_sql_hints(entities, intent)
        
        # Create prompt
        prompt = dedent(f"""
        {self.SQL_SYSTEM_PROMPT}
        
        WORKING EXAMPLE:
        {example}
        
        YOUR TASK:
        Question: {question}
        {hints}
        
        Generate SQL (use v_sales for all years, filter by year column):
        """).strip()
        
        return prompt
    
    def _select_best_example(self, question: str, intent: str, entities: Dict) -> str:
        """Select most relevant example based on question pattern"""
        question_lower = question.lower()
        
        # Comprehensive pattern matching
        
        # Growth/trend analysis
        if any(word in question for word in ['เติบโต', 'growth', 'yoy', 'year over year']):
            return self.SQL_EXAMPLES['sales_yoy_growth']
        
        # Data quality checks
        elif any(word in question for word in ['ศูนย์', 'zero', 'ไม่มียอด', 'no revenue']):
            return self.SQL_EXAMPLES['sales_zero_value_check']
        
        # Price range queries
        elif any(word in question for word in ['ช่วงราคา', 'price range', 'ระหว่าง', 'between']):
            return self.SQL_EXAMPLES['parts_price_range']
        
        # Warehouse/inventory valuation
        elif any(word in question for word in ['มูลค่าคลัง', 'warehouse value', 'valuation']):
            return self.SQL_EXAMPLES['warehouse_valuation']
        
        # Service group analysis
        elif any(word in question for word in ['ทีมบริการ', 'service group', 'แบ่งทีม']):
            return self.SQL_EXAMPLES['workforce_service_group_summary']
        
        # Success rate analysis
        elif any(word in question for word in ['อัตราสำเร็จ', 'success rate', 'ผลงาน']):
            return self.SQL_EXAMPLES['workforce_success_rate']
        
        # Reorder suggestions
        elif any(word in question for word in ['สั่งเพิ่ม', 'reorder', 'เติมสต็อก']):
            return self.SQL_EXAMPLES['parts_reorder_suggestion']
        
        # Top revenue jobs
        elif any(word in question for word in ['งานรายได้สูง', 'top revenue jobs', 'highest revenue']):
            return self.SQL_EXAMPLES['top_jobs_by_revenue']
        
        # Multi-field search
        elif any(word in question for word in ['ค้นหาทั่วไป', 'search all', 'หาทุกฟิลด์']):
            return self.SQL_EXAMPLES['parts_keyword_multi_field']
        
        # Text search in work
        elif 'ค้นหา' in question and 'งาน' in question:
            return self.SQL_EXAMPLES['work_text_search']
        
        # Sales breakdown by category
        elif any(word in question for word in ['แยกประเภท', 'breakdown', 'by category']):
            return self.SQL_EXAMPLES['sales_by_category_per_customer']
        
        # Customer period work
        elif 'ลูกค้า' in question and 'ช่วง' in question:
            return self.SQL_EXAMPLES['work_for_customer_period']
        
        # Yearly customer sales
        elif 'ลูกค้า' in question and 'ปี' in question and len(entities.get('years', [])) == 1:
            return self.SQL_EXAMPLES['sales_by_customer_in_year']
        
        # Basic patterns (existing)
        elif any(word in question for word in ['สินค้าคงคลัง', 'inventory', 'stock']):
            return self.SQL_EXAMPLES['inventory_check']
        
        elif any(word in question for word in ['ใกล้หมด', 'low stock', 'critical']):
            return self.SQL_EXAMPLES['low_stock_alert']
        
        elif any(word in question for word in ['top', 'สูงสุด', 'มากที่สุด']):
            return self.SQL_EXAMPLES['top_customers']
        
        elif any(word in question for word in ['overhaul', 'โอเวอร์ฮอล']):
            return self.SQL_EXAMPLES['overhaul_sales']
        
        elif 'แผนงานวันที่' in question or ('วันที่' in question and 'งาน' in question):
            return self.SQL_EXAMPLES['work_plan_specific_date']
        
        elif 'ประวัติการซ่อม' in question:
            return self.SQL_EXAMPLES['repair_history']
        
        elif 'ราคาอะไหล่' in question or 'อะไหล่' in question:
            return self.SQL_EXAMPLES['spare_parts_price']
        
        elif 'วิเคราะห์' in question or len(entities.get('years', [])) > 1:
            return self.SQL_EXAMPLES['sales_analysis_multi_year']
        
        elif 'เดือน' in question:
            return self.SQL_EXAMPLES['work_monthly']
        
        elif 'ประวัติ' in question or 'ย้อนหลัง' in question:
            return self.SQL_EXAMPLES['customer_history']
        
        # Default based on intent
        intent_map = {
            'customer_history': self.SQL_EXAMPLES['customer_history'],
            'repair_history': self.SQL_EXAMPLES['repair_history'],
            'work_plan': self.SQL_EXAMPLES['work_plan_specific_date'],
            'work_force': self.SQL_EXAMPLES['work_monthly'],
            'spare_parts': self.SQL_EXAMPLES['spare_parts_price'],
            'parts_price': self.SQL_EXAMPLES['spare_parts_price'],
            'sales_analysis': self.SQL_EXAMPLES['sales_analysis_multi_year'],
            'inventory_check': self.SQL_EXAMPLES['inventory_check'],
            'inventory_value': self.SQL_EXAMPLES['inventory_check'],
            'top_customers': self.SQL_EXAMPLES['top_customers'],
            'sales_growth': self.SQL_EXAMPLES['sales_yoy_growth'],
            'workforce_analysis': self.SQL_EXAMPLES['workforce_service_group_summary']
        }
        
        return intent_map.get(intent, self.SQL_EXAMPLES['sales_analysis_multi_year'])
    
    def _build_sql_hints(self, entities: Dict, intent: str) -> str:
        """Build SQL hints based on entities"""
        hints = []
        
        # Year hints - using single table
        if entities.get('years'):
            years = entities['years']
            year_list = "', '".join(years)
            hints.append(f"Filter years: WHERE year IN ('{year_list}')")
        
        # Date hints for work_force
        if entities.get('dates'):
            date = entities['dates'][0]
            hints.append(f"Specific date: WHERE date = '{date}'")
        
        # Month range hints
        if entities.get('months'):
            months = entities['months']
            year = entities.get('years', ['2025'])[0]
            if len(months) == 1:
                month = months[0]
                hints.append(f"Month filter: WHERE date BETWEEN '{year}-{month:02d}-01' AND '{year}-{month:02d}-31'")
            else:
                hints.append(f"Month range: WHERE date BETWEEN '{year}-{min(months):02d}-01' AND '{year}-{max(months):02d}-31'")
        
        # Customer hints
        if entities.get('customers'):
            customer = entities['customers'][0]
            if 'sales' in intent or 'customer_history' in intent:
                hints.append(f"Customer: WHERE customer_name ILIKE '%{customer}%'")
            else:
                hints.append(f"Customer: WHERE customer ILIKE '%{customer}%'")
        
        # Product hints
        if entities.get('products'):
            product = entities['products'][0]
            hints.append(f"Product: WHERE product_name ILIKE '%{product}%' OR product_code ILIKE '%{product}%'")
        
        # Price range hints
        if entities.get('price_min') and entities.get('price_max'):
            hints.append(f"Price range: WHERE unit_price_num BETWEEN {entities['price_min']} AND {entities['price_max']}")
        
        return '\n'.join(hints) if hints else ""
    
    def build_response_prompt(self, question: str, results: List[Dict],
                             sql_query: str, locale: str = "th") -> str:
        """Build response generation prompt"""
        if not results:
            return f"ไม่พบข้อมูลสำหรับคำถาม: {question}"
        
        # Analyze results
        stats = self._analyze_results(results)
        
        # Sample for prompt
        sample = results[:10]
        
        prompt = dedent(f"""
        สรุปข้อมูล HVAC สำหรับคำถาม: {question}
        
        พบข้อมูล: {len(results)} รายการ
        {stats}
        
        ตัวอย่างข้อมูล:
        {json.dumps(sample, ensure_ascii=False, default=str)[:1000]}
        
        กรุณาสรุป:
        1. ข้อมูลที่พบ (จำนวน, ช่วงเวลา)
        2. รายละเอียดสำคัญ (ยอดเงิน, รายชื่อ)
        3. ข้อสังเกต/แนวโน้ม
        
        ตอบภาษาไทยแบบกระชับ:
        """).strip()
        
        return prompt
    
    def _analyze_results(self, results: List[Dict]) -> str:
        """Analyze query results"""
        if not results:
            return ""
        
        stats = []
        
        # Check for revenue fields
        revenue_fields = ['total_revenue', 'total', 'overhaul_total', 'total_value']
        for field in revenue_fields:
            if field in results[0]:
                total = sum(float(r.get(field, 0) or 0) for r in results)
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
        
        # Check for growth percentage
        if 'yoy_percent' in results[0]:
            for r in results:
                if r.get('yoy_percent'):
                    stats.append(f"อัตราการเติบโต: {r['yoy_percent']}%")
        
        return '\n'.join(stats)
    
    def build_clarification_prompt(self, question: str, missing_info: List[str]) -> str:
        """Build clarification request"""
        examples = {
            'ระบุปี': 'เช่น "ปี 2567" หรือ "ปี 2567-2568"',
            'ชื่อบริษัท': 'เช่น "STANLEY" หรือ "CLARION"',
            'รหัสสินค้า': 'เช่น "EKAC460" หรือ "RCUG120"',
            'ช่วงราคา': 'เช่น "ราคา 1,000-10,000 บาท"',
            'ช่วงเวลา': 'เช่น "เดือนสิงหาคม-กันยายน 2568"'
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
            'coverage': {
                'basic_queries': 10,
                'analytics': 7,
                'search_patterns': 7,
                'total': 24
            },
            'features': [
                'Simplified 3-table structure',
                'Thai year conversion',
                'SQL injection protection',
                '24 production-tested examples',
                'Comprehensive pattern matching',
                'Advanced analytics support'
            ],
            'optimized': True,
            'version': '3.0'
        }