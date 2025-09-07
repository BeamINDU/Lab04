# Complete PromptManager with All Features
# File: agents/nlp/prompt_manager.py

import json
import logging
from typing import Dict, List, Any, Optional
from textwrap import dedent

logger = logging.getLogger(__name__)

class PromptManager:
    """
    Complete PromptManager with extensive Few-shot Examples and full validation
    """
    
    def __init__(self, db_handler=None):
        self.db_handler = db_handler
        
        # Database schema definitions
        self.VIEW_COLUMNS = {
            'v_sales2022': ['id', 'job_no', 'customer_name', 'description',
                           'overhaul_num', 'replacement_num', 'service_num',
                           'parts_num', 'product_num', 'solution_num', 'total_revenue'],
            'v_sales2023': ['id', 'job_no', 'customer_name', 'description',
                           'overhaul_num', 'replacement_num', 'service_num',
                           'parts_num', 'product_num', 'solution_num', 'total_revenue'],
            'v_sales2024': ['id', 'job_no', 'customer_name', 'description',
                           'overhaul_num', 'replacement_num', 'service_num',
                           'parts_num', 'product_num', 'solution_num', 'total_revenue'],
            'v_sales2025': ['id', 'job_no', 'customer_name', 'description',
                           'overhaul_num', 'replacement_num', 'service_num',
                           'parts_num', 'product_num', 'solution_num', 'total_revenue'],
            'v_work_force': ['id', 'date', 'customer', 'project', 'detail', 'duration',
                            'service_group', 'job_description_pm', 'job_description_replacement',
                            'job_description_overhaul', 'job_description_start_up',
                            'job_description_support_all', 'job_description_cpa',
                            'success', 'unsuccessful', 'failure_reason'],
            'v_spare_part': ['id', 'wh', 'product_code', 'product_name', 'unit',
                            'balance_num', 'unit_price_num', 'total_num',
                            'description', 'received'],
            'v_spare_part2': ['id', 'wh', 'product_code', 'product_name', 'unit',
                             'balance_num', 'unit_price_num', 'total_num',
                             'description', 'received']
        }
        
        # Load comprehensive SQL examples
        self.SQL_EXAMPLES = self._load_all_examples()
        
        # System prompt
        self.SQL_SYSTEM_PROMPT = self._get_system_prompt()
    
    def _load_all_examples(self) -> Dict[str, str]:
        """Load all SQL examples for few-shot learning"""
        return {
            # Work Plan - แผนงานวันที่เฉพาะ
            'work_plan_specific_date': dedent("""
                SELECT date, customer, project, detail, service_group,
                       CASE 
                           WHEN job_description_pm = true THEN 'PM'
                           WHEN job_description_replacement = true THEN 'Replacement'
                           WHEN job_description_overhaul IS NOT NULL THEN 'Overhaul'
                           ELSE 'Other'
                       END as job_type
                FROM v_work_force
                WHERE date = '2025-09-05'
                ORDER BY customer
                LIMIT 100;
            """).strip(),
            
            # Customer History - ประวัติลูกค้าหลายปี
            'customer_history_multi_year': dedent("""
                SELECT year_label, customer_name, 
                       COUNT(*) as transaction_count,
                       SUM(total_revenue) as total_amount,
                       SUM(overhaul_num) as overhaul,
                       SUM(replacement_num) as replacement,
                       SUM(service_num) as service
                FROM (
                    SELECT '2023' AS year_label, customer_name, total_revenue,
                           overhaul_num, replacement_num, service_num
                    FROM v_sales2023
                    WHERE customer_name ILIKE '%STANLEY%'
                    UNION ALL
                    SELECT '2024', customer_name, total_revenue,
                           overhaul_num, replacement_num, service_num
                    FROM v_sales2024
                    WHERE customer_name ILIKE '%STANLEY%'
                    UNION ALL
                    SELECT '2025', customer_name, total_revenue,
                           overhaul_num, replacement_num, service_num
                    FROM v_sales2025
                    WHERE customer_name ILIKE '%STANLEY%'
                ) combined
                GROUP BY year_label, customer_name
                ORDER BY year_label DESC;
            """).strip(),
            
            # Repair History - ประวัติการซ่อม
            'repair_history': dedent("""
                SELECT date, customer, detail, service_group,
                       CASE 
                           WHEN success IS NOT NULL THEN 'สำเร็จ'
                           WHEN unsuccessful IS NOT NULL THEN 'ไม่สำเร็จ'
                           ELSE 'กำลังดำเนินการ'
                       END as status
                FROM v_work_force
                WHERE customer ILIKE '%STANLEY%'
                ORDER BY date DESC
                LIMIT 100;
            """).strip(),
            
            # Spare Parts Price - ราคาอะไหล่
            'spare_parts_price': dedent("""
                SELECT product_code, product_name, wh,
                       balance_num as stock,
                       unit_price_num as price,
                       total_num as value
                FROM v_spare_part
                WHERE product_name ILIKE '%EKAC460%'
                   OR product_code ILIKE '%EKAC460%'
                ORDER BY total_num DESC
                LIMIT 50;
            """).strip(),
            
            # Sales Analysis - วิเคราะห์การขายหลายปี
            'sales_analysis_multi_year': dedent("""
                SELECT year_label,
                       SUM(overhaul_num) as overhaul,
                       SUM(replacement_num) as replacement,
                       SUM(service_num) as service,
                       SUM(parts_num) as parts,
                       SUM(product_num) as product,
                       SUM(solution_num) as solution,
                       SUM(total_revenue) as total
                FROM (
                    SELECT '2024' AS year_label, overhaul_num, replacement_num,
                           service_num, parts_num, product_num, solution_num, total_revenue
                    FROM v_sales2024
                    UNION ALL
                    SELECT '2025', overhaul_num, replacement_num,
                           service_num, parts_num, product_num, solution_num, total_revenue
                    FROM v_sales2025
                ) combined
                GROUP BY year_label
                ORDER BY year_label;
            """).strip(),
            
            # Monthly Work - งานในช่วงเดือน
            'work_monthly_range': dedent("""
                SELECT date, customer, detail, service_group
                FROM v_work_force
                WHERE date::date BETWEEN '2025-08-01' AND '2025-09-30'
                ORDER BY date
                LIMIT 200;
            """).strip(),
            
            # Inventory Check - ตรวจสอบสินค้าคงคลัง
            'inventory_check': dedent("""
                SELECT product_code, product_name, wh as warehouse,
                       balance_num as stock_quantity,
                       unit_price_num as unit_price,
                       (balance_num * unit_price_num) as total_value,
                       unit, description
                FROM v_spare_part
                WHERE balance_num > 0
                ORDER BY total_value DESC
                LIMIT 100;
            """).strip(),
            
            # Ratio Calculation - คำนวณอัตราส่วน
            'ratio_calculation': dedent("""
                WITH repair_stats AS (
                    SELECT COUNT(*) as total_repairs,
                        SUM(CASE WHEN success IS NOT NULL AND success != '' THEN 1 ELSE 0 END) as successful_repairs
                    FROM v_work_force
                    WHERE date >= '2024-01-01'
                ),
                sales_stats AS (
                    SELECT SUM(total_revenue) as total_sales,
                        SUM(service_num) as service_count
                    FROM v_sales2024
                )
                SELECT 
                    r.total_repairs,
                    r.successful_repairs,
                    s.total_sales,
                    s.service_count,
                    ROUND(r.successful_repairs::numeric / NULLIF(r.total_repairs, 0) * 100, 2) as success_rate
                FROM repair_stats r, sales_stats s;
            """).strip(),
            
            # Top Customers - ลูกค้าสูงสุด
            'top_customers': dedent("""
                SELECT customer_name,
                       COUNT(*) as transaction_count,
                       SUM(total_revenue) as total_revenue,
                       AVG(total_revenue) as avg_per_transaction
                FROM v_sales2024
                WHERE total_revenue > 0
                GROUP BY customer_name
                ORDER BY total_revenue DESC
                LIMIT 10;
            """).strip(),
            
            # Sales Comparison - เปรียบเทียบยอดขาย
            'sales_comparison': dedent("""
                SELECT 
                    'Q1' as quarter,
                    SUM(CASE WHEN EXTRACT(MONTH FROM date::date) BETWEEN 1 AND 3 
                        THEN total_revenue ELSE 0 END) as q1_revenue,
                    'Q2' as quarter2,
                    SUM(CASE WHEN EXTRACT(MONTH FROM date::date) BETWEEN 4 AND 6 
                        THEN total_revenue ELSE 0 END) as q2_revenue,
                    'Q3' as quarter3,
                    SUM(CASE WHEN EXTRACT(MONTH FROM date::date) BETWEEN 7 AND 9 
                        THEN total_revenue ELSE 0 END) as q3_revenue,
                    'Q4' as quarter4,
                    SUM(CASE WHEN EXTRACT(MONTH FROM date::date) BETWEEN 10 AND 12 
                        THEN total_revenue ELSE 0 END) as q4_revenue
                FROM v_sales2024;
            """).strip(),
            
            'repair_summary': dedent("""
                SELECT 
                    service_group,
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN success IS NOT NULL AND success != '' THEN 1 ELSE 0 END) as successful,
                    SUM(CASE WHEN unsuccessful IS NOT NULL AND unsuccessful != '' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN job_description_pm = true THEN 1 ELSE 0 END) as pm_jobs,
                    SUM(CASE WHEN job_description_replacement = true THEN 1 ELSE 0 END) as replacement_jobs
                FROM v_work_force
                WHERE date >= '2024-01-01'
                GROUP BY service_group
                ORDER BY total_jobs DESC;
            """).strip(),

            # แก้ trend_analysis
            'trend_analysis': dedent("""
                SELECT 
                    TO_CHAR(date::date, 'YYYY-MM') as month,
                    COUNT(*) as job_count,
                    COUNT(DISTINCT customer) as unique_customers,
                    SUM(CASE WHEN success IS NOT NULL AND success != '' THEN 1 ELSE 0 END) as success_count
                FROM v_work_force
                WHERE date >= '2024-01-01'
                GROUP BY TO_CHAR(date::date, 'YYYY-MM')
                ORDER BY month DESC
                LIMIT 12;
            """).strip(),
            
            # Low Stock Alert - อะไหล่ใกล้หมด
            'low_stock_alert': dedent("""
                SELECT product_code, product_name,
                       balance_num as current_stock,
                       unit_price_num as unit_price,
                       CASE 
                           WHEN balance_num = 0 THEN 'Out of Stock'
                           WHEN balance_num < 10 THEN 'Critical'
                           WHEN balance_num < 50 THEN 'Low'
                           ELSE 'Normal'
                       END as stock_status
                FROM v_spare_part
                WHERE balance_num < 50
                ORDER BY balance_num ASC, unit_price_num DESC
                LIMIT 50;
            """).strip(),
            
            # Work Statistics - สถิติการทำงาน
            'work_statistics': dedent("""
                SELECT 
                    COUNT(DISTINCT date) as working_days,
                    COUNT(*) as total_jobs,
                    COUNT(DISTINCT customer) as unique_customers,
                    SUM(CASE WHEN job_description_pm = true THEN 1 ELSE 0 END) as pm_jobs,
                    SUM(CASE WHEN job_description_replacement = true THEN 1 ELSE 0 END) as replacement_jobs,
                    SUM(CASE WHEN success IS NOT NULL AND success != '' THEN 1 ELSE 0 END) as successful_jobs
                FROM v_work_force
                WHERE date::date BETWEEN '2024-01-01' AND '2024-12-31';
            """).strip(),
            
            # Revenue by Service Type - รายได้แยกตามประเภท
            'revenue_by_type': dedent("""
                SELECT 
                    'Overhaul' as service_type,
                    SUM(overhaul_num) as revenue,
                    'Replacement' as service_type2,
                    SUM(replacement_num) as revenue2,
                    'Service' as service_type3,
                    SUM(service_num) as revenue3,
                    'Parts' as service_type4,
                    SUM(parts_num) as revenue4
                FROM v_sales2024;
            """).strip()
        }
    
    def _get_system_prompt(self) -> str:
        """Get the system prompt for SQL generation"""
        return dedent("""
        PostgreSQL Database Schema for Siamtemp HVAC:
        
        EXACT TABLE NAMES (USE THESE EXACTLY):
        1. v_sales2022 - Sales data for 2022
        2. v_sales2023 - Sales data for 2023  
        3. v_sales2024 - Sales data for 2024
        4. v_sales2025 - Sales data for 2025
        5. v_spare_part - Spare parts inventory
        6. v_spare_part2 - Additional spare parts
        7. v_work_force - Work/service records
        8. v_revenue_summary - Revenue summary by year
        
        DO NOT USE: Parts, Sales, Work, spare_parts (these tables don't exist!)
        
        COLUMN DETAILS:
        - v_sales20XX: customer_name, total_revenue, overhaul_num, service_num
        - v_work_force: customer, date, detail, service_group  
        - v_spare_part: product_code, product_name, balance_num, unit_price_num
        
        Always add LIMIT. Return SQL only.
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
        
        # Check for suspicious patterns
        suspicious_patterns = ['<script', 'javascript:', 'onclick', 'onerror']
        text_lower = text.lower()
        
        for pattern in suspicious_patterns:
            if pattern in text_lower:
                return False, f"Suspicious pattern detected: {pattern}"
        
        return True, "Input is valid"
    
    def validate_entities(self, entities: Dict) -> Dict:
        """Validate and sanitize entities"""
        validated = {}
        
        # Validate years
        if entities.get('years'):
            valid_years = []
            for year in entities['years']:
                converted = self.convert_thai_year(year)
                if 2020 <= converted <= 2030:
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
            sanitized_customers = []
            for customer in entities['customers']:
                clean = customer.replace("'", "").replace('"', '').replace(';', '').strip()
                if clean:
                    sanitized_customers.append(clean[:50])
            if sanitized_customers:
                validated['customers'] = sanitized_customers
        
        # Sanitize product codes
        if entities.get('products'):
            sanitized_products = []
            for product in entities['products']:
                clean = ''.join(c for c in product if c.isalnum() or c in '-_')
                if clean:
                    sanitized_products.append(clean[:30])
            if sanitized_products:
                validated['products'] = sanitized_products
        
        return validated
    
    def validate_generated_sql(self, sql: str) -> tuple[bool, str, str]:
        """Final validation before executing SQL"""
        if not sql:
            return False, "", "Empty SQL query"
        
        # Remove extra whitespace and standardize
        cleaned_sql = ' '.join(sql.split())
        
        # Safety check
        is_safe, safety_msg = self.validate_sql_safety(cleaned_sql)
        if not is_safe:
            return False, "", safety_msg
        
        # Ensure LIMIT clause
        if 'limit' not in cleaned_sql.lower():
            if cleaned_sql.rstrip().endswith(';'):
                cleaned_sql = cleaned_sql.rstrip(';') + ' LIMIT 100;'
            else:
                cleaned_sql = cleaned_sql + ' LIMIT 100'
        
        # Check for valid view references
        sql_lower = cleaned_sql.lower()
        valid_views = ['v_sales2022', 'v_sales2023', 'v_sales2024', 'v_sales2025',
                      'v_work_force', 'v_spare_part', 'v_spare_part2', 'v_revenue_summary']
        
        has_valid_table = any(view in sql_lower for view in valid_views)
        if not has_valid_table:
            return False, "", "Query must reference valid views"
        
        # Final format
        if not cleaned_sql.rstrip().endswith(';'):
            cleaned_sql = cleaned_sql + ';'
        
        return True, cleaned_sql, "Valid"
    
    # ===== UTILITY METHODS =====
    
    def convert_thai_year(self, year_value) -> int:
        """Convert Thai Buddhist year to AD year"""
        try:
            year = int(year_value)
            # If year > 2500, it's likely Thai Buddhist Era
            if year > 2500:
                return year - 543
            # If year is 2-digit, convert properly
            elif year < 100:
                if year >= 50:
                    return 2500 + year - 543
                else:
                    return 2600 + year - 543
            # Otherwise assume it's already AD
            return year
        except (ValueError, TypeError):
            return 2025
    
    def refresh_schema(self) -> bool:
        """Refresh schema from database if db_handler is available"""
        if not self.db_handler:
            logger.warning("No database handler available for schema refresh")
            return False
        
        try:
            schema_query = """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' 
                    AND table_name LIKE 'v_%'
                ORDER BY table_name, ordinal_position;
            """
            
            results = self.db_handler.execute_query(schema_query)
            
            new_schema = {}
            for row in results:
                view_name = row.get('table_name')
                column_name = row.get('column_name')
                
                if view_name and column_name:
                    if view_name not in new_schema:
                        new_schema[view_name] = []
                    new_schema[view_name].append(column_name)
            
            if new_schema:
                self.VIEW_COLUMNS = new_schema
                logger.info(f"Schema refreshed: {len(new_schema)} views loaded")
                return True
            else:
                logger.warning("No views found in database")
                return False
                
        except Exception as e:
            logger.error(f"Failed to refresh schema: {e}")
            return False
    
    # ===== MAIN METHODS =====
    
    def build_sql_prompt(self, question: str, intent: str, entities: Dict,
                        context: Dict = None, examples_override: List[str] = None) -> str:
        """Build SQL generation prompt with validation and examples"""
        
        # Validate input
        is_valid, msg = self.validate_input(question)
        if not is_valid:
            logger.error(f"Invalid input: {msg}")
            return ""
        
        # Validate and sanitize entities
        entities = self.validate_entities(entities)
        
        # Select best matching example
        example = self._select_best_example(question, intent, entities)
        
        # Build entity-specific hints
        hints = self._build_sql_hints(entities, intent)
        
        # Create compact prompt
        prompt = dedent(f"""
        {self.SQL_SYSTEM_PROMPT}
        
        REAL WORKING EXAMPLE:
        {example}
        
        YOUR TASK:
        Question: {question}
        {hints}
        
        Generate simple SQL like the example:
        """).strip()
        
        return prompt
    
    def _select_best_example(self, question: str, intent: str, entities: Dict) -> str:
        """Select most relevant example based on question pattern"""
        question_lower = question.lower()
        
        # Pattern matching for best example
        
        # Inventory & Stock patterns
        if any(word in question for word in ['สินค้าคงคลัง', 'คงคลัง', 'สต็อก', 'inventory', 'stock']):
            return self.SQL_EXAMPLES['inventory_check']
        
        # Low stock & Alert patterns  
        elif any(word in question for word in ['ต้องสั่ง', 'หมด', 'ใกล้หมด', 'critical', 'low stock']):
            return self.SQL_EXAMPLES['low_stock_alert']
        
        # Ratio & Percentage patterns
        elif any(word in question for word in ['อัตราส่วน', 'เปอร์เซ็นต์', 'ratio', 'percentage', '%']):
            return self.SQL_EXAMPLES['ratio_calculation']
        
        # Top & Ranking patterns
        elif any(word in question for word in ['top', 'อันดับ', 'สูงสุด', 'มากที่สุด', 'best']):
            return self.SQL_EXAMPLES['top_customers']
        
        # Comparison patterns
        elif any(word in question for word in ['เปรียบเทียบ', 'compare', 'vs', 'กับ', 'ต่างกัน']):
            return self.SQL_EXAMPLES['sales_comparison']
        
        # Summary patterns
        elif any(word in question for word in ['สรุป', 'summary', 'รวม', 'ภาพรวม', 'overview']):
            if 'ซ่อม' in question or 'repair' in question:
                return self.SQL_EXAMPLES['repair_summary']
            else:
                return self.SQL_EXAMPLES['work_statistics']
        
        # Trend patterns
        elif any(word in question for word in ['แนวโน้ม', 'trend', 'เทรนด์', 'การเปลี่ยนแปลง']):
            return self.SQL_EXAMPLES['trend_analysis']
        
        # Statistics patterns
        elif any(word in question for word in ['สถิติ', 'statistics', 'stat', 'ค่าเฉลี่ย', 'average']):
            return self.SQL_EXAMPLES['work_statistics']
        
        # Revenue by type patterns
        elif any(word in question for word in ['แยกประเภท', 'by type', 'แต่ละประเภท']):
            return self.SQL_EXAMPLES['revenue_by_type']
        
        # Work plan patterns
        elif 'แผนงานวันที่' in question or ('วันที่' in question and 'งาน' in question):
            return self.SQL_EXAMPLES['work_plan_specific_date']
        
        # Customer history patterns
        elif 'ย้อนหลัง' in question and 'ปี' in question:
            return self.SQL_EXAMPLES['customer_history_multi_year']
        
        # Repair history patterns
        elif 'ประวัติการซ่อม' in question:
            return self.SQL_EXAMPLES['repair_history']
        
        # Spare parts patterns
        elif 'ราคาอะไหล่' in question or 'อะไหล่' in question:
            return self.SQL_EXAMPLES['spare_parts_price']
        
        # Sales analysis patterns
        elif 'วิเคราะห์การขาย' in question or (len(entities.get('years', [])) > 1 and 'sales' in intent):
            return self.SQL_EXAMPLES['sales_analysis_multi_year']
        
        # Monthly work patterns
        elif 'เดือน' in question and entities.get('months'):
            return self.SQL_EXAMPLES['work_monthly_range']
        
        # Default based on intent mapping
        intent_map = {
            'work_plan': self.SQL_EXAMPLES['work_plan_specific_date'],
            'work_force': self.SQL_EXAMPLES['work_monthly_range'],
            'customer_history': self.SQL_EXAMPLES['customer_history_multi_year'],
            'repair_history': self.SQL_EXAMPLES['repair_history'],
            'parts_price': self.SQL_EXAMPLES['spare_parts_price'],
            'spare_parts': self.SQL_EXAMPLES['spare_parts_price'],
            'sales_analysis': self.SQL_EXAMPLES['sales_analysis_multi_year'],
            'inventory_check': self.SQL_EXAMPLES['inventory_check'],
            'inventory_value': self.SQL_EXAMPLES['inventory_check'],
            'top_customers': self.SQL_EXAMPLES['top_customers'],
            'sales_comparison': self.SQL_EXAMPLES['sales_comparison'],
            'trend_analysis': self.SQL_EXAMPLES['trend_analysis'],
            'statistics': self.SQL_EXAMPLES['work_statistics'],
            'revenue_by_type': self.SQL_EXAMPLES['revenue_by_type']
        }
        
        return intent_map.get(intent, self.SQL_EXAMPLES['sales_analysis_multi_year'])
    
    def _build_sql_hints(self, entities: Dict, intent: str) -> str:
        """Build specific SQL hints based on entities"""
        hints = []
        
        # Year hints with Thai year conversion
        if entities.get('years'):
            years = []
            for year in entities['years']:
                converted_year = self.convert_thai_year(year)
                years.append(converted_year)
            
            if len(years) == 1:
                hints.append(f"Single year: Use v_sales{years[0]}")
            else:
                views = [f"v_sales{y}" for y in years]
                hints.append(f"Multiple years: UNION ALL {', '.join(views)}")
        
        # Date hints
        if entities.get('months'):
            months = entities['months']
            year = entities.get('years', [2025])[0]
            year = self.convert_thai_year(year)
            if len(months) == 1:
                month = months[0]
                hints.append(f"Month {month}: WHERE date::date BETWEEN '{year}-{month:02d}-01' AND '{year}-{month:02d}-31'")
            else:
                hints.append(f"Months {min(months)}-{max(months)}: WHERE date::date BETWEEN '{year}-{min(months):02d}-01' AND '{year}-{max(months):02d}-31'")
        
        # Specific date
        if entities.get('dates'):
            date = entities['dates'][0]
            hints.append(f"Specific date: WHERE date = '{date}'")
        
        # Customer hints
        if entities.get('customers'):
            customer = entities['customers'][0]
            if 'sales' in intent:
                hints.append(f"Customer: WHERE customer_name ILIKE '%{customer}%'")
            else:
                hints.append(f"Customer: WHERE customer ILIKE '%{customer}%'")
        
        # Product hints
        if entities.get('products'):
            product = entities['products'][0]
            hints.append(f"Product: WHERE product_code ILIKE '%{product}%' OR product_name ILIKE '%{product}%'")
        
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
        
        ตัวอย่าง:
        {json.dumps(sample, ensure_ascii=False, default=str)[:1000]}
        
        กรุณาสรุป:
        1. ข้อมูลที่พบ (จำนวน, ช่วงเวลา)
        2. รายละเอียดสำคัญ (ยอดเงิน, รายชื่อ)
        3. ข้อสังเกต/แนวโน้ม
        
        ตอบภาษาไทย กระชับ ชัดเจน:
        """).strip()
        
        return prompt
    
    def _analyze_results(self, results: List[Dict]) -> str:
        """Quick analysis of results"""
        if not results:
            return ""
        
        stats = []
        
        # Check for revenue
        if 'total_revenue' in results[0] or 'total' in results[0]:
            field = 'total_revenue' if 'total_revenue' in results[0] else 'total'
            total = sum(float(r.get(field, 0) or 0) for r in results)
            if total > 0:
                stats.append(f"ยอดรวม: {total:,.0f} บาท")
        
        # Check for year grouping
        if 'year_label' in results[0] or 'year' in results[0]:
            years = set(r.get('year_label') or r.get('year') for r in results)
            if years:
                stats.append(f"ปีที่มีข้อมูล: {', '.join(sorted(str(y) for y in years if y))}")
        
        # Check for customer count
        if 'customer_name' in results[0] or 'customer' in results[0]:
            field = 'customer_name' if 'customer_name' in results[0] else 'customer'
            customers = set(r.get(field) for r in results if r.get(field))
            if customers:
                stats.append(f"จำนวนลูกค้า: {len(customers)} ราย")
        
        return '\n'.join(stats)
    
    def build_clarification_prompt(self, question: str, missing_info: List[str]) -> str:
        """Build clarification request"""
        examples = {
            'ระบุเดือน/ปี': 'เช่น "เดือนสิงหาคม 2568" หรือ "ปี 2567-2568"',
            'ชื่อบริษัท': 'เช่น "CLARION" หรือ "STANLEY"',
            'รหัสสินค้า': 'เช่น "EKAC460" หรือ "RCUG120"'
        }
        
        hints = [examples.get(info, info) for info in missing_info]
        
        return dedent(f"""
        ต้องการข้อมูลเพิ่มเติม:
        {chr(10).join(['• ' + h for h in hints])}
        
        กรุณาระบุให้ชัดเจน
        """).strip()
    
    def validate_column_usage(self, sql: str, view_name: str) -> tuple[bool, List[str]]:
        """Validate column usage in SQL"""
        issues = []
        
        # First check SQL safety
        is_safe, safety_msg = self.validate_sql_safety(sql)
        if not is_safe:
            issues.append(f"Security issue: {safety_msg}")
            return False, issues
        
        sql_lower = sql.lower()
        
        # Check for overly complex SQL
        if 'regexp_replace' in sql_lower:
            issues.append("Unnecessary regexp_replace - views have clean data")
        
        # Check column names
        if view_name in self.VIEW_COLUMNS:
            if view_name.startswith('v_sales') and 'revenue' in sql_lower and 'total_revenue' not in sql_lower:
                issues.append("Use 'total_revenue' not 'revenue'")
            
            if view_name == 'v_work_force' and 'customer_name' in sql_lower:
                issues.append("Use 'customer' not 'customer_name' in v_work_force")
        
        # Check for missing LIMIT
        if 'limit' not in sql_lower:
            issues.append("Missing LIMIT clause - always add LIMIT to prevent large result sets")
        
        return len(issues) == 0, issues
    
    def suggest_column_fix(self, invalid_column: str, view_name: str) -> Optional[str]:
        """Suggest correct column name"""
        fixes = {
            'revenue': 'total_revenue',
            'amount': 'total_revenue',
            'customer': 'customer_name' if view_name.startswith('v_sales') else 'customer',
            'balance': 'balance_num',
            'unit_price': 'unit_price_num',
            'price': 'unit_price_num'
        }
        return fixes.get(invalid_column.lower())
    
    def get_view_columns(self, view_name: str) -> List[str]:
        """Get columns for a view"""
        return self.VIEW_COLUMNS.get(view_name, [])
    
    def get_available_examples(self) -> List[str]:
        """Get available example keys"""
        return list(self.SQL_EXAMPLES.keys())
    
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get comprehensive schema summary"""
        return {
            'views': list(self.VIEW_COLUMNS.keys()),
            'examples': len(self.SQL_EXAMPLES),
            'examples_list': list(self.SQL_EXAMPLES.keys()),
            'features': [
                'Thai year conversion',
                'SQL injection protection',
                'Input validation',
                'Entity sanitization',
                '15+ few-shot examples',
                'Dynamic schema refresh'
            ],
            'optimized': True
        }