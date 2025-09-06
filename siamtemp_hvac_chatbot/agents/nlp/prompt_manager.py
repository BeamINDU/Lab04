# Updated PromptManager with Real-World Optimized Examples
# File: agents/nlp/prompt_manager.py

import json
import logging
from typing import Dict, List, Any, Optional
from textwrap import dedent

logger = logging.getLogger(__name__)

class PromptManager:
    """
    Final PromptManager with real-world tested SQL examples
    """
    
    def __init__(self):
        # Column definitions (same as before)
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
        
        # Load real-world optimized examples
        self.SQL_EXAMPLES = self._load_real_world_examples()
        
        # Simplified system prompt
        self.SQL_SYSTEM_PROMPT = self._get_simplified_prompt()
    
    def _load_real_world_examples(self) -> Dict[str, str]:
        """Load real-world tested and optimized SQL examples"""
        return {
            # แผนงานวันที่เฉพาะ
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
            
            # ประวัติการซื้อขายย้อนหลัง 3 ปี
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
            
            # ประวัติการซ่อม
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
            
            # ราคาอะไหล่
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
            
            # วิเคราะห์การขายหลายปี
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
            
            # งานในช่วงเดือน
            'work_monthly_range': dedent("""
                SELECT date, customer, detail, service_group
                FROM v_work_force
                WHERE date::date BETWEEN '2025-08-01' AND '2025-09-30'
                ORDER BY date
                LIMIT 200;
            """).strip()
        }
    
    def _get_simplified_prompt(self) -> str:
        """Get simplified system prompt"""
        return dedent("""
        PostgreSQL expert for Siamtemp HVAC. Generate simple, clean SQL.
        
        KEY RULES:
        1. Views have clean data - use *_num columns directly
        2. No need for regexp_replace or complex cleaning
        3. Use ILIKE for text search
        4. Date format: WHERE date = 'YYYY-MM-DD' or date::date BETWEEN
        5. Thai years: 2567→2024, 2568→2025, 2569→2026
        
        COLUMN NAMES:
        - Sales: customer_name, total_revenue, *_num fields
        - Work: customer (not customer_name!), date, detail
        - Parts: product_code, product_name, balance_num, unit_price_num
        
        Always add LIMIT. Return SQL only.
        """).strip()
    
    def build_sql_prompt(self, question: str, intent: str, entities: Dict,
                        context: Dict = None, examples_override: List[str] = None) -> str:
        """Build SQL generation prompt with real examples"""
        
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
        if 'แผนงานวันที่' in question or 'วันที่' in question and 'งาน' in question:
            return self.SQL_EXAMPLES['work_plan_specific_date']
        
        elif 'ย้อนหลัง' in question and 'ปี' in question:
            return self.SQL_EXAMPLES['customer_history_multi_year']
        
        elif 'ประวัติการซ่อม' in question:
            return self.SQL_EXAMPLES['repair_history']
        
        elif 'ราคาอะไหล่' in question or 'อะไหล่' in question:
            return self.SQL_EXAMPLES['spare_parts_price']
        
        elif 'วิเคราะห์การขาย' in question or (len(entities.get('years', [])) > 1 and 'sales' in intent):
            return self.SQL_EXAMPLES['sales_analysis_multi_year']
        
        elif 'เดือน' in question and entities.get('months'):
            return self.SQL_EXAMPLES['work_monthly_range']
        
        # Default based on intent
        intent_map = {
            'work_plan': self.SQL_EXAMPLES['work_plan_specific_date'],
            'work_force': self.SQL_EXAMPLES['work_monthly_range'],
            'customer_history': self.SQL_EXAMPLES['customer_history_multi_year'],
            'repair_history': self.SQL_EXAMPLES['repair_history'],
            'parts_price': self.SQL_EXAMPLES['spare_parts_price'],
            'spare_parts': self.SQL_EXAMPLES['spare_parts_price'],
            'sales_analysis': self.SQL_EXAMPLES['sales_analysis_multi_year']
        }
        
        return intent_map.get(intent, self.SQL_EXAMPLES['sales_analysis_multi_year'])
    
    def _build_sql_hints(self, entities: Dict, intent: str) -> str:
        """Build specific SQL hints based on entities"""
        hints = []
        
        # Year hints
        if entities.get('years'):
            years = entities['years']
            if len(years) == 1:
                hints.append(f"Single year: Use v_sales{years[0]}")
            else:
                views = [f"v_sales{y}" for y in years]
                hints.append(f"Multiple years: UNION ALL {', '.join(views)}")
        
        # Date hints
        if entities.get('months'):
            months = entities['months']
            year = entities.get('years', [2025])[0]
            if len(months) == 1:
                # Single month
                month = months[0]
                hints.append(f"Month {month}: WHERE date::date BETWEEN '{year}-{month:02d}-01' AND '{year}-{month:02d}-31'")
            else:
                # Month range
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
        """Get schema summary"""
        return {
            'views': list(self.VIEW_COLUMNS.keys()),
            'examples': len(self.SQL_EXAMPLES),
            'optimized': True
        }