import decimal, json
import logging
import math
import re
from typing import Dict, List, Any, Optional, Tuple
from textwrap import dedent
from datetime import datetime, timedelta ,date
from functools import lru_cache
import hashlib
from .template_config import TemplateConfig

logger = logging.getLogger(__name__)

class SchemaCache:
    """Schema caching with TTL support"""
    
    def __init__(self, ttl_seconds: int = 3600):
        self.cache = {}
        self.ttl = ttl_seconds
        self.last_update = {}
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value if not expired"""
        if key in self.cache:
            if datetime.now() - self.last_update[key] < timedelta(seconds=self.ttl):
                return self.cache[key]
            else:
                # Cache expired
                del self.cache[key]
                del self.last_update[key]
        return None
    
    def set(self, key: str, value: Any):
        """Set cache value with timestamp"""
        self.cache[key] = value
        self.last_update[key] = datetime.now()
    
    def invalidate(self, key: str = None):
        """Invalidate specific key or entire cache"""
        if key:
            self.cache.pop(key, None)
            self.last_update.pop(key, None)
        else:
            self.cache.clear()
            self.last_update.clear()

class PromptManager:
    """Dynamic PromptManager with real-time schema discovery - keeps original class name"""
    
    def __init__(self, db_handler=None, cache_ttl: int = 3600):
        self.db_handler = db_handler
        self.schema_cache = SchemaCache(ttl_seconds=cache_ttl)
        
        # Initialize with empty schema - will be loaded dynamically
        self.VIEW_COLUMNS = {}
        
        # Load production SQL examples
        self.SQL_EXAMPLES = self._load_production_examples()
        
        # System prompt
        self.SQL_SYSTEM_PROMPT = self._get_system_prompt()
        
        # Precompiled patterns for performance
        self._compile_patterns()
        
        # Days in month mapping
        self.DAYS_IN_MONTH = {
            1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
            7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
        }
        
        # Table metadata cache
        self.table_metadata = {}
        
        # Initialize schema on startup
        self._initialize_schema()
    
    def _initialize_schema(self):
        """Initialize schema on startup"""
        try:
            logger.info("Initializing dynamic schema...")
            self._load_dynamic_schema()
            logger.info(f"Schema initialized with {len(self.VIEW_COLUMNS)} tables")
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            # Fall back to minimal known schema
            self._load_fallback_schema()
    
    def _load_fallback_schema(self):
        """Load minimal fallback schema when DB is unavailable"""
        logger.warning("Using fallback schema")
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
    
    def _load_dynamic_schema(self) -> Dict[str, List[str]]:
        """Dynamically load schema from database"""
        cache_key = "table_schema"
        
        # Try to get from cache first
        cached_schema = self.schema_cache.get(cache_key)
        if cached_schema:
            logger.debug("Using cached schema")
            self.VIEW_COLUMNS = cached_schema
            return cached_schema
        
        if not self.db_handler:
            logger.warning("No database handler available, using fallback")
            self._load_fallback_schema()
            return self.VIEW_COLUMNS
        
        try:
            logger.info("Loading schema from database...")
            
            # Query to get all tables and their columns
            schema_query = """
                SELECT 
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema = 'public'
                    AND table_name IN ('v_sales', 'v_spare_part', 'v_work_force')
                ORDER BY table_name, ordinal_position;
            """
            
            # Execute query
            schema_results = self.db_handler.execute_query(schema_query)
            
            if not schema_results:
                logger.warning("No schema information retrieved, using fallback")
                self._load_fallback_schema()
                return self.VIEW_COLUMNS
            
            # Parse results into schema dictionary
            new_schema = {}
            table_metadata = {}
            
            for row in schema_results:
                table_name = row['table_name']
                column_name = row['column_name']
                data_type = row['data_type']
                
                if table_name not in new_schema:
                    new_schema[table_name] = []
                    table_metadata[table_name] = {}
                
                new_schema[table_name].append(column_name)
                
                # Store column metadata
                table_metadata[table_name][column_name] = {
                    'data_type': data_type,
                    'nullable': row['is_nullable'] == 'YES',
                    'default': row['column_default'],
                    'position': row['ordinal_position']
                }
            
            # Update instance variables
            self.VIEW_COLUMNS = new_schema
            self.table_metadata = table_metadata
            
            # Cache the schema
            self.schema_cache.set(cache_key, new_schema)
            self.schema_cache.set("table_metadata", table_metadata)
            
            logger.info(f"Schema loaded successfully: {len(new_schema)} tables")
            for table, columns in new_schema.items():
                logger.debug(f"  {table}: {len(columns)} columns")
            
            return new_schema
            
        except Exception as e:
            logger.error(f"Failed to load dynamic schema: {e}")
            self._load_fallback_schema()
            return self.VIEW_COLUMNS
    
    def refresh_schema(self):
        """Force refresh schema from database"""
        logger.info("Force refreshing schema...")
        self.schema_cache.invalidate("table_schema")
        self.schema_cache.invalidate("table_metadata")
        return self._load_dynamic_schema()
    
    def discover_new_columns(self) -> Dict[str, List[str]]:
        """Discover columns that were added since last check"""
        old_schema = self.VIEW_COLUMNS.copy()
        new_schema = self.refresh_schema()
        
        new_columns = {}
        for table_name in new_schema:
            if table_name in old_schema:
                old_cols = set(old_schema[table_name])
                new_cols = set(new_schema[table_name])
                added = new_cols - old_cols
                if added:
                    new_columns[table_name] = list(added)
            else:
                # Entire table is new
                new_columns[table_name] = new_schema[table_name]
        
        if new_columns:
            logger.info(f"Discovered new columns: {new_columns}")
        
        return new_columns
    
    def validate_column_exists(self, table_name: str, column_name: str) -> bool:
        """Check if a column exists in a table (with schema refresh if needed)"""
        # First check current schema
        if table_name in self.VIEW_COLUMNS:
            if column_name in self.VIEW_COLUMNS[table_name]:
                return True
        
        # Column not found - try refreshing schema
        logger.info(f"Column {column_name} not found in {table_name}, refreshing schema...")
        self.refresh_schema()
        
        # Check again after refresh
        if table_name in self.VIEW_COLUMNS:
            return column_name in self.VIEW_COLUMNS[table_name]
        
        return False
    
    def suggest_column(self, table_name: str, search_term: str) -> List[str]:
        """Suggest similar column names based on search term"""
        if table_name not in self.VIEW_COLUMNS:
            return []
        
        columns = self.VIEW_COLUMNS[table_name]
        search_lower = search_term.lower()
        
        suggestions = []
        
        # Exact matches
        for col in columns:
            if search_lower == col.lower():
                suggestions.append((col, 1.0))
        
        # Partial matches
        for col in columns:
            col_lower = col.lower()
            if search_lower in col_lower:
                score = len(search_lower) / len(col_lower)
                suggestions.append((col, score))
        
        # Sort by score and return top 5
        suggestions.sort(key=lambda x: x[1], reverse=True)
        return [col for col, _ in suggestions[:5]]
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get detailed information about a table"""
        if table_name not in self.VIEW_COLUMNS:
            # Try to refresh schema
            self._load_dynamic_schema()
        
        if table_name not in self.VIEW_COLUMNS:
            return {}
        
        metadata = self.table_metadata.get(table_name, {})
        
        return {
            'columns': self.VIEW_COLUMNS[table_name],
            'column_count': len(self.VIEW_COLUMNS[table_name]),
            'metadata': metadata,
            'numeric_columns': [
                col for col, meta in metadata.items()
                if meta.get('data_type') in ('numeric', 'integer', 'bigint', 'real', 'double precision')
            ],
            'text_columns': [
                col for col, meta in metadata.items()
                if meta.get('data_type') in ('character varying', 'text', 'varchar')
            ],
            'date_columns': [
                col for col, meta in metadata.items()
                if meta.get('data_type') in ('date', 'timestamp', 'timestamp without time zone')
            ]
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
        """
        The `_load_production_examples` function returns a dictionary containing SQL queries for various
        analytical tasks related to sales, work force, spare parts, and business overview.
        :return: A dictionary containing SQL queries for various analytical tasks related to production
        data, spare parts, and work force management. Each query is labeled with a specific task or question
        it aims to address.
        """
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
            
            # Total Revenue All Years - รายได้รวมทั้งหมด (ไม่กรองปี)
            'total_revenue_all': dedent("""
                SELECT SUM(total_revenue) AS total_income
                FROM v_sales;
            """).strip(),
            
            # Total Revenue Specific Year - รายได้รวมของปีเฉพาะ
            'total_revenue_year': dedent("""
                SELECT SUM(total_revenue) AS total_income
                FROM v_sales
                WHERE year = '2024';
            """).strip(),
            
            # Revenue by Year - รายได้แต่ละปี
            'revenue_by_year': dedent("""
                SELECT year,
                       SUM(total_revenue) AS annual_revenue
                FROM v_sales
                GROUP BY year
                ORDER BY year;
            """).strip(),
            
            # Compare Revenue Two Years - เปรียบเทียบรายได้ 2 ปี
            'compare_revenue_years': dedent("""
                SELECT 
                    SUM(CASE WHEN year = '2023' THEN total_revenue ELSE 0 END) AS revenue_2023,
                    SUM(CASE WHEN year = '2024' THEN total_revenue ELSE 0 END) AS revenue_2024,
                    SUM(CASE WHEN year = '2024' THEN total_revenue ELSE 0 END) - 
                    SUM(CASE WHEN year = '2023' THEN total_revenue ELSE 0 END) AS difference
                FROM v_sales
                WHERE year IN ('2023', '2024');
            """).strip(),
            
            # Count Total Customers - นับจำนวนลูกค้าทั้งหมด
            'count_total_customers': dedent("""
                SELECT COUNT(DISTINCT customer_name) AS total_customers
                FROM v_sales
                WHERE year = '2024';
            """).strip(),
            
            # Top Customers No Filter - ลูกค้าที่ใช้บริการมากที่สุด
            'top_customers_no_filter': dedent("""
                SELECT customer_name,
                       COUNT(*) AS transaction_count,
                       SUM(total_revenue) AS total_revenue
                FROM v_sales
                WHERE year = '2024'
                GROUP BY customer_name
                ORDER BY transaction_count DESC
                LIMIT 10;
            """).strip(),
            
            # Average Revenue Per Transaction - รายได้เฉลี่ยต่องาน
            'average_revenue_per_transaction': dedent("""
                SELECT 
                    AVG(total_revenue) AS average_revenue_per_transaction,
                    COUNT(*) AS total_transactions
                FROM v_sales
                WHERE year = '2024';
            """).strip(),
            
            # High Value Transactions - งานที่มีมูลค่าสูง
            'high_value_transactions': dedent("""
                SELECT customer_name,
                       job_no,
                       description,
                       total_revenue
                FROM v_sales
                WHERE total_revenue > 1000000
                  AND year = '2024'
                ORDER BY total_revenue DESC
                LIMIT 20;
            """).strip(),
            
            # Revenue by Service Type - รายได้แยกตามประเภทงาน
            'revenue_by_service_type': dedent("""
                SELECT 
                    SUM(overhaul_num) AS overhaul_revenue,
                    SUM(replacement_num) AS replacement_revenue,
                    SUM(service_num) AS service_revenue,
                    SUM(parts_num) AS parts_revenue,
                    SUM(product_num) AS product_revenue,
                    SUM(solution_num) AS solution_revenue,
                    SUM(total_revenue) AS total_revenue
                FROM v_sales
                WHERE year = '2024';
            """).strip(),
            
            # Max Revenue by Year - รายได้สูงสุดแต่ละปี
            'max_revenue_by_year': dedent("""
                SELECT year,
                       MAX(total_revenue) AS max_revenue,
                       customer_name
                FROM v_sales
                WHERE total_revenue = (
                    SELECT MAX(total_revenue) 
                    FROM v_sales AS s2 
                    WHERE s2.year = v_sales.year
                )
                GROUP BY year, customer_name
                ORDER BY year;
            """).strip(),
            
            # Min/Max Value Work - งานที่มีมูลค่าต่ำสุด/สูงสุด
            'min_value_work': dedent("""
                SELECT customer_name, 
                       job_no, 
                       description, 
                       total_revenue
                FROM v_sales
                WHERE total_revenue > 0
                ORDER BY total_revenue ASC
                LIMIT 1;
            """).strip(),
            
            'max_value_work': dedent("""
                SELECT customer_name, 
                       job_no, 
                       description, 
                       total_revenue
                FROM v_sales
                WHERE total_revenue > 0
                ORDER BY total_revenue DESC
                LIMIT 1;
            """).strip(),
            
            # Min/Max Duration Work - งานที่ใช้เวลาน้อยสุด/มากสุด  
            'min_duration_work': dedent("""
                SELECT date, 
                       customer, 
                       detail, 
                       duration
                FROM v_work_force
                WHERE duration IS NOT NULL
                ORDER BY duration ASC
                LIMIT 1;
            """).strip(),
            
            'max_duration_work': dedent("""
                SELECT date, 
                       customer, 
                       detail, 
                       duration
                FROM v_work_force
                WHERE duration IS NOT NULL
                ORDER BY duration DESC
                LIMIT 1;
            """).strip(),
            
            # Count Works by Year - จำนวนงานแต่ละปี
            'count_works_by_year': dedent("""
            SELECT 
                EXTRACT(YEAR FROM date::date) AS year,
                COUNT(*) AS total_works
            FROM v_work_force
            GROUP BY EXTRACT(YEAR FROM date::date)
            ORDER BY year;
            """).strip(),
            
            # Year with Min/Max Revenue - ปีที่มีรายได้ต่ำสุด/สูงสุด
            'year_min_revenue': dedent("""
                SELECT year,
                       SUM(total_revenue) AS annual_revenue
                FROM v_sales
                GROUP BY year
                ORDER BY annual_revenue ASC
                LIMIT 1;
            """).strip(),
            
            'year_max_revenue': dedent("""
                SELECT year,
                       SUM(total_revenue) AS annual_revenue
                FROM v_sales
                GROUP BY year
                ORDER BY annual_revenue DESC
                LIMIT 1;
            """).strip(),
            
            # Compare all years revenue - เปรียบเทียบรายได้ทุกปี
            'all_years_revenue_comparison': dedent("""
                SELECT year,
                       SUM(total_revenue) AS annual_revenue,
                       RANK() OVER (ORDER BY SUM(total_revenue) DESC) AS revenue_rank
                FROM v_sales
                GROUP BY year
                ORDER BY annual_revenue DESC;
            """).strip(),
            
            # Average values - ค่าเฉลี่ย
            'average_work_value': dedent("""
                SELECT 
                    AVG(total_revenue) AS average_revenue,
                    MIN(total_revenue) AS min_revenue,
                    MAX(total_revenue) AS max_revenue,
                    COUNT(*) AS total_count
                FROM v_sales
                WHERE total_revenue > 0;
            """).strip(),

            
            # Customer New in Year - ลูกค้าใหม่ในปีนั้นๆ
            'new_customers_in_year': dedent("""
                SELECT DISTINCT customer_name
                FROM v_sales
                WHERE year = '2024'
                  AND customer_name NOT IN (
                      SELECT DISTINCT customer_name
                      FROM v_sales
                      WHERE year < '2024'
                  )
                ORDER BY customer_name
                LIMIT 100;
            """).strip(),
            
            # Customers Using Overhaul - ลูกค้าที่ใช้บริการ overhaul
            'customers_using_overhaul': dedent("""
                SELECT customer_name,
                       SUM(overhaul_num) AS total_overhaul,
                       COUNT(*) AS transaction_count
                FROM v_sales
                WHERE year = '2024'
                  AND overhaul_num > 0
                GROUP BY customer_name
                ORDER BY total_overhaul DESC
                LIMIT 20;
            """).strip(),
            
            # Customers with continuous years - ลูกค้าที่ใช้บริการต่อเนื่อง
            'customers_continuous_years': dedent("""
                SELECT customer_name,
                       COUNT(DISTINCT year) AS years_count,
                       MIN(year) AS first_year,
                       MAX(year) AS last_year,
                       SUM(total_revenue) AS total_revenue
                FROM v_sales
                GROUP BY customer_name
                HAVING COUNT(DISTINCT year) >= 3
                ORDER BY years_count DESC, total_revenue DESC
                LIMIT 20;
            """).strip(),
            
            # Top Service Customers - ลูกค้าที่ใช้ service มากที่สุด
            'top_service_customers': dedent("""
                SELECT customer_name,
                       SUM(service_num) AS total_service,
                       COUNT(*) AS transaction_count,
                       SUM(total_revenue) AS total_revenue
                FROM v_sales
                WHERE service_num > 0
                GROUP BY customer_name
                ORDER BY total_service DESC
                LIMIT 10;
            """).strip(),
            
            # Most Frequent Customers - ลูกค้าที่ใช้บริการบ่อยที่สุด
            'most_frequent_customers': dedent("""
                SELECT customer_name,
                       COUNT(*) AS transaction_count,
                       COUNT(DISTINCT year) AS years_active,
                       SUM(total_revenue) AS total_revenue
                FROM v_sales
                GROUP BY customer_name
                ORDER BY transaction_count DESC
                LIMIT 10;
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

            # Overhaul Sales All Years - ยอดขาย overhaul ทั้งหมด (ไม่กรองปี)
            'overhaul_sales_all': dedent("""
                SELECT SUM(overhaul_num) AS total_overhaul
                FROM v_sales;
            """).strip(),
            
            # Overhaul Sales by Year - ยอดขาย overhaul แยกตามปี
            'overhaul_sales': dedent("""
                SELECT year AS year_label,
                       SUM(overhaul_num) AS overhaul
                FROM v_sales
                GROUP BY year
                ORDER BY year;
            """).strip(),
            
            # Overhaul Sales Specific Years - ยอดขาย overhaul ปีเฉพาะ
            'overhaul_sales_specific': dedent("""
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

            'pm_work_summary': dedent("""
                SELECT date,
                    customer,
                    project,
                    detail
                FROM v_work_force
                WHERE date::date BETWEEN '2025-06-01' AND '2025-08-31'
                AND job_description_pm is not null
                ORDER BY date DESC
                LIMIT 100;
            """).strip(),

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

            'replacement_monthly': dedent("""
                SELECT date,
                    customer,
                    project,
                    detail
                FROM v_work_force
                WHERE date::date BETWEEN '2025-07-01' AND '2025-07-31'
                AND job_description_replacement is not null
                ORDER BY date;
            """).strip(),

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
        'overhaul_total': dedent("""
            SELECT 
                SUM(overhaul_num) as total_overhaul,
                COUNT(CASE WHEN overhaul_num > 0 THEN 1 END) as overhaul_count
            FROM v_sales 
            WHERE overhaul_num > 0;
        """).strip(),
        
        # คำถามข้อ 6: ยอดขาย Parts/อะไหล่
        'parts_total': dedent("""
            SELECT 
                SUM(parts_num) as total_parts,
                COUNT(CASE WHEN parts_num > 0 THEN 1 END) as parts_transactions
            FROM v_sales 
            WHERE parts_num > 0;
        """).strip(),
        
        # คำถามข้อ 7: ยอดขาย Replacement
        'replacement_total': dedent("""
            SELECT 
                SUM(replacement_num) as total_replacement,
                COUNT(CASE WHEN replacement_num > 0 THEN 1 END) as replacement_count
            FROM v_sales 
            WHERE replacement_num > 0;
        """).strip(),
        
        # คำถามข้อ 10: รายได้เฉลี่ยต่อปี
        'average_annual_revenue': dedent("""
            SELECT 
                AVG(yearly_revenue) as avg_annual_revenue,
                MIN(yearly_revenue) as min_annual_revenue,
                MAX(yearly_revenue) as max_annual_revenue
            FROM (
                SELECT year, SUM(total_revenue) as yearly_revenue 
                FROM v_sales 
                GROUP BY year
            ) as yearly_totals;
        """).strip(),
        
        # คำถามข้อ 17: จำนวนงานทั้งหมด
        'count_all_jobs': dedent("""
            SELECT 
                COUNT(*) as total_jobs,
                COUNT(DISTINCT customer_name) as unique_customers,
                COUNT(DISTINCT year) as years_covered
            FROM v_sales;
        """).strip(),
        
        # คำถามข้อ 18: จำนวนงานปีเฉพาะ
        'count_jobs_year': dedent("""
            SELECT 
                COUNT(*) as jobs_count,
                COUNT(DISTINCT customer_name) as customers_count
            FROM v_sales 
            WHERE year = '2024';
        """).strip(),
        
        # คำถามข้อ 19: รายได้เฉลี่ยต่องาน
        'average_revenue_per_job': dedent("""
            SELECT 
                AVG(total_revenue) as avg_revenue_per_job,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_revenue) as median_revenue
            FROM v_sales 
            WHERE total_revenue > 0;
        """).strip(),
        
        # คำถามข้อ 22: การเติบโตรายได้
        'revenue_growth': dedent("""
            WITH revenue_by_year AS (
                SELECT year, SUM(total_revenue) as annual_revenue
                FROM v_sales
                WHERE year IN ('2023', '2024')
                GROUP BY year
            )
            SELECT 
                MAX(CASE WHEN year = '2024' THEN annual_revenue END) as revenue_2024,
                MAX(CASE WHEN year = '2023' THEN annual_revenue END) as revenue_2023,
                MAX(CASE WHEN year = '2024' THEN annual_revenue END) - 
                MAX(CASE WHEN year = '2023' THEN annual_revenue END) as growth_amount,
                ROUND(((MAX(CASE WHEN year = '2024' THEN annual_revenue END) - 
                        MAX(CASE WHEN year = '2023' THEN annual_revenue END)) * 100.0 / 
                        NULLIF(MAX(CASE WHEN year = '2023' THEN annual_revenue END), 0)), 2) as growth_percent
            FROM revenue_by_year;
        """).strip(),
        
        # คำถามข้อ 23: สัดส่วนรายได้แต่ละประเภท
        'revenue_proportion': dedent("""
            SELECT 
                ROUND((SUM(overhaul_num) * 100.0 / NULLIF(SUM(total_revenue), 0)), 2) as overhaul_percent,
                ROUND((SUM(service_num) * 100.0 / NULLIF(SUM(total_revenue), 0)), 2) as service_percent,
                ROUND((SUM(parts_num) * 100.0 / NULLIF(SUM(total_revenue), 0)), 2) as parts_percent,
                ROUND((SUM(replacement_num) * 100.0 / NULLIF(SUM(total_revenue), 0)), 2) as replacement_percent,
                ROUND((SUM(product_num) * 100.0 / NULLIF(SUM(total_revenue), 0)), 2) as product_percent,
                ROUND((SUM(solution_num) * 100.0 / NULLIF(SUM(total_revenue), 0)), 2) as solution_percent
            FROM v_sales 
            WHERE total_revenue > 0;
        """).strip(),
        
        # คำถามข้อ 24: รายได้สูงสุดต่อปี
        'max_revenue_each_year': dedent("""
            SELECT year, job_no, customer_name, description, total_revenue 
            FROM (
                SELECT *, 
                    ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_revenue DESC) as rn 
                FROM v_sales 
                WHERE total_revenue > 0
            ) ranked 
            WHERE rn = 1 
            ORDER BY year;
        """).strip(),
        
        # คำถามข้อ 25: มูลค่ารวมสินค้าคงคลัง
        'total_inventory_value': dedent("""
            SELECT 
                SUM(total_num) as total_inventory_value,
                COUNT(*) as total_items,
                SUM(balance_num) as total_quantity
            FROM v_spare_part 
            WHERE total_num > 0;
        """).strip(),   
        
        # คำถามข้อ 29-30: ประวัติลูกค้าเฉพาะราย (Stanley/Clarion)
        'customer_specific_history': dedent("""
            SELECT 
                job_no, 
                year,
                description, 
                total_revenue,
                overhaul_num,
                service_num,
                parts_num
            FROM v_sales 
            WHERE customer_name ILIKE '%stanley%' 
            ORDER BY year DESC;
        """).strip(),
        
        # คำถามข้อ 31: ลูกค้าใหม่ในปีเฉพาะ
        'new_customers_year': dedent("""
            SELECT DISTINCT customer_name 
            FROM v_sales 
            WHERE year = '2024' 
            AND customer_name NOT IN (
                SELECT DISTINCT customer_name 
                FROM v_sales 
                WHERE year < '2024'
            )
            ORDER BY customer_name;
        """).strip(),
        
        # คำถามข้อ 32: ลูกค้าที่ใช้บริการบ่อยที่สุด
        'frequent_customers': dedent("""
            SELECT 
                customer_name, 
                COUNT(*) as service_count,
                SUM(total_revenue) as total_spent,
                AVG(total_revenue) as avg_per_transaction
            FROM v_sales 
            GROUP BY customer_name 
            HAVING COUNT(*) >= 3
            ORDER BY service_count DESC, total_spent DESC
            LIMIT 10;
        """).strip(),
        
        # คำถามข้อ 34: ลูกค้าภาครัฐ
        'government_customers': dedent("""
            SELECT DISTINCT 
                customer_name, 
                COUNT(*) as transaction_count,
                SUM(total_revenue) as total_spent 
            FROM v_sales 
            WHERE customer_name ILIKE ANY(ARRAY['%กระทรวง%', '%กรม%', '%สำนักงาน%', '%การไฟฟ้า%'])
            GROUP BY customer_name 
            ORDER BY total_spent DESC;
        """).strip(),
        
        # คำถามข้อ 38: ลูกค้าที่ไม่ได้ใช้บริการในปีล่าสุด
        'inactive_customers': dedent("""
            SELECT DISTINCT customer_name 
            FROM v_sales 
            WHERE year IN ('2022', '2023') 
            AND customer_name NOT IN (
                SELECT DISTINCT customer_name 
                FROM v_sales 
                WHERE year = '2024'
            )
            ORDER BY customer_name;
        """).strip(),
        
        # คำถามข้อ 39: ลูกค้าที่ใช้บริการต่อเนื่อง
        'continuous_customers': dedent("""
            SELECT 
                customer_name, 
                COUNT(DISTINCT year) as years_active,
                STRING_AGG(DISTINCT year, ', ' ORDER BY year) as active_years,
                SUM(total_revenue) as total_lifetime_value
            FROM v_sales 
            GROUP BY customer_name 
            HAVING COUNT(DISTINCT year) >= 3 
            ORDER BY years_active DESC, total_lifetime_value DESC;
        """).strip(),
        
        # คำถามข้อ 42: ลูกค้าโรงพยาบาล
        'hospital_customers': dedent("""
            SELECT DISTINCT 
                customer_name, 
                COUNT(*) as service_count,
                SUM(total_revenue) as total_spent 
            FROM v_sales 
            WHERE customer_name ILIKE ANY(ARRAY['%โรงพยาบาล%', '%รพ.%', '%hospital%', '%clinic%'])
            GROUP BY customer_name
            ORDER BY total_spent DESC;
        """).strip(),
        
        # คำถามข้อ 43: ลูกค้าที่จ่ายมากกว่าเกณฑ์
        'high_value_customers': dedent("""
            SELECT 
                customer_name, 
                COUNT(*) as transaction_count,
                SUM(total_revenue) as total_spent,
                AVG(total_revenue) as avg_transaction
            FROM v_sales 
            GROUP BY customer_name 
            HAVING SUM(total_revenue) > 500000 
            ORDER BY total_spent DESC;
        """).strip(),
        
        # คำถามข้อ 45: ลูกค้าที่ใช้บริการเฉพาะ Parts
        'parts_only_customers': dedent("""
            SELECT DISTINCT 
                customer_name, 
                COUNT(*) as parts_orders,
                SUM(parts_num) as total_parts_value
            FROM v_sales 
            WHERE parts_num > 0 
                AND overhaul_num = 0 
                AND service_num = 0 
                AND replacement_num = 0 
            GROUP BY customer_name 
            ORDER BY total_parts_value DESC;
        """).strip(),
        
        # คำถามข้อ 49: ลูกค้าที่มีงาน Chiller
        'chiller_customers': dedent("""
            SELECT DISTINCT 
                customer_name, 
                COUNT(*) as chiller_jobs,
                SUM(total_revenue) as chiller_revenue
            FROM v_sales 
            WHERE description ILIKE '%chiller%' 
                OR description ILIKE '%ชิลเลอร์%'
            GROUP BY customer_name 
            ORDER BY chiller_jobs DESC;
        """).strip(),
        
        # คำถามข้อ 50: เปรียบเทียบรายได้ลูกค้าใหม่ vs เก่า
        'new_vs_returning_customers': dedent("""
            WITH customer_classification AS (
                SELECT 
                    customer_name,
                    MIN(year) as first_year,
                    CASE 
                        WHEN MIN(year) = '2024' THEN 'New'
                        ELSE 'Returning'
                    END as customer_type
                FROM v_sales
                GROUP BY customer_name
            )
            SELECT 
                cc.customer_type,
                COUNT(DISTINCT s.customer_name) as customer_count,
                SUM(s.total_revenue) as total_revenue,
                AVG(s.total_revenue) as avg_revenue
            FROM v_sales s
            JOIN customer_classification cc ON s.customer_name = cc.customer_name
            WHERE s.year = '2024'
            GROUP BY cc.customer_type
            ORDER BY cc.customer_type;
        """).strip(),
        
        # === SPARE PARTS QUERIES (คำถาม 51-70) ===
        
        # คำถามข้อ 51: จำนวนอะไหล่ทั้งหมด
        'count_all_parts': dedent("""
            SELECT 
                COUNT(*) as total_part_types,
                COUNT(DISTINCT product_code) as unique_codes,
                COUNT(DISTINCT wh) as warehouses
            FROM v_spare_part;
        """).strip(),
        
        # คำถามข้อ 52: อะไหล่ที่มีสต็อก
        'parts_in_stock': dedent("""
            SELECT 
                product_code, 
                product_name, 
                balance_num as stock_quantity, 
                unit_price_num,
                total_num as stock_value
            FROM v_spare_part 
            WHERE balance_num > 0 
            ORDER BY balance_num DESC, total_num DESC;
        """).strip(),
        
        # คำถามข้อ 53: อะไหล่ที่หมดสต็อก
        'parts_out_of_stock': dedent("""
            SELECT 
                product_code, 
                product_name, 
                unit_price_num,
                wh as warehouse
            FROM v_spare_part 
            WHERE balance_num = 0 OR balance_num IS NULL
            ORDER BY product_code;
        """).strip(),
        
        # คำถามข้อ 54: อะไหล่ราคาแพงที่สุด
        'most_expensive_parts': dedent("""
            SELECT 
                product_code, 
                product_name, 
                unit_price_num,
                balance_num as stock,
                total_num as total_value
            FROM v_spare_part 
            WHERE unit_price_num > 0 
            ORDER BY unit_price_num DESC 
            LIMIT 10;
        """).strip(),
        
        # คำถามข้อ 56: อะไหล่ใกล้หมด (< 5 ชิ้น)
        'low_stock_alert': dedent("""
            SELECT 
                product_code, 
                product_name, 
                balance_num as current_stock, 
                unit_price_num,
                wh as warehouse
            FROM v_spare_part 
            WHERE balance_num > 0 AND balance_num < 5 
            ORDER BY balance_num ASC, unit_price_num DESC;
        """).strip(),
        
        # คำถามข้อ 57: อะไหล่ในคลังเฉพาะ
        'warehouse_specific_parts': dedent("""
            SELECT 
                product_code, 
                product_name, 
                balance_num, 
                unit_price_num,
                total_num
            FROM v_spare_part 
            WHERE wh = 'A' 
            ORDER BY total_num DESC;
        """).strip(),
        
        # คำถามข้อ 60: ราคาเฉลี่ยอะไหล่
        'average_part_price': dedent("""
            SELECT 
                AVG(unit_price_num) as avg_price,
                MIN(unit_price_num) as min_price,
                MAX(unit_price_num) as max_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY unit_price_num) as median_price
            FROM v_spare_part 
            WHERE unit_price_num > 0;
        """).strip(),
        
        # คำถามข้อ 62: อะไหล่สำหรับ Compressor
        'compressor_parts': dedent("""
            SELECT 
                product_code, 
                product_name, 
                balance_num, 
                unit_price_num,
                description
            FROM v_spare_part 
            WHERE product_name ILIKE ANY(ARRAY['%comp%', '%compressor%', '%คอมเพรสเซอร์%'])
                OR description ILIKE ANY(ARRAY['%comp%', '%compressor%', '%คอมเพรสเซอร์%'])
            ORDER BY product_name;
        """).strip(),
        
        # คำถามข้อ 63: Filter parts
        'filter_parts': dedent("""
            SELECT 
                product_code, 
                product_name, 
                balance_num, 
                unit_price_num,
                description
            FROM v_spare_part 
            WHERE product_name ILIKE '%filter%' 
                OR description ILIKE '%filter%'
                OR product_name ILIKE '%กรอง%'
            ORDER BY product_name;
        """).strip(),
        
        # คำถามข้อ 67: คลังที่มีอะไหล่มากที่สุด
        'warehouse_comparison': dedent("""
            SELECT 
                wh as warehouse, 
                COUNT(*) as part_types, 
                SUM(balance_num) as total_quantity,
                SUM(total_num) as total_value
            FROM v_spare_part 
            WHERE wh IS NOT NULL
            GROUP BY wh 
            ORDER BY total_value DESC, part_types DESC;
        """).strip(),
        
        # === WORK FORCE QUERIES (คำถาม 71-90) ===
        
        # คำถามข้อ 71: จำนวนงานทั้งหมดใน work_force
        'count_all_works': dedent("""
            SELECT 
                COUNT(*) as total_work_records,
                COUNT(DISTINCT customer) as unique_customers,
                COUNT(DISTINCT service_group) as teams
            FROM v_work_force;
        """).strip(),
        
        # คำถามข้อ 72: งานเดือนเฉพาะ
        'work_specific_month': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail, 
                service_group
            FROM v_work_force 
            WHERE date::date >= '2024-09-01' 
                AND date::date <= '2024-09-30'
            ORDER BY date, customer;
        """).strip(),
        
        # คำถามข้อ 73: งาน PM ทั้งหมด
        'all_pm_works': dedent("""
            select * from public.v_work_force  
            where job_description_pm is not null 
        """).strip(),
        
        # คำถามข้อ 74: งาน Overhaul (work context)
        'work_overhaul': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail,
                job_description_overhaul
            FROM v_work_force 
            WHERE job_description_overhaul IS NOT NULL 
                AND job_description_overhaul != '' 
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 75: งาน Replacement
        'work_replacement': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail,
                service_group
            FROM v_work_force 
            WHERE job_description_replacement is not null
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 76: งานที่สำเร็จ
        'successful_works': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail, 
                success,
                service_group
            FROM v_work_force 
            WHERE success IS NOT NULL 
                AND success != '' 
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 77: งานที่ไม่สำเร็จ
        'unsuccessful_works': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail, 
                unsuccessful, 
                failure_reason
            FROM v_work_force 
            WHERE unsuccessful IS NOT NULL 
                AND unsuccessful != '' 
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 78: งานของทีมเฉพาะ
        'team_works': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail,
                service_group
            FROM v_work_force 
            WHERE service_group ILIKE '%A%' 
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 79: งานวันนี้
        'work_today': dedent("""
            SELECT 
                customer, 
                project, 
                detail, 
                service_group,
                job_description_pm,
                job_description_replacement
            FROM v_work_force 
            WHERE date = CURRENT_DATE 
            ORDER BY customer;
        """).strip(),
        
        # คำถามข้อ 80: งานสัปดาห์นี้
        'work_this_week': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail,
                service_group
            FROM v_work_force 
            WHERE date::date >= CURRENT_DATE - INTERVAL '7 days' 
                AND date::date <= CURRENT_DATE 
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 81: อัตราความสำเร็จ
        'success_rate': dedent("""
            SELECT 
                COUNT(*) as total_jobs,
                SUM(CASE WHEN success IS NOT NULL AND success != '' THEN 1 ELSE 0 END) as successful_jobs,
                SUM(CASE WHEN unsuccessful IS NOT NULL AND unsuccessful != '' THEN 1 ELSE 0 END) as failed_jobs,
                ROUND(
                    SUM(CASE WHEN success IS NOT NULL AND success != '' THEN 1 ELSE 0 END) * 100.0 / 
                    NULLIF(COUNT(*), 0), 2
                ) as success_rate_percent
            FROM v_work_force;
        """).strip(),
        
        # คำถามข้อ 82: งานที่เสร็จตรงเวลา
        'on_time_works': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail,
                report_kpi_2_days
            FROM v_work_force 
            WHERE report_kpi_2_days IS NOT NULL 
                AND report_kpi_2_days != '' 
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 83: งานที่เกินเวลา
        'overtime_works': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail, 
                report_over_kpi_2_days
            FROM v_work_force 
            WHERE report_over_kpi_2_days IS NOT NULL 
                AND report_over_kpi_2_days != '' 
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 85: งาน Start Up
        'startup_works_all': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail,
                job_description_start_up
            FROM v_work_force 
            WHERE job_description_start_up IS NOT NULL 
                AND job_description_start_up != '' 
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 86: งาน Support
        'support_works': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail,
                job_description_support_all
            FROM v_work_force 
            WHERE job_description_support_all IS NOT NULL 
                AND job_description_support_all != '' 
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 87: งาน CPA
        'cpa_works': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail
            FROM v_work_force 
            WHERE job_description_cpa is not null
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 88: สถิติงานแต่ละทีม
        'team_statistics': dedent("""
            SELECT 
                service_group, 
                COUNT(*) as total_jobs,
                SUM(CASE WHEN success IS NOT NULL AND success != '' THEN 1 ELSE 0 END) as successful_jobs,
                SUM(CASE WHEN report_kpi_2_days IS NOT NULL THEN 1 ELSE 0 END) as on_time_jobs,
                ROUND(
                    SUM(CASE WHEN success IS NOT NULL AND success != '' THEN 1 ELSE 0 END) * 100.0 / 
                    NULLIF(COUNT(*), 0), 2
                ) as success_rate
            FROM v_work_force 
            WHERE service_group IS NOT NULL 
            GROUP BY service_group 
            ORDER BY total_jobs DESC;
        """).strip(),
        
        # คำถามข้อ 89: ระยะเวลาการทำงาน
        'work_duration': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail, 
                duration
            FROM v_work_force 
            WHERE duration IS NOT NULL 
                AND duration != '' 
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 90: งานล่าสุด
        'latest_works': dedent("""
            SELECT 
                date,
                customer, 
                project, 
                detail, 
                service_group 
            FROM v_work_force 
            ORDER BY date DESC, id DESC 
            LIMIT 10;
        """).strip(),
        
        # === ANALYTICAL QUERIES (คำถาม 91-100) ===
        
        # คำถามข้อ 91: สรุปผลประกอบการรายปี
        'annual_performance_summary': dedent("""
            SELECT 
                year,
                COUNT(*) as total_jobs,
                COUNT(DISTINCT customer_name) as unique_customers,
                SUM(total_revenue) as total_revenue,
                SUM(overhaul_num) as overhaul_total,
                SUM(service_num) as service_total,
                SUM(parts_num) as parts_total,
                SUM(replacement_num) as replacement_total,
                SUM(product_num) as product_total,
                SUM(solution_num) as solution_total
            FROM v_sales 
            GROUP BY year 
            ORDER BY year;
        """).strip(),
        
        # คำถามข้อ 92: เทรนด์การเติบโต
        'growth_trend': dedent("""
            WITH yearly_revenue AS (
                SELECT 
                    year,
                    SUM(total_revenue) as revenue
                FROM v_sales 
                GROUP BY year
            )
            SELECT 
                year,
                revenue,
                LAG(revenue) OVER (ORDER BY year) as prev_year_revenue,
                revenue - LAG(revenue) OVER (ORDER BY year) as growth_amount,
                ROUND(
                    (revenue - LAG(revenue) OVER (ORDER BY year)) * 100.0 / 
                    NULLIF(LAG(revenue) OVER (ORDER BY year), 0), 2
                ) as growth_rate
            FROM yearly_revenue
            ORDER BY year;
        """).strip(),
        
        # คำถามข้อ 93: ประเภทงานที่นิยม
        'popular_service_types': dedent("""
            WITH service_summary AS (
                SELECT 
                    'Overhaul' as service_type, 
                    COUNT(CASE WHEN overhaul_num > 0 THEN 1 END) as job_count, 
                    SUM(overhaul_num) as total_value
                FROM v_sales
                UNION ALL
                SELECT 
                    'Service' as service_type, 
                    COUNT(CASE WHEN service_num > 0 THEN 1 END) as job_count, 
                    SUM(service_num) as total_value
                FROM v_sales
                UNION ALL
                SELECT 
                    'Parts' as service_type, 
                    COUNT(CASE WHEN parts_num > 0 THEN 1 END) as job_count, 
                    SUM(parts_num) as total_value
                FROM v_sales
                UNION ALL
                SELECT 
                    'Replacement' as service_type, 
                    COUNT(CASE WHEN replacement_num > 0 THEN 1 END) as job_count, 
                    SUM(replacement_num) as total_value
                FROM v_sales
            )
            SELECT * FROM service_summary
            ORDER BY total_value DESC;
        """).strip(),
        
        # คำถามข้อ 94: ลูกค้าที่มีศักยภาพ
        'high_potential_customers': dedent("""
            SELECT 
                customer_name,
                COUNT(*) as service_frequency,
                SUM(total_revenue) as total_spent,
                AVG(total_revenue) as avg_per_job,
                MAX(total_revenue) as max_transaction,
                STRING_AGG(DISTINCT year, ', ' ORDER BY year) as active_years
            FROM v_sales 
            GROUP BY customer_name 
            HAVING COUNT(*) >= 5 AND SUM(total_revenue) > 200000
            ORDER BY total_spent DESC;
        """).strip(),
        
        # คำถามข้อ 95: การกระจายรายได้
        'revenue_distribution': dedent("""
            SELECT 
                CASE 
                    WHEN total_revenue < 50000 THEN 'Small (< 50K)'
                    WHEN total_revenue < 200000 THEN 'Medium (50K-200K)'
                    WHEN total_revenue < 500000 THEN 'Large (200K-500K)'
                    ELSE 'Extra Large (> 500K)'
                END as job_size,
                COUNT(*) as job_count,
                SUM(total_revenue) as total_revenue,
                AVG(total_revenue) as avg_revenue
            FROM v_sales 
            WHERE total_revenue > 0
            GROUP BY 
                CASE 
                    WHEN total_revenue < 50000 THEN 'Small (< 50K)'
                    WHEN total_revenue < 200000 THEN 'Medium (50K-200K)'
                    WHEN total_revenue < 500000 THEN 'Large (200K-500K)'
                    ELSE 'Extra Large (> 500K)'
                END
            ORDER BY 
                CASE 
                    WHEN total_revenue < 50000 THEN 1
                    WHEN total_revenue < 200000 THEN 2
                    WHEN total_revenue < 500000 THEN 3
                    ELSE 4
                END;
        """).strip(),
        
        # คำถามข้อ 96: ประสิทธิภาพทีมงาน
        'team_performance': dedent("""
            SELECT 
                service_group,
                COUNT(*) as total_jobs,
                SUM(CASE WHEN success IS NOT NULL AND success != '' THEN 1 ELSE 0 END) as successful_jobs,
                SUM(CASE WHEN report_kpi_2_days IS NOT NULL AND report_kpi_2_days != '' THEN 1 ELSE 0 END) as on_time_jobs,
                SUM(CASE WHEN report_over_kpi_2_days IS NOT NULL AND report_over_kpi_2_days != '' THEN 1 ELSE 0 END) as overtime_jobs,
                ROUND(
                    SUM(CASE WHEN success IS NOT NULL AND success != '' THEN 1 ELSE 0 END) * 100.0 / 
                    NULLIF(COUNT(*), 0), 2
                ) as success_rate,
                ROUND(
                    SUM(CASE WHEN report_kpi_2_days IS NOT NULL AND report_kpi_2_days != '' THEN 1 ELSE 0 END) * 100.0 / 
                    NULLIF(COUNT(*), 0), 2
                ) as on_time_rate
            FROM v_work_force 
            WHERE service_group IS NOT NULL 
            GROUP BY service_group 
            ORDER BY success_rate DESC;
        """).strip(),
        
        # คำถามข้อ 97: แนวโน้มยอดขายรายเดือน
        'monthly_sales_trend': dedent("""
            WITH monthly_work AS (
                SELECT 
                    EXTRACT(YEAR FROM date::date) as year,
                    EXTRACT(MONTH FROM date::date) as month,
                    COUNT(*) as jobs_completed,
                    COUNT(DISTINCT customer) as unique_customers
                FROM v_work_force 
                WHERE date::date >= '2024-01-01'
                GROUP BY 
                    EXTRACT(YEAR FROM date::date),
                    EXTRACT(MONTH FROM date::date)
            )
            SELECT 
                year,
                month,
                TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') as month_name,
                jobs_completed,
                unique_customers
            FROM monthly_work
            ORDER BY year, month;
        """).strip(),
        
        # คำถามข้อ 98: ROI ของการบริการ
        'service_roi': dedent("""
            WITH service_analysis AS (
                SELECT 
                    'Overhaul' as service_type,
                    COUNT(CASE WHEN overhaul_num > 0 THEN 1 END) as job_count,
                    SUM(overhaul_num) as total_revenue,
                    AVG(CASE WHEN overhaul_num > 0 THEN overhaul_num END) as avg_revenue_per_job
                FROM v_sales
                UNION ALL
                SELECT 
                    'Service' as service_type,
                    COUNT(CASE WHEN service_num > 0 THEN 1 END) as job_count,
                    SUM(service_num) as total_revenue,
                    AVG(CASE WHEN service_num > 0 THEN service_num END) as avg_revenue_per_job
                FROM v_sales
                UNION ALL
                SELECT 
                    'Replacement' as service_type,
                    COUNT(CASE WHEN replacement_num > 0 THEN 1 END) as job_count,
                    SUM(replacement_num) as total_revenue,
                    AVG(CASE WHEN replacement_num > 0 THEN replacement_num END) as avg_revenue_per_job
                FROM v_sales
                UNION ALL
                SELECT 
                    'Parts' as service_type,
                    COUNT(CASE WHEN parts_num > 0 THEN 1 END) as job_count,
                    SUM(parts_num) as total_revenue,
                    AVG(CASE WHEN parts_num > 0 THEN parts_num END) as avg_revenue_per_job
                FROM v_sales
            )
            SELECT * FROM service_analysis
            WHERE job_count > 0
            ORDER BY avg_revenue_per_job DESC;
        """).strip(),
        
        # คำถามข้อ 99: คาดการณ์รายได้
        'revenue_forecast': dedent("""
            WITH revenue_trend AS (
                SELECT 
                    year,
                    SUM(total_revenue) as annual_revenue
                FROM v_sales
                WHERE year IN ('2023', '2024')
                GROUP BY year
            ),
            growth_calc AS (
                SELECT 
                    MAX(CASE WHEN year = '2024' THEN annual_revenue END) as revenue_2024,
                    MAX(CASE WHEN year = '2023' THEN annual_revenue END) as revenue_2023,
                    (MAX(CASE WHEN year = '2024' THEN annual_revenue END) - 
                     MAX(CASE WHEN year = '2023' THEN annual_revenue END)) / 
                     NULLIF(MAX(CASE WHEN year = '2023' THEN annual_revenue END), 0) as growth_rate
                FROM revenue_trend
            )
            SELECT 
                2025 as projected_year,
                revenue_2024 as current_year_revenue,
                ROUND(revenue_2024 * (1 + growth_rate)) as projected_revenue,
                ROUND(growth_rate * 100, 2) as growth_rate_percent
            FROM growth_calc;
        """).strip(),
        
        # คำถามข้อ 100: รายงานสถานะธุรกิจรวม
        'business_overview': dedent("""
            WITH summary AS (
                SELECT 
                    (SELECT SUM(total_revenue) FROM v_sales) as total_revenue,
                    (SELECT COUNT(DISTINCT customer_name) FROM v_sales) as total_customers,
                    (SELECT COUNT(*) FROM v_sales) as total_sales_jobs,
                    (SELECT COUNT(DISTINCT year) FROM v_sales) as active_years,
                    (SELECT SUM(total_num) FROM v_spare_part WHERE total_num > 0) as inventory_value,
                    (SELECT COUNT(*) FROM v_spare_part WHERE balance_num > 0) as parts_in_stock,
                    (SELECT COUNT(*) FROM v_work_force) as total_work_records,
                    (SELECT COUNT(DISTINCT customer) FROM v_work_force) as work_customers
            )
            SELECT 
                'Total Revenue' as metric, 
                total_revenue::text as value 
            FROM summary
            UNION ALL
            SELECT 
                'Total Customers' as metric, 
                total_customers::text as value 
            FROM summary
            UNION ALL
            SELECT 
                'Total Sales Jobs' as metric, 
                total_sales_jobs::text as value 
            FROM summary
            UNION ALL
            SELECT 
                'Active Years' as metric, 
                active_years::text as value 
            FROM summary
            UNION ALL
            SELECT 
                'Inventory Value' as metric, 
                inventory_value::text as value 
            FROM summary
            UNION ALL
            SELECT 
                'Parts in Stock' as metric, 
                parts_in_stock::text as value
            FROM summary
            UNION ALL
            SELECT 
                'Work Records' as metric, 
                total_work_records::text as value 
            FROM summary
            UNION ALL
            SELECT 
                'Work Customers' as metric, 
                work_customers::text as value
            FROM summary;
        """).strip(),

        'year_max_revenue': dedent("""
            SELECT year, SUM(total_revenue) AS annual_revenue
            FROM v_sales
            GROUP BY year
            ORDER BY annual_revenue DESC
            LIMIT 1;
        """).strip(),
        
        # ✅ SQL ที่ถูกต้องสำหรับหาปีที่มีรายได้ต่ำสุด
        'year_min_revenue': dedent("""
            SELECT year, SUM(total_revenue) AS annual_revenue
            FROM v_sales
            GROUP BY year
            ORDER BY annual_revenue ASC
            LIMIT 1;
        """).strip(),
        # เพิ่มใน _load_production_examples()

    # ข้อ 5: Service revenue for specific year
    'service_num': dedent("""
        SELECT SUM(service_num) as total_service
        FROM v_sales
        WHERE year = '2024'
        AND service_num > 0;
    """).strip(),

    # ข้อ 16: Service revenue 2023
    'service_revenue_2023': dedent("""
        SELECT SUM(service_num) as service_revenue
        FROM v_sales
        WHERE year = '2023'
        AND service_num > 0;
    """).strip(),

    # ข้อ 21: Low value transactions
    'low_value_transactions': dedent("""
        SELECT customer_name, job_no, description, total_revenue
        FROM v_sales
        WHERE total_revenue < 50000 
        AND total_revenue > 0
        ORDER BY total_revenue
        LIMIT 20;
    """).strip(),

    # ข้อ 35: Private sector customers
    'private_customers': dedent("""
        SELECT customer_name,
            COUNT(*) as transaction_count,
            SUM(total_revenue) as total_revenue
        FROM v_sales
        WHERE customer_name ILIKE ANY(ARRAY['%จำกัด%', '%บริษัท%', '%co%ltd%'])
        GROUP BY customer_name
        ORDER BY total_revenue DESC
        LIMIT 10;
    """).strip(),

    # ข้อ 40: Customers per year
    'customers_per_year': dedent("""
        SELECT year,
            COUNT(DISTINCT customer_name) as customer_count
        FROM v_sales
        GROUP BY year
        ORDER BY year;
    """).strip(),

    # ข้อ 41: Hitachi related
    'hitachi_customers': dedent("""
        SELECT job_no, customer_name, description, total_revenue
        FROM v_sales
        WHERE customer_name ILIKE '%hitachi%'
        OR description ILIKE '%hitachi%'
        ORDER BY total_revenue DESC;
    """).strip(),

    # ข้อ 44: Average revenue per customer
    'avg_revenue_per_customer': dedent("""
        SELECT AVG(customer_total) as avg_revenue_per_customer
        FROM (
            SELECT customer_name, SUM(total_revenue) as customer_total
            FROM v_sales
            GROUP BY customer_name
        ) customer_totals;
    """).strip(),

    # ข้อ 48: Foreign customers
    'foreign_customers': dedent("""
        SELECT DISTINCT customer_name,
            SUM(total_revenue) as total_spent
        FROM v_sales
        WHERE customer_name ~ '^[A-Z][A-Z]'
        AND customer_name NOT ILIKE '%จำกัด%'
        AND customer_name NOT ILIKE '%บริษัท%'
        GROUP BY customer_name
        ORDER BY total_spent DESC;
    """).strip(),

    # ข้อ 55: Cheapest parts
    'cheapest_parts': dedent("""
        SELECT product_code, product_name, unit_price_num
        FROM v_spare_part
        WHERE unit_price_num > 0
        ORDER BY unit_price_num ASC
        LIMIT 10;
    """).strip(),

    # ข้อ 58: EKAC parts
    'ekac_parts': dedent("""
        SELECT product_code, product_name, balance_num, unit_price_num
        FROM v_spare_part
        WHERE product_code ILIKE '%EKAC%'
        ORDER BY product_code;
    """).strip(),

    # ข้อ 61: Total stock quantity
    'total_stock_quantity': dedent("""
        SELECT SUM(balance_num) as total_stock_quantity
        FROM v_spare_part
        WHERE balance_num > 0;
    """).strip(),

    # ข้อ 64: Reorder parts
    'reorder_parts': dedent("""
        SELECT product_code, product_name, balance_num, unit_price_num
        FROM v_spare_part
        WHERE balance_num < 10 
        AND balance_num > 0
        ORDER BY balance_num ASC;
    """).strip(),

    # ข้อ 66: Unpriced parts
    'unpriced_parts': dedent("""
        SELECT product_code, product_name, balance_num
        FROM v_spare_part
        WHERE unit_price_num = 0 
        OR unit_price_num IS NULL;
    """).strip(),

    # ข้อ 68: EKAC460 specific
    'ekac460_info': dedent("""
        SELECT *
        FROM v_spare_part
        WHERE product_code = 'EKAC460';
    """).strip(),

    # ข้อ 69: SET parts
    'set_parts': dedent("""
        SELECT product_code, product_name, balance_num, unit_price_num
        FROM v_spare_part
        WHERE unit ILIKE '%set%';
    """).strip(),

    # ข้อ 70: Recently received
    'recently_received': dedent("""
        SELECT product_code, product_name, balance_num, received
        FROM v_spare_part
        WHERE received IS NOT NULL
        ORDER BY id DESC
        LIMIT 20;
    """).strip(),

    # ข้อ 84: Stanley works
    'stanley_works': dedent("""
        SELECT project, detail, date, success
        FROM v_work_force
        WHERE customer ILIKE '%stanley%'
        ORDER BY date DESC;
    """).strip(),
        }
    
    def _get_system_prompt(self) -> str:
        """Enhanced system prompt - เน้นย้ำว่ามี table เดียว และกฎการกรองปี"""
        # Check if schema is loaded dynamically
        if self.VIEW_COLUMNS:
            table_list = ', '.join(self.VIEW_COLUMNS.keys())
            prompt = dedent(f"""
            ⚠️ CRITICAL: You have EXACTLY {len(self.VIEW_COLUMNS)} tables: {table_list}
            NO OTHER TABLES EXIST!
            
            Current schema is dynamically loaded from database.
            """)
        else:
            # Fallback prompt
            prompt = dedent("""
            ⚠️ CRITICAL: You have EXACTLY 3 tables. NO OTHER TABLES EXIST!
            
            1. v_sales (ONE table for ALL years)
            2. v_work_force (work/repair records)  
            3. v_spare_part (inventory)
            """)
        
        prompt += dedent("""
        
        ⚠️ FORBIDDEN - THESE TABLES DO NOT EXIST:
        - v_sales2022, v_sales2023, v_sales2024, v_sales2025
        - sales_2023, sales_2024, sales_2025
        - Any year-specific table variants
        
        📌 CRITICAL RULES FOR YEAR FILTERING:
        1. If the question does NOT mention any specific year:
           → DO NOT add WHERE year clause
           → Query should include ALL years in the database
           → Example: "รายได้รวมทั้งหมด" = ALL years, NO WHERE year
        
        2. If the question mentions specific year(s):
           → Add WHERE year IN ('year1', 'year2', ...)
           → Use ONLY the years explicitly mentioned
           → Example: "รายได้ปี 2024" = WHERE year = '2024'
        
        3. Common patterns to recognize:
           - "ทั้งหมด/รวม" without year = ALL years (NO WHERE clause)
           - "แต่ละปี" = GROUP BY year (NO WHERE clause)
           - "ปี X" = WHERE year = 'X'
           - "เปรียบเทียบปี X กับ Y" = WHERE year IN ('X','Y')
        
        📌 CRITICAL SQL RULES:
        1. When using aggregate functions (MIN, MAX, AVG, SUM, COUNT):
           - Either use them WITHOUT other columns
           - OR include ALL non-aggregate columns in GROUP BY clause
           - NEVER mix aggregate and non-aggregate columns without GROUP BY
        
        2. To find records with min/max values:
           - Use ORDER BY column ASC/DESC with LIMIT 1
           - NOT MIN(column) with other fields
           - Example: ORDER BY total_revenue ASC LIMIT 1 (for minimum)
        
        3. For year-related min/max queries:
           - "ปีไหนมีรายได้ต่ำสุด" = GROUP BY year first, then ORDER BY SUM(total_revenue) ASC LIMIT 1
           - "ปีไหนมีรายได้สูงสุด" = GROUP BY year first, then ORDER BY SUM(total_revenue) DESC LIMIT 1
           - NOT SELECT MIN(year) with ORDER BY total_revenue (this is WRONG!)
        
        4. IMPORTANT DATA TYPE NOTES:
           - Column 'year' in v_sales is TEXT type, not numeric
           - To do arithmetic with year: CAST(year AS INTEGER) or year::int
           - DO NOT use: MAX(year) - MIN(year) without casting
           - CORRECT: MAX(year::int) - MIN(year::int)
           - For counting distinct years: COUNT(DISTINCT year) works fine
        
        5. Common SQL patterns:
           - Find year with min revenue: GROUP BY year ORDER BY SUM(total_revenue) ASC LIMIT 1
           - Find year with max revenue: GROUP BY year ORDER BY SUM(total_revenue) DESC LIMIT 1
           - Count years active: COUNT(DISTINCT year)
           - Year range: MAX(year::int) - MIN(year::int) + 1
           - Find minimum record: ORDER BY column ASC LIMIT 1
           - Find maximum record: ORDER BY column DESC LIMIT 1
        
        ALWAYS use the EXACT structure from the provided example!
        """)
        
        return prompt.strip()
    
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
        """Build SQL generation prompt with centralized template configuration"""
        
        try:
            # ============================================
            # INITIALIZATION
            # ============================================
            
            # Ensure schema is loaded
            if not self.VIEW_COLUMNS:
                self._load_dynamic_schema()
            
            # Validate input
            is_valid, msg = self.validate_input(question)
            if not is_valid:
                logger.error(f"Invalid input: {msg}")
                raise ValueError(f"Invalid input: {msg}")
            
            # Validate and convert entities
            entities = self.validate_entities(entities)
            question_lower = question.lower()
            
            # Convert Buddhist Era years in question
            question = re.sub(r'\b25[67]\d\b', lambda m: str(int(m.group())-543), question)
            
            # Use context if provided
            if context:
                logger.debug(f"Using context: {context}")
                for key, value in context.get('entities', {}).items():
                    if key not in entities or not entities[key]:
                        entities[key] = value
            
            # ============================================
            # INTENT OVERRIDE LOGIC
            # ============================================
            
            original_intent = intent
            
            # Override Rule 1: Money/Value keywords → force sales intent
            money_indicators = ['มูลค่า', 'ราคา', 'revenue', 'ยอดขาย', 'รายได้', 'บาท']
            if any(word in question_lower for word in money_indicators):
                if intent in ['work_force', 'work_plan', 'repair_history']:
                    logger.warning(f"Override intent: {intent} → sales (found money keywords)")
                    intent = 'sales'
            
            # Override Rule 2: Specific patterns
            if 'งาน' in question_lower and any(kw in question_lower for kw in ['มูลค่า', 'สูงสุด', 'ต่ำสุด']):
                logger.warning(f"Pattern 'งาน + มูลค่า' detected → forcing sales intent")
                intent = 'sales'
            
            # ============================================
            # DETERMINE TARGET TABLE
            # ============================================
            
            target_table = self._get_target_table(intent)
            
            if target_table is None:
                logger.info(f"No table mapping for intent '{intent}', detecting from keywords")
                target_table = self._detect_table_from_keywords(question)
            
            # Final validation for money-related queries
            if 'มูลค่า' in question_lower and target_table != 'v_sales':
                logger.warning(f"Final override: {target_table} → v_sales (มูลค่า requires v_sales)")
                target_table = 'v_sales'
                intent = 'sales'
            
            logger.info(f"🎯 Target table: {target_table} (intent: {original_intent} → {intent})")
            
            # ============================================
            # SELECT BEST EXAMPLE
            # ============================================
            
            example = self._select_best_example(question, intent, entities)
            example_name = self._get_example_name(example)
            
            if not example_name or example_name == 'custom':
                logger.warning(f"No specific template found, using fallback")
                return self._get_fallback_prompt(question)
            
            logger.info(f"Selected SQL example: {example_name} for table {target_table}")
            
            # ============================================
            # GET TEMPLATE CONFIGURATION
            # ============================================
            
            template_config = TemplateConfig.get_template_config(example_name)
            
            if not template_config:
                logger.warning(f"No configuration found for template: {example_name}")
                # Fallback to normal mode if no config
                return self._build_normal_prompt(
                    example, question, intent, entities, target_table
                )
            
            complexity = template_config.get('complexity', 'NORMAL')
            logger.info(f"Template complexity: {complexity}")
            
            # ============================================
            # HANDLE BASED ON COMPLEXITY
            # ============================================
            
            # CASE 1: COMPLEX LOGIC TEMPLATES
            if complexity == 'COMPLEX':
                logger.warning(f"⚠️ COMPLEX template: {example_name}")
                
                # Apply smart year adjustment if needed
                modified_example = example
                if TemplateConfig.requires_smart_year_adjustment(example_name):
                    modified_example = self._apply_smart_year_adjustment(
                        example, entities, question_lower
                    )
                
                return self._build_complex_prompt(
                    modified_example, question, intent, template_config
                )
            
            # CASE 2: EXACT TEMPLATES
            elif complexity == 'EXACT':
                logger.warning(f"📌 EXACT template: {example_name}")
                return self._build_exact_prompt(example)
            
            # CASE 3: NORMAL TEMPLATES
            else:
                logger.info(f"NORMAL template: {example_name}")
                
                # Apply simple year adjustment if needed
                modified_example = example
                if template_config.get('year_adjustment') == 'simple' and entities.get('years'):
                    modified_example = self._apply_simple_year_adjustment(example, entities)
                
                return self._build_normal_prompt(
                    modified_example, question, intent, entities, target_table
                )
                
        except Exception as e:
            logger.error(f"Failed to build SQL prompt: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return self._get_fallback_prompt(question)


    def _apply_smart_year_adjustment(self, template: str, entities: Dict, 
                                    question_lower: str) -> str:
        """Apply smart year adjustment for complex templates"""
        if not entities.get('years'):
            return template
            
        target_year = entities['years'][0]
        modified = template
        
        logger.info(f"Applying smart year adjustment for year: {target_year}")
        
        # Check context for NOT IN queries
        if any(phrase in question_lower for phrase in ['ไม่ได้ใช้', 'inactive', 'ไม่ใช้', 'เคยใช้แต่ไม่']):
            logger.debug("Detected inactive/not-in context")
            
            # Adjust NOT IN clause (the exclusion year)
            modified = re.sub(
                r"WHERE year = '\d{4}'(?=\s*\))",  # Year in NOT IN subquery
                f"WHERE year = '{target_year}'",
                modified
            )
            
            # Adjust main WHERE for previous years
            prev_year = str(int(target_year) - 1)
            prev_year2 = str(int(target_year) - 2)
            
            # Replace year range in main WHERE clause
            modified = re.sub(
                r"WHERE year IN \([^)]+\)(?!\s*\))",  # Main WHERE clause (not in subquery)
                f"WHERE year IN ('{prev_year2}', '{prev_year}')",
                modified,
                count=1  # Only first occurrence
            )
            
            logger.debug(f"Adjusted NOT IN year to {target_year}, main years to {prev_year2}-{prev_year}")
        
        elif 'ใหม่' in question_lower or 'new' in question_lower:
            logger.debug("Detected new customers context")
            
            # For new customers: current year in main, previous years in NOT IN
            modified = re.sub(r"'2024'", f"'{target_year}'", modified)
            modified = re.sub(r"'2023'", f"'{str(int(target_year)-1)}'", modified)
            modified = re.sub(r"'2022'", f"'{str(int(target_year)-2)}'", modified)
        
        else:
            # Standard year replacement
            logger.debug("Applying standard year replacement")
            modified = modified.replace("'2024'", f"'{target_year}'")
            modified = modified.replace("'2023'", f"'{str(int(target_year)-1)}'")
            modified = modified.replace("'2022'", f"'{str(int(target_year)-2)}'")
        
        return modified


    def _apply_simple_year_adjustment(self, template: str, entities: Dict) -> str:
        """Apply simple year replacement"""
        if not entities.get('years'):
            return template
        
        target_year = entities['years'][0]
        modified = template
        
        logger.debug(f"Applying simple year adjustment: {target_year}")
        
        # Simple replacement
        modified = re.sub(r"year = '\d{4}'", f"year = '{target_year}'", modified)
        modified = re.sub(r"year IN \('[0-9, ']+'\)", f"year = '{target_year}'", modified)
        
        return modified


    def _build_complex_prompt(self, template: str, question: str, 
                            intent: str, config: Dict) -> str:
        """Build prompt for complex logic templates"""
        
        # Get additional context
        schema_info = self._get_strict_schema_for_intent(intent)
        common_errors = self._get_common_errors()
        
        # Build warning about complexity
        complexity_warning = []
        if config.get('has_subquery'):
            complexity_warning.append("- Contains subqueries that must be preserved")
        if config.get('has_not_in'):
            complexity_warning.append("- Contains NOT IN logic that must be kept intact")
        
        prompt = dedent(f"""
        You are a SQL query generator. Output ONLY the SQL query with no explanation.
        
        ⚠️ CRITICAL: This is a COMPLEX LOGIC query. The structure MUST be preserved exactly.
        
        Template Type: {config.get('description', 'Complex query')}
        Complexity Warnings:
        {chr(10).join(complexity_warning) if complexity_warning else '- Complex business logic'}
        
        {schema_info}
        
        VERIFIED SQL TEMPLATE (DO NOT SIMPLIFY):
        ----------------------------------------
        {template}
        ----------------------------------------
        
        {common_errors}
        
        STRICT RULES:
        1. DO NOT remove or simplify NOT IN clauses
        2. DO NOT remove or change subqueries
        3. DO NOT change the WHERE condition logic
        4. DO NOT simplify the query structure
        5. ONLY adjust literal values if absolutely necessary
        
        Question: {question}
        
        Output the SQL above exactly as shown:
        """).strip()
        
        return prompt


    def _build_exact_prompt(self, template: str) -> str:
        """Build prompt for exact templates (no modification allowed)"""
        
        prompt = dedent(f"""
        You are a SQL query generator. Output ONLY the SQL query with no explanation.
        
        📌 EXACT TEMPLATE - Copy without ANY modifications:
        
        {template}
        
        CRITICAL: Output the exact SQL above. Do not change ANYTHING.
        
        SQL:
        """).strip()
        
        return prompt

    def _build_normal_prompt(self, template: str, question: str, intent: str,
                            entities: Dict, target_table: str) -> str:
        """Build prompt for normal templates (flexible modification allowed)"""
        
        # Get schema and hints
        schema_prompt = self._get_dynamic_schema_prompt(target_table)
        hints = self._build_sql_hints(entities, intent)
        column_rules = self._get_column_rules_for_intent(intent)
        
        # Generate column hints
        column_hints = self._generate_column_hints(target_table, 
                                                self.VIEW_COLUMNS.get(target_table, []))
        column_hints_str = '\n'.join(column_hints) if column_hints else ''
        
        # Special rules for v_work_force table
        work_force_rules = ""
        if target_table == 'v_work_force':
            work_force_rules = dedent("""
            ⚠️ IMPORTANT RULES FOR WORK FORCE QUERIES:
            1. DO NOT add date filters unless the question mentions specific dates/months/years
            2. If the question asks for "งาน overhaul" without date, show ALL overhaul work
            3. Only add WHERE date conditions if explicitly mentioned in the question
            4. Default should be to show ALL relevant records without date restrictions
            """)
        
        # Check if date/time is mentioned in question
        question_lower = question.lower()
        has_date_context = any(word in question_lower for word in [
            'วันนี้', 'today', 'เมื่อวาน', 'yesterday',
            'สัปดาห์', 'week', 'เดือน', 'month', 'ปี', 'year',
            '2022', '2023', '2024', '2025', '2026',
            'มกราคม', 'กุมภา', 'มีนา', 'เมษา', 'พฤษภา', 'มิถุนา',
            'กรกฎา', 'สิงหา', 'กันยา', 'ตุลา', 'พฤศจิกา', 'ธันวา'
        ])
        
        # Build date instruction
        date_instruction = ""
        if target_table == 'v_work_force' and not has_date_context:
            date_instruction = dedent("""
            📌 DATE FILTER INSTRUCTION:
            The question does NOT mention any specific date/time period.
            Therefore, DO NOT add any date filters to the WHERE clause.
            Show ALL records that match the other criteria.
            """)
        
        prompt = dedent(f"""
        You are a SQL query generator. Output ONLY the SQL query with no explanation.
        
        DATABASE SCHEMA:
        ----------------------------------------
        {schema_prompt}
        ----------------------------------------
        
        COLUMN USAGE HINTS:
        {column_hints_str}
        
        REFERENCE TEMPLATE (modify as needed):
        ----------------------------------------
        {template}
        ----------------------------------------
        
        {column_rules}
        
        {work_force_rules}
        
        YOUR TASK: {question}
        
        {date_instruction}
        
        {hints}
        
        GUIDELINES:
        1. Follow the general structure of the template
        2. Modify values (dates, names, years) ONLY if mentioned in the question
        3. Keep the same table and column names
        4. Add LIMIT 1000 if not present
        5. DO NOT add extra filters that are not in the question
        6. If the question is simple (like "งาน overhaul"), keep the query simple
        
        SQL:
        """).strip()
        
        return prompt

    def _get_example_name(self, example: str) -> str:
        """Get example name for logging and exact match checking"""
        for name, sql in self.SQL_EXAMPLES.items():
            # Strip and normalize for comparison
            if sql.strip() == example.strip():
                return name
        return 'custom'


    def _should_use_exact_template(self, example_name: str, question: str) -> bool:
        """Determine if we should use the template exactly without modification"""
        
        # List of examples that should ALWAYS be used exactly
        exact_use_examples = {
            'max_value_work': ['งานที่มีมูลค่าสูงสุด', 'มูลค่าสูงสุด'],
            'min_value_work': ['งานที่มีมูลค่าต่ำสุด', 'มูลค่าต่ำสุด'],
            'year_max_revenue': ['ปีที่มีรายได้สูงสุด', 'ปีไหนรายได้สูงสุด'],
            'year_min_revenue': ['ปีที่มีรายได้ต่ำสุด', 'ปีไหนรายได้ต่ำสุด'],
            'total_revenue_all': ['รายได้รวมทั้งหมด', 'รายได้ทั้งหมด'],
            'count_total_customers': ['จำนวนลูกค้า', 'มีลูกค้ากี่ราย'],
            'count_all_jobs': ['จำนวนงาน', 'มีงานกี่งาน'],
        }
        
        question_lower = question.lower()
        
        if example_name in exact_use_examples:
            patterns = exact_use_examples[example_name]
            for pattern in patterns:
                if pattern in question_lower:
                    logger.info(f"Exact template use triggered for {example_name} by pattern '{pattern}'")
                    return True
        
        return False

    def _get_dynamic_schema_prompt(self, table_name: str) -> str:
        """Generate schema prompt dynamically based on current database structure"""
        table_info = self.get_table_info(table_name)
        
        if not table_info:
            return f"Table {table_name} not found in schema"
        
        prompt_parts = [f"TABLE: {table_name}"]
        prompt_parts.append(f"COLUMNS ({len(table_info['columns'])} total):")
        
        # List all columns
        for col in table_info['columns']:
            prompt_parts.append(f"  - {col}")
        
        # Add usage hints based on column names
        hints = self._generate_column_hints(table_name, table_info['columns'])
        if hints:
            prompt_parts.append("\nUSAGE HINTS:")
            prompt_parts.extend(hints)
        
        return "\n".join(prompt_parts)
    
    def _generate_column_hints(self, table_name: str, columns: List[str]) -> List[str]:
        """Generate intelligent hints based on column names"""
        hints = []
        
        # Common patterns
        if table_name == 'v_sales':
            for col in columns:
                if 'revenue' in col.lower():
                    hints.append(f"  - {col}: Use for revenue/income queries")
                elif '_num' in col and col in ['overhaul_num', 'replacement_num', 'service_num', 'parts_num']:
                    base_name = col.replace('_num', '')
                    hints.append(f"  - {col}: Numeric value for {base_name}")
        
        elif table_name == 'v_spare_part':
            if 'balance_num' in columns:
                hints.append(f"  - balance_num: Stock quantity")
            if 'unit_price_num' in columns:
                hints.append(f"  - unit_price_num: Price per unit")
            if 'total_num' in columns:
                hints.append(f"  - total_num: Total value")
        
        elif table_name == 'v_work_force':
            for col in columns:
                if col.startswith('job_description_'):
                    job_type = col.replace('job_description_', '')
                    hints.append(f"  - {col}: Filter for {job_type} jobs")
        
        return hints[:5]  # Limit hints
    
    def _detect_table_from_keywords(self, question: str) -> str:
        """Detect table from question keywords - FIXED priority detection"""
        q_lower = question.lower()
        
        # Priority 1: Money/Value/Revenue keywords → v_sales (สูงสุด)
        money_keywords = [
            'มูลค่า', 'ราคา', 'รายได้', 'ยอดขาย', 'revenue', 
            'บาท', 'ค่า', 'จ่าย', 'value', 'amount', 'cost',
            'สูงสุด', 'ต่ำสุด', 'มากที่สุด', 'น้อยที่สุด',  # เพิ่ม min/max
            'รวม', 'total', 'sum', 'ยอดรวม'
        ]
        if any(kw in q_lower for kw in money_keywords):
            logger.info(f"Detected money/value keywords in '{question[:50]}...' → v_sales")
            return 'v_sales'
        
        # Priority 2: Parts/Inventory keywords → v_spare_part
        parts_keywords = [
            'อะไหล่', 'สต็อก', 'คลัง', 'spare', 'part', 'inventory',
            'balance', 'unit_price', 'warehouse', 'wh', 'สินค้า',
            'product_code', 'product_name', 'จำนวนคงเหลือ'
        ]
        if any(kw in q_lower for kw in parts_keywords):
            logger.info(f"Detected parts/inventory keywords → v_spare_part")
            return 'v_spare_part'
        
        # Priority 3: Work/Service keywords → v_work_force (ต่ำสุด)
        # ต้องไม่มี money keywords
        work_keywords = [
            'แผนงาน', 'ที่ทำ', 'ที่วางแผน', 'pm', 'ติดตั้ง', 
            'ทีม', 'ซ่อม', 'บำรุง', 'service_group', 'kpi', 'duration',
            'สำเร็จ', 'ไม่สำเร็จ', 'failure', 'report', 'วันที่ทำงาน'
        ]
        # เช็คว่ามี work keywords และไม่มี money keywords
        if any(kw in q_lower for kw in work_keywords):
            # Double check: ถ้ามีคำเกี่ยวกับเงิน ให้ใช้ v_sales
            if not any(kw in q_lower for kw in money_keywords):
                logger.info(f"Detected work keywords (no money context) → v_work_force")
                return 'v_work_force'
            else:
                logger.info(f"Detected work + money keywords → v_sales (money priority)")
                return 'v_sales'
        
        # Default: v_sales (เพราะส่วนใหญ่เกี่ยวกับการขาย/รายได้)
        logger.info(f"No specific keywords detected → v_sales (default)")
        return 'v_sales'

    # 2. แก้ไข _get_target_table ให้ใช้ keyword detection เมื่อไม่รู้จัก intent
    def _get_target_table(self, intent: str) -> str:
        """Determine target table based on intent"""
        intent_to_table = {
            'sales': 'v_sales',
            'sales_analysis': 'v_sales',
            'customer_history': 'v_sales',
            'overhaul_report': 'v_sales',
            'top_customers': 'v_sales',
            'revenue': 'v_sales',
            'annual_revenue': 'v_sales',
            'work_plan': 'v_work_force',
            'work_force': 'v_work_force',
            'work_summary': 'v_work_force',
            'repair_history': 'v_work_force',
            'pm_summary': 'v_work_force',
            'startup_summary': 'v_work_force',
            'successful_works': 'v_work_force',
            'spare_parts': 'v_spare_part',
            'parts_price': 'v_spare_part',
            'inventory_check': 'v_spare_part',
            'inventory_value': 'v_spare_part',
            'warehouse_summary': 'v_spare_part'
        }
        
        # ถ้ารู้จัก intent ใช้ mapping
        if intent in intent_to_table:
            return intent_to_table[intent]
        
        # ถ้าไม่รู้จัก ให้ detect จาก keywords (ไม่ใช่ default v_sales)
        # ส่ง question ไปด้วย - ต้องรับจาก build_sql_prompt
        # เพื่อความง่าย return None และให้ build_sql_prompt เรียก _detect_table_from_keywords
        return None

    # 3. เพิ่ม method กรอง examples ตามตาราง
    def _filter_examples_by_table(self, table: str) -> Dict[str, str]:
        """Filter SQL examples that match the target table using centralized config"""
        
        # Get template names from centralized config
        template_names = TemplateConfig.get_templates_by_table(table)
        
        filtered = {}
        for name in template_names:
            if name in self.SQL_EXAMPLES:
                filtered[name] = self.SQL_EXAMPLES[name]
        
        logger.debug(f"Filtered {len(filtered)} examples for table {table} using TemplateConfig")
        return filtered


    # Update _should_use_exact_template to use centralized config
    def _should_use_exact_template(self, template_name: str, question: str = None) -> bool:
        """Determine if template should be used exactly (using centralized config)"""
        
        # Check centralized config first
        if TemplateConfig.is_exact_template(template_name):
            logger.debug(f"Template {template_name} marked as EXACT in config")
            return True
        
        # Optionally check question patterns if needed
        # (keeping backward compatibility)
        if question:
            question_lower = question.lower()
            exact_patterns = {
                'total_revenue_all': ['รายได้รวมทั้งหมด', 'รายได้ทั้งหมด'],
                'count_total_customers': ['จำนวนลูกค้า', 'มีลูกค้ากี่ราย'],
            }
            
            if template_name in exact_patterns:
                patterns = exact_patterns[template_name]
                for pattern in patterns:
                    if pattern in question_lower:
                        logger.debug(f"Pattern '{pattern}' suggests exact template usage")
                        return True
        
        return False

    def _has_exact_matching_example(self, question: str, example_name: str) -> bool:
        """Check if we have an exact matching example for this question type"""
        question_lower = question.lower()
        
        # Define exact match patterns
        exact_matches = {
            'customer_years_count': ['ซื้อขายมากี่ปี', 'กี่ปีแล้ว', 'how many years'],
            'customer_history': ['ประวัติลูกค้า', 'ประวัติการซื้อขาย', 'customer history'],
            'work_monthly': ['งานที่วางแผน', 'แผนงาน', 'work plan'],
            'overhaul_sales_all': ['ยอดขาย overhaul ทั้งหมด', 'overhaul ทั้งหมด', 'total overhaul'],
            'overhaul_sales': ['ยอดขาย overhaul', 'overhaul sales'],
            'sales_analysis': ['วิเคราะห์การขาย', 'sales analysis'],
            'spare_parts_price': ['ราคาอะไหล่', 'spare parts price'],
            'total_revenue_all': ['รายได้รวมทั้งหมด', 'รายได้ทั้งหมด', 'total revenue all'],
        }
        
        # Check if current example has exact match patterns
        if example_name in exact_matches:
            patterns = exact_matches[example_name]
            for pattern in patterns:
                if pattern in question_lower:
                    logger.info(f"Found exact match pattern '{pattern}' for example '{example_name}'")
                    # Special handling for "ทั้งหมด" queries - they should use no-filter examples
                    if 'ทั้งหมด' in pattern and 'ทั้งหมด' in question_lower:
                        return True
                    elif 'ทั้งหมด' not in pattern:
                        return True
        
        return False
    
    def _get_strict_schema_for_intent(self, intent: str) -> str:
        """Get exact schema based on intent - dynamically"""
        target_table = self._get_target_table(intent)
        return self._get_dynamic_schema_prompt(target_table)
    
    def _get_column_rules_for_intent(self, intent: str) -> str:
        """Get specific rules for common queries - Enhanced version"""
        
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
            - For PM jobs: WHERE job_description_pm is not null
            - Never use COUNT(*) for "มีกี่งาน" - show the actual records
            - DO NOT add date filters unless explicitly asked
            """,
            
            'work_force': """
            COLUMN USAGE RULES:
            - For overhaul work: WHERE job_description_overhaul IS NOT NULL AND job_description_overhaul != ''
            - For PM work: WHERE job_description_pm is not null
            - For replacement: WHERE job_description_replacement is not null
            - DO NOT add date filters unless the question mentions dates
            - Show ALL relevant records by default
            """,
            
            'work_analysis': """
            COLUMN USAGE RULES:
            - For overhaul: Check job_description_overhaul column
            - For successful: Check success column
            - For team: Check service_group column
            - Default: Show ALL matching records without date restriction
            - Only filter by date if explicitly mentioned
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
        """Select most relevant example - COMPLETE MAPPING VERSION"""
        question_lower = question.lower()
        
        # === PHASE 0: Direct Exact Match - เพิ่มคำถามจาก 100 ข้อ ===
        exact_matches = {
            # ข้อ 1-10: รายได้และยอดขาย
            'รายได้รวมทั้งหมดเท่าไหร่': 'total_revenue_all',
            'รายได้ปี 2024': 'total_revenue_year',
            'เปรียบเทียบรายได้ปี 2023 กับ 2024': 'compare_revenue_years',
            'ยอดขาย overhaul ทั้งหมด': 'overhaul_total',
            'ยอดขาย service ปี 2024': 'service_num',  # ต้องสร้างใหม่
            'ยอดขาย parts/อะไหล่': 'parts_total',
            'ยอดขาย replacement/เปลี่ยนอุปกรณ์': 'replacement_total',
            'รายได้แต่ละปีเป็นอย่างไร': 'revenue_by_year',
            'ยอดขายแยกตามประเภทงาน': 'revenue_by_service_type',
            'รายได้เฉลี่ยต่อปี': 'average_annual_revenue',
            
            # ข้อ 11-25: การวิเคราะห์รายได้
            'ปีไหนมีรายได้สูงสุด': 'year_max_revenue',
            'ปีไหนมีรายได้ต่ำสุด': 'year_min_revenue',
            'งานที่มีมูลค่าสูงสุด': 'max_value_work',
            'งานที่มีมูลค่าต่ำสุด': 'min_value_work',
            'รายได้จาก overhaul ปี 2024': 'overhaul_sales_specific',
            'รายได้จาก service ปี 2023': 'service_revenue_2023',  # ต้องสร้างใหม่
            'มีงานทั้งหมดกี่งาน': 'count_all_jobs',
            'มีงานปี 2024 กี่งาน': 'count_jobs_year',
            'รายได้เฉลี่ยต่องาน': 'average_revenue_per_job',
            'งานที่มีรายได้มากกว่า 1 ล้าน': 'high_value_transactions',
            'งานที่มีรายได้น้อยกว่า 50,000': 'low_value_transactions',  # ต้องสร้างใหม่
            'การเติบโตรายได้จาก 2023 เป็น 2024': 'revenue_growth',
            'สัดส่วนรายได้แต่ละประเภท': 'revenue_proportion',
            'รายได้สูงสุดต่อปี': 'max_revenue_each_year',
            'มูลค่ารวมของสินค้าคงคลัง': 'total_inventory_value',
            
            # ข้อ 26-50: ลูกค้า
            'มีลูกค้าทั้งหมดกี่ราย': 'count_total_customers',
            'top 10 ลูกค้าที่ใช้บริการมากที่สุด': 'top_customers',
            'ลูกค้าที่มียอดการใช้บริการสูงสุด': 'top_customers',
            'ประวัติการใช้บริการของ stanley': 'customer_specific_history',
            'ประวัติการใช้บริการของ clarion': 'customer_specific_history',
            'ลูกค้าใหม่ปี 2024': 'new_customers_year',
            'ลูกค้าที่ใช้บริการบ่อยที่สุด': 'frequent_customers',
            'ลูกค้าที่ใช้บริการ overhaul': 'customers_using_overhaul',
            'ลูกค้าภาครัฐมีใครบ้าง': 'government_customers',
            'ลูกค้าเอกชนที่ใหญ่ที่สุด': 'private_customers',  # ต้องสร้างใหม่
            'ข้อมูลลูกค้า': 'customer_specific_history',
            'ลูกค้าที่เคยใช้บริการแต่ไม่ได้ใช้ปี 2024': 'inactive_customers',
            'ลูกค้าที่ใช้บริการต่อเนื่องทุกปี': 'continuous_customers',
            'จำนวนลูกค้าที่ใช้บริการแต่ละปี': 'customers_per_year',  # ต้องสร้างใหม่
            'ลูกค้าที่เกี่ยวข้องกับ hitachi': 'hitachi_customers',  # ต้องสร้างใหม่
            'ลูกค้าโรงพยาบาลมีใครบ้าง': 'hospital_customers',
            'ลูกค้าที่จ่ายมากกว่า 500,000 บาท': 'high_value_customers',
            'รายได้เฉลี่ยต่อลูกค้า': 'avg_revenue_per_customer',  # ต้องสร้างใหม่
            'ลูกค้าที่ใช้บริการเฉพาะซื้ออะไหล่': 'parts_only_customers',
            'ลูกค้าต่างชาติมีใครบ้าง': 'foreign_customers',  # ต้องสร้างใหม่
            'ลูกค้าที่มีงาน chiller': 'chiller_customers',
            'เปรียบเทียบรายได้จากลูกค้าใหม่กับลูกค้าเก่า': 'new_vs_returning_customers',
            
            # ข้อ 51-70: อะไหล่และสินค้าคงคลัง
            'มีอะไหล่ทั้งหมดกี่รายการ': 'count_all_parts',
            'อะไหล่ที่มีสต็อกคงเหลือ': 'parts_in_stock',
            'อะไหล่ที่หมดสต็อก': 'parts_out_of_stock',
            'อะไหล่ที่มีราคาแพงที่สุด': 'most_expensive_parts',
            'อะไหล่ที่มีราคาถูกที่สุด': 'cheapest_parts',  # ต้องสร้างใหม่
            'อะไหล่ที่มีสต็อกน้อยกว่า 5 ชิ้น': 'low_stock_alert',
            'อะไหล่ในคลัง a': 'warehouse_specific_parts',
            'อะไหล่ที่มีรหัส ekac': 'ekac_parts',  # ต้องสร้างใหม่
            'อะไหล่ที่มีมูลค่ารวมสูงสุด': 'highest_value_items',
            'ราคาเฉลี่ยของอะไหล่': 'average_part_price',
            'จำนวนอะไหล่ทั้งหมดที่มีในสต็อก': 'total_stock_quantity',  # ต้องสร้างใหม่
            'อะไหล่สำหรับ compressor': 'compressor_parts',
            'อะไหล่ filter มีอะไรบ้าง': 'filter_parts',
            'อะไหล่ที่ต้องสั่งเติม': 'reorder_parts',  # ต้องสร้างใหม่
            'มูลค่าสต็อกรวมทั้งหมด': 'total_inventory_value',
            'อะไหล่ที่ไม่มีการตั้งราคา': 'unpriced_parts',  # ต้องสร้างใหม่
            'คลังไหนมีอะไหล่มากที่สุด': 'warehouse_comparison',
            'ข้อมูลอะไหล่รหัส ekac460': 'ekac460_info',  # ต้องสร้างใหม่
            'อะไหล่ที่ขายเป็น set': 'set_parts',  # ต้องสร้างใหม่
            'สินค้าที่เพิ่งรับเข้าล่าสุด': 'recently_received',  # ต้องสร้างใหม่
            
            # ข้อ 71-90: งานบริการและทีมงาน
            'มีงานทั้งหมดกี่งานในระบบ': 'count_all_works',
            'งานเดือนกันยายน 2024': 'work_specific_month',
            'งานบำรุงรักษา (pm) ทั้งหมด': 'all_pm_works',
            'งาน overhaul ที่ทำ': 'work_overhaul',
            'งานเปลี่ยนอุปกรณ์': 'work_replacement',
            'งานที่ทำสำเร็จ': 'successful_works',
            'งานที่ไม่สำเร็จ': 'unsuccessful_works',
            'งานของทีม a': 'team_works',
            'งานวันนี้มีอะไรบ้าง': 'work_today',
            'งานสัปดาห์นี้': 'work_this_week',
            'อัตราความสำเร็จของงาน': 'success_rate',
            'งานที่เสร็จตรงเวลา': 'on_time_works',
            'งานที่ทำเกินเวลา': 'overtime_works',
            'งานทั้งหมดของลูกค้า stanley': 'stanley_works',  # ต้องสร้างใหม่
            'งาน start up/เริ่มต้นระบบ': 'startup_works_all',
            'งานสนับสนุนทั่วไป': 'support_works',
            'งาน cpa': 'cpa_works',
            'สถิติงานของแต่ละทีม': 'team_statistics',
            'ระยะเวลาการทำงานแต่ละงาน': 'work_duration',
            'งาน 10 งานล่าสุด': 'latest_works',
            
            # ข้อ 91-100: การวิเคราะห์และรายงาน
            'สรุปผลประกอบการแต่ละปี': 'annual_performance_summary',
            'เทรนด์การเติบโตของธุรกิจ': 'growth_trend',
            'ประเภทงานที่ลูกค้านิยมใช้บริการมากที่สุด': 'popular_service_types',
            'ลูกค้าที่มีศักยภาพสูง': 'high_potential_customers',
            'การกระจายรายได้ตามขนาดงาน': 'revenue_distribution',
            'ประสิทธิภาพการทำงานของแต่ละทีม': 'team_performance',
            'แนวโน้มยอดขายรายเดือนปี 2024': 'monthly_sales_trend',
            'ผลตอบแทนจากการลงทุนในแต่ละประเภทบริการ': 'service_roi',
            'คาดการณ์รายได้ปี 2025 จากเทรนด์': 'revenue_forecast',
            'รายงานสถานะธุรกิจโดยรวม': 'business_overview',
        }
        
        for pattern, example_name in exact_matches.items():
            if pattern in question_lower:
                logger.info(f"Exact match found: {example_name} for pattern '{pattern}'")
                if example_name in self.SQL_EXAMPLES:
                    return self.SQL_EXAMPLES[example_name]
        
        # === PHASE 1: Pattern-Based Priority Rules ===
        
        # Rule 1: "แยกตาม" = Breakdown query
        if any(word in question_lower for word in ['แยกตาม', 'แต่ละประเภท', 'breakdown', 'by type']):
            if any(word in question_lower for word in ['ประเภทงาน', 'ประเภท', 'type', 'service']):
                logger.info("Priority rule: แยกตามประเภท → revenue_by_service_type")
                return self.SQL_EXAMPLES.get('revenue_by_service_type', '')
        
        # Rule 2: Value/Price + Max/Min
        if any(word in question_lower for word in ['มูลค่า', 'ราคา', 'value', 'price']):
            if any(word in question_lower for word in ['สูงสุด', 'มากที่สุด', 'แพงที่สุด', 'highest', 'maximum']):
                if 'งาน' in question_lower:
                    logger.info("Priority rule: งาน + มูลค่าสูงสุด → max_value_work")
                    return self.SQL_EXAMPLES.get('max_value_work', '')
                elif 'อะไหล่' in question_lower or 'parts' in question_lower:
                    logger.info("Priority rule: อะไหล่ + ราคาสูงสุด → most_expensive_parts")
                    return self.SQL_EXAMPLES.get('most_expensive_parts', '')
            elif any(word in question_lower for word in ['ต่ำสุด', 'น้อยที่สุด', 'ถูกที่สุด', 'lowest', 'minimum']):
                if 'งาน' in question_lower:
                    logger.info("Priority rule: งาน + มูลค่าต่ำสุด → min_value_work")
                    return self.SQL_EXAMPLES.get('min_value_work', '')
        
        # Rule 3: Customer history with years
        if any(word in question_lower for word in ['ซื้อขาย', 'ประวัติ', 'history', 'ย้อนหลัง']):
            if entities.get('customers'):
                logger.info("Priority rule: Customer history → customer_history")
                return self.SQL_EXAMPLES.get('customer_history', '')
        
        # === PHASE 2: Full EXAMPLE_KEYWORDS Mapping (All 100+ examples) ===
        EXAMPLE_KEYWORDS = {
            # === REVENUE/SALES EXAMPLES (หมวด 1: ข้อ 1-25) ===
            'total_revenue_all': ['รายได้รวมทั้งหมด', 'รายได้ทั้งหมด', 'total revenue', 'รายได้รวม'],
            'total_revenue_year': ['รายได้ปี', 'revenue year', 'รายได้ใน'],
            'revenue_by_year': ['รายได้แต่ละปี', 'รายได้รายปี', 'annual revenue', 'revenue by year'],
            'compare_revenue_years': ['เปรียบเทียบรายได้', 'compare revenue', 'เทียบรายได้'],
            'overhaul_total': ['ยอดขาย overhaul', 'total overhaul', 'overhaul ทั้งหมด'],
            'overhaul_sales': ['overhaul แต่ละปี', 'overhaul by year'],
            'overhaul_sales_all': ['overhaul รวม', 'ยอด overhaul', 'overhaul revenue'],
            'overhaul_sales_specific': ['overhaul ปี', 'overhaul เฉพาะปี'],
            'service_num': ['ยอด service', 'service revenue', 'บริการ'],
            'parts_total': ['ยอดขาย parts', 'ยอดอะไหล่', 'parts revenue'],
            'replacement_total': ['ยอดขาย replacement', 'ยอดเปลี่ยน', 'replacement revenue'],
            'average_annual_revenue': ['รายได้เฉลี่ยต่อปี', 'average annual', 'เฉลี่ยต่อปี'],
            'year_min_revenue': ['ปีที่มีรายได้ต่ำสุด', 'ปีไหนรายได้น้อยสุด', 'lowest year'],
            'year_max_revenue': ['ปีที่มีรายได้สูงสุด', 'ปีไหนรายได้มากสุด', 'highest year'],
            'max_value_work': ['งานที่มีมูลค่าสูงสุด', 'มูลค่าสูงสุด', 'งานมูลค่าสูง', 'highest value work'],
            'min_value_work': ['งานที่มีมูลค่าต่ำสุด', 'มูลค่าต่ำสุด', 'งานมูลค่าต่ำ', 'lowest value work'],
            'average_revenue_per_job': ['รายได้เฉลี่ยต่องาน', 'average per job', 'เฉลี่ยต่องาน'],
            'high_value_transactions': ['งานมูลค่าสูง', 'มากกว่า 1 ล้าน', 'high value'],
            'revenue_growth': ['การเติบโตรายได้', 'revenue growth', 'อัตราการเติบโต'],
            'revenue_proportion': ['สัดส่วนรายได้', 'proportion', 'แบ่งสัดส่วน'],
            'max_revenue_each_year': ['รายได้สูงสุดแต่ละปี', 'max revenue year', 'สูงสุดของแต่ละปี'],
            'total_inventory_value': ['มูลค่าสินค้าคงคลัง', 'inventory value', 'มูลค่ารวม'],
            'all_years_revenue_comparison': ['เปรียบเทียบรายได้ทุกปี', 'compare all years'],
            'average_work_value': ['ค่าเฉลี่ย', 'average value'],
            'sales_analysis': ['วิเคราะห์การขาย', 'sales analysis'],
            
            # === CUSTOMER EXAMPLES (หมวด 2: ข้อ 26-50) ===
            'count_total_customers': ['จำนวนลูกค้า', 'กี่ลูกค้า', 'มีลูกค้ากี่ราย', 'customer count'],
            'top_customers': ['top ลูกค้า', 'ลูกค้าสูงสุด', 'best customers'],
            'top_customers_no_filter': ['ลูกค้าที่ใช้บริการมาก', 'frequent customers'],
            'customer_specific_history': ['ประวัติลูกค้า', 'customer history', 'ประวัติการซื้อ'],
            'customer_history': ['ประวัติการใช้บริการ', 'service history'],
            'new_customers_year': ['ลูกค้าใหม่', 'new customers', 'ลูกค้าใหม่ปี'],
            'new_customers_in_year': ['ลูกค้าใหม่ในปี', 'new in year'],
            'frequent_customers': ['ลูกค้าที่ใช้บริการบ่อย', 'ลูกค้าประจำ', 'frequent'],
            'most_frequent_customers': ['ใช้บริการบ่อยที่สุด', 'most frequent'],
            'government_customers': ['ลูกค้าภาครัฐ', 'กระทรวง', 'government', 'หน่วยงานราชการ'],
            'hospital_customers': ['ลูกค้าโรงพยาบาล', 'hospital', 'รพ.', 'คลินิก'],
            'continuous_customers': ['ลูกค้าต่อเนื่อง', 'ใช้บริการต่อเนื่อง', 'loyal customers'],
            'customers_continuous_years': ['ลูกค้าต่อเนื่องทุกปี', 'every year customer'],
            'inactive_customers': ['ลูกค้าที่ไม่ได้ใช้บริการ', 'inactive', 'หายไป', 'ไม่กลับมา'],
            'high_value_customers': ['ลูกค้าที่จ่ายมาก', 'มูลค่าสูง', 'high value', 'จ่ายเยอะ'],
            'parts_only_customers': ['ลูกค้าที่ซื้อแต่อะไหล่', 'parts only', 'เฉพาะอะไหล่'],
            'chiller_customers': ['ลูกค้า chiller', 'งาน chiller', 'ชิลเลอร์'],
            'new_vs_returning_customers': ['เปรียบเทียบลูกค้าใหม่กับเก่า', 'new vs returning'],
            'customers_using_overhaul': ['ลูกค้าที่ใช้ overhaul', 'overhaul customers'],
            'top_service_customers': ['ลูกค้าที่ใช้ service มาก', 'service customers'],
            'customer_years_count': ['ซื้อขายมากี่ปี', 'กี่ปีแล้ว', 'how many years'],
            'top_parts_customers': ['ลูกค้าที่ซื้ออะไหล่มาก', 'top parts customers'],
            'service_vs_replacement': ['เปรียบเทียบ service กับ replacement'],
            'solution_customers': ['ลูกค้า solution', 'solution sales'],
            
            # === SPARE PARTS EXAMPLES (หมวด 3: ข้อ 51-70) ===
            'count_all_parts': ['จำนวนอะไหล่ทั้งหมด', 'กี่รายการ', 'total parts types'],
            'parts_in_stock': ['อะไหล่ที่มีสต็อก', 'in stock', 'มีในคลัง'],
            'parts_out_of_stock': ['อะไหล่หมดสต็อก', 'out of stock', 'หมด', 'ไม่มีของ'],
            'most_expensive_parts': ['อะไหล่ราคาแพง', 'expensive parts', 'ราคาสูง'],
            'low_stock_alert': ['อะไหล่ใกล้หมด', 'low stock', 'เหลือน้อย', 'ต้องสั่ง'],
            'warehouse_specific_parts': ['อะไหล่ในคลัง', 'คลัง a', 'warehouse a'],
            'spare_parts_price': ['ราคาอะไหล่', 'spare parts price'],
            'parts_search_multi': ['ค้นหาอะไหล่', 'search parts'],
            'average_part_price': ['ราคาเฉลี่ยอะไหล่', 'average price parts'],
            'compressor_parts': ['อะไหล่ compressor', 'คอมเพรสเซอร์', 'comp'],
            'filter_parts': ['อะไหล่ filter', 'กรอง', 'ฟิลเตอร์'],
            'warehouse_comparison': ['เปรียบเทียบคลัง', 'คลังไหนมีมาก', 'warehouse compare'],
            'inventory_check': ['ตรวจสอบสินค้าคงคลัง', 'inventory', 'stock check'],
            'highest_value_items': ['สินค้ามูลค่าสูง', 'highest value items'],
            'warehouse_summary': ['สรุปคลัง', 'warehouse summary'],
            'low_stock_items': ['ใกล้หมด', 'สต็อกน้อย', 'low stock'],
            'high_unit_price': ['ราคาต่อหน่วยสูง', 'ราคาแพง', 'expensive'],
            
            # === WORK FORCE EXAMPLES (หมวด 4: ข้อ 71-90) ===
            'count_all_works': ['จำนวนงานทั้งหมดในระบบ', 'work records', 'บันทึกงาน'],
            'count_works_by_year': ['จำนวนงานแต่ละปี', 'works by year'],
            'work_monthly': ['งานรายเดือน', 'งานเดือน', 'monthly work', 'แผนงานเดือน'],
            'work_summary_monthly': ['สรุปงานเดือน', 'monthly summary'],
            'work_specific_month': ['งานเดือนที่', 'งานเดือนนี้', 'work this month'],
            'work_plan_date': ['แผนงานวันที่', 'work plan date'],
            'all_pm_works': ['งาน pm', 'preventive', 'บำรุงรักษา'],
            'pm_work_summary': ['สรุปงาน pm', 'pm summary'],
            'work_overhaul': ['งาน overhaul', 'overhaul ที่ทำ', 'overhaul work'],
            'work_replacement': ['งาน replacement', 'งานเปลี่ยน', 'replacement work'],
            'replacement_monthly': ['งานเปลี่ยนรายเดือน', 'replacement monthly'],
            'successful_works': ['งานที่สำเร็จ', 'successful', 'งานเสร็จ'],
            'successful_work_monthly': ['งานสำเร็จรายเดือน', 'successful monthly'],
            'unsuccessful_works': ['งานที่ไม่สำเร็จ', 'failed', 'งานล้มเหลว'],
            'work_today': ['งานวันนี้', 'today work', 'วันนี้มีงานอะไร'],
            'work_this_week': ['งานสัปดาห์นี้', 'this week', 'อาทิตย์นี้'],
            'success_rate': ['อัตราความสำเร็จ', 'success rate', 'เปอร์เซ็นต์สำเร็จ'],
            'on_time_works': ['งานตรงเวลา', 'on time', 'ทันเวลา', 'ไม่ล่าช้า'],
            'kpi_reported_works': ['งานที่มี kpi', 'kpi report'],
            'overtime_works': ['งานเกินเวลา', 'overtime', 'ล่าช้า', 'delay'],
            'startup_works': ['งาน startup', 'start up', 'เริ่มระบบ'],
            'startup_works_all': ['งาน startup ทั้งหมด', 'all startup'],
            'support_works': ['งาน support', 'งานสนับสนุน', 'support all'],
            'cpa_works': ['งาน cpa', 'cpa'],
            'team_statistics': ['สถิติทีม', 'team stats', 'ผลงานทีม'],
            'team_specific_works': ['งานของทีม', 'team work'],
            'team_works': ['งานของทีม', 'team a', 'ทีม a'],
            'work_duration': ['ระยะเวลาทำงาน', 'duration', 'ใช้เวลา'],
            'min_duration_work': ['งานที่ใช้เวลาน้อยสุด', 'ใช้เวลาน้อยที่สุด', 'shortest duration'],
            'max_duration_work': ['งานที่ใช้เวลานานสุด', 'ใช้เวลานานที่สุด', 'longest duration'],
            'long_duration_works': ['ใช้เวลานาน', 'หลายวัน', 'งานนาน'],
            'latest_works': ['งานล่าสุด', 'latest', 'งานใหม่ล่าสุด'],
            'repair_history': ['ประวัติการซ่อม', 'repair history'],
            
            # === ANALYTICAL EXAMPLES (หมวด 5: ข้อ 91-100) ===
            'annual_performance_summary': ['สรุปผลประกอบการ', 'annual summary', 'สรุปรายปี'],
            'growth_trend': ['เทรนด์การเติบโต', 'growth trend', 'แนวโน้ม'],
            'popular_service_types': ['ประเภทงานที่นิยม', 'popular service', 'บริการยอดนิยม'],
            'high_potential_customers': ['ลูกค้าที่มีศักยภาพ', 'potential', 'ลูกค้าดี'],
            'revenue_distribution': ['การกระจายรายได้', 'distribution', 'กระจาย'],
            'team_performance': ['ประสิทธิภาพทีม', 'team performance', 'ผลงานทีม'],
            'monthly_sales_trend': ['แนวโน้มรายเดือน', 'monthly trend', 'ยอดขายรายเดือน'],
            'service_roi': ['roi', 'ผลตอบแทน', 'return on investment'],
            'revenue_forecast': ['คาดการณ์รายได้', 'forecast', 'พยากรณ์', 'ปีหน้า'],
            'business_overview': ['สถานะธุรกิจรวม', 'overview', 'ภาพรวม', 'สรุปทั้งหมด'],
            'quarterly_summary': ['ไตรมาส', 'quarterly', 'รายไตรมาส', 'quarter'],
            'sales_yoy_growth': ['เติบโต', 'อัตราการเติบโต', 'growth', 'yoy', 'year over year'],
        }
        
        # === PHASE 3: Smart Scoring with Required/Forbidden Words ===
        # === PHASE 3: Simple Scoring (Backward Compatible) ===
        best_matches = []
        
        for example_name, keywords in EXAMPLE_KEYWORDS.items():
            score = 0
            matched_keywords = []
            
            # Handle both list and dict format
            if isinstance(keywords, dict):
                # New format with config - extract just keywords
                keyword_list = keywords.get('keywords', [])
            else:
                # Old format - simple list
                keyword_list = keywords
            
            for keyword in keyword_list:
                keyword_lower = keyword.lower()
                
                # Exact phrase match
                if keyword_lower in question_lower:
                    score += 20
                    matched_keywords.append(keyword)
                # All words match (สำหรับ multi-word keywords)
                elif len(keyword_lower.split()) > 1:
                    if all(word in question_lower for word in keyword_lower.split() if len(word) > 2):
                        score += 12
                        matched_keywords.append(keyword)
                # Partial match
                elif any(word in question_lower for word in keyword_lower.split() if len(word) > 2):
                    score += 3
                    matched_keywords.append(keyword)
            
            # === PENALTIES for wrong context ===
            
            # Penalty 1: Work examples shouldn't match money queries
            if 'work' in example_name or 'pm' in example_name:
                if any(word in question_lower for word in ['มูลค่า', 'ราคา', 'value', 'revenue', 'ยอดขาย']):
                    score = score * 0.3
            
            # Penalty 2: Single type shouldn't match breakdown queries
            if any(word in question_lower for word in ['แยกตาม', 'แต่ละ', 'breakdown']):
                if example_name in ['overhaul_sales_all', 'parts_total', 'service_num']:
                    score = score * 0.5
            
            # Bonus for intent match
            if intent and intent.lower() in example_name.lower():
                score += 5
            
            # Bonus for entity match
            if entities.get('years') and ('year' in example_name or 'annual' in example_name):
                score += 3
            if entities.get('customers') and 'customer' in example_name:
                score += 5
            
            if score > 0:
                best_matches.append((score, example_name, matched_keywords))
        
        # Sort by score
        best_matches.sort(key=lambda x: x[0], reverse=True)
        
        # Log top matches
        if best_matches:
            logger.info(f"Top matches: 1. {best_matches[0][1]} (score: {best_matches[0][0]})")
            if len(best_matches) > 1:
                logger.debug(f"  2. {best_matches[1][1]} (score: {best_matches[1][0]})")
        
        # Return best match
        if best_matches and best_matches[0][0] > 5:
            selected = best_matches[0][1]
            if selected in self.SQL_EXAMPLES:
                logger.info(f"✓ Selected: {selected} (score: {best_matches[0][0]}, matched: {best_matches[0][2]})")
                return self.SQL_EXAMPLES[selected]
        
        # === PHASE 4: Intent-based fallback ===
        intent_map = {
            'customer_history': 'customer_history',
            'sales': 'sales_analysis',
            'sales_analysis': 'sales_analysis',
            'revenue': 'revenue_by_year',
            'work_plan': 'work_monthly',
            'work_force': 'work_monthly',
            'spare_parts': 'spare_parts_price',
            'parts_price': 'spare_parts_price',
            'top_customers': 'top_customers',
            'overhaul_report': 'overhaul_sales',
        }
        
        if intent in intent_map:
            example = intent_map[intent]
            if example in self.SQL_EXAMPLES:
                logger.info(f"Selected: {example} (intent fallback)")
                return self.SQL_EXAMPLES[example]
        
        # Final fallback
        logger.info("Selected: sales_analysis (final fallback)")
        return self.SQL_EXAMPLES.get('sales_analysis', '')


    def _detect_context(self, question: str) -> str:
        """Helper method to detect context from question"""
        q_lower = question.lower()
        
        # ตรวจสอบ context จาก keywords
        if any(word in q_lower for word in ['งาน', 'ทีม', 'ซ่อม', 'pm', 'ติดตั้ง']):
            return 'v_work'
        elif any(word in q_lower for word in ['อะไหล่', 'สต็อก', 'คลัง', 'spare']):
            return 'v_spare'
        else:
            return 'v_sales'
    
    def _build_sql_hints(self, entities: Dict, intent: str) -> str:
        """Build SQL hints with table specification and year filtering rules"""
        hints = []
        
        # Get target table
        target_table = self._get_target_table(intent)
        hints.append(f"USE TABLE: {target_table}")
        
        # Table-specific hints
        if target_table == 'v_work_force':
            hints.append("Format: WHERE date::date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'")
            hints.append("SELECT: date, customer, detail (do NOT use COUNT)")
        elif target_table == 'v_sales':
            # Check if years are specified
            if entities.get('years'):
                hints.append("Filter by year column using specified years only")
            else:
                hints.append("⚠️ NO YEAR FILTER - Query ALL years in database")
                hints.append("DO NOT add WHERE year clause unless years are explicitly mentioned")
        
        # Year hints - only if years are specified
        if entities.get('years'):
            years = entities['years']
            logger.debug(f"Building SQL hints for years: {years}")
            
            year_list = []
            for year in years:
                year_str = str(year)
                year_list.append(year_str)
                logger.debug(f"Using year: {year_str}")
            
            year_str = "', '".join(year_list)
            
            if target_table == 'v_sales':
                hint = f"WHERE year IN ('{year_str}')"
                hints.append(hint)
                logger.info(f"Generated year hint: {hint}")
        else:
            # No years specified - make it explicit
            if target_table == 'v_sales' and intent in ['sales', 'sales_analysis', 'revenue', 'top_customers']:
                hints.append("📌 NO YEAR SPECIFIED = Query ALL available years")
                hints.append("Example: SELECT SUM(total_revenue) FROM v_sales; -- No WHERE year")
        
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
            # Filter out Thai words that shouldn't be customer names
            excluded_words = [
                'ที่', 'ทั้งหมด', 'กี่ราย', 'ที่ใช้บริการ', 'ที่ใช้บริ', 
                'มากที่สุด', 'บ่อยที่สุด', 'น้อยที่สุด', 'สูงสุด', 'ต่ำสุด',
                'ทุกราย', 'แต่ละราย', 'หลายราย', 'บางราย'
            ]
            
            # Check if the customer string is actually a Thai phrase
            is_thai_phrase = any(word in customer for word in excluded_words)
            
            if not is_thai_phrase and len(customer) > 2:  # Real customer names are usually longer than 2 chars
                if target_table == 'v_sales':
                    hints.append(f"WHERE customer_name LIKE '%{customer}%'")
                else:
                    hints.append(f"WHERE customer LIKE '%{customer}%'")
            else:
                logger.warning(f"Ignoring invalid customer filter: '{customer}'")
                hints.append("# Note: Do not filter by customer name unless explicitly mentioned")
        
        # Product hints
        if entities.get('products'):
            product = entities['products'][0]
            hints.append(f"WHERE product_name LIKE '%{product}%' OR product_name LIKE '%{product}%'")
        
        return '\n'.join(hints) if hints else ""
    

    def build_response_prompt(self, question: str, results: List[Dict],
                            sql_query: str, locale: str = "th") -> str:
        """Build response generation prompt - แสดงข้อมูลครบ"""
        if not results:
            return f"ไม่พบข้อมูลสำหรับคำถาม: {question}"
        
        stats = self._analyze_results(results)
        
        # เอา limit ออก - แสดงทั้งหมด
        sample_json = json.dumps(results, ensure_ascii=False, default=str)
        
        prompt = dedent(f"""
        สรุปข้อมูลสำหรับคำถาม: {question}
        
        พบข้อมูลทั้งหมด: {len(results)} รายการ
        {stats}
        
        ข้อมูลทั้งหมด ({len(results)} รายการ):
        {sample_json}
        
        กรุณาสรุปข้อมูลให้ครบถ้วน:
        1. จำนวนรายการทั้งหมดที่พบ
        2. รายละเอียดของแต่ละรายการ (แสดงให้ครบทุกรายการ)
        3. ข้อสังเกตหรือการวิเคราะห์
        
        ข้อกำหนดสำคัญ:
        ⚠️ ห้ามแต่งหรือเพิ่มข้อมูลที่ไม่มีอยู่ในข้อมูลต้นฉบับ
        ✔ ใช้ข้อมูลจากที่ให้มาเท่านั้น
        ✔ หากข้อมูลมีมากให้จัดกลุ่มหรือสรุปแบบตาราง
        
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