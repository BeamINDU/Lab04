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
                'id', 'year','month', 'job_no', 'customer_name', 'description',
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
            'monthly_transaction_count': dedent("""
                SELECT 
                    COUNT(DISTINCT customer_name) AS unique_customers,
                    COUNT(*) AS total_transactions,
                    SUM(total_revenue) AS total_revenue
                FROM v_sales 
                WHERE year = '2025' AND month = '8';
            """).strip(),
            
            'customer_transaction_frequency': dedent("""
                SELECT 
                    customer_name,
                    COUNT(*) AS transaction_count,
                    SUM(total_revenue) AS total_revenue
                FROM v_sales 
                WHERE year = '2025' AND month = '8'
                GROUP BY customer_name
                ORDER BY transaction_count DESC;
            """).strip(),
            
            'yearly_transaction_summary': dedent("""
                SELECT 
                    year,
                    COUNT(DISTINCT customer_name) AS unique_customers,
                    COUNT(*) AS total_transactions
                FROM v_sales
                WHERE year = '2025'
                GROUP BY year;
            """).strip(),
            
            'total_transaction_count': dedent("""
                SELECT COUNT(*) AS total_transactions
                FROM v_sales;
            """).strip(),

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
                  AND year IN ('2022','2023','2024','2025')
                GROUP BY year, customer_name
                ORDER BY year, total_revenue DESC;
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
            'customer_repair_history': dedent("""
                SELECT 
                    customer,
                    date,
                    detail,
                    service_group,
                FROM v_work_force 
                WHERE customer LIKE '%{customer}%'
                ORDER BY date DESC;
            """).strip(),
            
            'repair_history': dedent("""
                SELECT 
                    customer,
                    date,
                    detail,
                    service_group,
                    CASE 
                        WHEN success IS NOT NULL THEN 'สำเร็จ'
                        WHEN unsuccessful IS NOT NULL THEN 'ไม่สำเร็จ'
                        ELSE 'ดำเนินการ'
                    END as status
                FROM v_work_force 
                WHERE detail LIKE '%ซ่อม%' OR detail LIKE '%repair%'
                ORDER BY date DESC;
            """).strip(),
            
            'service_history': dedent("""
                SELECT 
                    customer,
                    date,
                    detail,
                    service_group,
                    job_description_pm,
                    job_description_replacement,
                    job_description_overhaul
                FROM v_work_force 
                WHERE customer LIKE '%{customer}%'
                ORDER BY date DESC;
            """).strip(),
            
            'maintenance_history': dedent("""
                SELECT
                    customer,
                    date,
                    detail,
                    service_group
                FROM v_work_force
                WHERE job_description_pm = '1'
                OR job_description_pm = 'true'
                OR detail LIKE '%บำรุงรักษา%'
                OR detail LIKE '%maintenance%'
                ORDER BY date DESC;
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
                ORDER BY date;
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
                   OR product_name LIKE '%model%'
                   OR product_name LIKE '%EKAC460%'
                ORDER BY total_num DESC
                LIMIT 50;
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
                ORDER BY date DESC;
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
            ORDER BY service_count DESC, total_spent DESC;
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

        'employee_work_history': dedent("""
            select * from v_work_force  where service_group like '%อานนท์%' 
        """).strip(),

        # คำถามข้อ 73: งาน PM ทั้งหมด
        'all_pm_works': dedent("""
            SELECT date,customer ,project ,job_description_pm ,detail ,service_group  
            FROM public.v_work_force 
            WHERE job_description_pm IS NOT NULL;
        """).strip(),
        
        # คำถามข้อ 74: งาน Overhaul (work context)
        'work_overhaul': dedent("""
            select * from public.v_work_force  
            where job_description_overhaul is not null 
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
            SELECT date, customer, project, detail, job_description_start_up 
            FROM v_work_force 
            WHERE job_description_start_up IS NOT NULL 
            AND job_description_start_up != ''
            {year_condition}
            ORDER BY date DESC;
        """).strip(),
        
        # คำถามข้อ 86: งาน Support
        'support_works': dedent("""
            SELECT date, customer, project, detail, job_description_support_all 
            FROM v_work_force 
            WHERE job_description_support_all IS NOT NULL 
            AND job_description_support_all != ''
            {year_condition}
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
        WHERE year IN ('2022','2023','2024','2025')
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
        ORDER BY total_revenue;
    """).strip(),

    # ข้อ 35: Private sector customers
    'private_customers': dedent("""
        SELECT customer_name,
            COUNT(*) as transaction_count,
            SUM(total_revenue) as total_revenue
        FROM v_sales
        WHERE customer_name ILIKE ANY(ARRAY['%จำกัด%', '%บริษัท%', '%co%ltd%'])
        GROUP BY customer_name
        ORDER BY total_revenue DESC;
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
    
    def _extract_employees(self, text: str) -> List[str]:
        """
        Extract employee/staff names from text
        Enhanced version ที่จัดการชื่อไทยได้ดีขึ้น
        """
        employees = []
        text_lower = text.lower()
        
        # Keywords indicating employee search
        employee_keywords = [
            'พนักงาน', 'ช่าง', 'ทีม', 'คนทำงาน', 
            'ผู้รับผิดชอบ', 'staff', 'employee', 'technician'
        ]
        
        # Check if searching for employee
        is_employee_search = any(kw in text_lower for kw in employee_keywords)
        
        if is_employee_search:
            # Pattern 1: พนักงานชื่อ + ชื่อไทย
            pattern1 = r'(?:พนักงาน|ช่าง|ทีม)(?:ชื่อ|ชื่อว่า|คือ)?\s*([ก-๙]+(?:\s+[ก-๙]+)?)'
            matches1 = re.findall(pattern1, text)
            for match in matches1:
                clean_name = match.strip()
                if len(clean_name) >= 2:  # อย่างน้อย 2 ตัวอักษร
                    employees.append(clean_name)
                    logger.info(f"✅ Found employee (pattern 1): {clean_name}")
            
            # Pattern 2: ชื่อ + ชื่อไทย (generic)
            if not employees:
                pattern2 = r'ชื่อ\s+([ก-๙]+(?:\s+[ก-๙]+)?)'
                matches2 = re.findall(pattern2, text)
                for match in matches2:
                    clean_name = match.strip()
                    # ตรวจสอบว่าไม่ใช่คำทั่วไป
                    exclude_words = ['เดือน', 'ปี', 'วัน', 'ครั้ง', 'บริษัท']
                    if len(clean_name) >= 2 and clean_name not in exclude_words:
                        employees.append(clean_name)
                        logger.info(f"✅ Found employee (pattern 2): {clean_name}")
            
            # Pattern 3: ทีม + ชื่อ
            pattern3 = r'ทีม\s*([ก-๙]+(?:\s+[ก-๙]+)?)'
            matches3 = re.findall(pattern3, text)
            for match in matches3:
                clean_name = match.strip()
                if len(clean_name) >= 2:
                    employees.append(clean_name)
                    logger.info(f"✅ Found employee (pattern 3): {clean_name}")
        
        # Remove duplicates
        employees = list(dict.fromkeys(employees))
        
        if employees:
            logger.info(f"✅ Final extracted employees: {employees}")
        else:
            logger.warning(f"⚠️ No employees extracted from: '{text}'")
        
        return employees

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
            
            # ============================================
            # 🔧 FIX 1: CUSTOMER NAME OPTIMIZATION
            # ============================================
            original_question = question  # Keep original for logging
            
            # If we have customer entities, optimize the question
            if entities.get('customers'):
                question = self._optimize_customer_in_question(question, entities)
                logger.info(f"🔄 Question optimized: '{original_question}' -> '{question}'")
            
            # ============================================
            # EMPLOYEE EXTRACTION
            # ============================================
            employees = self._extract_employees(question)
            if employees:
                # Clean up employee names
                cleaned_employees = []
                for emp in employees:
                    # Remove common prefixes
                    emp_clean = emp
                    for prefix in ['ขอข้อมูลการทำงานพนักงานชื่อ', 'พนักงานชื่อ', 'ช่างชื่อ', 'ชื่อ']:
                        emp_clean = emp_clean.replace(prefix, '').strip()
                    
                    if emp_clean and len(emp_clean) > 1:
                        cleaned_employees.append(emp_clean)
                
                if cleaned_employees:
                    entities['employees'] = cleaned_employees
                    logger.info(f"Detected employees: {cleaned_employees}")

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
            
            # เพิ่มหลังบรรทัด 103 ใน build_sql_prompt()

            TEMPLATE_TABLE_OVERRIDE = {
                # ========================================
                # V_SALES TEMPLATES (ยอดขาย/รายได้)
                # ========================================
                'total_revenue_all': 'v_sales',
                'total_revenue_year': 'v_sales',
                'revenue_by_year': 'v_sales',
                'revenue_by_service_type': 'v_sales',
                'compare_revenue_years': 'v_sales',
                'year_comparison': 'v_sales',
                'average_annual_revenue': 'v_sales',
                'year_max_revenue': 'v_sales',
                'year_min_revenue': 'v_sales',
                
                # Sales Analysis
                'sales_analysis': 'v_sales',
                'sales_summary': 'v_sales',
                'sales_monthly': 'v_sales',
                'sales_by_month': 'v_sales',
                
                # Overhaul Sales
                'overhaul_sales_specific': 'v_sales',
                'overhaul_sales': 'v_sales',
                'overhaul_total': 'v_sales',
                'overhaul_sales_all': 'v_sales',
                'overhaul_report': 'v_sales',
                
                # Service/Parts Sales
                'service_revenue': 'v_sales',
                'service_num': 'v_sales',
                'parts_total': 'v_sales',
                'replacement_total': 'v_sales',
                'product_sales': 'v_sales',
                'solution_sales': 'v_sales',
                
                # Work Value/Amount
                'max_value_work': 'v_sales',
                'min_value_work': 'v_sales',
                'average_revenue_per_job': 'v_sales',
                'high_value_transactions': 'v_sales',
                'low_value_transactions': 'v_sales',
                
                # Customer Sales
                'customer_history': 'v_sales',  # Purchase history
                'customer_sales': 'v_sales',
                'customer_revenue': 'v_sales',
                'customer_specific_history': 'v_sales',
                'top_customers': 'v_sales',
                'frequent_customers': 'v_sales',
                'new_customers_year': 'v_sales',
                'new_customers_in_year': 'v_sales',
                'inactive_customers': 'v_sales',
                'customers_using_overhaul': 'v_sales',
                'continuous_customers': 'v_sales',
                'customers_continuous_years': 'v_sales',
                'customer_years_count': 'v_sales',
                
                # Customer Counts
                'count_total_customers': 'v_sales',
                'count_all_jobs': 'v_sales',
                'count_jobs_year': 'v_sales',
                
                # Government/Private
                'government_customers': 'v_sales',
                'private_customers': 'v_sales',
                
                # ========================================
                # V_WORK_FORCE TEMPLATES (งาน/การทำงาน)
                # ========================================
                'work_monthly': 'v_work_force',
                'work_daily': 'v_work_force',
                'work_summary': 'v_work_force',
                'work_summary_monthly': 'v_work_force',
                'work_force_base': 'v_work_force',
                'work_specific_month': 'v_work_force',
                'latest_works': 'v_work_force',
                
                # Work Types
                'work_overhaul': 'v_work_force',
                'work_replacement': 'v_work_force',
                'work_service': 'v_work_force',
                'all_pm_works': 'v_work_force',
                'pm_work': 'v_work_force',
                
                # Work Status
                'successful_works': 'v_work_force',
                'unsuccessful_works': 'v_work_force',
                'completed_work': 'v_work_force',
                'pending_work': 'v_work_force',
                
                # Team/Employee
                'employee_work_history': 'v_work_force',
                'employee_monthly': 'v_work_force',
                'team_works': 'v_work_force',
                'work_team_specific': 'v_work_force',
                'team_a_works': 'v_work_force',
                'service_group_search': 'v_work_force',
                
                # Repair/Service History
                'repair_history': 'v_work_force',
                'customer_repair_history': 'v_work_force',
                'service_history': 'v_work_force',
                'maintenance_history': 'v_work_force',
                
                # Special Work
                'cpa_works': 'v_work_force',
                'government_work': 'v_work_force',
                'project_work': 'v_work_force',
                'work_duration': 'v_work_force',
                
                # Work Counts
                'count_all_works': 'v_work_force',
                
                # ========================================
                # V_SPARE_PART TEMPLATES (อะไหล่/สต๊อก)
                # ========================================
                'spare_parts_stock': 'v_spare_part',
                'spare_parts_price': 'v_spare_part',
                'spare_parts_all': 'v_spare_part',
                'spare_parts_search': 'v_spare_part',
                'inventory_check': 'v_spare_part',
                'inventory_value': 'v_spare_part',
                'warehouse_summary': 'v_spare_part',
                'stock_balance': 'v_spare_part',
                'low_stock_items': 'v_spare_part',
                'parts_by_warehouse': 'v_spare_part',
                'parts_total_value': 'v_spare_part',
                
                # ========================================
                # BASIC/FALLBACK TEMPLATES
                # ========================================
                'basic_query': None,  # Depends on context
                'simple_select': None,  # Depends on context
                'custom': None,  # Custom query
            }

            # Check if template requires specific table
            if example_name in TEMPLATE_TABLE_OVERRIDE:
                required_table = TEMPLATE_TABLE_OVERRIDE[example_name]
                
                # Only override if we have a specific requirement
                if required_table and target_table != required_table:
                    logger.warning(f"🔧 OVERRIDE: Template '{example_name}' requires table '{required_table}' (was: {target_table})")
                    target_table = required_table
                    
                    # Update intent for consistency
                    if required_table == 'v_sales':
                        if 'overhaul' in example_name:
                            intent = 'overhaul_sales'
                        elif 'customer' in example_name:
                            intent = 'customer_analysis'
                        else:
                            intent = 'sales'
                            
                    elif required_table == 'v_work_force':
                        if 'overhaul' in example_name:
                            intent = 'work_overhaul'
                        elif 'employee' in example_name:
                            intent = 'employee_work'
                        else:
                            intent = 'work_force'
                            
                    elif required_table == 'v_spare_part':
                        intent = 'spare_parts'
                    
                    logger.info(f"✅ Final configuration: table={target_table}, intent={intent}")
        
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
            # 🔧 FIX 2: CUSTOMER KEYWORD INJECTION
            # ============================================
            
            # Modify example to use customer keyword if applicable
            if entities.get('customers') and 'customer' in example_name.lower():
                example = self._inject_customer_keyword(example, entities)
            
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
                
                return self._build_complex_prompt_with_customer_hint(
                    modified_example, question, intent, template_config, entities
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
                
                return self._build_normal_prompt_with_customer_hint(
                    modified_example, question, intent, entities, target_table
                )
                
        except Exception as e:
            logger.error(f"Failed to build SQL prompt: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return self._get_fallback_prompt(question)

    # ============================================
    # 🆕 HELPER METHODS FOR CUSTOMER OPTIMIZATION
    # ============================================

    def _optimize_customer_in_question(self, question: str, entities: Dict) -> str:
        """
        Replace long company names with keywords in the question
        This is the ROOT CAUSE FIX!
        """
        if not entities.get('customers'):
            return question
        
        modified = question
        
        # Get the best keyword
        keyword = self._get_best_customer_keyword(entities['customers'])
        
        # Known long names to replace
        replacements = {
            'บริษัทบิโก้ ปราจีนบุรี (ไทยแลนด์) จำกัด': 'บิโก้',
            'บริษัท ซีพี ออลล์ จำกัด (มหาชน)': 'ซีพี',
            'บริษัท สยามแม็คโคร จำกัด (มหาชน)': 'แม็คโคร',
            'บริษัท เซ็นทรัล รีเทล คอร์ปอเรชั่น จำกัด (มหาชน)': 'เซ็นทรัล',
            'บริษัท โฮม โปรดักส์ เซ็นเตอร์ จำกัด (มหาชน)': 'โฮมโปร',
        }
        
        # Replace known long names
        for long_name, short_name in replacements.items():
            if long_name in modified:
                modified = modified.replace(long_name, short_name)
                logger.info(f"🔄 Replaced '{long_name}' with '{short_name}'")
        
        # Generic pattern replacement
        patterns = [
            r'บริษัท[^จ]{10,}(?:จำกัด|จก\.)',
            r'บ\.[^จ]{10,}(?:จำกัด|จก\.)',
            r'หจก\.[^ม]{10,}',
            r'บมจ\.[^ม]{10,}',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, modified)
            for match in matches:
                if len(match) > 20:  # Only replace long names
                    modified = modified.replace(match, keyword)
                    logger.info(f"🔄 Replaced long name with '{keyword}'")
        
        return modified

    def _get_best_customer_keyword(self, customers: List[str]) -> str:
        """
        Select the best keyword from customer list
        """
        if not customers:
            return ""
        
        # Priority 1: Known keywords
        known_keywords = ['บิโก้', 'โลตัส', 'แม็คโคร', 'ซีพี', 'เซ็นทรัล', 'โฮมโปร']
        for customer in customers:
            for keyword in known_keywords:
                if keyword in customer:
                    return keyword
        
        # Priority 2: Shortest clean name
        cleaned = []
        for customer in customers:
            # Remove company markers
            clean = customer
            for marker in ['บริษัท', 'บ.', 'จำกัด', 'จก.', 'มหาชน', '(ไทยแลนด์)', '(ประเทศไทย)']:
                clean = clean.replace(marker, '').strip()
            
            if clean and len(clean) > 2:
                cleaned.append(clean)
        
        if cleaned:
            return min(cleaned, key=len)
        
        # Priority 3: Shortest original
        return min(customers, key=len)

    def _inject_customer_keyword(self, example: str, entities: Dict) -> str:
        """
        Inject customer keyword into SQL template
        """
        if not entities.get('customers'):
            return example
        
        keyword = self._get_best_customer_keyword(entities['customers'])
        
        # Replace placeholder patterns
        patterns = [
            (r"customer_name LIKE '%[^'%]{15,}%'", f"customer_name LIKE '%{keyword}%'"),
            (r"customer_name = '[^']{15,}'", f"customer_name LIKE '%{keyword}%'"),
            (r"WHERE customer_name LIKE '%X%'", f"WHERE customer_name LIKE '%{keyword}%'"),
        ]
        
        modified = example
        for pattern, replacement in patterns:
            modified = re.sub(pattern, replacement, modified)
        
        return modified

    def _build_complex_prompt_with_customer_hint(self, template: str, question: str, 
                                                intent: str, config: Dict, entities: Dict) -> str:
        """
        Build complex prompt with customer keyword hint
        """
        # Get base prompt
        base_prompt = self._build_complex_prompt(template, question, intent, config)
        
        # Add customer hint if applicable
        if entities.get('customers'):
            keyword = self._get_best_customer_keyword(entities['customers'])
            customer_hint = f"""
            
            🔍 CUSTOMER SEARCH INSTRUCTION:
            ================================
            The customer mentioned is: {entities['customers'][0]}
            USE THIS for search: WHERE customer_name LIKE '%{keyword}%'
            DO NOT use the full company name with suffixes like "จำกัด" or "(ไทยแลนด์)"
            """
            base_prompt += customer_hint
        
        return base_prompt

    def _build_normal_prompt_with_customer_hint(self, template: str, question: str,
                                            intent: str, entities: Dict, target_table: str) -> str:
        """
        Build normal prompt with customer keyword hint
        """
        # Get base prompt
        base_prompt = self._build_normal_prompt(template, question, intent, entities, target_table)
        
        # Add customer hint if applicable
        if entities.get('customers'):
            keyword = self._get_best_customer_keyword(entities['customers'])
            customer_hint = f"""
            
            🔍 CUSTOMER SEARCH RULE:
            ========================
            Extracted customer: {entities['customers']}
            ✅ CORRECT: WHERE customer_name LIKE '%{keyword}%'
            ❌ WRONG: WHERE customer_name LIKE '%บริษัท...จำกัด%'
            
            Always use the shortest keyword for customer search.
            """
            
            # Insert hint before the final SQL instruction
            base_prompt = base_prompt.replace("SQL:", customer_hint + "\n\nSQL:")
        
        return base_prompt

    def _apply_smart_year_adjustment(self, template: str, entities: Dict, question_lower: str) -> str:
        # ตรวจสอบก่อนใช้
        if not entities or 'years' not in entities or not entities['years']:
            logger.warning("No year entities for adjustment")
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
        """Build prompt for normal templates with date handling"""
        
        # Get schema and hints
        schema_prompt = self._get_dynamic_schema_prompt(target_table)
        hints = self._build_sql_hints(entities, intent)
        
        # =================================================================
        # 🔥 NEW: PARTS_PRICE SPECIAL HANDLING (แก้ปัญหาหลัก)
        # =================================================================
        
        if intent == 'parts_price' and entities.get('products'):
            products = entities['products']
            logger.info(f"🎯 Parts price query with products: {products}")
            
            # สร้าง WHERE clause ที่ชัดเจนจาก products ที่ extract มา
            where_conditions = []
            for product in products:
                where_conditions.append(f"product_name LIKE '%{product}%'")
            
            where_clause = " OR ".join(where_conditions)
            
            # สร้าง explicit SQL โดยไม่พึ่ง template ที่อาจมีปัญหา
            explicit_sql = f"""
    SELECT 
        product_code,
        product_name, 
        balance_num,
        unit_price_num,
        total_num,
        wh
    FROM v_spare_part 
    WHERE {where_clause}
    ORDER BY total_num DESC;
            """.strip()
            
            prompt = dedent(f"""
            You are a SQL query generator. Output ONLY the SQL query with no explanation.
            
            🎯 CRITICAL INSTRUCTION FOR PARTS PRICE QUERY:
            
            The user is asking for price of these specific products: {products}
            
            Use this EXACT SQL (already customized for the products):
            ----------------------------------------
            {explicit_sql}
            ----------------------------------------
            
            DO NOT modify the WHERE clause!
            DO NOT use generic '%model%' or placeholder values!
            DO NOT use different product codes like EKAC460!
            
            Question: {question}
            Detected products: {products}
            
            Output the SQL above EXACTLY as shown:
            """).strip()
            
            return prompt
        
        # =================================================================
        # EXISTING LOGIC (เดิม)
        # =================================================================
        
        # ตรวจสอบว่า template มี date conditions อยู่แล้วหรือไม่
        has_date_conditions = any(keyword in template.lower() for keyword in [
            'date', 'year', 'month', 'between', 'extract(',
            '2024', '2025', 'where date', 'and date', 'year in'
        ])
        
        # === CUSTOMER HISTORY SPECIAL HANDLING ===
        if intent == 'customer_history' and entities.get('customers') and entities.get('years'):
            customer = entities['customers'][0]
            years_list = "', '".join(map(str, entities['years']))
            
            # Replace customer name and years in template
            template = re.sub(r'%\w+%', f'%{customer}%', template)
            template = re.sub(r"year\s*IN\s*\([^)]+\)", f"year IN ('{years_list}')", template)
            
            logger.info(f"🔄 Customer template updated: {customer}, years: {years_list}")
            
            prompt = dedent(f"""
            You are a SQL query generator. Output ONLY the SQL query with no explanation.
            
            Use this template exactly (customer and years already updated):
            ----------------------------------------
            {template}
            ----------------------------------------
            
            Question: {question}
            
            SQL:
            """).strip()
            
            return prompt
         

        if intent == 'work_force' or intent == 'employee_work':
            # Extract employee names
            employees = self._extract_employees(question)
            
            if employees:
                logger.info(f"🎯 Employee query for: {employees}")
                
                # สร้าง WHERE clause สำหรับ employee
                employee_conditions = []
                for emp in employees:
                    employee_conditions.append(f"service_group LIKE '%{emp}%'")
                
                where_clause = " OR ".join(employee_conditions)
                
                # สร้าง explicit SQL
                explicit_sql = f"""
                    SELECT 
                        date,
                        customer,
                        project,
                        detail,
                        service_group,
                        success,
                        unsuccessful
                    FROM v_work_force 
                    WHERE {where_clause}
                    ORDER BY date DESC;
                """.strip()
                
                prompt = dedent(f"""
                You are a SQL query generator. Output ONLY the SQL query with no explanation.
                
                🎯 EMPLOYEE WORK HISTORY QUERY:
                
                The user wants work history for employee(s): {employees}
                
                Use this EXACT SQL (already customized):
                ----------------------------------------
                {explicit_sql}
                ----------------------------------------
                
                CRITICAL INSTRUCTIONS:
                1. Employee names are in 'service_group' column, NOT 'customer'
                2. Use LIKE '%name%' for partial matching
                3. DO NOT change the WHERE clause or column names
                4. DO NOT use 'customer' column for employee search
                
                Question: {question}
                
                Output the SQL above EXACTLY as shown:
                """).strip()
                
                return prompt

        # === FIX 1: Handle multiple years replacement ===
        if entities.get('years') and len(entities['years']) > 1:
            years_list = "', '".join(map(str, entities['years']))
            years_clause = f"year IN ('{years_list}')"
            
            # Replace year patterns in template
            template = re.sub(
                r"year\s*=\s*'[^']*'", 
                years_clause, 
                template
            )
            template = re.sub(
                r"year\s*IN\s*\([^)]+\)", 
                years_clause, 
                template
            )
            
            logger.info(f"🔄 Replaced years clause: {years_clause}")
        
        # === FIX 2: Handle date replacement for monthly queries ===
        if intent in ['work_summary', 'work_plan'] and entities.get('months'):
            # Get month and year variables
            month = entities['months'][0]
            year = entities.get('years', ['2025'])[0]
            
            # Calculate correct date range
            if isinstance(month, int):
                first_day = f"{year}-{month:02d}-01"
                last_day_num = self._get_last_day_of_month(int(year), month)
                last_day = f"{year}-{month:02d}-{last_day_num:02d}"
            else:
                month_int = int(month)
                first_day = f"{year}-{month_int:02d}-01"
                last_day_num = self._get_last_day_of_month(int(year), month_int)
                last_day = f"{year}-{month_int:02d}-{last_day_num:02d}"
            
            # Replace any date range in template
            template = re.sub(
                r'\d{4}-\d{2}-\d{2}\'?\s+AND\s+\'?\d{4}-\d{2}-\d{2}',
                f"{first_day}' AND '{last_day}",
                template
            )
            
            logger.info(f"🔄 Replaced date range: {first_day} to {last_day}")
        
        # === NEW: DYNAMIC DATE CONDITIONS FOR TEMPLATES WITHOUT DATE ===
        if not has_date_conditions:
            # เพิ่ม date conditions ถ้าคำถามมีเวลาระบุ แต่ template ไม่มี
            additional_conditions = []
            
            if entities.get('months'):
                month = entities['months'][0]
                additional_conditions.append(f'AND EXTRACT(MONTH FROM "date") = {month}')
            
            if entities.get('years'):
                year = entities['years'][0]
                additional_conditions.append(f'AND EXTRACT(YEAR FROM "date") = {year}')
            
            if additional_conditions:
                conditions_text = ' '.join(additional_conditions)
                
                prompt = dedent(f"""
                You are a SQL query generator. Output ONLY the SQL query with no explanation.
                
                BASE TEMPLATE:
                {template}
                
                Add these date conditions to the WHERE clause:
                {conditions_text}
                
                INSTRUCTIONS:
                1. Take the base template above
                2. Add the date conditions to the existing WHERE clause
                3. If template has no WHERE clause, add WHERE with the date conditions
                4. Output the complete modified SQL
                
                Question: {question}
                
                SQL:
                """).strip()
                
                return prompt
        
        # Check if this is planned work
        is_planned_work = ('วางแผน' in question.lower() or 
                        'planned' in question.lower())
        
        prompt = dedent(f"""
        You are a SQL query generator. Output ONLY the SQL query with no explanation.
        
        ⚠️ CRITICAL INSTRUCTIONS:
        1. Use the EXACT SQL template below - it already has the correct dates/years
        2. DO NOT change any dates or year conditions - they are already set correctly
        3. DO NOT add any WHERE conditions not in the template
        4. DO NOT add job_description filters unless in the template
        {"5. This asks for ALL work - DO NOT filter by job type" if is_planned_work else ""}
        
        {schema_prompt}
        
        SQL Template (USE EXACTLY):
        ----------------------------------------
        {template}
        ----------------------------------------
        
        Question: {question}
        
        ⚠️ OUTPUT THE EXACT SQL ABOVE - NO CHANGES!
        
        SQL:
        """).strip()
        
        return prompt


    # =================================================================
    # เพิ่ม method สำหรับ debug parts_price
    # =================================================================

    def debug_parts_price_prompt(self, question: str, entities: Dict) -> Dict:
        """
        🔧 DEBUG: ช่วยดู parts_price prompt generation
        """
        
        if not entities.get('products'):
            return {
                'error': 'No products in entities',
                'entities': entities
            }
        
        products = entities['products']
        
        # สร้าง WHERE clause
        where_conditions = []
        for product in products:
            where_conditions.append(f"product_name LIKE '%{product}%'")
        
        where_clause = " OR ".join(where_conditions)
        
        # สร้าง SQL
        explicit_sql = f"""
    SELECT 
        product_code,
        product_name, 
        balance_num,
        unit_price_num,
        total_num,
        wh
    FROM v_spare_part 
    WHERE {where_clause}
    ORDER BY total_num DESC;
        """.strip()
        
        return {
            'question': question,
            'detected_products': products,
            'where_clause': where_clause,
            'generated_sql': explicit_sql,
            'should_find_products': products
        }


    # =================================================================
    # เพิ่ม validation หลัง SQL generation
    # =================================================================

    def validate_parts_price_result(self, sql: str, original_entities: Dict) -> Dict:
        """
        🔧 VALIDATION: ตรวจสอบ SQL ที่ generate มาว่าใช้ products ถูกต้องหรือไม่
        """
        
        validation_result = {
            'sql': sql,
            'original_products': original_entities.get('products', []),
            'found_in_sql': [],
            'missing_from_sql': [],
            'false_positives': [],
            'is_valid': False,
            'issues': []
        }
        
        original_products = original_entities.get('products', [])
        if not original_products:
            validation_result['is_valid'] = True
            validation_result['issues'].append('No products to validate')
            return validation_result
        
        sql_upper = sql.upper()
        
        # ตรวจสอบ products ที่ควรมี
        for product in original_products:
            if product.upper() in sql_upper:
                validation_result['found_in_sql'].append(product)
            else:
                validation_result['missing_from_sql'].append(product)
                validation_result['issues'].append(f"Missing product: {product}")
        
        # ตรวจสอบ false positives (product codes ที่ไม่ควรมี)
        known_false_positives = ['EKAC460', 'MODEL', '%MODEL%']
        
        for fp in known_false_positives:
            if fp in sql_upper:
                # ตรวจสอบว่าเป็น false positive จริงหรือไม่
                if fp.replace('%', '') not in [p.upper() for p in original_products]:
                    validation_result['false_positives'].append(fp)
                    validation_result['issues'].append(f"False positive: {fp}")
        
        # ประเมินผลรวม
        has_all_products = len(validation_result['missing_from_sql']) == 0
        has_no_false_positives = len(validation_result['false_positives']) == 0
        
        validation_result['is_valid'] = has_all_products and has_no_false_positives
        
        if validation_result['is_valid']:
            validation_result['issues'].append('✅ SQL validation passed')
        
        return validation_result

    def _get_example_name(self, example: str) -> str:
        """Get example name for logging and exact match checking"""
        for name, sql in self.SQL_EXAMPLES.items():
            # Strip and normalize for comparison
            if sql.strip() == example.strip():
                return name
        return 'custom'


    def _should_use_exact_template(self, template_name: str, question: str = None) -> bool:
        """
        Determine if template should be used exactly without modification
        Combines both centralized config and pattern matching
        """
        
        # 1. Check centralized config first (จาก TemplateConfig)
        if TemplateConfig.is_exact_template(template_name):
            logger.debug(f"Template {template_name} marked as EXACT in config")
            return True
        
        # 2. Check pattern-based rules (Business Logic สำคัญ!)
        if question:
            question_lower = question.lower()
            
            # These patterns require exact template usage
            exact_use_patterns = {
                'max_value_work': ['งานที่มีมูลค่าสูงสุด', 'มูลค่าสูงสุด'],
                'min_value_work': ['งานที่มีมูลค่าต่ำสุด', 'มูลค่าต่ำสุด'],
                'year_max_revenue': ['ปีที่มีรายได้สูงสุด', 'ปีไหนรายได้สูงสุด'],
                'year_min_revenue': ['ปีที่มีรายได้ต่ำสุด', 'ปีไหนรายได้ต่ำสุด'],
                'total_revenue_all': ['รายได้รวมทั้งหมด', 'รายได้ทั้งหมด'],
                'count_total_customers': ['จำนวนลูกค้า', 'มีลูกค้ากี่ราย'],
                'count_all_jobs': ['จำนวนงาน', 'มีงานกี่งาน'],
            }
            
            if template_name in exact_use_patterns:
                patterns = exact_use_patterns[template_name]
                for pattern in patterns:
                    if pattern in question_lower:
                        logger.info(f"Exact template triggered for {template_name} by pattern '{pattern}'")
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
        """
        Detect table from keywords when intent mapping fails
        Enhanced version with better context detection
        """
        question_lower = question.lower()
        
        # ========================================
        # PRIORITY 1: Money/Sales indicators
        # ========================================
        sales_keywords = [
            'ยอดขาย', 'รายได้', 'มูลค่า', 'ราคา', 'revenue', 
            'sales', 'income', 'price', 'cost', 'บาท', 'เงิน',
            'ค่า', 'จ่าย', 'รับ', 'กำไร', 'ขาดทุน'
        ]
        if any(word in question_lower for word in sales_keywords):
            logger.info(f"Detected sales/money keywords → v_sales")
            return 'v_sales'
        
        # ========================================
        # PRIORITY 2: Work/Operations indicators
        # ========================================
        work_keywords = [
            'งาน', 'ทำงาน', 'การทำงาน', 'work', 'job', 'task',
            'ซ่อม', 'repair', 'บำรุง', 'maintenance', 'pm',
            'สำเร็จ', 'successful', 'ไม่สำเร็จ', 'unsuccessful',
            'ทีม', 'team', 'พนักงาน', 'employee', 'ช่าง', 'technician',
            'วันที่', 'date', 'เวลา', 'time', 'แผน', 'plan'
        ]
        
        # Special check for overhaul context
        if 'overhaul' in question_lower:
            # If has sales context → v_sales
            if any(word in question_lower for word in sales_keywords):
                logger.info("Overhaul with sales context → v_sales")
                return 'v_sales'
            # Otherwise → v_work_force
            else:
                logger.info("Overhaul with work context → v_work_force")
                return 'v_work_force'
        
        # General work check
        if any(word in question_lower for word in work_keywords):
            logger.info(f"Detected work/operations keywords → v_work_force")
            return 'v_work_force'
        
        # ========================================
        # PRIORITY 3: Parts/Inventory indicators
        # ========================================
        parts_keywords = [
            'อะไหล่', 'spare', 'part', 'ชิ้นส่วน', 'stock',
            'คลัง', 'warehouse', 'inventory', 'จำนวน', 'balance'
        ]
        if any(word in question_lower for word in parts_keywords):
            logger.info(f"Detected parts/inventory keywords → v_spare_part")
            return 'v_spare_part'
        
        # ========================================
        # DEFAULT: Most common use case
        # ========================================
        logger.warning(f"No specific keywords detected → v_sales (default)")
        return 'v_sales'

    # 2. แก้ไข _get_target_table ให้ใช้ keyword detection เมื่อไม่รู้จัก intent
    def _get_target_table(self, intent: str) -> str:
        """
        Map intent to database table - COMPLETE MAPPING
        Covers all possible intents in the system
        """
        intent_table_map = {
            # ========================================
            # SALES & REVENUE (v_sales)
            # ========================================
            'sales': 'v_sales',
            'sales_analysis': 'v_sales',
            'revenue': 'v_sales',
            'revenue_analysis': 'v_sales',
            'pricing': 'v_sales',
            'quotation': 'v_sales',
            
            # Overhaul related sales
            'overhaul_sales': 'v_sales',
            'overhaul_report': 'v_sales',
            'overhaul_revenue': 'v_sales',
            
            # Service/Parts sales
            'service_sales': 'v_sales',
            'parts_sales': 'v_sales',
            'replacement_sales': 'v_sales',
            
            # Customer sales analysis
            'customer_revenue': 'v_sales',
            'customer_analysis': 'v_sales',
            'customer_history': 'v_sales',
            'top_customers': 'v_sales',
            'new_customers': 'v_sales',
            'inactive_customers': 'v_sales',
            
            # Value/Amount queries
            'max_value': 'v_sales',
            'min_value': 'v_sales',
            'total_value': 'v_sales',
            'average_value': 'v_sales',
            
            # ========================================
            # WORK & OPERATIONS (v_work_force)
            # ========================================
            'work_force': 'v_work_force',
            'work_plan': 'v_work_force',
            'work_summary': 'v_work_force',
            'work_analysis': 'v_work_force',
            'work_schedule': 'v_work_force',
            
            # Specific work types
            'work_overhaul': 'v_work_force',
            'work_replacement': 'v_work_force',
            'work_service': 'v_work_force',
            'pm_work': 'v_work_force',
            'preventive_maintenance': 'v_work_force',
            
            # Work status
            'successful_work': 'v_work_force',
            'unsuccessful_work': 'v_work_force',
            'completed_work': 'v_work_force',
            'pending_work': 'v_work_force',
            'customer_repair_history': 'v_work_force',
            # Team/Employee related
            'team_work': 'v_work_force',
            'employee_work': 'v_work_force',
            'technician_work': 'v_work_force',
            'staff_performance': 'v_work_force',
            
            # Repair/Service history
            'repair_history': 'v_work_force',
            'service_history': 'v_work_force',
            'maintenance_history': 'v_work_force',
            'work_history': 'v_work_force',
            
            # Special work queries
            'cpa_work': 'v_work_force',
            'government_work': 'v_work_force',
            'project_work': 'v_work_force',
            
            # ========================================
            # SPARE PARTS & INVENTORY (v_spare_part)
            # ========================================
            'spare_parts': 'v_spare_part',
            'parts_inventory': 'v_spare_part',
            'inventory': 'v_spare_part',
            'inventory_check': 'v_spare_part',
            'stock_check': 'v_spare_part',
            'warehouse': 'v_spare_part',
            
            # Parts pricing
            'parts_price': 'v_spare_part',
            'parts_cost': 'v_spare_part',
            'parts_value': 'v_spare_part',
            
            # Stock management
            'low_stock': 'v_spare_part',
            'stock_movement': 'v_spare_part',
            'stock_balance': 'v_spare_part',

            'transaction_count': 'v_sales',
            'transaction_frequency': 'v_sales',
            'transaction_summary': 'v_sales',
            'high_value_transactions': 'v_sales',
            'low_value_transactions': 'v_sales',
            'customer_transaction_frequency': 'v_sales',
            'monthly_transaction': 'v_sales',
            'yearly_transaction': 'v_sales',
            'total_transaction': 'v_sales',

            # Revenue analysis
            'revenue_growth': 'v_sales',
            'revenue_comparison': 'v_sales',
            'revenue_by_year': 'v_sales',
            'revenue_by_service': 'v_sales',
            'revenue_proportion': 'v_sales',
            'revenue_distribution': 'v_sales',
            'revenue_forecast': 'v_sales',
            'annual_revenue': 'v_sales',
            'max_revenue': 'v_sales',
            'min_revenue': 'v_sales',
            'average_revenue': 'v_sales',
            'total_revenue': 'v_sales',
            'yoy_growth': 'v_sales',

            # Customer segmentation
            'government_customers': 'v_sales',
            'private_customers': 'v_sales',
            'hospital_customers': 'v_sales',
            'foreign_customers': 'v_sales',
            'hitachi_customers': 'v_sales',
            'chiller_customers': 'v_sales',
            'frequent_customers': 'v_sales',
            'high_value_customers': 'v_sales',
            'high_potential_customers': 'v_sales',
            'continuous_customers': 'v_sales',
            'new_vs_returning_customers': 'v_sales',
            'customer_specific_history': 'v_sales',
            'parts_only_customers': 'v_sales',
            'customers_using_overhaul': 'v_sales',

            # Service type analysis
            'overhaul_total': 'v_sales',
            'overhaul_analysis': 'v_sales',
            'service_total': 'v_sales',
            'service_analysis': 'v_sales',
            'parts_total': 'v_sales',
            'parts_analysis': 'v_sales',
            'replacement_total': 'v_sales',
            'replacement_analysis': 'v_sales',
            'product_sales': 'v_sales',
            'solution_sales': 'v_sales',
            'service_vs_replacement': 'v_sales',
            'popular_service_types': 'v_sales',
            'service_roi': 'v_sales',

            # Job/Work value analysis
            'count_all_jobs': 'v_sales',
            'count_jobs_year': 'v_sales',
            'max_value_work': 'v_sales',
            'min_value_work': 'v_sales',
            'average_work_value': 'v_sales',
            'job_analysis': 'v_sales',

            # Time-based analysis
            'quarterly_summary': 'v_sales',
            'monthly_sales_trend': 'v_sales',
            'customers_per_year': 'v_sales',

            # Performance metrics
            'business_overview': 'v_sales',
            'annual_performance_summary': 'v_sales',
            'growth_trend': 'v_sales',

            # ========================================
            # WORK FORCE ADDITIONS (v_work_force)
            # ========================================
            'work_monthly': 'v_work_force',
            'work_summary_monthly': 'v_work_force',
            'work_plan_date': 'v_work_force',
            'work_specific_month': 'v_work_force',
            'work_today': 'v_work_force',
            'work_this_week': 'v_work_force',
            'latest_works': 'v_work_force',

            # Work duration analysis
            'work_duration': 'v_work_force',
            'min_duration_work': 'v_work_force',
            'max_duration_work': 'v_work_force',
            'long_duration_works': 'v_work_force',

            # Work types
            'pm_works_all': 'v_work_force',
            'startup_works': 'v_work_force',
            'support_works': 'v_work_force',
            'cpa_works': 'v_work_force',
            'kpi_works': 'v_work_force',
            'replacement_monthly': 'v_work_force',

            # Work performance
            'success_rate': 'v_work_force',
            'on_time_works': 'v_work_force',
            'overtime_works': 'v_work_force',
            'kpi_reported_works': 'v_work_force',

            # Team analysis
            'team_statistics': 'v_work_force',
            'team_performance': 'v_work_force',
            'team_specific_works': 'v_work_force',
            'employee_work_history': 'v_work_force',

            # Work counting
            'count_all_works': 'v_work_force',
            'count_works_by_year': 'v_work_force',

            # Customer-specific work
            'stanley_works': 'v_work_force',
            'customer_sales_and_service': 'v_work_force',

            # ========================================
            # SPARE PARTS ADDITIONS (v_spare_part)
            # ========================================
            'spare_parts_price': 'v_spare_part',
            'spare_parts_stock': 'v_spare_part',
            'spare_parts_all': 'v_spare_part',
            'parts_search_multi': 'v_spare_part',

            # Inventory management
            'inventory_value': 'v_spare_part',
            'total_inventory_value': 'v_spare_part',
            'warehouse_summary': 'v_spare_part',
            'warehouse_comparison': 'v_spare_part',
            'warehouse_specific_parts': 'v_spare_part',

            # Stock status
            'parts_in_stock': 'v_spare_part',
            'parts_out_of_stock': 'v_spare_part',
            'low_stock_alert': 'v_spare_part',
            'low_stock_items': 'v_spare_part',
            'reorder_parts': 'v_spare_part',

            # Parts analysis
            'count_all_parts': 'v_spare_part',
            'most_expensive_parts': 'v_spare_part',
            'cheapest_parts': 'v_spare_part',
            'high_unit_price': 'v_spare_part',
            'highest_value_items': 'v_spare_part',
            'average_part_price': 'v_spare_part',
            'unpriced_parts': 'v_spare_part',

            # Specific parts
            'compressor_parts': 'v_spare_part',
            'filter_parts': 'v_spare_part',
            'set_parts': 'v_spare_part',

            # Stock quantity
            'total_stock_quantity': 'v_spare_part',
            'recently_received': 'v_spare_part',

            # ========================================
            # CROSS-TABLE QUERIES (Need special handling)
            # ========================================
            'customer_sales_and_service': 'v_sales',  # Requires both v_sales and v_work_force

            # ========================================
            # ANALYTICAL QUERIES (Primary table based on focus)
            # ========================================
            'performance_analysis': 'v_work_force',  # Focus on work performance
            'roi_analysis': 'v_sales',              # Focus on revenue ROI
            'trend_analysis': 'v_sales',            # Usually revenue trends
            # ========================================
            # AMBIGUOUS/GENERAL (Need context)
            # ========================================
            'general': None,  # Will use keyword detection
            'unknown': None,  # Will use keyword detection
            'greeting': None,  # No table needed
            'help': None,      # No table needed
        }
        
        # Get mapping
        table = intent_table_map.get(intent)
        
        # Log mapping result
        if table:
            logger.info(f"Intent '{intent}' mapped to table '{table}'")
        else:
            logger.warning(f"No table mapping for intent '{intent}', will use keyword detection")
        
        return table

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
        """Select most relevant example - FIXED VERSION with all improvements"""
        question_lower = question.lower()
        if entities.get('employees'):
            # Log for debugging
            logger.info(f"🎯 Employee entity found: {entities['employees']}")
            
            # Check if employee template exists
            if 'employee_work_history' in self.SQL_EXAMPLES:
                logger.info("Using employee_work_history template")
                return self.SQL_EXAMPLES['employee_work_history']
            
            # Fallback: create custom template
            emp = entities['employees'][0]
            logger.warning(f"Creating custom template for employee: {emp}")
            return f"""
                SELECT date, customer, project, detail, service_group
                FROM v_work_force  
                WHERE service_group LIKE '%{emp}%'
                ORDER BY date DESC
            """
        if ('ยอดขาย' in question_lower or 'sales' in question_lower or 'รายงาน' in question_lower) and 'overhaul' in question_lower:
            logger.info("🎯 Sales/Report + Overhaul → overhaul_sales_specific")
            return self.SQL_EXAMPLES.get('overhaul_sales_specific', '')
        
        # ========================================
        # PRIORITY 2: Customer with history
        # ========================================
        
        
        # === PRIORITY FIX: Direct mapping for work_plan with 'วางแผน' ===
        if intent == 'work_plan' and 'วางแผน' in question_lower:
            logger.info("Priority: งานที่วางแผน → work_monthly")
            return self.SQL_EXAMPLES.get('work_monthly', '')
        
        # === PHASE 0: Direct Exact Match ===
        exact_matches = {
            # ข้อ 1-10: รายได้และยอดขาย
            'รายได้รวมทั้งหมดเท่าไหร่': 'total_revenue_all',
            'รายได้ปี 2024': 'total_revenue_year',
            'เปรียบเทียบรายได้ปี 2023 กับ 2024': 'compare_revenue_years',
            'ยอดขาย overhaul ทั้งหมด': 'overhaul_total',
            'ยอดขาย service ปี 2024': 'service_num',
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
            'รายได้จาก service ปี 2023': 'service_revenue_2023',
            'มีงานทั้งหมดกี่งาน': 'count_all_jobs',
            'มีงานปี 2024 กี่งาน': 'count_jobs_year',
            'รายได้เฉลี่ยต่องาน': 'average_revenue_per_job',
            'งานที่มีรายได้มากกว่า 1 ล้าน': 'high_value_transactions',
            'งานที่มีรายได้น้อยกว่า 50,000': 'low_value_transactions',
            
            # ข้อ 26-50: ลูกค้า
            'มีลูกค้าทั้งหมดกี่ราย': 'count_total_customers',
            'top 10 ลูกค้าที่ใช้บริการมากที่สุด': 'top_customers',
            'ลูกค้าที่มียอดการใช้บริการสูงสุด': 'top_customers',
            'ประวัติการใช้บริการของ stanley': 'customer_specific_history',
            'ลูกค้าใหม่ปี 2024': 'new_customers_year',
            'ลูกค้าที่ใช้บริการบ่อยที่สุด': 'frequent_customers',
            'ลูกค้าที่ใช้บริการ overhaul': 'customers_using_overhaul',
            'ลูกค้าภาครัฐมีใครบ้าง': 'government_customers',
            'ลูกค้าเอกชนที่ใหญ่ที่สุด': 'private_customers',
            
            # ข้อ 71-90: งานบริการและทีมงาน
            'มีงานทั้งหมดกี่งานในระบบ': 'count_all_works',
            'งานเดือนกันยายน 2024': 'work_specific_month',
            'งานบำรุงรักษา (pm) ทั้งหมด': 'all_pm_works',
            'งาน overhaul ที่ทำ': 'work_overhaul',
            'งานเปลี่ยนอุปกรณ์': 'work_replacement',
            'งานที่ทำสำเร็จ': 'successful_works',
            'งานที่ไม่สำเร็จ': 'unsuccessful_works',
            'งานของทีม a': 'team_works',
        }
        
        for pattern, example_name in exact_matches.items():
            if pattern in question_lower:
                logger.info(f"Exact match: {example_name}")
                if example_name in self.SQL_EXAMPLES:
                    return self.SQL_EXAMPLES[example_name]
        
        # === PHASE 1: Pattern-Based Priority Rules ===
        
        # Priority 1: งานที่วางแผน
        if any(word in question_lower for word in ['งานที่วางแผน', 'วางแผน', 'แผนงาน']):
            if entities.get('months'):
                logger.info("Priority: planned work + months → work_monthly")
                return self.SQL_EXAMPLES.get('work_monthly', '')
        
        # Priority 2: แยกตามประเภท
        if any(word in question_lower for word in ['แยกตาม', 'แต่ละประเภท', 'breakdown']):
            if any(word in question_lower for word in ['ประเภทงาน', 'ประเภท', 'type', 'service']):
                logger.info("Priority: breakdown by type → revenue_by_service_type")
                return self.SQL_EXAMPLES.get('revenue_by_service_type', '')
        
        # Priority 3: มูลค่า + สูง/ต่ำสุด
        if any(word in question_lower for word in ['มูลค่า', 'ราคา', 'value', 'price']):
            if any(word in question_lower for word in ['สูงสุด', 'มากที่สุด', 'แพงที่สุด']):
                if 'งาน' in question_lower:
                    logger.info("Priority: งาน + มูลค่าสูงสุด → max_value_work")
                    return self.SQL_EXAMPLES.get('max_value_work', '')
                elif 'อะไหล่' in question_lower or 'parts' in question_lower:
                    logger.info("Priority: อะไหล่ + ราคาสูงสุด → most_expensive_parts")
                    return self.SQL_EXAMPLES.get('most_expensive_parts', '')
            elif any(word in question_lower for word in ['ต่ำสุด', 'น้อยที่สุด', 'ถูกที่สุด']):
                if 'งาน' in question_lower:
                    logger.info("Priority: งาน + มูลค่าต่ำสุด → min_value_work")
                    return self.SQL_EXAMPLES.get('min_value_work', '')
        
        
        # === PHASE 2: Updated EXAMPLE_KEYWORDS with fixes ===
        EXAMPLE_KEYWORDS = {
            # ========================================
            # CUSTOMER TEMPLATES
            # ========================================
            'monthly_transaction_count': [
                'ซื้อมากี่ครั้ง', 'มีลูกค้าซื้อกี่ครั้ง', 'transaction count',
                'จำนวนการซื้อ', 'ครั้งการซื้อ', 'frequency purchase',
                'เดือนมีลูกค้าซื้อกี่ครั้ง', 'มีการซื้อขายกี่ครั้ง'
            ],
            
            'customer_transaction_frequency': [
                'ลูกค้าซื้อกี่ครั้ง', 'frequency customer', 'ลูกค้าซื้อบ่อย',
                'ลูกค้าซื้อมากครั้ง', 'customer frequency', 'ความถี่การซื้อ'
            ],
            
            'total_transaction_count': [
                'การซื้อขายทั้งหมด', 'transaction ทั้งหมด', 'total transaction',
                'รวมการซื้อขาย', 'ซื้อขายรวม', 'จำนวนรวมทั้งหมด'
            ],
            
            'yearly_transaction_summary': [
                'สรุปการซื้อขายปี', 'transaction summary year', 'ซื้อขายปี',
                'รายงานการซื้อขายประจำปี', 'yearly transaction'
            ],

            'customer_history_3year': [
                'มีการซื้อขายย้อนหลัง', 'มีการซื้อขายย้อนหลัง 3 ปี มีอะไรบ้าง'
            ],
            'customer_history': [
                'ประวัติลูกค้า', 'ประวัติการซื้อขาย', 'customer history', 
                'การซื้อขายย้อนหลัง', 'ข้อมูลลูกค้า', 'รายละเอียดลูกค้า'
            ],
            'customer_years_count': [
                'ซื้อขายมากี่ปี', 'กี่ปีแล้ว', 'how many years', 
                'ซื้อขายมาแล้วกี่ปี', 'ใช้บริการมากี่ปี', 'years operation'
            ],
            
            'top_customers_no_filter': [
                'ลูกค้าอันดับต้น', 'top customer', 'ลูกค้าสูงสุด', 
                'ลูกค้ามากที่สุด', 'ลูกค้าที่ใช้บริการมาก', 'best customer'
            ],
            
            'top_customers_by_year': [
                'ลูกค้าปี', 'top customer year', 'ลูกค้าปีสูง', 
                'ลูกค้าอันดับต้นปี', 'ลูกค้าดีที่สุดปี'
            ],
            
            'count_total_customers': [
                'จำนวนลูกค้า', 'มีลูกค้ากี่ราย', 'total customer', 
                'นับลูกค้า', 'count customer', 'ลูกค้าทั้งหมด'
            ],
            
            'inactive_customers': [
                'ลูกค้าไม่ใช้', 'ลูกค้าหยุด', 'inactive customer', 
                'ลูกค้าเลิก', 'ลูกค้าที่ไม่ได้ใช้บริการ', 'ลูกค้าไม่กลับมา'
            ],
            
            'new_customers_year': [
                'ลูกค้าใหม่ปี', 'new customer year', 'ลูกค้าใหม่ในปี', 
                'ลูกค้าที่เพิ่งมา', 'ลูกค้าใหม่'
            ],
            
            'continuous_customers': [
                'ลูกค้าต่อเนื่อง', 'ลูกค้า loyal', 'continuous customer', 
                'ลูกค้าประจำ', 'ลูกค้าคงที่'
            ],
            
            # ========================================
            # REVENUE & SALES TEMPLATES  
            # ========================================
            'total_revenue_all': [
                'รายได้ทั้งหมด', 'total revenue all', 'รายได้รวมทั้งหมด', 
                'รายได้รวม', 'income all', 'รายได้ปีทั้งหมด'
            ],
            
            'total_revenue_year': [
                'รายได้ปี', 'revenue year', 'รายได้รวมปี', 
                'รายได้ของปี', 'income ปี', 'ยอดรวมปี'
            ],
            
            'revenue_by_year': [
                'รายได้แต่ละปี', 'revenue by year', 'รายได้แยกปี', 
                'รายได้ปีต่อปี', 'income by year'
            ],
            
            'compare_revenue_years': [
                'เปรียบเทียบรายได้', 'compare revenue', 'เปรียบเทียบปี', 
                'รายได้ 2 ปี', 'revenue comparison'
            ],
            
            'year_max_revenue': [
                'ปีรายได้สูงสุด', 'year max revenue', 'ปีไหนรายได้สูงสุด', 
                'ปีที่ดีที่สุด', 'highest revenue year'
            ],
            
            'year_min_revenue': [
                'ปีรายได้ต่ำสุด', 'year min revenue', 'ปีไหนรายได้ต่ำสุด', 
                'ปีที่แย่ที่สุด', 'lowest revenue year'
            ],
            
            'average_annual_revenue': [
                'รายได้เฉลี่ย', 'average revenue', 'รายได้เฉลี่ยปี', 
                'ค่าเฉลี่ยรายได้', 'mean revenue'
            ],
            
            'revenue_by_service_type': [
                'รายได้แยกประเภท', 'revenue by service', 'รายได้แต่ละประเภท', 
                'breakdown service', 'แยกตามประเภท'
            ],
            
            # ========================================
            # OVERHAUL TEMPLATES
            # ========================================
            'overhaul_sales_specific': [
                'ยอดขาย overhaul', 'overhaul sales', 'รายงาน overhaul', 
                'ขาย overhaul', 'overhaul revenue'
            ],
            
            'overhaul_sales_all': [
                'overhaul ทั้งหมด', 'total overhaul', 'ยอดขาย overhaul ทั้งหมด', 
                'overhaul รวม', 'all overhaul'
            ],
            
            'overhaul_report': [
                'รายงาน overhaul', 'overhaul report', 'สรุป overhaul', 
                'รายงาน compressor', 'overhaul summary'
            ],
            
            'work_overhaul': [
                'งาน overhaul', 'overhaul work', 'งาน compressor', 
                'งานซ่อม compressor', 'overhaul job'
            ],
            
            # ========================================
            # WORK FORCE TEMPLATES
            # ========================================
            'work_monthly': [
                'งานเดือน', 'work monthly', 'งานรายเดือน', 
                'งานที่วางแผน', 'แผนงานเดือน', 'monthly work'
            ],
            
            'work_plan': [
                'งานที่วางแผน', 'work plan', 'แผนงาน', 
                'planned work', 'งานแผน'
            ],
            
            'work_replacement': [
                'งานซ่อม', 'replacement work', 'งาน replacement', 
                'งานเปลี่ยน', 'replacement job'
            ],
            
            'successful_work_monthly': [
                'งานสำเร็จ', 'งานเสร็จ', 'successful work', 
                'completed work', 'งานที่สำเร็จ'
            ],
            
            'all_pm_works': [
                'งาน pm', 'preventive maintenance', 'บำรุงรักษาเชิงป้องกัน', 
                'pm work', 'maintenance work'
            ],
            
            'startup_works_all': [
                'start up', 'startup', 'สตาร์ทอัพ', 
                'สตาร์ท อัพ', 'งาน startup', 'เริ่มเครื่อง'
            ],
            
            'cpa_works': [
                'งาน cpa', 'cpa ทั้งหมด', 'งาน cpa work',
                'job_description_cpa', 'cpa jobs', 'cpa work'
            ],
            
            'kpi_reported_works': [
                'kpi', 'รายงาน kpi', 'งาน kpi', 
                'kpi work', 'report kpi'
            ],
            
            'team_specific_works': [
                'งานทีม', 'งานสุพรรณ', 'งานช่าง', 
                'team work', 'ทีม a', 'service group'
            ],
            
            'replacement_monthly': [
                'งาน replacement', 'งานเปลี่ยน', 'replacement เดือน', 
                'replacement monthly', 'งานเปลี่ยนรายเดือน'
            ],
            
            'long_duration_works': [
                'ใช้เวลานาน', 'หลายวัน', 'งานนาน', 
                'long duration', 'งานใช้เวลานาน'
            ],
            
            'count_all_works': [
                'จำนวนงาน', 'มีงานกี่งาน', 'count work', 
                'นับงาน', 'งานทั้งหมด', 'total work'
            ],
            
            'employee_work_history': [
                'งานของพนักงาน', 'employee work', 'งานช่าง', 
                'พนักงานชื่อ', 'ช่างชื่อ', 'ทีมของ'
            ],
            
            # ========================================
            # REPAIR & SERVICE TEMPLATES
            # ========================================
            'customer_repair_history': [
                'ประวัติการซ่อม', 'ประวัติซ่อม', 'repair history',
                'ซ่อมอะไรบ้าง', 'เคยซ่อม', 'การซ่อมแซม',
                'ลูกค้าซ่อม', 'ประวัติงานซ่อม', 'customer repair'
            ],
            
            'repair_history': [
                'ประวัติการซ่อม', 'repair history', 'ประวัติซ่อมแซม',
                'งานซ่อม', 'การซ่อม', 'maintenance record'
            ],
            
            'service_history': [
                'ประวัติบริการ', 'service history', 'ประวัติการบริการ',
                'งานบริการ', 'การบริการ', 'บริการอะไรบ้าง'
            ],
            
            'maintenance_history': [
                'ประวัติบำรุงรักษา', 'maintenance history', 'งานบำรุงรักษา',
                'การบำรุง', 'pm history', 'preventive maintenance'
            ],
            
            # ========================================
            # SPARE PARTS TEMPLATES
            # ========================================
            'spare_parts_price': [
                'ราคาอะไหล่', 'spare parts price', 'ราคา parts', 
                'อะไหล่ราคา', 'price spare'
            ],
            
            'spare_parts_stock': [
                'สต็อกอะไหล่', 'spare parts stock', 'คลังอะไหล่', 
                'สต๊อค parts', 'inventory parts'
            ],
            
            'spare_parts_all': [
                'อะไหล่ทั้งหมด', 'all spare parts', 'parts ทั้งหมด', 
                'อะไหล่รวม', 'total parts'
            ],
            
            'inventory_check': [
                'ตรวจสอบคลัง', 'inventory check', 'เช็คสต็อก', 
                'ดูคลัง', 'check stock'
            ],
            
            'inventory_value': [
                'มูลค่าคลัง', 'inventory value', 'คลังมูลค่า', 
                'ราคาคลัง', 'value inventory'
            ],
            
            'warehouse_summary': [
                'สรุปคลัง', 'warehouse summary', 'มูลค่าแต่ละคลัง', 
                'คลังแยก', 'สรุป warehouse'
            ],
            
            'low_stock_items': [
                'ใกล้หมด', 'สต็อกน้อย', 'สินค้าเหลือน้อย', 
                'low stock', 'อะไหล่ใกล้หมด'
            ],
            
          
            'high_unit_price': [
                'ราคาต่อหน่วยสูง', 'ราคาแพง', 'expensive parts', 
                'high price', 'อะไหล่แพง'
            ],
            
            'highest_value_items': [
                'สินค้ามูลค่าสูง', 'มูลค่าสูงสุดคลัง', 'highest value item', 
                'อะไหล่มูลค่าสูง', 'expensive inventory'
            ],
            
            'parts_by_warehouse': [
                'อะไหล่แยกคลัง', 'parts by warehouse', 'คลังแยก', 
                'แต่ละคลัง', 'warehouse breakdown'
            ],
            
            'parts_total_value': [
                'มูลค่ารวมอะไหล่', 'total parts value', 'ราคารวม parts', 
                'มูลค่าอะไหล่ทั้งหมด', 'total inventory value'
            ],
            
            # ========================================
            # SALES ANALYSIS TEMPLATES
            # ========================================
            'sales_analysis': [
                'วิเคราะห์การขาย', 'sales analysis', 'วิเคราะห์ยอดขาย', 
                'การวิเคราะห์ขาย', 'analyze sales'
            ],
            
            'sales_summary': [
                'สรุปการขาย', 'sales summary', 'สรุปยอดขาย', 
                'รายงานการขาย', 'sales report'
            ],
            
            'sales_by_month': [
                'ยอดขายรายเดือน', 'sales by month', 'ขายแยกเดือน', 
                'monthly sales', 'ขายเดือน'
            ],
            
            'top_parts_customers': [
                'ลูกค้าซื้ออะไหล่', 'ลูกค้า parts สูง', 'top parts customer', 
                'ลูกค้าอะไหล่', 'customer parts'
            ],
            
            'service_vs_replacement': [
                'เปรียบเทียบ service replacement', 'service กับ replacement', 
                'service vs replacement', 'เปรียบเทียบบริการ'
            ],
            
            'solution_sales': [
                'ยอด solution', 'solution สูง', 'ลูกค้า solution', 
                'solution sales', 'ขาย solution'
            ],
            
            'quarterly_summary': [
                'ไตรมาส', 'quarterly', 'รายไตรมาส', 
                'quarter', 'สรุปไตรมาส'
            ],
            
            # ========================================
            # PRICING TEMPLATES
            # ========================================
            'pricing_standard': [
                'ราคา standard', 'เสนอราคา standard', 'standard price', 
                'งาน standard', 'quotation standard'
            ],
            
            'pricing_summary': [
                'สรุปราคา', 'price summary', 'รายการราคา', 
                'เสนอราคาทั้งหมد', 'quotation summary'
            ],
            
            # ========================================
            # GOVERNMENT & SPECIAL CUSTOMER TEMPLATES
            # ========================================
            'government_customers': [
                'ลูกค้าภาครัฐ', 'government customer', 'หน่วยงานราชการ', 
                'ลูกค้าราชการ', 'gov customer'
            ],
            
            'private_customers': [
                'ลูกค้าเอกชน', 'private customer', 'ลูกค้าเอกชนใหญ่', 
                'private sector', 'เอกชนใหญ่'
            ],
            
            # ========================================
            # VALUE & AMOUNT TEMPLATES  
            # ========================================
            'max_value_work': [
                'งานมูลค่าสูงสุด', 'งานที่มีมูลค่าสูงสุด', 'highest value work', 
                'งานแพงที่สุด', 'most expensive work'
            ],
            
            'min_value_work': [
                'งานมูลค่าต่ำสุด', 'งานที่มีมูลค่าต่ำสุด', 'lowest value work', 
                'งานถูกที่สุด', 'cheapest work'
            ],
            
            'total_value_all': [
                'มูลค่ารวมทั้งหมด', 'total value all', 'ราคารวมทั้งหมด', 
                'มูลค่าทั้งหมด', 'grand total'
            ],
            
            # ========================================
            # YEARLY ANALYSIS TEMPLATES
            # ========================================
            'year_comparison': [
                'เปรียบเทียบปี', 'year comparison', 'เปรียบเทียบรายปี', 
                'ปีต่อปี', 'compare year'
            ],
            
            'year_analysis': [
                'วิเคราะห์ปี', 'year analysis', 'วิเคราะห์รายปี', 
                'การวิเคราะห์ปี', 'analyze year'
            ],
            
            # ========================================
            # MONTHLY ANALYSIS TEMPLATES
            # ========================================
            'monthly_summary': [
                'สรุปรายเดือน', 'monthly summary', 'สรุปเดือน', 
                'รายงานเดือน', 'monthly report'
            ],
            
            'work_specific_month': [
                'งานเดือนกันยายน', 'งานเดือนเฉพาะ', 'work specific month', 
                'งานเดือนที่ระบุ', 'monthly work specific'
            ],
            
            # ========================================
            # SUCCESS & PERFORMANCE TEMPLATES
            # ========================================
            'successful_works': [
                'งานที่สำเร็จ', 'งานสำเร็จ', 'successful work', 
                'งานเสร็จ', 'completed work'
            ],
            
            'unsuccessful_works': [
                'งานที่ไม่สำเร็จ', 'งานไม่สำเร็จ', 'unsuccessful work', 
                'งานล้มเหลว', 'failed work'
            ],
            
            # ========================================
            # PRODUCT & PARTS TEMPLATES
            # ========================================
            'product_sales': [
                'ยอดขายสินค้า', 'product sales', 'ขายสินค้า', 
                'product revenue', 'รายได้สินค้า'
            ],
            
            'parts_sales': [
                'ยอดขายอะไหล่', 'parts sales', 'ขายอะไหล่', 
                'spare parts sales', 'รายได้อะไหล่'
            ],
            
            'replacement_sales': [
                'ยอดขาย replacement', 'replacement sales', 'ขาย replacement', 
                'งานเปลี่ยนขาย', 'replacement revenue'
            ],
            
            'service_sales': [
                'ยอดขายบริการ', 'service sales', 'ขายบริการ', 
                'service revenue', 'รายได้บริการ'
            ],
            'average_revenue_per_transaction': [
            'รายได้เฉลี่ยต่องาน', 'average revenue per transaction', 'ค่าเฉลี่ยต่องาน',
            'รายได้เฉลี่ยแต่ละงาน', 'avg revenue per job', 'เฉลี่ยต่อการทำงาน'
        ],

        'high_value_transactions': [
            'งานมูลค่าสูง', 'high value transaction', 'งานราคาแพง',
            'งานมูลค่าสูงสุด', 'expensive transaction', 'งานมูลค่าสูงกว่า'
        ],

        'max_revenue_by_year': [
            'รายได้สูงสุดแต่ละปี', 'max revenue by year', 'รายได้สูงสุดรายปี',
            'รายได้สูงสุดของปี', 'highest revenue each year'
        ],

        'all_years_revenue_comparison': [
            'เปรียบเทียบรายได้ทุกปี', 'all years revenue comparison', 'รายได้ทุกปี',
            'เปรียบเทียบรายได้แต่ละปี', 'compare all years revenue'
        ],

        'average_work_value': [
            'ค่าเฉลี่ยงาน', 'average work value', 'มูลค่าเฉลี่ยงาน',
            'ราคาเฉลี่ยงาน', 'avg work value', 'ค่าเฉลี่ยของงาน'
        ],

        'new_customers_in_year': [
            'ลูกค้าใหม่ในปี', 'new customers in year', 'ลูกค้าใหม่ปีนี้',
            'ลูกค้าใหม่ของปี', 'new customer specific year'
        ],

        'customers_using_overhaul': [
            'ลูกค้าใช้ overhaul', 'customers using overhaul', 'ลูกค้า overhaul',
            'ลูกค้าทำ overhaul', 'overhaul customers'
        ],

        'customers_continuous_years': [
            'ลูกค้าใช้บริการต่อเนื่อง', 'customers continuous years', 'ลูกค้าติดต่อกันหลายปี',
            'ลูกค้าต่อเนื่องหลายปี', 'continuous service customers'
        ],

        'top_service_customers': [
            'ลูกค้า service มากที่สุด', 'top service customers', 'ลูกค้าใช้บริการมาก',
            'ลูกค้า service สูงสุด', 'customers top service'
        ],

        'most_frequent_customers': [
            'ลูกค้าใช้บริการบ่อยที่สุด', 'most frequent customers', 'ลูกค้าใช้บริการบ่อย',
            'ลูกค้าความถี่สูง', 'frequent service customers'
        ],

        'work_plan_date': [
            'แผนงานวันที่', 'work plan date', 'งานวันที่เฉพาะ',
            'แผนงานวันเฉพาะ', 'specific date work plan'
        ],

        'work_summary_monthly': [
            'สรุปงานเดือน', 'work summary monthly', 'สรุปงานรายเดือน',
            'รายงานงานเดือน', 'monthly work summary'
        ],

        'parts_search_multi': [
            'ค้นหาอะไหล่หลายคำ', 'parts search multiple', 'ค้นหา parts หลายคำ',
            'search parts multi', 'หาอะไหล่หลายคำ'
        ],

        'sales_yoy_growth': [
            'การเติบโต year over year', 'sales yoy growth', 'เปรียบเทียบปีต่อปี',
            'yoy growth', 'การเติบโตรายปี'
        ],

        'customer_sales_and_service': [
            'ลูกค้าขายและบริการ', 'customer sales and service', 'ลูกค้าทั้งขายและซ่อม',
            'customer both sales service', 'ลูกค้าครบวงจร'
        ],

        'min_duration_work': [
            'งานใช้เวลาน้อยสุด', 'min duration work', 'งานเสร็จเร็วสุด',
            'งานใช้เวลาต่ำสุด', 'shortest duration work'
        ],

        'max_duration_work': [
            'งานใช้เวลามากสุด', 'max duration work', 'งานใช้เวลานานสุด',
            'งานใช้เวลาสูงสุด', 'longest duration work'
        ],

        'count_works_by_year': [
            'จำนวนงานแต่ละปี', 'count works by year', 'นับงานรายปี',
            'จำนวนงานรายปี', 'work count by year'
        ],

        'overhaul_total': [
            'overhaul ทั้งหมด', 'overhaul total', 'ยอดรวม overhaul',
            'รวม overhaul', 'total overhaul all'
        ],

        'parts_total': [
            'parts ทั้งหมด', 'parts total', 'ยอดรวม parts',
            'รวม parts', 'total parts all'
        ],

        'replacement_total': [
            'replacement ทั้งหมด', 'replacement total', 'ยอดรวม replacement',
            'รวม replacement', 'total replacement all'
        ],

        'count_all_jobs': [
            'จำนวนงานทั้งหมด', 'count all jobs', 'นับงานทั้งหมด',
            'งานทั้งหมดกี่งาน', 'total jobs count'
        ],

        'count_jobs_year': [
            'จำนวนงานปีเฉพาะ', 'count jobs year', 'นับงานในปี',
            'งานปีนี้กี่งาน', 'jobs count specific year'
        ],

        'average_revenue_per_job': [
            'รายได้เฉลี่ยต่องาน', 'average revenue per job', 'ค่าเฉลี่ยต่องาน',
            'รายได้เฉลี่ยแต่ละงาน', 'avg revenue job'
        ],

        'revenue_growth': [
            'การเติบโตรายได้', 'revenue growth', 'เติบโตรายได้',
            'การเพิ่มขึ้นรายได้', 'income growth'
        ],

        'revenue_proportion': [
            'สัดส่วนรายได้', 'revenue proportion', 'อัตราส่วนรายได้',
            'เปอร์เซ็นต์รายได้', 'revenue percentage'
        ],

        'max_revenue_each_year': [
            'รายได้สูงสุดต่อปี', 'max revenue each year', 'รายได้สูงสุดแต่ละปี',
            'รายได้สูงสุดของแต่ละปี', 'highest revenue per year'
        ],

        'total_inventory_value': [
            'มูลค่ารวมสินค้าคงคลัง', 'total inventory value', 'มูลค่าคลังรวม',
            'ราคาคลังทั้งหมด', 'total stock value'
        ],

        'customer_specific_history': [
            'ประวัติลูกค้าเฉพาะราย', 'customer specific history', 'ประวัติลูกค้าเฉพาะ',
            'ข้อมูลลูกค้าราย', 'individual customer history','ข้อมูลลูกค้า','ข้อมูลลูกค้าบริษัท','ขอข้อมูลบริษัท'
        ],

        'frequent_customers': [
            'ลูกค้าที่ใช้บริการบ่อย', 'frequent customers', 'ลูกค้าใช้บริการบ่อยที่สุด',
            'ลูกค้าความถี่สูง', 'high frequency customers'
        ],

        'hospital_customers': [
            'ลูกค้าโรงพยาบาล', 'hospital customers', 'ลูกค้าสถานพยาบาล',
            'โรงพยาบาลลูกค้า', 'medical customers'
        ],

        'high_value_customers': [
            'ลูกค้าจ่ายเงินมาก', 'high value customers', 'ลูกค้ามูลค่าสูง',
            'ลูกค้าใช้เงินเยอะ', 'big spending customers'
        ],

        'parts_only_customers': [
            'ลูกค้าซื้อแต่ parts', 'parts only customers', 'ลูกค้าอะไหล่อย่างเดียว',
            'ลูกค้าเฉพาะ parts', 'customers parts only'
        ],

        'chiller_customers': [
            'ลูกค้าชิลเลอร์', 'chiller customers', 'ลูกค้า chiller',
            'ลูกค้าระบบทำความเย็น', 'cooling system customers'
        ],

        'new_vs_returning_customers': [
            'ลูกค้าใหม่ vs เก่า', 'new vs returning customers', 'เปรียบเทียบลูกค้าใหม่เก่า',
            'ลูกค้าใหม่กับเก่า', 'new versus old customers'
        ],

        'count_all_parts': [
            'จำนวนอะไหล่ทั้งหมด', 'count all parts', 'นับอะไหล่ทั้งหมด',
            'อะไหล่ทั้งหมดกี่รายการ', 'total parts count'
        ],

        'parts_in_stock': [
            'อะไหล่ที่มีสต็อก', 'parts in stock', 'อะไหล่คงเหลือ',
            'อะไหล่ที่มี', 'available parts'
        ],

        'parts_out_of_stock': [
            'อะไหล่หมดสต็อก', 'parts out of stock', 'อะไหล่หมด',
            'อะไหล่ไม่มี', 'unavailable parts'
        ],

        'most_expensive_parts': [
            'อะไหล่แพงที่สุด', 'most expensive parts', 'อะไหล่ราคาสูงสุด',
            'อะไหล่ราคาแพง', 'highest price parts'
        ],

        'low_stock_alert': [
            'อะไหล่ใกล้หมด', 'low stock alert', 'แจ้งเตือนสต็อกต่ำ',
            'อะไหล่เหลือน้อย', 'parts running low'
        ],

        'warehouse_specific_parts': [
            'อะไหล่ในคลังเฉพาะ', 'warehouse specific parts', 'อะไหล่คลังเฉพาะ',
            'parts ในคลัง', 'specific warehouse parts'
        ],

        'average_part_price': [
            'ราคาเฉลี่ยอะไหล่', 'average part price', 'ค่าเฉลี่ยราคา parts',
            'ราคา parts เฉลี่ย', 'avg parts price'
        ],

        'compressor_parts': [
            'อะไหล่คอมเพรสเซอร์', 'compressor parts', 'parts คอมเพรสเซอร์',
            'อะไหล่ compressor', 'compressor spare parts'
        ],

        'filter_parts': [
            'อะไหล่ filter', 'filter parts', 'อะไหล่กรอง',
            'parts filter', 'filter spare parts'
        ],

        'warehouse_comparison': [
            'เปรียบเทียบคลัง', 'warehouse comparison', 'เปรียบเทียบแต่ละคลัง',
            'compare warehouse', 'คลังเปรียบเทียบ'
        ],

        'work_specific_month': [
            'งานเดือนเฉพาะ', 'work specific month', 'งานในเดือน',
            'งานเดือนที่กำหนด', 'specific month work'
        ],

        'all_pm_works': [
            'งาน PM ทั้งหมด', 'all pm works', 'งาน preventive maintenance ทั้งหมด',
            'pm works all', 'งานบำรุงรักษาทั้งหมด'
        ],

        'work_today': [
            'งานวันนี้', 'work today', 'งานประจำวัน',
            'งานของวันนี้', 'today work schedule'
        ],

        'work_this_week': [
            'งานสัปดาห์นี้', 'work this week', 'งานในสัปดาห์',
            'งานสัปดาห์ปัจจุบัน', 'current week work'
        ],

        'success_rate': [
            'อัตราความสำเร็จ', 'success rate', 'เปอร์เซ็นต์สำเร็จ',
            'ความสำเร็จงาน', 'work success rate'
        ],

        'on_time_works': [
            'งานตรงเวลา', 'on time works', 'งานเสร็จตรงเวลา',
            'งานไม่เกินเวลา', 'punctual work completion'
        ],

        'overtime_works': [
            'งานเกินเวลา', 'overtime works', 'งานล่าช้า',
            'งานไม่ทันเวลา', 'delayed works'
        ],

        'support_works': [
            'งาน support', 'support works', 'งานสนับสนุน',
            'งานช่วยเหลือ', 'support jobs'
        ],

        'team_statistics': [
            'สถิติทีม', 'team statistics', 'สถิติแต่ละทีม',
            'ข้อมูลทีมงาน', 'team performance stats'
        ],

        'work_duration': [
            'ระยะเวลาทำงาน', 'work duration', 'เวลาทำงาน',
            'ช่วงเวลางาน', 'job duration'
        ],

        'latest_works': [
            'งานล่าสุด', 'latest works', 'งานใหม่ล่าสุด',
            'งานที่ผ่านมา', 'recent works'
        ],

        'annual_performance_summary': [
            'สรุปผลประกอบการรายปี', 'annual performance summary', 'สรุปผลงานรายปี',
            'รายงานประจำปี', 'yearly performance report'
        ],

        'growth_trend': [
            'เทรนด์การเติบโต', 'growth trend', 'แนวโน้มการเติบโต',
            'ทิศทางการเติบโต', 'growth direction'
        ],

        'popular_service_types': [
            'ประเภทงานที่นิยม', 'popular service types', 'บริการที่นิยม',
            'งานที่ได้รับความนิยม', 'most popular services'
        ],

        'high_potential_customers': [
            'ลูกค้าที่มีศักยภาพ', 'high potential customers', 'ลูกค้าแนวโน้มดี',
            'ลูกค้าน่าสนใจ', 'promising customers'
        ],

        'revenue_distribution': [
            'การกระจายรายได้', 'revenue distribution', 'การแจกแจงรายได้',
            'กระจายตัวรายได้', 'income distribution'
        ],

        'team_performance': [
            'ประสิทธิภาพทีมงาน', 'team performance', 'ผลงานทีม',
            'การปฏิบัติงานทีม', 'team efficiency'
        ],

        'monthly_sales_trend': [
            'แนวโน้มยอดขายรายเดือน', 'monthly sales trend', 'เทรนด์ขายเดือน',
            'ทิศทางขายรายเดือน', 'monthly sales direction'
        ],

        'service_roi': [
            'ROI ของการบริการ', 'service roi', 'ผลตอบแทนการบริการ',
            'return on investment service', 'roi งานบริการ'
        ],

        'revenue_forecast': [
            'คาดการณ์รายได้', 'revenue forecast', 'พยากรณ์รายได้',
            'ทำนายรายได้', 'predict revenue'
        ],

        'business_overview': [
            'ภาพรวมธุรกิจ', 'business overview', 'สรุปภาพรวม',
            'รายงานภาพรวม', 'overall business summary'
        ],

        'service_num': [
            'ยอด service', 'service num', 'รายได้ service',
            'ขาย service', 'service revenue'
        ],

        'service_revenue_2023': [
            'รายได้ service 2023', 'service revenue 2023', 'ยอดขาย service ปี',
            'service ปี 2023', 'service income 2023'
        ],

        'low_value_transactions': [
            'งานมูลค่าต่ำ', 'low value transactions', 'งานราคาต่ำ',
            'งานมูลค่าน้อย', 'cheap transactions'
        ],

        'customers_per_year': [
            'ลูกค้าต่อปี', 'customers per year', 'จำนวนลูกค้าแต่ละปี',
            'ลูกค้าแยกปี', 'customers by year'
        ],

        'hitachi_customers': [
            'ลูกค้า hitachi', 'hitachi customers', 'งาน hitachi',
            'ลูกค้าฮิตาชิ', 'hitachi related'
        ],

        'avg_revenue_per_customer': [
            'รายได้เฉลี่ยต่อลูกค้า', 'avg revenue per customer', 'ค่าเฉลี่ยลูกค้า',
            'รายได้เฉลี่ยแต่ละลูกค้า', 'average customer value'
        ],

        'foreign_customers': [
            'ลูกค้าต่างชาติ', 'foreign customers', 'ลูกค้าต่างประเทศ',
            'international customers', 'overseas customers'
        ],

        'cheapest_parts': [
            'อะไหล่ถูกที่สุด', 'cheapest parts', 'อะไหล่ราคาต่ำสุด',
            'อะไหล่ราคาถูก', 'lowest price parts'
        ],


        'total_stock_quantity': [
            'จำนวนสต็อกรวม', 'total stock quantity', 'สต็อกทั้งหมด',
            'จำนวนคลังรวม', 'total inventory quantity'
        ],

        'reorder_parts': [
            'อะไหล่ต้องสั่งเพิ่ม', 'reorder parts', 'อะไหล่ควรสั่ง',
            'อะไหล่ต้องเติม', 'parts need reorder'
        ],

        'unpriced_parts': [
            'อะไหล่ไม่มีราคา', 'unpriced parts', 'อะไหล่ยังไม่ตั้งราคา',
            'parts no price', 'อะไหล่ราคาเป็นศูนย์'
        ],


        'set_parts': [
            'อะไหล่ชุด', 'set parts', 'อะไหล่หน่วยชุด',
            'parts หน่วย set', 'อะไหล่ที่ขายเป็นชุด'
        ],

        'recently_received': [
            'ที่เพิ่งได้รับ', 'recently received', 'เพิ่งเข้าคลัง',
            'ได้รับล่าสุด', 'latest received'
        ],

        'stanley_works': [
            'งาน stanley', 'stanley works', 'งานแสตนเลย์',
            'stanley jobs', 'ลูกค้า stanley งาน'
        ]
        }
        
        # === PHASE 3: Smart Scoring with penalties ===
        best_matches = []
        
        for example_name, keywords in EXAMPLE_KEYWORDS.items():
            score = 0
            matched_keywords = []
            
            for keyword in keywords:
                keyword_lower = keyword.lower()
                
                # Exact phrase match
                if keyword_lower in question_lower:
                    score += 20
                    matched_keywords.append(keyword)
                # All words match
                elif len(keyword_lower.split()) > 1:
                    if all(word in question_lower for word in keyword_lower.split() if len(word) > 2):
                        score += 12
                        matched_keywords.append(keyword)
                # Partial match
                elif any(word in question_lower for word in keyword_lower.split() if len(word) > 2):
                    score += 3
                    matched_keywords.append(keyword)
            
            # === PENALTIES ===
            
            # Penalty for PM works when not asking for PM
            if example_name == 'all_pm_works':
                if not any(word in question_lower for word in ['pm', 'บำรุงรักษา', 'preventive']):
                    score = score * 0.2  # Heavy penalty
            
            # Penalty for work examples on money queries
            if 'work' in example_name or 'pm' in example_name:
                if any(word in question_lower for word in ['มูลค่า', 'ราคา', 'value', 'revenue']):
                    score = score * 0.3
            
            # Penalty for single type on breakdown queries
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
            if entities.get('months') and 'monthly' in example_name:
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
        if best_matches and best_matches[0][0] >= 5:
            selected = best_matches[0][1]
            if selected in self.SQL_EXAMPLES:
                logger.info(f"✓ Selected: {selected} (score: {best_matches[0][0]})")
                return self.SQL_EXAMPLES[selected]
        
        # === PHASE 4: Intent-based fallback ===
        intent_map = {
            # === Work-related (เพิ่มให้ครบ) ===
            'customer_repair_history': 'customer_repair_history',
            'work_summary': 'work_summary_monthly',
            'work_plan': 'work_monthly',
            'work_force': 'work_monthly',
            'work_analysis': 'work_monthly',
            'pm_work': 'all_pm_works',           # เพิ่ม
            'work_overhaul': 'work_overhaul',    # เพิ่ม
            'work_replacement': 'work_replacement', # เพิ่ม
            'successful_work': 'successful_works', # เพิ่ม
            'repair_history': 'repair_history',   # เพิ่ม
            'cpa_work': 'cpa_works',
            # === Sales/Revenue ===
            'sales': 'sales_analysis',
            'sales_analysis': 'sales_analysis',
            'revenue': 'revenue_by_year',
            'revenue_analysis': 'revenue_by_year', # เพิ่ม
            'overhaul_report': 'overhaul_sales',
            'max_value': 'max_value_work',        # เพิ่ม
            'min_value': 'min_value_work',        # เพิ่ม
            
            # === Customer ===
            'customer_history': 'customer_history',
            'top_customers': 'top_customers',
            'customer_analysis': 'top_customers',  # เพิ่ม
            'new_customers': 'new_customers_year', # เพิ่ม
            
            # === Spare Parts ===
            'spare_parts': 'spare_parts_price',
            'parts_price': 'spare_parts_price',
            'inventory': 'inventory_check',        # เพิ่ม
            'inventory_check': 'inventory_check',  # เพิ่ม
            'warehouse': 'warehouse_summary',      # เพิ่ม
            'monthly_transaction_count': 'monthly_transaction_count',
            'customer_transaction_frequency': 'customer_transaction_frequency',
            'total_transaction_count': 'total_transaction_count',
            'yearly_transaction_summary': 'yearly_transaction_summary',

            # === Revenue Analysis (Missing) ===
            'total_revenue_all': 'total_revenue_all',
            'total_revenue_year': 'total_revenue_year',
            'compare_revenue_years': 'compare_revenue_years',
            'year_max_revenue': 'year_max_revenue',
            'year_min_revenue': 'year_min_revenue',
            'average_annual_revenue': 'average_annual_revenue',
            'revenue_growth': 'revenue_growth',
            'revenue_proportion': 'revenue_proportion',
            'all_years_revenue_comparison': 'all_years_revenue_comparison',
            'average_revenue_per_transaction': 'average_revenue_per_transaction',
            'high_value_transactions': 'high_value_transactions',
            'low_value_transactions': 'low_value_transactions',
            'revenue_by_service_type': 'revenue_by_service_type',
            'max_revenue_by_year': 'max_revenue_by_year',
            'max_revenue_each_year': 'max_revenue_each_year',
            'sales_yoy_growth': 'sales_yoy_growth',
            'revenue_forecast': 'revenue_forecast',
            'revenue_distribution': 'revenue_distribution',

            # === Service Type Analysis ===
            'overhaul_sales': 'overhaul_sales',
            'overhaul_sales_all': 'overhaul_sales_all',
            'overhaul_sales_specific': 'overhaul_sales_specific',
            'overhaul_total': 'overhaul_total',
            'service_num': 'service_num',
            'service_revenue_2023': 'service_revenue_2023',
            'parts_total': 'parts_total',
            'replacement_total': 'replacement_total',
            'service_vs_replacement': 'service_vs_replacement',
            'popular_service_types': 'popular_service_types',
            'service_roi': 'service_roi',

            # === Customer Analysis (Missing) ===
            'count_total_customers': 'count_total_customers',
            'new_customers_in_year': 'new_customers_in_year',
            'inactive_customers': 'inactive_customers',
            'continuous_customers': 'continuous_customers',
            'customers_continuous_years': 'customers_continuous_years',
            'customers_using_overhaul': 'customers_using_overhaul',
            'top_service_customers': 'top_service_customers',
            'most_frequent_customers': 'most_frequent_customers',
            'frequent_customers': 'frequent_customers',
            'government_customers': 'government_customers',
            'private_customers': 'private_customers',
            'hospital_customers': 'hospital_customers',
            'foreign_customers': 'foreign_customers',
            'hitachi_customers': 'hitachi_customers',
            'high_value_customers': 'high_value_customers',
            'parts_only_customers': 'parts_only_customers',
            'chiller_customers': 'chiller_customers',
            'new_vs_returning_customers': 'new_vs_returning_customers',
            'customer_specific_history': 'customer_specific_history',
            'customers_per_year': 'customers_per_year',
            'avg_revenue_per_customer': 'avg_revenue_per_customer',
            'customer_sales_and_service': 'customer_sales_and_service',
            'customer_years_count': 'customer_years_count',
            'high_potential_customers': 'high_potential_customers',
            'top_parts_customers': 'top_parts_customers',
            'solution_customers': 'solution_customers',

            # === Work Analysis (Missing) ===
            'work_monthly': 'work_monthly',
            'work_summary_monthly': 'work_summary_monthly',
            'work_plan_date': 'work_plan_date',
            'work_specific_month': 'work_specific_month',
            'work_today': 'work_today',
            'work_this_week': 'work_this_week',
            'latest_works': 'latest_works',
            'count_all_works': 'count_all_works',
            'count_works_by_year': 'count_works_by_year',
            'min_duration_work': 'min_duration_work',
            'max_duration_work': 'max_duration_work',
            'long_duration_works': 'long_duration_works',
            'successful_work_monthly': 'successful_work_monthly',
            'unsuccessful_works': 'unsuccessful_works',
            'pm_work_summary': 'pm_work_summary',
            'startup_works': 'startup_works',
            'startup_works_all': 'startup_works_all',
            'kpi_reported_works': 'kpi_reported_works',
            'team_specific_works': 'team_specific_works',
            'replacement_monthly': 'replacement_monthly',
            'success_rate': 'success_rate',
            'on_time_works': 'on_time_works',
            'overtime_works': 'overtime_works',
            'support_works': 'support_works',
            'team_statistics': 'team_statistics',
            'work_duration': 'work_duration',
            'stanley_works': 'stanley_works',
            'employee_work_history': 'employee_work_history',
            'team_performance': 'team_performance',
            'service_history': 'service_history',
            'maintenance_history': 'maintenance_history',

            # === Job/Work Value Analysis ===
            'count_all_jobs': 'count_all_jobs',
            'count_jobs_year': 'count_jobs_year',
            'average_work_value': 'average_work_value',
            'average_revenue_per_job': 'average_revenue_per_job',

            # === Spare Parts Analysis (Missing) ===
            'spare_parts_all': 'spare_parts_all',
            'parts_search_multi': 'parts_search_multi',
            'count_all_parts': 'count_all_parts',
            'parts_in_stock': 'parts_in_stock',
            'parts_out_of_stock': 'parts_out_of_stock',
            'most_expensive_parts': 'most_expensive_parts',
            'cheapest_parts': 'cheapest_parts',
            'low_stock_alert': 'low_stock_alert',
            'warehouse_specific_parts': 'warehouse_specific_parts',
            'average_part_price': 'average_part_price',
            'compressor_parts': 'compressor_parts',
            'filter_parts': 'filter_parts',
            'set_parts': 'set_parts',
            'recently_received': 'recently_received',
            'total_stock_quantity': 'total_stock_quantity',
            'reorder_parts': 'reorder_parts',
            'unpriced_parts': 'unpriced_parts',

            # === Inventory Analysis (Missing) ===
            'total_inventory_value': 'total_inventory_value',
            'highest_value_items': 'highest_value_items',
            'warehouse_comparison': 'warehouse_comparison',
            'low_stock_items': 'low_stock_items',
            'high_unit_price': 'high_unit_price',

            # === Time-based Analysis ===
            'quarterly_summary': 'quarterly_summary',
            'monthly_sales_trend': 'monthly_sales_trend',

            # === Analytical Queries ===
            'annual_performance_summary': 'annual_performance_summary',
            'growth_trend': 'growth_trend',
            'business_overview': 'business_overview',

            # === Alternative mappings for flexibility ===
            'overhaul_analysis': 'overhaul_sales',
            'parts_analysis': 'parts_total',
            'replacement_analysis': 'replacement_total',
            'service_analysis': 'service_num',
            'work_efficiency': 'team_performance',
            'technician_performance': 'employee_work_history',
            'inventory_value': 'total_inventory_value',
            'stock_analysis': 'inventory_check',
            'customer_segmentation': 'high_potential_customers',
            'market_analysis': 'popular_service_types',
            'performance_analysis': 'annual_performance_summary',
            'trend_analysis': 'growth_trend',
            'roi_analysis': 'service_roi',
            'forecast_analysis': 'revenue_forecast',

            # === Generic mappings for common intents ===
            'overview': 'business_overview',
            'summary': 'annual_performance_summary',
            'comparison': 'compare_revenue_years',
            'growth': 'sales_yoy_growth',
            'trend': 'growth_trend',
            'performance': 'team_performance',
            'efficiency': 'success_rate',
            'value': 'high_value_transactions',
            'cost': 'average_part_price',
            'quality': 'success_rate',
            'productivity': 'team_performance'
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

        # === EMPLOYEE SEARCH (v_work_force only) ===
        if entities.get('employees') and target_table == 'v_work_force':
            employee = entities['employees'][0]
            hints.append(f"""
    ⚠️ EMPLOYEE SEARCH:
    Looking for employee/staff named: {employee}

    CRITICAL INSTRUCTIONS:
    1. DO NOT use: WHERE customer = '{employee}' (customer is for company names!)
    2. USE: WHERE service_group LIKE '%{employee}%'
    3. The detail and service_group columns contain employee/technician names
    4. Show ALL work records related to this employee
    """)
            # Don't add other hints for employee search
            return '\n'.join(hints)

        # === HANDLE BASED ON TABLE TYPE ===
        
        if target_table == 'v_sales':
            # --- v_sales uses 'year' and 'month' columns ---
            
            # Year filtering
            if entities.get('years'):
                years = entities['years']
                year_str = "', '".join(years)
                hints.append(f"WHERE year IN ('{year_str}')")
            else:
                hints.append("NO YEAR SPECIFIED - Query ALL years")
            
            # Month filtering (v_sales has 'month' column)
            if entities.get('months'):
                # Force everything to string, no leading zeros
                months = [str(int(str(m))) if str(m).isdigit() else str(m) 
                        for m in entities['months']]
                month_list = ','.join([f"'{m}'" for m in months])
                hints.append(f"AND month IN ({month_list})")
                hints.append("AND month IS NOT NULL")
        
        elif target_table == 'v_work_force':
            # --- v_work_force uses 'date' column ---
            
            # Year filtering via date
            if entities.get('years'):
                year = entities['years'][0]
                
                # Month-specific filtering
                if entities.get('months'):
                    months = entities['months']
                    
                    if len(months) == 1:
                        month = int(months[0])
                        last_day = self._get_last_day_of_month(int(year), month)
                        hints.append(f"WHERE date::date BETWEEN '{year}-{month:02d}-01' "
                                f"AND '{year}-{month:02d}-{last_day:02d}'")
                    else:
                        min_month = min(int(m) for m in months)
                        max_month = max(int(m) for m in months)
                        last_day = self._get_last_day_of_month(int(year), max_month)
                        hints.append(f"WHERE date::date BETWEEN '{year}-{min_month:02d}-01' "
                                f"AND '{year}-{max_month:02d}-{last_day:02d}'")
                else:
                    # Full year
                    hints.append(f"WHERE EXTRACT(YEAR FROM date::date) = {year}")
            else:
                hints.append("NO YEAR FILTER - Show all records")
        
        elif target_table == 'v_spare_part':
            # v_spare_part doesn't have date/year/month columns
            hints.append("Note: v_spare_part has no time-based columns")
        
        # === CUSTOMER FILTERING (all tables) ===
        if entities.get('customers'):
            customer = entities['customers'][0]
            
            # Filter out Thai phrases that aren't customer names
            excluded_words = [
                'ที่', 'ทั้งหมด', 'กี่ราย', 'ที่ใช้บริการ', 
                'มากที่สุด', 'บ่อยที่สุด', 'ซื้อมากี่ครั้ง',
                'ทุกราย', 'แต่ละราย', 'หลายราย', 'บางราย'
            ]
            
            is_thai_phrase = any(word in customer for word in excluded_words)
            
            if not is_thai_phrase and len(customer) > 2:
                if target_table == 'v_sales':
                    hints.append(f"AND customer_name LIKE '%{customer}%'")
                elif target_table == 'v_work_force':
                    hints.append(f"AND customer LIKE '%{customer}%'")
            else:
                logger.warning(f"Ignoring invalid customer filter: '{customer}'")
        
        # === PRODUCT FILTERING (v_spare_part only) ===
        if entities.get('products') and target_table == 'v_spare_part':
            product = entities['products'][0]
            hints.append(f"WHERE product_name LIKE '%{product}%'")
        
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