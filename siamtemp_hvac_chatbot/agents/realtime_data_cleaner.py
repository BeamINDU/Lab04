"""
Enhanced Real-time Data Cleaning Module for Siamtemp HVAC Chatbot
==================================================================
Version: 2.0
Enhanced with better Thai encoding fix, comprehensive field mapping,
and intelligent data type detection based on actual database structure.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, date
from decimal import Decimal
import unicodedata
from collections import defaultdict

logger = logging.getLogger(__name__)

class EnhancedRealTimeDataCleaner:
    """Enhanced data cleaner with comprehensive field handling"""
    
    def __init__(self):
        # === SALES TABLES FIELDS ===
        self.sales_numeric_fields = {
            'overhaul_', 'replacement', 'service_contact_',
            'parts_all_', 'product_all', 'solution_'
        }
        
        # === SPARE PART TABLES FIELDS ===
        self.spare_part_numeric_fields = {
            'unit', 'balance', 'unit_price', 'total', 'received'
        }
        
        # === WORK FORCE TABLE FIELDS ===
        self.work_force_boolean_fields = {
            'job_description_pm', 'job_description_replacement',
            'job_description_overhaul', 'job_description_start_up',
            'job_description_support_all', 'job_description_cpa',
            'success', 'unsuccessful', 
            'report_kpi_2_days', 'report_over_kpi_2_days'
        }
        
        # Combined numeric fields
        self.all_numeric_fields = self.sales_numeric_fields | self.spare_part_numeric_fields
        
        # Thai encoding patterns (expanded)
        self.thai_mojibake_patterns = {
            'à¸': 'ก',  # Start of Thai character range
            'à¹': 'ก',
            'Ã': '',
            'â': '',
            '€': '',
            '™': '',
            'à¸„à¸¥à¸±à¸‡à¸à¸¥à¸²à¸‡': 'คลังกลาง',
            'à¸šà¸£à¸´à¸©à¸±à¸—': 'บริษัท',
            'à¸‡à¸²à¸™': 'งาน',
            'à¹€à¸„à¸£à¸·à¹ˆà¸­à¸‡': 'เครื่อง',
            'à¸£à¸²à¸¢à¸›à¸µ': 'รายปี'
        }
        
        # Date patterns (including Excel serial numbers)
        self.date_patterns = [
            r'^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$',  # DD/MM/YYYY or DD-MM-YYYY
            r'^\d{1,2}[-/]\d{1,2}$',              # DD/MM
            r'^\d{5}$',                            # Excel serial (e.g., 45751)
            r'^\d{1,2}-\d{1,2}/\d{1,2}/\d{4}$'    # Range: 1-5/04/2025
        ]
        
        # Job number patterns
        self.job_patterns = {
            'standard': r'^(SV|JAE)\d{2}-\d{2}-\d{3}[A-Z]*(-[A-Z]+)?$',
            'legacy': r'^SV\.\d{2}-\d{2}-\d{2}\s*[A-Z]*'
        }
        
        # Cache for performance
        self.encoding_cache = {}
        self.cache_size = 1000
        
        # Statistics tracking
        self.stats = defaultdict(int)
        
        logger.info("🚀 Enhanced Real-time Data Cleaner v2.0 initialized")
    
    # =========================================================================
    # MAIN ENTRY POINTS
    # =========================================================================
    
    def clean_query_results(self, results: List[Dict], query: str = None, 
                           table_hint: str = None) -> List[Dict]:
        """
        Main entry point - intelligently clean query results
        
        Args:
            results: Raw query results
            query: Optional SQL query for context
            table_hint: Optional table name hint for better cleaning
        """
        if not results:
            return results
        
        # Detect table type from query or data structure
        table_type = self._detect_table_type(results, query, table_hint)
        
        cleaned_results = []
        for row in results:
            cleaned_row = self._clean_row_by_type(row, table_type)
            cleaned_results.append(cleaned_row)
        
        self._log_statistics()
        return cleaned_results
    
    def _detect_table_type(self, results: List[Dict], query: str = None, 
                          table_hint: str = None) -> str:
        """Detect which table type we're dealing with"""
        if table_hint:
            if 'spare' in table_hint.lower():
                return 'spare_part'
            elif 'work' in table_hint.lower():
                return 'work_force'
            elif 'sales' in table_hint.lower():
                return 'sales'
        
        if query:
            query_lower = query.lower()
            if 'spare_part' in query_lower:
                return 'spare_part'
            elif 'work_force' in query_lower:
                return 'work_force'
            elif 'sales' in query_lower:
                return 'sales'
        
        # Detect from column names
        if results and len(results) > 0:
            columns = set(results[0].keys())
            if 'product_code' in columns or 'unit_price' in columns:
                return 'spare_part'
            elif 'service_group' in columns or 'job_description_pm' in columns:
                return 'work_force'
            elif 'overhaul_' in columns or 'service_contact_' in columns:
                return 'sales'
        
        return 'generic'
    
    def _clean_row_by_type(self, row: Dict, table_type: str) -> Dict:
        """Clean row based on detected table type"""
        cleaned = {}
        
        for key, value in row.items():
            if table_type == 'sales':
                cleaned[key] = self._clean_sales_field(key, value)
            elif table_type == 'spare_part':
                cleaned[key] = self._clean_spare_part_field(key, value)
            elif table_type == 'work_force':
                cleaned[key] = self._clean_work_force_field(key, value)
            else:
                cleaned[key] = self._clean_generic_field(key, value)
        
        return cleaned
    
    # =========================================================================
    # TABLE-SPECIFIC CLEANING
    # =========================================================================
    
    def _clean_sales_field(self, field_name: str, value: Any) -> Any:
        """Clean sales table fields"""
        if field_name in self.sales_numeric_fields:
            return self._clean_numeric_value(value, field_name)
        elif field_name == 'job_no':
            return self._clean_job_number(value)
        elif field_name == 'customer_name':
            return self._fix_thai_encoding_advanced(value)
        elif field_name == 'description':
            return self._fix_thai_encoding_advanced(value)
        else:
            return self._clean_generic_field(field_name, value)
    
    def _clean_spare_part_field(self, field_name: str, value: Any) -> Any:
        """Clean spare part table fields"""
        if field_name in self.spare_part_numeric_fields:
            return self._clean_numeric_value(value, field_name)
        elif field_name == 'wh':
            # Warehouse field with Thai text
            return self._fix_thai_encoding_advanced(value)
        elif field_name in ['product_code', 'product_name', 'description']:
            return self._fix_thai_encoding_advanced(value)
        else:
            return self._clean_generic_field(field_name, value)
    
    def _clean_work_force_field(self, field_name: str, value: Any) -> Any:
        """Clean work force table fields"""
        if field_name in self.work_force_boolean_fields:
            return self._clean_boolean_value(value, field_name)
        elif field_name == 'date':
            return self._clean_date_field(value)
        elif field_name in ['customer', 'project', 'detail', 'service_group', 'failure_reason']:
            return self._fix_thai_encoding_advanced(value)
        else:
            return self._clean_generic_field(field_name, value)
    
    def _clean_generic_field(self, field_name: str, value: Any) -> Any:
        """Generic field cleaning"""
        if value is None or value == 'NULL':
            return self._get_default_value(field_name)
        
        if isinstance(value, str):
            return self._fix_thai_encoding_advanced(value)
        
        return value
    
    # =========================================================================
    # DATA TYPE CLEANERS
    # =========================================================================
    
    def _clean_numeric_value(self, value: Any, field_name: str) -> float:
        """Enhanced numeric cleaning with better error handling"""
        self.stats['numeric_processed'] += 1
        
        if value is None or value == 'NULL' or value == '':
            self.stats['nulls_converted'] += 1
            return 0.0
        
        if isinstance(value, (int, float, Decimal)):
            return float(value)
        
        if isinstance(value, str):
            # Remove all formatting
            cleaned = re.sub(r'[^\d.-]', '', value)
            
            if not cleaned or cleaned == '-':
                self.stats['nulls_converted'] += 1
                return 0.0
            
            try:
                result = float(cleaned)
                self.stats['numeric_converted'] += 1
                return result
            except ValueError:
                logger.debug(f"Failed to convert '{value}' to numeric in {field_name}")
                self.stats['conversion_errors'] += 1
                return 0.0
        
        return 0.0
    
    def _clean_boolean_value(self, value: Any, field_name: str) -> bool:
        """Enhanced boolean cleaning"""
        self.stats['boolean_processed'] += 1
        
        if value is None or value == 'NULL':
            return False
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            value_lower = value.lower().strip()
            return value_lower in ['true', 't', 'yes', 'y', '1', 'checked', 'x']
        
        if isinstance(value, (int, float)):
            return bool(value)
        
        return False
    
    def _clean_date_field(self, value: Any) -> str:
        """Clean and standardize date fields"""
        if value is None or value == 'NULL':
            return ''
        
        value_str = str(value).strip()
        
        # Check for Excel serial number (5 digits)
        if re.match(r'^\d{5}$', value_str):
            try:
                # Excel serial number to date (Excel epoch: 1900-01-01)
                excel_date = datetime(1900, 1, 1) + timedelta(days=int(value_str) - 2)
                return excel_date.strftime('%Y-%m-%d')
            except:
                self.stats['date_conversion_errors'] += 1
                return value_str
        
        # Check for date range (e.g., "1-5/04/2025")
        if '-' in value_str and '/' in value_str:
            # Extract end date from range
            parts = value_str.split('/')
            if len(parts) >= 2:
                try:
                    # Take the end date
                    day_range = parts[0].split('-')[-1]
                    month = parts[1] if len(parts) == 3 else parts[0].split('-')[0]
                    year = parts[2] if len(parts) == 3 else parts[1]
                    return f"{year}-{month.zfill(2)}-{day_range.zfill(2)}"
                except:
                    pass
        
        # Standard date formats
        for pattern in [r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', r'(\d{1,2})[/-](\d{1,2})']:
            match = re.match(pattern, value_str)
            if match:
                try:
                    if len(match.groups()) == 3:
                        day, month, year = match.groups()
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    elif len(match.groups()) == 2:
                        day, month = match.groups()
                        year = datetime.now().year
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except:
                    pass
        
        return value_str
    
    def _clean_job_number(self, value: Any) -> str:
        """Clean and standardize job numbers"""
        if value is None or value == 'NULL':
            return ''
        
        job_str = str(value).strip().upper()
        
        # Fix common patterns
        job_str = job_str.replace('SV.', 'SV')
        job_str = re.sub(r'\s+', '', job_str)  # Remove spaces
        
        return job_str
    
    # =========================================================================
    # THAI ENCODING FIX
    # =========================================================================
    
    def _fix_thai_encoding_advanced(self, text: Any) -> str:
        """Advanced Thai encoding fix with caching"""
        if text is None or text == 'NULL':
            return ''
        
        if not isinstance(text, str):
            return str(text)
        
        # Check cache
        if text in self.encoding_cache:
            self.stats['cache_hits'] += 1
            return self.encoding_cache[text]
        
        # Clean whitespace first
        cleaned = ' '.join(text.split())
        
        # Check for known mojibake patterns
        if self._has_mojibake(cleaned):
            fixed = self._fix_mojibake(cleaned)
            if fixed != cleaned:
                self.stats['encoding_fixed'] += 1
                self._update_cache(text, fixed)
                return fixed
        
        # Cache even if no fix needed
        self._update_cache(text, cleaned)
        return cleaned
    
    def _has_mojibake(self, text: str) -> bool:
        """Check if text has mojibake patterns"""
        return any(pattern in text for pattern in self.thai_mojibake_patterns.keys())
    
    def _fix_mojibake(self, text: str) -> str:
        """Fix mojibake with multiple strategies"""
        # Strategy 1: Try direct pattern replacement for known phrases
        for pattern, replacement in self.thai_mojibake_patterns.items():
            if pattern in text:
                text = text.replace(pattern, replacement)
        
        # Strategy 2: Try encoding/decoding if still has issues
        if 'à' in text or 'Ã' in text:
            try:
                # Most common: UTF-8 interpreted as Latin-1
                fixed = text.encode('latin-1').decode('utf-8', errors='ignore')
                if self._is_valid_thai(fixed):
                    return fixed
            except:
                pass
            
            try:
                # Alternative: Windows-1252 to UTF-8
                fixed = text.encode('windows-1252').decode('utf-8', errors='ignore')
                if self._is_valid_thai(fixed):
                    return fixed
            except:
                pass
        
        return text
    
    def _is_valid_thai(self, text: str) -> bool:
        """Check if text contains valid Thai characters"""
        thai_chars = 0
        total_chars = 0
        
        for char in text:
            if char.strip():
                total_chars += 1
                if '\u0E00' <= char <= '\u0E7F':
                    thai_chars += 1
        
        # If more than 30% Thai characters, consider it Thai text
        return thai_chars > 0 and (thai_chars / max(total_chars, 1)) > 0.3
    
    def _update_cache(self, key: str, value: str):
        """Update cache with size limit"""
        if len(self.encoding_cache) >= self.cache_size:
            # Remove oldest entries (simple FIFO)
            remove_count = self.cache_size // 4
            for _ in range(remove_count):
                self.encoding_cache.pop(next(iter(self.encoding_cache)))
        
        self.encoding_cache[key] = value
    
    def _get_default_value(self, field_name: str) -> Any:
        """Get appropriate default value for field"""
        if field_name in self.all_numeric_fields:
            return 0.0
        elif field_name in self.work_force_boolean_fields:
            return False
        else:
            return ''
    
    # =========================================================================
    # SQL GENERATION
    # =========================================================================
    
    def generate_safe_sql(self, intent: str, entities: Dict, 
                         table_info: Dict = None) -> str:
        """Generate SQL with automatic cleaning built-in"""
        generator = EnhancedSQLGenerator(self)
        return generator.generate(intent, entities, table_info)
    
    # =========================================================================
    # STATISTICS AND LOGGING
    # =========================================================================
    
    def _log_statistics(self):
        """Log cleaning statistics"""
        if self.stats:
            logger.info(f"🧹 Cleaning stats: {dict(self.stats)}")
    
    def get_statistics(self) -> Dict[str, int]:
        """Get current statistics"""
        return dict(self.stats)
    
    def reset_statistics(self):
        """Reset statistics"""
        self.stats.clear()
        logger.info("📊 Statistics reset")


class EnhancedSQLGenerator:
    """Generate safe SQL queries with built-in data cleaning"""
    
    def __init__(self, cleaner: EnhancedRealTimeDataCleaner):
        self.cleaner = cleaner
        
        # Enhanced templates with proper handling for all table types
        self.templates = {
            # Sales queries
            'revenue_sum': """
                SELECT 
                    COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as overhaul_total,
                    COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as replacement_total,
                    COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as service_total,
                    COALESCE(SUM(CAST(NULLIF(parts_all_, '') AS NUMERIC)), 0) as parts_total,
                    COALESCE(SUM(CAST(NULLIF(product_all, '') AS NUMERIC)), 0) as product_total,
                    COALESCE(SUM(
                        CAST(NULLIF(overhaul_, '') AS NUMERIC) + 
                        CAST(NULLIF(replacement, '') AS NUMERIC) + 
                        CAST(NULLIF(service_contact_, '') AS NUMERIC) +
                        CAST(NULLIF(parts_all_, '') AS NUMERIC) +
                        CAST(NULLIF(product_all, '') AS NUMERIC)
                    ), 0) as grand_total
                FROM {table}
                WHERE 1=1 {conditions}
            """,
            
            'customer_revenue': """
                SELECT 
                    customer_name,
                    COUNT(*) as job_count,
                    COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as total_service,
                    COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as total_replacement,
                    COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as total_overhaul,
                    COALESCE(SUM(
                        CAST(NULLIF(service_contact_, '') AS NUMERIC) +
                        CAST(NULLIF(replacement, '') AS NUMERIC) +
                        CAST(NULLIF(overhaul_, '') AS NUMERIC)
                    ), 0) as total_revenue
                FROM {table}
                WHERE customer_name IS NOT NULL 
                    AND customer_name != '' {conditions}
                GROUP BY customer_name
                ORDER BY total_revenue DESC
                LIMIT 20
            """,
            
            # Spare parts queries
            'inventory_value': """
                SELECT 
                    wh as warehouse,
                    COUNT(*) as item_count,
                    COALESCE(SUM(CAST(NULLIF(balance::text, '') AS NUMERIC)), 0) as total_units,
                    COALESCE(SUM(
                        CAST(NULLIF(balance::text, '') AS NUMERIC) * 
                        CAST(NULLIF(unit_price::text, '') AS NUMERIC)
                    ), 0) as total_value
                FROM {table}
                WHERE 1=1 {conditions}
                GROUP BY wh
                ORDER BY total_value DESC
            """,
            
            'spare_parts_list': """
                SELECT 
                    product_code,
                    product_name,
                    CAST(NULLIF(balance::text, '') AS NUMERIC) as current_balance,
                    CAST(NULLIF(unit_price::text, '') AS NUMERIC) as price_per_unit,
                    CAST(NULLIF(balance::text, '') AS NUMERIC) * 
                    CAST(NULLIF(unit_price::text, '') AS NUMERIC) as total_value
                FROM {table}
                WHERE CAST(NULLIF(balance::text, '') AS NUMERIC) > 0 {conditions}
                ORDER BY total_value DESC
                LIMIT 50
            """,
            
            # Work force queries
            'work_summary': """
                SELECT 
                    service_group,
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN job_description_pm = true THEN 1 ELSE 0 END) as pm_jobs,
                    SUM(CASE WHEN job_description_replacement = true THEN 1 ELSE 0 END) as replacement_jobs,
                    SUM(CASE WHEN job_description_overhaul = true THEN 1 ELSE 0 END) as overhaul_jobs,
                    SUM(CASE WHEN success = true THEN 1 ELSE 0 END) as successful_jobs,
                    SUM(CASE WHEN report_kpi_2_days = true THEN 1 ELSE 0 END) as within_kpi
                FROM {table}
                WHERE service_group IS NOT NULL {conditions}
                GROUP BY service_group
                ORDER BY total_jobs DESC
            """,
            
            'customer_work': """
                SELECT 
                    customer,
                    COUNT(*) as job_count,
                    COUNT(DISTINCT service_group) as teams_involved,
                    STRING_AGG(DISTINCT detail, ', ') as work_details
                FROM {table}
                WHERE customer IS NOT NULL {conditions}
                GROUP BY customer
                ORDER BY job_count DESC
                LIMIT 20
            """
        }
    
    def generate(self, intent: str, entities: Dict, table_info: Dict = None) -> str:
        """Generate appropriate SQL based on intent and entities"""
        # Determine template
        template = self._select_template(intent)
        
        # Determine table
        table = self._determine_table(intent, entities)
        
        # Build conditions
        conditions = self._build_conditions(entities, table)
        
        # Generate SQL
        sql = template.format(table=table, conditions=conditions)
        
        return sql
    
    def _select_template(self, intent: str) -> str:
        """Select appropriate SQL template"""
        intent_lower = intent.lower()
        
        # Sales related
        if any(word in intent_lower for word in ['revenue', 'sales', 'income']):
            if 'customer' in intent_lower:
                return self.templates['customer_revenue']
            return self.templates['revenue_sum']
        
        # Spare parts related
        elif any(word in intent_lower for word in ['spare', 'part', 'inventory', 'stock']):
            if 'value' in intent_lower or 'total' in intent_lower:
                return self.templates['inventory_value']
            return self.templates['spare_parts_list']
        
        # Work force related
        elif any(word in intent_lower for word in ['work', 'team', 'service', 'technician']):
            if 'customer' in intent_lower:
                return self.templates['customer_work']
            return self.templates['work_summary']
        
        # Default
        return self.templates['revenue_sum']
    
    def _determine_table(self, intent: str, entities: Dict) -> str:
        """Determine which table to query"""
        intent_lower = intent.lower()
        
        # Check for explicit table mention
        if 'spare' in intent_lower or 'part' in intent_lower:
            return 'spare_part'
        elif 'work' in intent_lower or 'team' in intent_lower:
            return 'work_force'
        
        # Check for year in entities
        if entities.get('years'):
            year = str(entities['years'][0])
            if '2022' in year:
                return 'sales2022'
            elif '2023' in year:
                return 'sales2023'
            elif '2024' in year:
                return 'sales2024'
            elif '2025' in year:
                return 'sales2025'
        
        # Default to current year
        return 'sales2024'
    
    def _build_conditions(self, entities: Dict, table: str) -> str:
        """Build WHERE conditions from entities"""
        conditions = []
        
        # Year conditions (for sales tables)
        if 'sales' in table and entities.get('years'):
            years = entities['years']
            year_conditions = []
            for year in years:
                year_short = str(year)[-2:]
                year_conditions.append(f"job_no LIKE '%{year_short}-%'")
            if year_conditions:
                conditions.append(f"AND ({' OR '.join(year_conditions)})")
        
        # Month conditions
        if entities.get('months'):
            months = entities['months']
            month_conditions = []
            for month in months:
                month_conditions.append(f"job_no LIKE '%-{month:02d}-%'")
            if month_conditions:
                conditions.append(f"AND ({' OR '.join(month_conditions)})")
        
        # Customer conditions
        if entities.get('companies'):
            companies = entities['companies']
            customer_conditions = []
            for company in companies:
                # Handle both 'customer_name' and 'customer' fields
                if 'sales' in table:
                    customer_conditions.append(f"customer_name ILIKE '%{company}%'")
                elif 'work' in table:
                    customer_conditions.append(f"customer ILIKE '%{company}%'")
            if customer_conditions:
                conditions.append(f"AND ({' OR '.join(customer_conditions)})")
        
        # Date range conditions (for work_force)
        if 'work' in table and entities.get('date_range'):
            date_range = entities['date_range']
            if 'start' in date_range:
                conditions.append(f"AND date >= '{date_range['start']}'")
            if 'end' in date_range:
                conditions.append(f"AND date <= '{date_range['end']}'")
        
        # Product conditions (for spare_part)
        if 'spare' in table and entities.get('products'):
            products = entities['products']
            product_conditions = []
            for product in products:
                product_conditions.append(
                    f"(product_code ILIKE '%{product}%' OR product_name ILIKE '%{product}%')"
                )
            if product_conditions:
                conditions.append(f"AND ({' OR '.join(product_conditions)})")
        
        return ' '.join(conditions)


# =========================================================================
# VALIDATION AND RESPONSE CLEANING
# =========================================================================

class ResponseValidator:
    """Validate and clean AI responses"""
    
    def __init__(self, cleaner: EnhancedRealTimeDataCleaner):
        self.cleaner = cleaner
    
    def validate_response(self, response: str, sql_results: List[Dict]) -> Dict:
        """Validate AI response against actual data"""
        validation = {
            'is_valid': True,
            'confidence': 1.0,
            'warnings': [],
            'corrections': [],
            'cleaned_response': response
        }
        
        # Extract numbers from response
        numbers_in_response = re.findall(r'[\d,]+\.?\d*', response)
        
        # Check if numbers match data
        for num_str in numbers_in_response:
            num = float(num_str.replace(',', ''))
            
            # Check if this number exists in results
            found = False
            for row in sql_results:
                for value in row.values():
                    if isinstance(value, (int, float)):
                        if abs(float(value) - num) < 0.01:
                            found = True
                            break
            
            if not found and num > 100:  # Only check significant numbers
                validation['warnings'].append(f"Value {num_str} not found in query results")
                validation['confidence'] *= 0.9
        
        # Add warning if confidence is low
        if validation['confidence'] < 0.8:
            validation['cleaned_response'] += "\n\n⚠️ หมายเหตุ: ข้อมูลบางส่วนอาจต้องการการตรวจสอบเพิ่มเติม"
        
        return validation


# =========================================================================
# USAGE EXAMPLES
# =========================================================================

if __name__ == "__main__":
    from datetime import timedelta
    
    # Initialize cleaner
    cleaner = EnhancedRealTimeDataCleaner()
    
    # Example 1: Clean sales data with Thai encoding issues
    print("=" * 60)
    print("Example 1: Sales Data Cleaning")
    print("=" * 60)
    
    sales_data = [
        {
            'id': 1,
            'job_no': 'SV.67-01-03 S - PM',
            'customer_name': 'à¸à¸²à¸£à¹„à¸Ÿà¸Ÿà¹‰à¸² à¸à¹ˆà¸²à¸¢à¸œà¸¥à¸´à¸•',
            'description': 'à¸‡à¸²à¸™à¸šà¸³à¸£à¸¸à¸‡à¸£à¸±à¸à¸©à¸² à¸£à¸²à¸¢à¸›à¸µ',
            'service_contact_': 'NULL',
            'replacement': '100,000',
            'overhaul_': None
        }
    ]
    
    cleaned_sales = cleaner.clean_query_results(sales_data, table_hint='sales')
    print("Original:", sales_data[0])
    print("Cleaned:", cleaned_sales[0])
    print()
    
    # Example 2: Clean spare parts data
    print("=" * 60)
    print("Example 2: Spare Parts Data Cleaning")
    print("=" * 60)
    
    spare_data = [
        {
            'id': 1,
            'wh': '00 à¸„à¸¥à¸±à¸‡à¸à¸¥à¸²à¸‡',
            'product_code': 'AC03900203',
            'product_name': 'MOTOR Y132S-40,5.5KW',
            'balance': '10',
            'unit_price': '14,900',
            'total': '149000'
        }
    ]
    
    cleaned_spare = cleaner.clean_query_results(spare_data, table_hint='spare_part')
    print("Original:", spare_data[0])
    print("Cleaned:", cleaned_spare[0])
    print()
    
    # Example 3: Clean work force data
    print("=" * 60)
    print("Example 3: Work Force Data Cleaning")
    print("=" * 60)
    
    work_data = [
        {
            'id': 1,
            'date': '45751',  # Excel serial number
            'customer': 'à¸šà¸£à¸´à¸©à¸±à¸—à¸­à¸£à¸¸à¸"à¸ªà¸§à¸±à¸ªà¸"à¸´à¹Œ',
            'service_group': 'อนุรัก เปรม ทวิชชัย',
            'job_description_pm': 'true',
            'job_description_replacement': None,
            'success': 'NULL'
        }
    ]
    
    cleaned_work = cleaner.clean_query_results(work_data, table_hint='work_force')
    print("Original:", work_data[0])
    print("Cleaned:", cleaned_work[0])
    print()
    
    # Example 4: Generate safe SQL
    print("=" * 60)
    print("Example 4: Safe SQL Generation")
    print("=" * 60)
    
    sql = cleaner.generate_safe_sql(
        intent='customer_revenue',
        entities={
            'years': [2024],
            'companies': ['AGC', 'Honda']
        }
    )
    print("Generated SQL:")
    print(sql)
    print()
    
    # Show statistics
    print("=" * 60)
    print("Cleaning Statistics:")
    print("=" * 60)
    print(cleaner.get_statistics())