"""
Real-time Data Cleaning Module for Siamtemp HVAC Chatbot
=========================================================
This module integrates with the chatbot to clean data in real-time
before processing queries, without modifying the original database.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger(__name__)

class RealTimeDataCleaner:
    """Clean data in real-time during query processing"""
    
    def __init__(self):
        # Thai encoding fix mappings
        self.encoding_fixes = {
            'à¸': 'Thai_',  # Common mojibake pattern
            'à¹': 'Thai_',
            'Ã': '',
            'â': '',
        }
        
        # Revenue fields that should be numeric
        self.numeric_fields = {
            'overhaul_', 'replacement', 'service_contact_',
            'parts_all_', 'product_all', 'solution_',
            'unit_price', 'balance'
        }
        
        # Boolean fields
        self.boolean_fields = {
            'job_description_pm', 'job_description_replacement',
            'job_description_overhaul', 'job_description_start_up',
            'job_description_support_all', 'job_description_cpa',
            'success', 'unsuccessful', 'report_kpi_2_days',
            'report_over_kpi_2_days'
        }
        
        # Cache for cleaned data
        self.cache = {}
        self.cache_size = 1000
        
        logger.info("🧹 Real-time Data Cleaner initialized")
    
    def clean_query_results(self, results: List[Dict], query: str = None) -> List[Dict]:
        """Main entry point - clean query results before sending to AI"""
        if not results:
            return results
        
        cleaned_results = []
        
        for row in results:
            cleaned_row = self.clean_row(row)
            cleaned_results.append(cleaned_row)
        
        # Log cleaning statistics
        self._log_cleaning_stats(results, cleaned_results)
        
        return cleaned_results
    
    def clean_row(self, row: Dict) -> Dict:
        """Clean a single row of data"""
        cleaned = {}
        
        for key, value in row.items():
            # Clean based on field type
            if key in self.numeric_fields:
                cleaned[key] = self.clean_numeric(value)
            elif key in self.boolean_fields:
                cleaned[key] = self.clean_boolean(value)
            else:
                cleaned[key] = self.clean_text(value)
        
        return cleaned
    
    def clean_numeric(self, value: Any) -> float:
        """Convert to numeric, handling NULL and various formats"""
        if value is None or value == 'NULL' or value == '':
            return 0.0
        
        if isinstance(value, (int, float, Decimal)):
            return float(value)
        
        if isinstance(value, str):
            # Remove common formatting
            cleaned = value.replace(',', '').replace(' ', '').strip()
            cleaned = cleaned.replace('฿', '').replace('$', '')  # Remove currency symbols
            
            if not cleaned or cleaned.lower() == 'null':
                return 0.0
            
            try:
                return float(cleaned)
            except ValueError:
                logger.debug(f"Could not convert '{value}' to numeric, using 0")
                return 0.0
        
        return 0.0
    
    def clean_boolean(self, value: Any) -> bool:
        """Convert to boolean"""
        if value is None or value == 'NULL':
            return False
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ['true', 't', 'yes', 'y', '1', 'checked']
        
        if isinstance(value, (int, float)):
            return bool(value)
        
        return False
    
    def clean_text(self, value: Any) -> str:
        """Clean text fields, fix encoding issues"""
        if value is None or value == 'NULL':
            return ''
        
        if not isinstance(value, str):
            return str(value)
        
        # Fix Thai encoding issues
        cleaned = self.fix_thai_encoding(value)
        
        # Remove extra whitespace
        cleaned = ' '.join(cleaned.split())
        
        return cleaned
    
    def fix_thai_encoding(self, text: str) -> str:
        """Fix common Thai encoding issues (mojibake)"""
        if not text:
            return text
        
        # Check for mojibake patterns
        has_mojibake = any(pattern in text for pattern in self.encoding_fixes.keys())
        
        if has_mojibake:
            try:
                # Try to decode and re-encode
                fixed = text.encode('latin-1').decode('utf-8', errors='ignore')
                
                # Verify it's actually Thai
                if self._is_thai(fixed):
                    return fixed
            except:
                pass
            
            # Fallback: remove problematic characters
            for pattern, replacement in self.encoding_fixes.items():
                text = text.replace(pattern, replacement)
        
        return text
    
    def _is_thai(self, text: str) -> bool:
        """Check if text contains Thai characters"""
        thai_pattern = re.compile(r'[\u0E00-\u0E7F]+')
        return bool(thai_pattern.search(text))
    
    def generate_safe_sql(self, base_query: str) -> str:
        """Wrap SQL query with cleaning functions"""
        # Add COALESCE and CAST for numeric fields
        safe_query = base_query
        
        # Replace numeric field references with safe versions
        for field in self.numeric_fields:
            # Pattern to find field references
            patterns = [
                (f"SUM({field})", f"SUM(COALESCE(CAST({field} AS NUMERIC), 0))"),
                (f"AVG({field})", f"AVG(COALESCE(CAST({field} AS NUMERIC), 0))"),
                (f"MAX({field})", f"MAX(COALESCE(CAST({field} AS NUMERIC), 0))"),
                (f"MIN({field})", f"MIN(COALESCE(CAST({field} AS NUMERIC), 0))"),
                (f"COUNT({field})", f"COUNT(CASE WHEN {field} IS NOT NULL THEN 1 END)"),
                (f"{field} >", f"COALESCE(CAST({field} AS NUMERIC), 0) >"),
                (f"{field} <", f"COALESCE(CAST({field} AS NUMERIC), 0) <"),
                (f"{field} =", f"COALESCE(CAST({field} AS NUMERIC), 0) ="),
                (f"{field} +", f"COALESCE(CAST({field} AS NUMERIC), 0) +"),
                (f"{field} -", f"COALESCE(CAST({field} AS NUMERIC), 0) -"),
            ]
            
            for pattern, replacement in patterns:
                safe_query = safe_query.replace(pattern, replacement)
        
        return safe_query
    
    def validate_and_clean_response(self, response: str, sql_results: List[Dict]) -> Dict:
        """Validate AI response against actual data"""
        validation = {
            'original_response': response,
            'cleaned_response': response,
            'confidence': 1.0,
            'warnings': [],
            'data_citations': []
        }
        
        # Extract numbers from response
        numbers_in_response = re.findall(r'\d+(?:,\d+)*(?:\.\d+)?', response)
        
        # Check if numbers match actual data
        for num_str in numbers_in_response:
            num = float(num_str.replace(',', ''))
            
            # Check if this number exists in results
            found = False
            for row in sql_results:
                for value in row.values():
                    if isinstance(value, (int, float)):
                        if abs(float(value) - num) < 0.01:  # Allow small float differences
                            found = True
                            validation['data_citations'].append({
                                'value': num,
                                'source': 'verified from query results'
                            })
                            break
            
            if not found and num > 100:  # Only warn for significant numbers
                validation['warnings'].append(f"Value {num_str} not found in query results")
                validation['confidence'] *= 0.9
        
        # Add warning if confidence is low
        if validation['confidence'] < 0.8:
            validation['cleaned_response'] += "\n\n⚠️ หมายเหตุ: ข้อมูลบางส่วนอาจต้องการการตรวจสอบเพิ่มเติม"
        
        return validation
    
    def _log_cleaning_stats(self, original: List[Dict], cleaned: List[Dict]):
        """Log statistics about data cleaning"""
        if not original:
            return
        
        stats = {
            'total_rows': len(original),
            'nulls_converted': 0,
            'encoding_fixed': 0,
            'numeric_converted': 0
        }
        
        for orig_row, clean_row in zip(original, cleaned):
            for key in orig_row:
                orig_val = orig_row[key]
                clean_val = clean_row[key]
                
                # Count NULL conversions
                if orig_val in [None, 'NULL'] and clean_val == 0:
                    stats['nulls_converted'] += 1
                
                # Count numeric conversions
                if key in self.numeric_fields:
                    if isinstance(orig_val, str) and isinstance(clean_val, float):
                        stats['numeric_converted'] += 1
        
        logger.info(f"🧹 Cleaning stats: {stats}")


class SmartSQLGenerator:
    """Generate SQL with built-in data cleaning"""
    
    def __init__(self):
        self.cleaner = RealTimeDataCleaner()
        
    def generate_safe_query(self, intent: str, entities: Dict, table_info: Dict) -> str:
        """Generate SQL query with automatic data cleaning"""
        
        # Base query templates with COALESCE and CAST
        templates = {
            'revenue_sum': """
                SELECT 
                    COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) as overhaul_total,
                    COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) as replacement_total,
                    COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) as service_total,
                    COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) + 
                    COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) + 
                    COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) as grand_total
                FROM {table}
                WHERE 1=1 {conditions}
            """,
            
            'customer_analysis': """
                SELECT 
                    customer_name,
                    COUNT(*) as job_count,
                    COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) as total_service,
                    COALESCE(AVG(CAST(service_contact_ AS NUMERIC)), 0) as avg_service
                FROM {table}
                WHERE customer_name IS NOT NULL {conditions}
                GROUP BY customer_name
                ORDER BY total_service DESC
                LIMIT 20
            """,
            
            'spare_parts_inventory': """
                SELECT 
                    product_code,
                    product_name,
                    COALESCE(CAST(balance AS NUMERIC), 0) as current_balance,
                    COALESCE(CAST(unit_price AS NUMERIC), 0) as price,
                    COALESCE(CAST(balance AS NUMERIC), 0) * 
                    COALESCE(CAST(unit_price AS NUMERIC), 0) as total_value
                FROM {table}
                WHERE COALESCE(CAST(balance AS NUMERIC), 0) > 0
                ORDER BY total_value DESC
            """,
            
            'work_force_analysis': """
                SELECT 
                    service_group,
                    COUNT(*) as job_count,
                    SUM(CASE WHEN job_description_pm = true THEN 1 ELSE 0 END) as pm_jobs,
                    SUM(CASE WHEN job_description_overhaul = true THEN 1 ELSE 0 END) as overhaul_jobs,
                    SUM(CASE WHEN job_description_replacement = true THEN 1 ELSE 0 END) as replacement_jobs
                FROM {table}
                WHERE service_group IS NOT NULL
                GROUP BY service_group
                ORDER BY job_count DESC
            """
        }
        
        # Select appropriate template
        if 'revenue' in intent or 'sales' in intent:
            template = templates['revenue_sum']
        elif 'customer' in intent:
            template = templates['customer_analysis']
        elif 'spare' in intent or 'part' in intent:
            template = templates['spare_parts_inventory']
        elif 'work' in intent or 'team' in intent:
            template = templates['work_force_analysis']
        else:
            # Default safe query
            template = "SELECT * FROM {table} LIMIT 10"
        
        # Build conditions
        conditions = self._build_conditions(entities)
        
        # Determine table
        table = self._determine_table(intent, entities)
        
        # Generate final query
        query = template.format(
            table=table,
            conditions=conditions
        )
        
        return query
    
    def _build_conditions(self, entities: Dict) -> str:
        """Build WHERE conditions from entities"""
        conditions = []
        
        if entities.get('years'):
            years = entities['years']
            if len(years) == 1:
                conditions.append(f"AND job_no LIKE '%{years[0][-2:]}-%'")
            else:
                year_conditions = [f"job_no LIKE '%{y[-2:]}-%'" for y in years]
                conditions.append(f"AND ({' OR '.join(year_conditions)})")
        
        if entities.get('months'):
            months = entities['months']
            month_conditions = [f"job_no LIKE '%-{m:02d}-%'" for m in months]
            conditions.append(f"AND ({' OR '.join(month_conditions)})")
        
        if entities.get('companies'):
            companies = entities['companies']
            company_conditions = [f"customer_name ILIKE '%{c}%'" for c in companies]
            conditions.append(f"AND ({' OR '.join(company_conditions)})")
        
        return ' '.join(conditions)
    
    def _determine_table(self, intent: str, entities: Dict) -> str:
        """Determine which table to query"""
        # Check for specific year
        if entities.get('years'):
            year = entities['years'][0]
            if '2022' in year:
                return 'sales2022'
            elif '2023' in year:
                return 'sales2023'
            elif '2024' in year:
                return 'sales2024'
            elif '2025' in year:
                return 'sales2025'
        
        # Check for spare parts query
        if 'spare' in intent or 'part' in intent:
            return 'spare_part'
        
        # Check for work force query
        if 'work' in intent or 'team' in intent:
            return 'work_force'
        
        # Default to current year
        return 'sales2024'


# Integration with existing chatbot
class EnhancedDataProcessor:
    """Integrate data cleaning with existing chatbot system"""
    
    def __init__(self):
        self.cleaner = RealTimeDataCleaner()
        self.sql_generator = SmartSQLGenerator()
        
    async def process_with_cleaning(self, question: str, db_handler, ollama_client) -> Dict:
        """Process question with automatic data cleaning"""
        
        try:
            # Generate safe SQL
            # This would integrate with your existing intent analysis
            sql_query = self.sql_generator.generate_safe_query(
                intent='revenue_analysis',  # From your intent analyzer
                entities={'years': ['2024']},  # From your entity extractor
                table_info={}
            )
            
            # Execute query
            raw_results = await db_handler.execute_query(sql_query)
            
            # Clean results
            cleaned_results = self.cleaner.clean_query_results(raw_results, sql_query)
            
            # Generate response (using your existing NL model)
            response = await ollama_client.generate_response(
                question=question,
                data=cleaned_results
            )
            
            # Validate response
            validation = self.cleaner.validate_and_clean_response(response, cleaned_results)
            
            return {
                'success': True,
                'answer': validation['cleaned_response'],
                'sql_query': sql_query,
                'confidence': validation['confidence'],
                'warnings': validation['warnings'],
                'results_count': len(cleaned_results)
            }
            
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            return {
                'success': False,
                'answer': f"เกิดข้อผิดพลาด: {str(e)}",
                'error': str(e)
            }


# Usage example
if __name__ == "__main__":
    # Initialize cleaner
    cleaner = RealTimeDataCleaner()
    
    # Example: Clean query results
    sample_results = [
        {
            'id': 1,
            'customer_name': 'à¸šà¸£à¸´à¸©à¸±à¸—',  # Corrupted Thai text
            'overhaul_': '100000',  # String that should be numeric
            'replacement': None,  # NULL value
            'service_contact_': 'NULL',  # String NULL
        }
    ]
    
    cleaned = cleaner.clean_query_results(sample_results)
    print("Original:", sample_results)
    print("Cleaned:", cleaned)
    
    # Example: Generate safe SQL
    generator = SmartSQLGenerator()
    safe_sql = generator.generate_safe_query(
        intent='revenue_analysis',
        entities={'years': ['2024'], 'months': [4]},
        table_info={}
    )
    print("\nSafe SQL:", safe_sql)