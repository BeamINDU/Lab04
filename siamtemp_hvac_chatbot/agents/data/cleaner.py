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

class DataCleaningEngine:
    """
    Enhanced Data Cleaning Engine - ปรับปรุงคุณภาพข้อมูลและ response
    - Thai encoding fixes
    - Date normalization  
    - Response data enhancement
    - Summary generation for better insights
    """
    
    def __init__(self):
        # =================================================================
        # THAI ENCODING FIXES (ขยายจากเดิม)
        # =================================================================
        self.thai_encoding_fixes = {
            # Unicode issues
            'à¸': '',
            'Ã': '',
            'â€': '',
            'â€™': "'",
            'â€œ': '"',
            'â€�': '"',
            
            # Common encoding problems
            'à¸„à¸¥à¸µà¸™à¸´à¸„à¸›à¸£à¸°à¸à¸­à¸šà¹‚à¸£à¸„à¸¨à¸´à¸¥à¸›à¹Œà¸¯': 'คลีนิคประกอบโรคศิลป์',
            'à¸šà¸£à¸´à¸©à¸±à¸—': 'บริษัท',
            'à¸ˆà¸³à¸à¸±à¸"': 'จำกัด',
            'à¹à¸¥à¸°': 'และ',
            'à¸‡à¸²à¸™': 'งาน',
            
            # HTML entities
            '&amp;': '&',
            '&lt;': '<',
            '&gt;': '>',
            '&quot;': '"',
            '&#39;': "'",
        }

        # =================================================================
        # DATE FORMATS AND PATTERNS
        # =================================================================
        self.date_patterns = [
            # Standard formats
            (r'^(\d{4})-(\d{2})-(\d{2})$', '%Y-%m-%d'),  # YYYY-MM-DD (preferred)
            (r'^(\d{1,2})/(\d{1,2})/(\d{4})$', '%d/%m/%Y'),  # DD/MM/YYYY
            (r'^(\d{1,2})-(\d{1,2})-(\d{4})$', '%d-%m-%Y'),  # DD-MM-YYYY
            (r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$', '%d.%m.%Y'),  # DD.MM.YYYY
            
            # Excel serial numbers (approximate)
            (r'^4\d{4}$', 'excel_serial'),  # Excel dates start around 44000+
        ]

        # =================================================================
        # BUSINESS TERMS FOR CLEANING
        # =================================================================
        self.business_term_fixes = {
            # Service types
            'PM': 'Preventive Maintenance',
            'pm': 'Preventive Maintenance', 
            'OVERHAUL': 'Overhaul',
            'overhaul': 'Overhaul',
            'REPLACEMENT': 'Replacement',
            'replacement': 'Replacement',
            
            # Equipment
            'CH': 'Chiller',
            'AHU': 'Air Handling Unit',
            'FCU': 'Fan Coil Unit',
            'VRF': 'Variable Refrigerant Flow',
            
            # Common abbreviations
            'CO.,LTD.': 'Company Limited',
            'LTD.': 'Limited',
            'INC.': 'Incorporated',
        }

        # =================================================================
        # RESPONSE ENHANCEMENT TEMPLATES
        # =================================================================
        self.response_templates = {
            'work_force': {
                'summary_fields': ['customer', 'job_type', 'service_group'],
                'count_field': 'id',
                'status_fields': ['success', 'unsuccessful'],
                'date_field': 'date'
            },
            'sales': {
                'summary_fields': ['customer_name'],
                'count_field': 'id', 
                'amount_fields': ['overhaul_num', 'replacement_num', 'service_num', 'total_revenue'],
                'date_field': 'job_no'  # Contains date info
            },
            'spare_parts': {
                'summary_fields': ['wh', 'product_name'],
                'count_field': 'id',
                'amount_fields': ['balance_num', 'unit_price_num', 'total_num'],
                'key_field': 'product_code'
            }
        }

    # =================================================================
    # MAIN CLEANING METHODS
    # =================================================================
    
    def clean_results(self, results: List[Dict], intent: str = None) -> Tuple[List[Dict], Dict]:
        """
        Main cleaning method with enhanced capabilities
        """
        if not results:
            return results, {'cleaned': 0}
        
        cleaned_results = []
        stats = {
            'cleaned': 0,
            'null_values': 0,
            'dates_parsed': 0,
            'encoding_fixed': 0,
            'business_terms_fixed': 0,
            'total_rows': len(results)
        }
        
        for row in results:
            cleaned_row = self._clean_single_row(row, stats)
            cleaned_results.append(cleaned_row)
        
        # Enhanced processing based on intent
        if intent:
            cleaned_results = self._enhance_results_by_intent(cleaned_results, intent)
            stats['intent_enhancement'] = True
        
        return cleaned_results, stats
    
    def _clean_single_row(self, row: Dict, stats: Dict) -> Dict:
        """Clean a single data row"""
        cleaned_row = {}
        
        for key, value in row.items():
            original_value = value
            
            # 1. Handle NULL values
            if value is None or value == 'NULL' or value == '':
                value = None
                stats['null_values'] += 1
            
            # 2. Clean strings
            elif isinstance(value, str):
                # Fix encoding issues
                value = self._fix_thai_encoding(value)
                if value != original_value:
                    stats['encoding_fixed'] += 1
                
                # Parse dates if it's a date field
                if self._is_date_field(key):
                    parsed_date = self._parse_date_field(value)
                    if parsed_date != value:
                        value = parsed_date
                        stats['dates_parsed'] += 1
                
                # Fix business terms
                fixed_term = self._fix_business_terms(value)
                if fixed_term != value:
                    value = fixed_term
                    stats['business_terms_fixed'] += 1
            
            # 3. Clean numeric values
            elif self._is_numeric_field(key):
                value = self._clean_numeric_value(value)
                if value != original_value:
                    stats['cleaned'] += 1
            
            cleaned_row[key] = value
        
        return cleaned_row
    
    def _fix_thai_encoding(self, text: str) -> str:
        """Fix Thai encoding issues"""
        if not isinstance(text, str):
            return text
        
        fixed_text = text
        for broken, correct in self.thai_encoding_fixes.items():
            if broken in fixed_text:
                fixed_text = fixed_text.replace(broken, correct)
        
        # Additional cleanup
        fixed_text = re.sub(r'\s+', ' ', fixed_text).strip()
        
        return fixed_text
    
    def _parse_date_field(self, date_str: str) -> str:
        """Parse and normalize date fields"""
        if not date_str or not isinstance(date_str, str):
            return date_str
        
        # Already in YYYY-MM-DD format
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
        
        # Try other patterns
        for pattern, format_str in self.date_patterns:
            if re.match(pattern, date_str):
                try:
                    if format_str == 'excel_serial':
                        # Rough Excel serial to date conversion
                        excel_serial = int(date_str)
                        # Excel epoch starts 1900-01-01, but has leap year bug
                        days_since_epoch = excel_serial - 25569  # Adjust for Unix epoch
                        timestamp = days_since_epoch * 86400  # Convert to seconds
                        dt = datetime.fromtimestamp(timestamp)
                        return dt.strftime('%Y-%m-%d')
                    else:
                        # Standard date parsing
                        dt = datetime.strptime(date_str, format_str)
                        return dt.strftime('%Y-%m-%d')
                except:
                    continue
        
        return date_str  # Return original if can't parse
    
    def _fix_business_terms(self, text: str) -> str:
        """Fix business terminology"""
        if not isinstance(text, str):
            return text
        
        fixed_text = text
        for term, replacement in self.business_term_fixes.items():
            # Case-insensitive replacement but preserve case
            if term.lower() in fixed_text.lower():
                pattern = re.compile(re.escape(term), re.IGNORECASE)
                fixed_text = pattern.sub(replacement, fixed_text)
        
        return fixed_text
    
    def _clean_numeric_value(self, value: Any) -> Any:
        """Clean numeric values"""
        if value is None:
            return 0
        
        try:
            if isinstance(value, str):
                # Remove commas and convert
                cleaned = re.sub(r'[,\s]', '', value)
                return float(cleaned) if '.' in cleaned else int(cleaned)
            else:
                return float(value)
        except:
            return 0

    # =================================================================
    # INTENT-BASED ENHANCEMENT
    # =================================================================
    
    def _enhance_results_by_intent(self, results: List[Dict], intent: str) -> List[Dict]:
        """Enhance results based on intent for better response generation"""
        
        if intent in ['work_force', 'work_plan', 'work_summary']:
            return self._enhance_work_force_data(results)
        elif intent in ['sales', 'sales_analysis', 'overhaul_report']:
            return self._enhance_sales_data(results)
        elif intent in ['spare_parts', 'parts_price', 'inventory_value']:
            return self._enhance_spare_parts_data(results)
        elif intent in ['customer_history', 'top_customers']:
            return self._enhance_customer_data(results)
        
        return results
    
    def _enhance_work_force_data(self, results: List[Dict]) -> List[Dict]:
        """Enhance work force data for better insights"""
        enhanced_results = []
        
        for row in results:
            enhanced_row = row.copy()
            
            # Add job_type if not present (from job_description fields)
            if 'job_type' not in enhanced_row or not enhanced_row['job_type']:
                enhanced_row['job_type'] = self._derive_job_type(row)
            
            # Add status summary
            enhanced_row['status'] = self._get_work_status(row)
            
            # Clean customer names
            if 'customer' in enhanced_row:
                enhanced_row['customer'] = self._clean_customer_name(enhanced_row['customer'])
            
            enhanced_results.append(enhanced_row)
        
        return enhanced_results
    
    def _enhance_sales_data(self, results: List[Dict]) -> List[Dict]:
        """Enhance sales data"""
        enhanced_results = []
        
        for row in results:
            enhanced_row = row.copy()
            
            # Add service type breakdown
            enhanced_row['primary_service'] = self._get_primary_service(row)
            
            # Clean customer names
            if 'customer_name' in enhanced_row:
                enhanced_row['customer_name'] = self._clean_customer_name(enhanced_row['customer_name'])
            
            # Add year info if job_no contains date
            if 'job_no' in enhanced_row:
                enhanced_row['year'] = self._extract_year_from_job_no(enhanced_row['job_no'])
            
            enhanced_results.append(enhanced_row)
        
        return enhanced_results
    
    def _enhance_spare_parts_data(self, results: List[Dict]) -> List[Dict]:
        """Enhance spare parts data"""
        enhanced_results = []
        
        for row in results:
            enhanced_row = row.copy()
            
            # Add availability status
            enhanced_row['availability'] = self._get_part_availability(row)
            
            # Clean product names
            if 'product_name' in enhanced_row:
                enhanced_row['product_name'] = self._clean_product_name(enhanced_row['product_name'])
            
            # Add value category
            enhanced_row['value_category'] = self._get_value_category(row)
            
            enhanced_results.append(enhanced_row)
        
        return enhanced_results
    
    def _enhance_customer_data(self, results: List[Dict]) -> List[Dict]:
        """Enhance customer data"""
        enhanced_results = []
        
        for row in results:
            enhanced_row = row.copy()
            
            # Clean customer names
            for field in ['customer_name', 'customer']:
                if field in enhanced_row:
                    enhanced_row[field] = self._clean_customer_name(enhanced_row[field])
            
            # Add customer type
            enhanced_row['customer_type'] = self._get_customer_type(enhanced_row)
            
            enhanced_results.append(enhanced_row)
        
        return enhanced_results

    # =================================================================
    # HELPER METHODS
    # =================================================================
    
    def _is_date_field(self, field_name: str) -> bool:
        """Check if field is a date field"""
        date_field_indicators = ['date', 'วันที่', 'received', 'created', 'updated']
        return any(indicator in field_name.lower() for indicator in date_field_indicators)
    
    def _is_numeric_field(self, field_name: str) -> bool:
        """Check if field should be numeric"""
        numeric_indicators = ['_num', '_text', 'revenue', 'price', 'total', 'balance', 'amount']
        return any(indicator in field_name.lower() for indicator in numeric_indicators)
    
    def _derive_job_type(self, row: Dict) -> str:
        """Derive job type from job_description fields"""
        if row.get('job_description_pm'):
            return 'PM'
        elif row.get('job_description_replacement'):
            return 'Replacement'
        elif row.get('job_description_overhaul'):
            return 'Overhaul'
        elif row.get('job_description_start_up'):
            return 'Start Up'
        elif row.get('job_description_support_all'):
            return 'Support'
        elif row.get('job_description_cpa'):
            return 'CPA'
        else:
            return 'Other'
    
    def _get_work_status(self, row: Dict) -> str:
        """Get work status summary"""
        if row.get('success'):
            return 'Completed'
        elif row.get('unsuccessful'):
            return 'Failed'
        else:
            return 'Pending'
    
    def _get_primary_service(self, row: Dict) -> str:
        """Get primary service type from sales data"""
        services = {
            'overhaul_num': row.get('overhaul_num', 0) or 0,
            'replacement_num': row.get('replacement_num', 0) or 0,
            'service_num': row.get('service_num', 0) or 0,
            'parts_num': row.get('parts_num', 0) or 0,
            'product_num': row.get('product_num', 0) or 0,
            'solution_num': row.get('solution_num', 0) or 0
        }
        
        max_service = max(services.items(), key=lambda x: float(x[1]))
        return max_service[0].replace('_num', '') if max_service[1] > 0 else 'unknown'
    
    def _get_part_availability(self, row: Dict) -> str:
        """Get part availability status"""
        balance = row.get('balance_num', 0)
        if balance is None:
            return 'Unknown'
        elif balance > 0:
            return 'In Stock'
        else:
            return 'Out of Stock'
    
    def _get_value_category(self, row: Dict) -> str:
        """Categorize part by value"""
        total_value = row.get('total_num', 0) or 0
        
        if total_value > 50000:
            return 'High Value'
        elif total_value > 10000:
            return 'Medium Value'  
        elif total_value > 0:
            return 'Low Value'
        else:
            return 'No Value'
    
    def _get_customer_type(self, row: Dict) -> str:
        """Determine customer type"""
        customer_name = row.get('customer_name') or row.get('customer', '')
        
        if not customer_name:
            return 'Unknown'
        
        customer_lower = customer_name.lower()
        
        if any(term in customer_lower for term in ['co.', 'ltd.', 'limited', 'inc.', 'corporation']):
            return 'Corporate'
        elif 'บริษัท' in customer_name:
            return 'Thai Company'
        else:
            return 'Individual'
    
    def _clean_customer_name(self, name: str) -> str:
        """Clean customer/company names"""
        if not name or not isinstance(name, str):
            return name
        
        # Fix encoding first
        cleaned = self._fix_thai_encoding(name)
        
        # Standardize company suffixes
        cleaned = self._fix_business_terms(cleaned)
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def _clean_product_name(self, name: str) -> str:
        """Clean product names"""
        if not name or not isinstance(name, str):
            return name
        
        # Fix encoding
        cleaned = self._fix_thai_encoding(name)
        
        # Fix common product name issues
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Standardize model numbers
        cleaned = re.sub(r'model\s*:?\s*', 'Model ', cleaned, flags=re.IGNORECASE)
        
        return cleaned
    
    def _extract_year_from_job_no(self, job_no: str) -> Optional[int]:
        """Extract year from job number"""
        if not job_no:
            return None
        
        # Look for year patterns in job number
        year_patterns = [
            r'(\d{4})',  # 4-digit year
            r'/(\d{2})',  # 2-digit year
            r'-(\d{2})'   # 2-digit year with dash
        ]
        
        for pattern in year_patterns:
            matches = re.findall(pattern, job_no)
            for match in matches:
                year = int(match)
                if len(match) == 2:
                    # Convert 2-digit to 4-digit
                    if year > 50:  # Assume 1950-1999
                        year += 1900
                    else:  # Assume 2000-2049
                        year += 2000
                
                if 2020 <= year <= 2030:
                    return year
        
        return None

    # =================================================================
    # RESPONSE ENHANCEMENT FOR BETTER INSIGHTS
    # =================================================================
    
    def create_summary_insights(self, results: List[Dict], intent: str) -> Dict[str, Any]:
        """
        Create summary insights for better response generation
        """
        if not results:
            return {'total_count': 0, 'summary': 'No data found'}
        
        insights = {
            'total_count': len(results),
            'summary': {},
            'top_items': {},
            'statistics': {}
        }
        
        if intent in ['work_force', 'work_plan', 'work_summary']:
            insights.update(self._create_work_force_insights(results))
        elif intent in ['sales', 'sales_analysis', 'overhaul_report']:
            insights.update(self._create_sales_insights(results))
        elif intent in ['spare_parts', 'parts_price', 'inventory_value']:
            insights.update(self._create_spare_parts_insights(results))
        elif intent in ['customer_history', 'top_customers']:
            insights.update(self._create_customer_insights(results))
        
        return insights
    
    def _create_work_force_insights(self, results: List[Dict]) -> Dict[str, Any]:
        """Create work force specific insights"""
        job_types = Counter()
        customers = Counter()
        statuses = Counter()
        teams = Counter()
        
        for row in results:
            # Count job types
            job_type = row.get('job_type') or self._derive_job_type(row)
            job_types[job_type] += 1
            
            # Count customers
            customer = self._clean_customer_name(row.get('customer', ''))
            if customer:
                customers[customer] += 1
            
            # Count statuses
            status = self._get_work_status(row)
            statuses[status] += 1
            
            # Count teams
            team = row.get('service_group', '')
            if team:
                teams[team] += 1
        
        return {
            'summary': {
                'job_types': dict(job_types.most_common(5)),
                'statuses': dict(statuses),
            },
            'top_items': {
                'customers': dict(customers.most_common(5)),
                'teams': dict(teams.most_common(3))
            },
            'statistics': {
                'success_rate': statuses.get('Completed', 0) / len(results) * 100 if results else 0
            }
        }
    
    def _create_sales_insights(self, results: List[Dict]) -> Dict[str, Any]:
        """Create sales specific insights"""
        customers = Counter()
        service_types = Counter()
        total_revenue = 0
        yearly_data = defaultdict(float)
        
        for row in results:
            # Count customers and revenue
            customer = self._clean_customer_name(row.get('customer_name', ''))
            revenue = float(row.get('total_revenue', 0) or 0)
            
            if customer:
                customers[customer] += revenue
            
            total_revenue += revenue
            
            # Count service types
            primary_service = self._get_primary_service(row)
            service_types[primary_service] += 1
            
            # Yearly breakdown
            year = self._extract_year_from_job_no(row.get('job_no', ''))
            if year:
                yearly_data[year] += revenue
        
        return {
            'summary': {
                'service_types': dict(service_types.most_common(5)),
                'yearly_breakdown': dict(yearly_data)
            },
            'top_items': {
                'customers': dict(customers.most_common(5))
            },
            'statistics': {
                'total_revenue': total_revenue,
                'average_revenue': total_revenue / len(results) if results else 0,
                'max_revenue': max((float(row.get('total_revenue', 0) or 0) for row in results), default=0)
            }
        }
    
    def _create_spare_parts_insights(self, results: List[Dict]) -> Dict[str, Any]:
        """Create spare parts specific insights"""
        warehouses = Counter()
        availability = Counter()
        total_value = 0
        high_value_items = []
        
        for row in results:
            # Count warehouses
            wh = row.get('wh', '')
            if wh:
                warehouses[wh] += 1
            
            # Count availability
            avail_status = self._get_part_availability(row)
            availability[avail_status] += 1
            
            # Calculate values
            item_value = float(row.get('total_num', 0) or 0)
            total_value += item_value
            
            if item_value > 10000:  # High value items
                high_value_items.append({
                    'product_code': row.get('product_code', ''),
                    'product_name': row.get('product_name', ''),
                    'value': item_value
                })
        
        return {
            'summary': {
                'warehouses': dict(warehouses.most_common(5)),
                'availability': dict(availability)
            },
            'top_items': {
                'high_value_items': sorted(high_value_items, key=lambda x: x['value'], reverse=True)[:5]
            },
            'statistics': {
                'total_inventory_value': total_value,
                'average_item_value': total_value / len(results) if results else 0,
                'in_stock_percentage': availability.get('In Stock', 0) / len(results) * 100 if results else 0
            }
        }
    
    def _create_customer_insights(self, results: List[Dict]) -> Dict[str, Any]:
        """Create customer specific insights"""
        customer_types = Counter()
        purchase_history = defaultdict(list)
        
        for row in results:
            customer_name = self._clean_customer_name(row.get('customer_name') or row.get('customer', ''))
            customer_type = self._get_customer_type(row)
            customer_types[customer_type] += 1
            
            # Track purchase history
            revenue = float(row.get('total_revenue', 0) or 0)
            if customer_name and revenue > 0:
                purchase_history[customer_name].append(revenue)
        
        # Calculate customer metrics
        top_customers = {}
        for customer, revenues in purchase_history.items():
            top_customers[customer] = {
                'total_revenue': sum(revenues),
                'transaction_count': len(revenues),
                'average_transaction': sum(revenues) / len(revenues)
            }
        
        return {
            'summary': {
                'customer_types': dict(customer_types)
            },
            'top_items': {
                'customers': dict(sorted(top_customers.items(), 
                                       key=lambda x: x[1]['total_revenue'], 
                                       reverse=True)[:5])
            },
            'statistics': {
                'unique_customers': len(purchase_history),
                'total_transactions': sum(len(revenues) for revenues in purchase_history.values())
            }
        }
  