# agents/data/cleaner.py
"""
DataCleaningEngine - Optimized Version for Clean Database
Since encoding issues are resolved, focus on standardization and formatting
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import Counter, defaultdict

logger = logging.getLogger(__name__)

class DataCleaningEngine:
    """
    DataCleaningEngine - Optimized for Siamtemp data
    Focus on standardization and formatting since encoding is fixed
    """
    
    def __init__(self):
        # Company name standardization
        self.company_standardizations = {
            # Fix spacing issues
            'CLARION  ASIA': 'CLARION ASIA',
            'CLARION   ASIA': 'CLARION ASIA',
            
            # Standardize variations
            'คลีนิค ประกอบโรคศิลปะฯ': 'คลีนิคประกอบโรคศิลป์ฯ',
            'IRPC PUBIC': 'IRPC PUBLIC',
            'sadesa': 'Sadesa',
            'STANLEY': 'Stanley Electric',
            'HONDA': 'Honda Automobile',
            
            # Fix company suffixes
            'CO.,LTD.': 'Co., Ltd.',
            'Co.,Ltd.': 'Co., Ltd.',
            '(THAILAND)': '(Thailand)',
            'ASIA': 'Asia',
        }
        
        # Service type mappings
        self.service_types = {
            'PM': 'Preventive Maintenance',
            'OH': 'Overhaul',
            'OV': 'Overhaul',
            'RE': 'Replacement',
            'SP': 'Spare Parts',
            'SV': 'Service',
            'SO': 'Solution'
        }
        
        # Equipment name standardization
        self.equipment_standards = {
            'CH': 'Chiller',
            'AHU': 'Air Handling Unit',
            'FCU': 'Fan Coil Unit',
            'VRF': 'Variable Refrigerant Flow',
            'COMP': 'Compressor'
        }
    
    def clean_results(self, results: List[Dict], intent: str = None) -> Tuple[List[Dict], Dict]:
        """
        Main cleaning method - simplified for clean data
        """
        if not results:
            return results, {'cleaned': 0}
        
        cleaned_results = []
        stats = {
            'total_rows': len(results),
            'standardized_names': 0,
            'null_values_handled': 0,
            'numeric_cleaned': 0
        }
        
        for row in results:
            cleaned_row = self._clean_single_row(row, stats)
            cleaned_results.append(cleaned_row)
        
        # Add insights if intent provided
        if intent:
            cleaned_results = self._enhance_for_intent(cleaned_results, intent)
        
        return cleaned_results, stats
    
    def _clean_single_row(self, row: Dict, stats: Dict) -> Dict:
        """Clean a single row - optimized version"""
        cleaned_row = {}
        
        for key, value in row.items():
            # Handle NULL values
            if value is None or value == 'NULL' or value == '':
                cleaned_row[key] = None
                stats['null_values_handled'] += 1
                continue
            
            # Process strings
            if isinstance(value, str):
                # Standardize company names
                if 'customer' in key.lower() or 'name' in key.lower():
                    original = value
                    value = self._standardize_company_name(value)
                    if value != original:
                        stats['standardized_names'] += 1
                
                # Clean extra whitespaces
                value = re.sub(r'\s+', ' ', value).strip()
            
            # Handle numeric fields
            elif self._is_numeric_field(key):
                value = self._clean_numeric_value(value)
                if value is not None:
                    stats['numeric_cleaned'] += 1
            
            cleaned_row[key] = value
        
        return cleaned_row
    
    def _standardize_company_name(self, name: str) -> str:
        """Standardize company names"""
        if not name:
            return name
        
        # Apply standardizations
        for old, new in self.company_standardizations.items():
            if old in name:
                name = name.replace(old, new)
        
        # Clean multiple spaces
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    def _is_numeric_field(self, field_name: str) -> bool:
        """Check if field should be numeric"""
        numeric_indicators = ['_num', 'revenue', 'price', 'total', 'balance', 'amount']
        return any(indicator in field_name.lower() for indicator in numeric_indicators)
    
    def _clean_numeric_value(self, value: Any) -> Optional[float]:
        """Clean numeric values"""
        if value is None:
            return 0
        
        try:
            if isinstance(value, str):
                # Remove commas and convert
                cleaned = re.sub(r'[,\s]', '', str(value))
                return float(cleaned) if '.' in cleaned else int(cleaned)
            else:
                return float(value) if value else 0
        except:
            return 0
    
    def _enhance_for_intent(self, results: List[Dict], intent: str) -> List[Dict]:
        """Add enhancements based on intent"""
        if intent in ['sales', 'sales_analysis']:
            return self._enhance_sales_data(results)
        elif intent in ['work_force', 'work_plan']:
            return self._enhance_work_data(results)
        elif intent in ['spare_parts', 'parts_price']:
            return self._enhance_parts_data(results)
        return results
    
    def _enhance_sales_data(self, results: List[Dict]) -> List[Dict]:
        """Enhance sales data"""
        return results
    
    def _enhance_work_data(self, results: List[Dict]) -> List[Dict]:
        """Enhance work force data"""
        for row in results:
            # Derive job type
            row['job_type'] = self._get_job_type(row)
            
            # Parse date if string
            if 'date' in row and isinstance(row['date'], str):
                row['parsed_date'] = self._parse_date(row['date'])
            
            # Get work status
            row['status'] = self._get_work_status(row)
        
        return results
    
    def _enhance_parts_data(self, results: List[Dict]) -> List[Dict]:
        """Enhance spare parts data"""
        # for row in results:
        #     # Add availability status
        #     balance = row.get('balance_num', 0) or 0
        #     row['availability'] = 'In Stock' if balance > 0 else 'Out of Stock'
            
        #     # Add value category
        #     total_value = row.get('total_num', 0) or 0
        #     if total_value > 50000:
        #         row['value_category'] = 'High Value'
        #     elif total_value > 10000:
        #         row['value_category'] = 'Medium Value'
        #     else:
        #         row['value_category'] = 'Low Value'
        
        return results
    
    def _get_job_type(self, row: Dict) -> str:
        """Derive job type from job description fields"""
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
        return 'Other'
    
    def _get_work_status(self, row: Dict) -> str:
        """Determine work status"""
        if row.get('success'):
            return 'Completed'
        elif row.get('unsuccessful'):
            return 'Failed'
        return 'Pending'
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to standard format"""
        if not date_str:
            return None
        
        # Already in correct format
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
        
        # Try other formats
        patterns = [
            (r'^(\d{1,2})/(\d{1,2})/(\d{4})$', lambda m: f"{m[3]}-{m[2]:0>2}-{m[1]:0>2}"),
            (r'^(\d{1,2})-(\d{1,2})-(\d{4})$', lambda m: f"{m[3]}-{m[2]:0>2}-{m[1]:0>2}"),
        ]
        
        for pattern, formatter in patterns:
            match = re.match(pattern, date_str)
            if match:
                try:
                    return formatter(match.groups())
                except:
                    continue
        
        return date_str
    
    def create_summary_insights(self, results: List[Dict], intent: str) -> Dict[str, Any]:
        """Create summary insights for response generation"""
        if not results:
            return {'total_count': 0, 'summary': 'ไม่พบข้อมูล'}
        
        insights = {
            'total_count': len(results),
            'summary': {},
            'top_items': {},
            'statistics': {}
        }
        
        if intent in ['sales', 'sales_analysis']:
            insights = self._create_sales_insights(results, insights)
        elif intent in ['work_force', 'work_plan', 'work_summary']:
            insights = self._create_work_insights(results, insights)
        elif intent in ['spare_parts', 'parts_price', 'inventory_value']:
            insights = self._create_parts_insights(results, insights)
        elif intent in ['customer_history', 'top_customers']:
            insights = self._create_customer_insights(results, insights)
        
        return insights
    
    def _create_sales_insights(self, results: List[Dict], insights: Dict) -> Dict:
        """Create sales-specific insights"""
        
        return insights
    
    def _create_work_insights(self, results: List[Dict], insights: Dict) -> Dict:
        """Create work-specific insights"""
        job_types = Counter()
        statuses = Counter()
        teams = Counter()
        
        for r in results:
            if 'job_type' in r:
                job_types[r['job_type']] += 1
            if 'status' in r:
                statuses[r['status']] += 1
            if r.get('service_group'):
                teams[r['service_group']] += 1
        
        insights['summary'] = {
            'job_types': dict(job_types.most_common()),
            'status_breakdown': dict(statuses)
        }
        
        insights['top_items'] = {
            'busiest_teams': dict(teams.most_common(5))
        }
        
        completed = statuses.get('Completed', 0)
        total = len(results)
        insights['statistics'] = {
            'total_jobs': total,
            'success_rate': f"{(completed/total*100):.1f}%" if total > 0 else "0%",
            'pending_jobs': statuses.get('Pending', 0)
        }
        
        return insights
    
    def _create_parts_insights(self, results: List[Dict], insights: Dict) -> Dict:
        """Create spare parts insights"""
        total_value = sum(r.get('total_num', 0) or 0 for r in results)
        in_stock = sum(1 for r in results if (r.get('balance_num', 0) or 0) > 0)
        
        # Group by warehouse
        warehouse_counts = Counter()
        for r in results:
            if r.get('wh'):
                warehouse_counts[r['wh']] += 1
        
        insights['summary'] = {
            'total_inventory_value': f"{total_value:,.0f}",
            'in_stock_percentage': f"{(in_stock/len(results)*100):.1f}%" if results else "0%"
        }
        
        insights['top_items'] = {
            'warehouse_distribution': dict(warehouse_counts.most_common())
        }
        
        insights['statistics'] = {
            'total_items': len(results),
            'in_stock_items': in_stock,
            'out_of_stock_items': len(results) - in_stock
        }
        
        return insights
    
    def _create_customer_insights(self, results: List[Dict], insights: Dict) -> Dict:
        """Create customer-specific insights"""
        customer_totals = defaultdict(lambda: {'revenue': 0, 'transactions': 0})
        
        for r in results:
            customer = self._standardize_company_name(
                r.get('customer_name') or r.get('customer', '')
            )
            if customer:
                revenue = r.get('total_revenue', 0) or 0
                customer_totals[customer]['revenue'] += revenue
                customer_totals[customer]['transactions'] += 1
        
        # Sort by revenue
        top_customers = sorted(
            customer_totals.items(),
            key=lambda x: x[1]['revenue'],
            reverse=True
        )[:10]
        
        insights['summary'] = {
            'unique_customers': len(customer_totals),
            'total_transactions': sum(c['transactions'] for c in customer_totals.values())
        }
        
        insights['top_items'] = {
            'top_10_customers': {
                name: {
                    'revenue': f"{data['revenue']:,.0f}",
                    'transactions': data['transactions'],
                    'avg_transaction': f"{data['revenue']/data['transactions']:,.0f}" 
                        if data['transactions'] > 0 else "0"
                }
                for name, data in top_customers
            }
        }
        
        return insights