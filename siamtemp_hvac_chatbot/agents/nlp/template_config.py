"""
Template Configuration Module
Centralized configuration for SQL templates to avoid duplication
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TemplateMetadata:
    """Metadata for each SQL template"""
    table: str
    complexity: str  # COMPLEX, EXACT, NORMAL
    keywords: List[str]
    has_subquery: bool = False
    has_not_in: bool = False
    year_adjustment: str = 'simple'  # smart, simple, none
    intent: str = 'general'
    description: str = ''


class TemplateConfig:
    """Centralized template configuration to avoid duplication"""
    
    # ============================================
    # TEMPLATE METADATA CONFIGURATION
    # ============================================
    TEMPLATE_METADATA = {
        # === COMPLEX LOGIC TEMPLATES (need careful handling) ===
        'inactive_customers': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['ลูกค้าที่เคยใช้บริการแต่ไม่ได้ใช้', 'inactive', 'ไม่ได้ใช้บริการ', 'ลูกค้าที่ไม่ใช้'],
            'has_subquery': True,
            'has_not_in': True,
            'year_adjustment': 'smart',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่เคยใช้บริการในอดีตแต่ไม่ได้ใช้ในปีที่ระบุ'
        },
        
        'new_customers_year': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['ลูกค้าใหม่', 'new customers', 'ลูกค้าที่เพิ่งมา'],
            'has_subquery': True,
            'has_not_in': True,
            'year_adjustment': 'smart',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าใหม่ที่ไม่เคยใช้บริการมาก่อน'
        },
        
        'new_customers_in_year': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['ลูกค้าใหม่ในปี', 'new in year'],
            'has_subquery': True,
            'has_not_in': True,
            'year_adjustment': 'smart',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าใหม่ในปีที่ระบุ'
        },
        
        'continuous_customers': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['ลูกค้าต่อเนื่อง', 'continuous', 'loyal customers'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'smart',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่ใช้บริการต่อเนื่อง'
        },
        
        'customers_continuous_years': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['ลูกค้าที่ใช้ต่อเนื่องทุกปี', 'every year'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่ใช้บริการต่อเนื่องหลายปี'
        },
        
        'new_vs_returning_customers': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['เปรียบเทียบลูกค้าใหม่กับเก่า', 'new vs returning'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'smart',
            'intent': 'customer_analysis',
            'description': 'เปรียบเทียบลูกค้าใหม่กับลูกค้าเก่า'
        },
        
        'customer_sales_and_service': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['ลูกค้าที่มีทั้ง sales และ service'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่ใช้ทั้งบริการขายและซ่อม'
        },
        
        'revenue_growth': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['การเติบโตรายได้', 'revenue growth', 'growth rate'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'smart',
            'intent': 'revenue_analysis',
            'description': 'อัตราการเติบโตของรายได้'
        },
        
        'revenue_proportion': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['สัดส่วนรายได้', 'proportion', 'percentage'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'revenue_analysis',
            'description': 'สัดส่วนรายได้แต่ละประเภท'
        },
        
        'sales_yoy_growth': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['เติบโต', 'อัตราการเติบโต', 'growth', 'yoy', 'year over year'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'smart',
            'intent': 'sales_analysis',
            'description': 'การเติบโตแบบ Year over Year'
        },
        
        'business_overview': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['สถานะธุรกิจโดยรวม', 'overview', 'ภาพรวม'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'overview',
            'description': 'ภาพรวมธุรกิจทั้งหมด'
        },
        
        # === EXACT TEMPLATES (use as-is, no modification) ===
        'max_value_work': {
            'table': 'v_sales',
            'complexity': 'EXACT',
            'keywords': ['งานที่มีมูลค่าสูงสุด', 'มูลค่าสูงสุด', 'highest value'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'sales',
            'description': 'งานที่มีมูลค่าสูงที่สุด'
        },
        
        'min_value_work': {
            'table': 'v_sales',
            'complexity': 'EXACT',
            'keywords': ['งานที่มีมูลค่าต่ำสุด', 'มูลค่าต่ำสุด', 'lowest value'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'sales',
            'description': 'งานที่มีมูลค่าต่ำที่สุด'
        },
        
        'year_max_revenue': {
            'table': 'v_sales',
            'complexity': 'EXACT',
            'keywords': ['ปีที่มีรายได้สูงสุด', 'ปีไหนรายได้สูงสุด', 'highest revenue year'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'revenue',
            'description': 'ปีที่มีรายได้สูงที่สุด'
        },
        
        'year_min_revenue': {
            'table': 'v_sales',
            'complexity': 'EXACT',
            'keywords': ['ปีที่มีรายได้ต่ำสุด', 'ปีไหนรายได้ต่ำสุด', 'lowest revenue year'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'revenue',
            'description': 'ปีที่มีรายได้ต่ำที่สุด'
        },
        
        'total_revenue_all': {
            'table': 'v_sales',
            'complexity': 'EXACT',
            'keywords': ['รายได้รวมทั้งหมด', 'รายได้ทั้งหมด', 'total revenue all'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'revenue',
            'description': 'รายได้รวมทั้งหมดทุกปี'
        },
        
        'count_all_jobs': {
            'table': 'v_sales',
            'complexity': 'EXACT',
            'keywords': ['จำนวนงานทั้งหมด', 'มีงานกี่งาน', 'total jobs'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'sales',
            'description': 'จำนวนงานทั้งหมด'
        },
        
        'count_total_customers': {
            'table': 'v_sales',
            'complexity': 'EXACT',
            'keywords': ['จำนวนลูกค้า', 'มีลูกค้ากี่ราย', 'customer count'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'จำนวนลูกค้าทั้งหมด'
        },
        
        # === NORMAL TEMPLATES (flexible modification allowed) ===
        'customer_history': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ประวัติลูกค้า', 'customer history', 'ประวัติการซื้อขาย'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'customer_analysis',
            'description': 'ประวัติการใช้บริการของลูกค้า'
        },
        
        'total_revenue_year': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['รายได้ปี', 'revenue year', 'รายได้ใน'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'revenue',
            'description': 'รายได้ในปีที่ระบุ'
        },
        
        'revenue_by_year': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['รายได้แต่ละปี', 'รายได้รายปี', 'annual revenue'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'revenue',
            'description': 'รายได้แยกตามปี'
        },
        
        'compare_revenue_years': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['เปรียบเทียบรายได้', 'compare revenue', 'เทียบรายได้'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'smart',
            'intent': 'revenue',
            'description': 'เปรียบเทียบรายได้ระหว่างปี'
        },
        
        'top_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้าสูงสุด', 'top customers', 'best customers'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่มียอดซื้อสูงสุด'
        },
        
        # === SPARE PARTS TEMPLATES ===
        'spare_parts_price': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['ราคาอะไหล่', 'spare parts price', 'parts price'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'spare_parts',
            'description': 'ราคาอะไหล่'
        },
        
        'parts_in_stock': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['อะไหล่ที่มีสต็อก', 'in stock', 'มีในคลัง'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'inventory',
            'description': 'อะไหล่ที่มีในสต็อก'
        },
        
        'parts_out_of_stock': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['อะไหล่หมดสต็อก', 'out of stock', 'หมด'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'inventory',
            'description': 'อะไหล่ที่หมดสต็อก'
        },
        
        # === WORK FORCE TEMPLATES ===
        'work_monthly': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานที่วางแผน','แผนงาน','งานรายเดือน', 'งานเดือน', 'monthly work', 'แผนงานเดือน'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_plan',
            'description': 'งานรายเดือน'
        },
        
        'work_summary_monthly': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['สรุปงานเดือน', 'งานที่ทำเดือน', 'monthly summary'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_plan',
            'description': 'สรุปงานประจำเดือน'
        },
        
        'work_plan_date': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['แผนงานวันที่', 'work plan date', 'งานวันที่'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_plan',
            'description': 'แผนงานในวันที่ระบุ'
        },
        
        'repair_history': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['ประวัติการซ่อม', 'repair history', 'ประวัติซ่อม'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'repair_history',
            'description': 'ประวัติการซ่อมบำรุง'
        },
        
        'min_duration_work': {
            'table': 'v_work_force',
            'complexity': 'EXACT',
            'keywords': ['งานที่ใช้เวลาน้อยที่สุด', 'ใช้เวลาน้อยสุด', 'shortest duration'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานที่ใช้เวลาน้อยที่สุด'
        },
        
        'max_duration_work': {
            'table': 'v_work_force',
            'complexity': 'EXACT',
            'keywords': ['งานที่ใช้เวลานานที่สุด', 'ใช้เวลานานสุด', 'longest duration'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานที่ใช้เวลานานที่สุด'
        },
        
        'count_works_by_year': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['จำนวนงานแต่ละปี', 'งานรายปี', 'works by year'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'จำนวนงานแต่ละปี'
        },
        
        'successful_work_monthly': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานสำเร็จ', 'งานเสร็จ', 'successful work', 'completed work'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานที่ทำสำเร็จรายเดือน'
        },
        
        'pm_work_summary': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งาน pm', 'preventive', 'บำรุงรักษาเชิงป้องกัน'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'สรุปงาน PM'
        },
        
        'startup_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['start up', 'สตาร์ทอัพ', 'เริ่มเครื่อง', 'งานติดตั้ง'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งาน Start Up'
        },
        
        'kpi_reported_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['kpi', 'รายงาน kpi', 'งาน kpi'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานที่รายงาน KPI'
        },
        
        'team_specific_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานทีม', 'งานสุพรรณ', 'งานช่าง', 'team work'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานของทีมเฉพาะ'
        },
        
        'replacement_monthly': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งาน replacement', 'งานเปลี่ยน', 'replacement เดือน'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานเปลี่ยนอุปกรณ์รายเดือน'
        },
        
        'long_duration_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['ใช้เวลานาน', 'หลายวัน', 'งานนาน', 'long duration'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานที่ใช้เวลานาน'
        },
        
        # === ADDITIONAL SALES TEMPLATES ===
        'average_revenue_per_transaction': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['รายได้เฉลี่ยต่องาน', 'average revenue', 'เฉลี่ยต่อรายการ'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'revenue_analysis',
            'description': 'รายได้เฉลี่ยต่อ transaction'
        },
        
        'high_value_transactions': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['งานมูลค่าสูง', 'มากกว่า 1 ล้าน', 'high value'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'งานที่มีมูลค่าสูง'
        },
        
        'revenue_by_service_type': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['รายได้แยกตามประเภท', 'แยกประเภท', 'by type'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'revenue_analysis',
            'description': 'รายได้แยกตามประเภทบริการ'
        },
        
        'max_revenue_by_year': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['รายได้สูงสุดแต่ละปี', 'max revenue year', 'สูงสุดของแต่ละปี'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'revenue_analysis',
            'description': 'รายได้สูงสุดในแต่ละปี'
        },
        
        'all_years_revenue_comparison': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['เปรียบเทียบรายได้ทุกปี', 'compare all years', 'รายได้ทุกปี'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'revenue_analysis',
            'description': 'เปรียบเทียบรายได้ทุกปี'
        },
        
        'average_work_value': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ค่าเฉลี่ย', 'average value', 'มูลค่าเฉลี่ย'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'มูลค่าเฉลี่ยของงาน'
        },
        
        'top_customers_no_filter': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้าที่ใช้บริการมาก', 'frequent customers', 'ลูกค้าประจำ'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่ใช้บริการบ่อย'
        },
        
        'most_frequent_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ใช้บริการบ่อยที่สุด', 'most frequent', 'ลูกค้าที่มาบ่อย'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่ใช้บริการบ่อยที่สุด'
        },
        
        'top_service_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้าที่ใช้ service มาก', 'service customers', 'ลูกค้า service'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่ใช้บริการ service มาก'
        },
        
        'customers_using_overhaul': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้าที่ใช้ overhaul', 'overhaul customers', 'ลูกค้า overhaul'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่ใช้บริการ overhaul'
        },
        
        'overhaul_sales': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ยอดขาย overhaul', 'overhaul sales', 'overhaul แต่ละปี'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'sales_analysis',
            'description': 'ยอดขาย overhaul แยกตามปี'
        },
        
        'overhaul_sales_all': {
            'table': 'v_sales',
            'complexity': 'EXACT',
            'keywords': ['overhaul ทั้งหมด', 'total overhaul', 'overhaul รวม'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'sales_analysis',
            'description': 'ยอดขาย overhaul ทั้งหมด'
        },
        
        'overhaul_sales_specific': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['overhaul ปี', 'overhaul เฉพาะปี'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'ยอดขาย overhaul ปีเฉพาะ'
        },
        
        'sales_analysis': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['วิเคราะห์การขาย', 'sales analysis', 'วิเคราะห์ยอดขาย'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'วิเคราะห์การขายหลายปี'
        },
        
        'inventory_check': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['ตรวจสอบสินค้าคงคลัง', 'inventory', 'stock check', 'เช็คสต็อก'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'inventory',
            'description': 'ตรวจสอบสินค้าคงคลัง'
        },
        
        # === MORE SALES TEMPLATES ===
        'service_revenue_2023': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['service ปี 2023', 'service revenue 2023'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'รายได้ service ปี 2023'
        },
        
        'low_value_transactions': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['งานมูลค่าน้อย', 'น้อยกว่า 50000', 'low value'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'งานที่มีมูลค่าต่ำ'
        },
        
        'private_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้าเอกชน', 'บริษัท', 'private sector'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าภาคเอกชน'
        },
        
        'customers_per_year': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['จำนวนลูกค้าแต่ละปี', 'customers per year', 'ลูกค้ารายปี'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'จำนวนลูกค้าแต่ละปี'
        },
        
        'hitachi_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['hitachi', 'ฮิตาชิ', 'เกี่ยวกับ hitachi'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่เกี่ยวข้องกับ Hitachi'
        },
        
        'avg_revenue_per_customer': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['รายได้เฉลี่ยต่อลูกค้า', 'average per customer'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'revenue_analysis',
            'description': 'รายได้เฉลี่ยต่อลูกค้า'
        },
        
        'foreign_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้าต่างชาติ', 'foreign', 'international'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าต่างประเทศ'
        },
        
        # === MORE SPARE PARTS TEMPLATES ===
        'cheapest_parts': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['อะไหล่ถูกที่สุด', 'cheapest', 'ราคาต่ำสุด'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'spare_parts',
            'description': 'อะไหล่ที่มีราคาถูกที่สุด'
        },
        
        'ekac_parts': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['ekac', 'รหัส ekac', 'อะไหล่ ekac'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'spare_parts',
            'description': 'อะไหล่รหัส EKAC'
        },
        
        'total_stock_quantity': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['จำนวนอะไหล่ทั้งหมด', 'total stock', 'สต็อกรวม'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'inventory',
            'description': 'จำนวนสต็อกทั้งหมด'
        },
        
        'reorder_parts': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['ต้องสั่งเติม', 'reorder', 'ต้องสั่งซื้อ'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'inventory',
            'description': 'อะไหล่ที่ต้องสั่งเติม'
        },
        
        'unpriced_parts': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['ไม่มีราคา', 'unpriced', 'ราคาเป็นศูนย์'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'spare_parts',
            'description': 'อะไหล่ที่ไม่มีการตั้งราคา'
        },
        
        'ekac460_info': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['ekac460', 'รหัส ekac460'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'spare_parts',
            'description': 'ข้อมูลอะไหล่รหัส EKAC460'
        },
        
        'set_parts': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['ขายเป็น set', 'unit set', 'ชุด'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'spare_parts',
            'description': 'อะไหล่ที่ขายเป็นชุด'
        },
        
        'recently_received': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['เพิ่งรับเข้า', 'recently received', 'รับล่าสุด'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'inventory',
            'description': 'สินค้าที่เพิ่งรับเข้ามา'
        },
        
        # === MORE WORK FORCE TEMPLATES ===
        'stanley_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งาน stanley', 'stanley works'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานของลูกค้า Stanley'
        },
        
        # === ADDITIONAL MISSING TEMPLATES ===
        'service_num': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ยอด service', 'service revenue', 'บริการ'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'ยอดขาย service ปีเฉพาะ'
        },
        
        'parts_total': {
            'table': 'v_sales',
            'complexity': 'EXACT',
            'keywords': ['ยอดขาย parts', 'ยอดอะไหล่', 'parts revenue'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'sales_analysis',
            'description': 'ยอดขายอะไหล่ทั้งหมด'
        },
        
        'replacement_total': {
            'table': 'v_sales',
            'complexity': 'EXACT',
            'keywords': ['ยอดขาย replacement', 'ยอดเปลี่ยน', 'replacement revenue'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'sales_analysis',
            'description': 'ยอดขาย replacement ทั้งหมด'
        },
        
        'overhaul_total': {
            'table': 'v_sales',
            'complexity': 'EXACT',
            'keywords': ['ยอดขาย overhaul ทั้งหมด', 'total overhaul', 'overhaul รวม'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'sales_analysis',
            'description': 'ยอดขาย overhaul รวมทั้งหมด'
        },
        
        'average_annual_revenue': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['รายได้เฉลี่ยต่อปี', 'average annual', 'เฉลี่ยต่อปี'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'revenue_analysis',
            'description': 'รายได้เฉลี่ยต่อปี'
        },
        
        'count_jobs_year': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['จำนวนงานปี', 'jobs count', 'มีงานกี่งาน'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'จำนวนงานในปีเฉพาะ'
        },
        
        'average_revenue_per_job': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['รายได้เฉลี่ยต่องาน', 'average per job', 'เฉลี่ยต่องาน'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'revenue_analysis',
            'description': 'รายได้เฉลี่ยต่องาน'
        },
        
        'max_revenue_each_year': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['รายได้สูงสุดต่อปี', 'max per year', 'สูงสุดแต่ละปี'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'revenue_analysis',
            'description': 'รายได้สูงสุดของแต่ละปี'
        },
        
        'total_inventory_value': {
            'table': 'v_spare_part',
            'complexity': 'EXACT',
            'keywords': ['มูลค่าสินค้าคงคลัง', 'inventory value', 'มูลค่าสต็อกรวม'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'inventory',
            'description': 'มูลค่าสินค้าคงคลังรวมทั้งหมด'
        },
        
        'customer_specific_history': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ประวัติ stanley', 'ประวัติ clarion', 'specific customer'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'ประวัติลูกค้าเฉพาะราย'
        },
        
        'government_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้าภาครัฐ', 'กระทรวง', 'government'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าภาครัฐ'
        },
        
        'hospital_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['โรงพยาบาล', 'รพ.', 'hospital', 'clinic'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าโรงพยาบาล'
        },
        
        'high_value_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้าที่จ่ายมาก', 'มากกว่า 500000', 'big spenders'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่มียอดซื้อสูง'
        },
        
        'parts_only_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้าที่ซื้อแต่อะไหล่', 'parts only', 'เฉพาะอะไหล่'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่ซื้อเฉพาะอะไหล่'
        },
        
        'chiller_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้า chiller', 'งาน chiller', 'ชิลเลอร์'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่มีงาน chiller'
        },
        
        'quarterly_summary': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ไตรมาส', 'quarterly', 'รายไตรมาส', 'quarter'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'สรุปยอดขายรายไตรมาส'
        },
        
        'highest_value_items': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['สินค้ามูลค่าสูง', 'highest value items', 'อะไหล่แพง'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'inventory',
            'description': 'สินค้าที่มีมูลค่าสูง'
        },
        
        'warehouse_summary': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['สรุปคลัง', 'warehouse summary', 'มูลค่าแต่ละคลัง'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'inventory',
            'description': 'สรุปมูลค่าแต่ละคลัง'
        },
        
        'low_stock_items': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['ใกล้หมด', 'สต็อกน้อย', 'low stock', 'สินค้าเหลือน้อย'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'inventory',
            'description': 'สินค้าที่ใกล้หมด'
        },
        
        'high_unit_price': {
            'table': 'v_spare_part',
            'complexity': 'NORMAL',
            'keywords': ['ราคาต่อหน่วยสูง', 'ราคาแพง', 'expensive', 'high price'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'spare_parts',
            'description': 'อะไหล่ที่มีราคาต่อหน่วยสูง'
        },
        
        # Analytics templates
        'annual_performance_summary': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['สรุปผลประกอบการ', 'annual summary', 'สรุปรายปี'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'overview',
            'description': 'สรุปผลประกอบการรายปี'
        },
        
        'growth_trend': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['เทรนด์การเติบโต', 'growth trend', 'แนวโน้ม'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'revenue_analysis',
            'description': 'เทรนด์การเติบโตของธุรกิจ'
        },
        
        'popular_service_types': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['ประเภทงานที่นิยม', 'popular service', 'บริการยอดนิยม'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'ประเภทบริการที่นิยม'
        },
        
        'high_potential_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้าที่มีศักยภาพ', 'potential', 'ลูกค้าดี'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่มีศักยภาพสูง'
        },
        
        'revenue_distribution': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['การกระจายรายได้', 'distribution', 'กระจาย'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'revenue_analysis',
            'description': 'การกระจายของรายได้'
        },
        
        'team_performance': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['ประสิทธิภาพทีม', 'team performance', 'ผลงานทีม'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'ประสิทธิภาพการทำงานของทีม'
        },
        
        'monthly_sales_trend': {
            'table': 'v_work_force',
            'complexity': 'COMPLEX',
            'keywords': ['แนวโน้มรายเดือน', 'monthly trend', 'ยอดขายรายเดือน'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'แนวโน้มยอดขายรายเดือน'
        },
        
        'service_roi': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['roi', 'ผลตอบแทน', 'return on investment'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'revenue_analysis',
            'description': 'ผลตอบแทนจากการลงทุน'
        },
        
        'revenue_forecast': {
            'table': 'v_sales',
            'complexity': 'COMPLEX',
            'keywords': ['คาดการณ์รายได้', 'forecast', 'พยากรณ์', 'ปีหน้า'],
            'has_subquery': True,
            'has_not_in': False,
            'year_adjustment': 'smart',
            'intent': 'revenue_analysis',
            'description': 'คาดการณ์รายได้'
        },
        
        'top_parts_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้าซื้ออะไหล่มาก', 'top parts customers'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่ซื้ออะไหล่มากที่สุด'
        },
        
        'service_vs_replacement': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['เปรียบเทียบ service กับ replacement'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'sales_analysis',
            'description': 'เปรียบเทียบ service กับ replacement'
        },
        
        'solution_customers': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ลูกค้า solution', 'solution sales'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'simple',
            'intent': 'customer_analysis',
            'description': 'ลูกค้าที่ใช้ solution'
        },
        
        'customer_years_count': {
            'table': 'v_sales',
            'complexity': 'NORMAL',
            'keywords': ['ซื้อขายมากี่ปี', 'กี่ปีแล้ว', 'how many years'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'customer_analysis',
            'description': 'จำนวนปีที่ลูกค้าใช้บริการ'
        },
        
        # === MISSING WORK FORCE TEMPLATES ===
        'work_overhaul': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งาน overhaul', 'การทำงาน overhaul', 'overhaul ที่ทำ', 'overhaul work'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งาน Overhaul ที่ทำ',
            'important_note': 'DO NOT add date filter unless specified'
        },
        
        'count_all_works': {
            'table': 'v_work_force',
            'complexity': 'EXACT',
            'keywords': ['จำนวนงานทั้งหมดในระบบ', 'work records', 'บันทึกงาน'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'จำนวนงานทั้งหมดในระบบ'
        },
        
        'work_specific_month': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานเดือนที่', 'งานเดือนนี้', 'work this month'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_plan',
            'description': 'งานในเดือนที่ระบุ'
        },
        
        'all_pm_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งาน pm ทั้งหมด', 'all pm', 'preventive maintenance'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งาน PM ทั้งหมด'
        },
        
        'work_replacement': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งาน replacement', 'งานเปลี่ยน', 'replacement work'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานเปลี่ยนอุปกรณ์'
        },

        'employee_work_history': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['พนักงานชื่อ', 'ช่างชื่อ', 'ทีม', 'การทำงานของ', 'employee name'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'ประวัติการทำงานของพนักงาน',
            'search_columns': ['detail', 'service_group']
        },

        'successful_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานที่สำเร็จ', 'successful', 'งานเสร็จ'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานที่ทำสำเร็จ'
        },
        
        'unsuccessful_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานที่ไม่สำเร็จ', 'failed', 'งานล้มเหลว'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานที่ไม่สำเร็จ'
        },
        
        'work_today': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานวันนี้', 'today work', 'วันนี้มีงานอะไร'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_plan',
            'description': 'งานวันนี้'
        },
        
        'work_this_week': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานสัปดาห์นี้', 'this week', 'อาทิตย์นี้'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_plan',
            'description': 'งานสัปดาห์นี้'
        },
        
        'success_rate': {
            'table': 'v_work_force',
            'complexity': 'COMPLEX',
            'keywords': ['อัตราความสำเร็จ', 'success rate', 'เปอร์เซ็นต์สำเร็จ'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'อัตราความสำเร็จของงาน'
        },
        
        'on_time_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานตรงเวลา', 'on time', 'ทันเวลา', 'ไม่ล่าช้า'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานที่เสร็จตรงเวลา'
        },
        
        'overtime_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานเกินเวลา', 'overtime', 'ล่าช้า', 'delay'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานที่เกินเวลา'
        },
        
        'startup_works_all': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งาน startup ทั้งหมด', 'all startup'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งาน Startup ทั้งหมด'
        },
        
        'support_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งาน support', 'งานสนับสนุน', 'support all'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานสนับสนุน'
        },
        
        'cpa_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งาน cpa', 'cpa'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งาน CPA'
        },
        
        'team_statistics': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['สถิติทีม', 'team stats', 'ผลงานทีม'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'สถิติงานของแต่ละทีม'
        },
        
        'team_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานของทีม', 'team a', 'ทีม a'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานของทีมเฉพาะ'
        },
        
        'work_duration': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['ระยะเวลาทำงาน', 'duration', 'ใช้เวลา'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'ระยะเวลาการทำงาน'
        },
        
        'latest_works': {
            'table': 'v_work_force',
            'complexity': 'NORMAL',
            'keywords': ['งานล่าสุด', 'latest', 'งานใหม่ล่าสุด'],
            'has_subquery': False,
            'has_not_in': False,
            'year_adjustment': 'none',
            'intent': 'work_analysis',
            'description': 'งานล่าสุด'
        }
    }
    
    # ============================================
    # CLASS METHODS
    # ============================================
    
    @classmethod
    def get_template_config(cls, template_name: str) -> Dict[str, Any]:
        """Get configuration for a specific template"""
        return cls.TEMPLATE_METADATA.get(template_name, {})
    
    @classmethod
    def get_templates_by_complexity(cls, complexity: str) -> List[str]:
        """Get all templates of a specific complexity"""
        return [
            name for name, config in cls.TEMPLATE_METADATA.items()
            if config.get('complexity') == complexity
        ]
    
    @classmethod
    def get_templates_by_table(cls, table: str) -> List[str]:
        """Get all templates for a specific table"""
        return [
            name for name, config in cls.TEMPLATE_METADATA.items()
            if config.get('table') == table
        ]
    
    @classmethod
    def get_templates_by_intent(cls, intent: str) -> List[str]:
        """Get all templates for a specific intent"""
        return [
            name for name, config in cls.TEMPLATE_METADATA.items()
            if config.get('intent') == intent
        ]
    
    @classmethod
    def is_complex_template(cls, template_name: str) -> bool:
        """Check if template is complex"""
        config = cls.get_template_config(template_name)
        return config.get('complexity') == 'COMPLEX'
    
    @classmethod
    def is_exact_template(cls, template_name: str) -> bool:
        """Check if template should be used exactly"""
        config = cls.get_template_config(template_name)
        return config.get('complexity') == 'EXACT'
    
    @classmethod
    def is_normal_template(cls, template_name: str) -> bool:
        """Check if template allows flexible modification"""
        config = cls.get_template_config(template_name)
        return config.get('complexity') == 'NORMAL'
    
    @classmethod
    def requires_smart_year_adjustment(cls, template_name: str) -> bool:
        """Check if template needs smart year adjustment"""
        config = cls.get_template_config(template_name)
        return config.get('year_adjustment') == 'smart'
    
    @classmethod
    def has_subquery(cls, template_name: str) -> bool:
        """Check if template has subquery"""
        config = cls.get_template_config(template_name)
        return config.get('has_subquery', False)
    
    @classmethod
    def has_not_in_clause(cls, template_name: str) -> bool:
        """Check if template has NOT IN clause"""
        config = cls.get_template_config(template_name)
        return config.get('has_not_in', False)
    
    @classmethod
    def get_template_keywords(cls, template_name: str) -> List[str]:
        """Get keywords associated with a template"""
        config = cls.get_template_config(template_name)
        return config.get('keywords', [])
    
    @classmethod
    def get_template_description(cls, template_name: str) -> str:
        """Get description of what the template does"""
        config = cls.get_template_config(template_name)
        return config.get('description', '')
    
    @classmethod
    def search_template_by_keyword(cls, keyword: str) -> List[str]:
        """Search templates that match a keyword"""
        keyword_lower = keyword.lower()
        matching_templates = []
        
        for name, config in cls.TEMPLATE_METADATA.items():
            keywords = config.get('keywords', [])
            for kw in keywords:
                if keyword_lower in kw.lower():
                    matching_templates.append(name)
                    break
        
        return matching_templates
    
    @classmethod
    def get_all_complex_templates(cls) -> List[str]:
        """Get all complex templates"""
        return cls.get_templates_by_complexity('COMPLEX')
    
    @classmethod
    def get_all_exact_templates(cls) -> List[str]:
        """Get all exact templates"""
        return cls.get_templates_by_complexity('EXACT')
    
    @classmethod
    def get_all_normal_templates(cls) -> List[str]:
        """Get all normal templates"""
        return cls.get_templates_by_complexity('NORMAL')
    
    @classmethod
    def validate_template_name(cls, template_name: str) -> bool:
        """Check if template name exists in configuration"""
        return template_name in cls.TEMPLATE_METADATA
    
    @classmethod
    def get_template_statistics(cls) -> Dict[str, Any]:
        """Get statistics about templates"""
        total = len(cls.TEMPLATE_METADATA)
        
        stats = {
            'total_templates': total,
            'complex_templates': len(cls.get_all_complex_templates()),
            'exact_templates': len(cls.get_all_exact_templates()),
            'normal_templates': len(cls.get_all_normal_templates()),
            'tables': {},
            'intents': {}
        }
        
        # Count by table
        for config in cls.TEMPLATE_METADATA.values():
            table = config.get('table', 'unknown')
            stats['tables'][table] = stats['tables'].get(table, 0) + 1
            
            intent = config.get('intent', 'unknown')
            stats['intents'][intent] = stats['intents'].get(intent, 0) + 1
        
        return stats
    
    @classmethod
    def log_configuration(cls):
        """Log configuration statistics"""
        stats = cls.get_template_statistics()
        logger.info("=" * 60)
        logger.info("Template Configuration Loaded")
        logger.info(f"Total Templates: {stats['total_templates']}")
        logger.info(f"  - Complex: {stats['complex_templates']}")
        logger.info(f"  - Exact: {stats['exact_templates']}")
        logger.info(f"  - Normal: {stats['normal_templates']}")
        logger.info("Tables:")
        for table, count in stats['tables'].items():
            logger.info(f"  - {table}: {count} templates")
        logger.info("Intents:")
        for intent, count in stats['intents'].items():
            logger.info(f"  - {intent}: {count} templates")
        logger.info("=" * 60)


# Initialize and log configuration when module is loaded
if __name__ == "__main__":
    TemplateConfig.log_configuration()
    
    # Example usage
    print("\nExample: Check if 'inactive_customers' is complex:")
    print(TemplateConfig.is_complex_template('inactive_customers'))
    
    print("\nExample: Get all templates for v_sales table:")
    print(TemplateConfig.get_templates_by_table('v_sales')[:5], "...")
    
    print("\nExample: Search templates with keyword 'ลูกค้า':")
    print(TemplateConfig.search_template_by_keyword('ลูกค้า')[:5], "...")