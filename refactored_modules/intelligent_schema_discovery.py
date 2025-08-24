# refactored_modules/intelligent_schema_discovery.py
# 🔧 ปรับปรุงสำหรับธุรกิจ HVAC Service & Spare Parts

import time
import re
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class IntelligentSchemaDiscovery:
    """🧠 Intelligent Schema Discovery สำหรับธุรกิจ HVAC"""
    
    def __init__(self, database_handler):
        self.db_handler = database_handler
        
        # เก็บ cache ของข้อมูลที่เรียนรู้แล้ว
        self.learned_schemas = {
            'customers': {},
            'spare_parts': {},
            'services': {},
            'work_schedules': {}
        }
        
        # HVAC Business Knowledge Base (แทน metadata ในฐานข้อมูล)
        self.hvac_knowledge_base = {
            'table_purposes': {
                'sales2024': 'ข้อมูลการขายและบริการประจำปี 2024',
                'sales2023': 'ข้อมูลการขายและบริการประจำปี 2023', 
                'sales2022': 'ข้อมูลการขายและบริการประจำปี 2022',
                'sales2025': 'ข้อมูลการขายและบริการประจำปี 2025',
                'spare_part': 'คลังอะไหล่หลัก',
                'spare_part2': 'คลังอะไหล่สำรอง',
                'work_force': 'การจัดทีมงานและแผนการทำงาน'
            },
            
            'column_meanings': {
                'job_no': 'หมายเลขงาน รูปแบบ SV.ปี-เดือน-ลำดับ-ประเภท',
                'customer_name': 'ชื่อบริษัทลูกค้าเต็ม',
                'customer': 'ชื่อลูกค้าแบบย่อ',
                'description': 'รายละเอียดงานบริการ',
                'service_contact_': 'มูลค่างานบริการ (หน่วย: บาท)',
                'overhaul_': 'งานยกเครื่อง/ซ่อมครั้งใหญ่',
                'replacement': 'งานเปลี่ยนชิ้นส่วน',
                'product_code': 'รหัสอะไหล่ของบริษัท',
                'product_name': 'ชื่ออะไหล่ภาษาอังกฤษ',
                'unit_price': 'ราคาต่อหน่วย (บาท)',
                'balance': 'จำนวนคงเหลือในสต็อก',
                'wh': 'รหัสคลังสินค้า',
                'date': 'วันที่ทำงาน',
                'project': 'ชื่อโปรเจค/สถานที่',
                'detail': 'รายละเอียดงานเฉพาะ',
                'service_group': 'ชื่อทีมช่างที่รับผิดชอบ',
                'job_description_pm': 'งาน Preventive Maintenance',
                'job_description_replacement': 'งานเปลี่ยนชิ้นส่วน'
            },
            
            'business_patterns': {
                'job_number_format': 'SV.{year}-{month}-{sequence}-{type}',
                'service_types': ['PM', 'Repair', 'Replacement', 'Overhaul', 'Installation'],
                'brands': ['Hitachi', 'Daikin', 'EuroKlimat', 'Toyota', 'Mitsubishi', 'York', 'Carrier'],
                'customer_types': ['บริษัท', 'การไฟฟ้า', 'โรงแรม', 'โรงพยาบาล', 'โรงงาน'],
                'technical_terms': {
                    'Chiller': 'เครื่องทำน้ำเย็น',
                    'Compressor': 'คอมเพรสเซอร์',
                    'PM': 'Preventive Maintenance',
                    'Air Cooled': 'ระบายความร้อนด้วยอากาศ',
                    'Water Cooled': 'ระบายความร้อนด้วยน้ำ',
                    'RCUA': 'Roof Top Unit Air Cooled',
                    'Set Free': 'ระบบ VRF',
                    'FTG': 'Floor Type Gas',
                    'EKAC': 'EuroKlimat Air Cooled'
                }
            },
            
            'query_templates': {
                'customer_count': 'SELECT COUNT(DISTINCT customer_name) FROM sales{year}',
                'customer_history': '''
                    SELECT job_no, description, service_contact_ 
                    FROM sales{year} 
                    WHERE customer_name ILIKE '%{customer}%'
                ''',
                'spare_parts_search': '''
                    SELECT product_code, product_name, unit_price, balance, description
                    FROM spare_part 
                    WHERE product_name ILIKE '%{keyword}%' 
                    OR description ILIKE '%{keyword}%'
                ''',
                'work_schedule': '''
                    SELECT customer, detail, service_group,
                           CASE WHEN job_description_pm THEN 'PM'
                                WHEN job_description_replacement THEN 'Replacement'
                                ELSE 'Other' END as job_type
                    FROM work_force 
                    WHERE date = '{date}'
                ''',
                'sales_analysis': '''
                    SELECT 
                        COUNT(*) as total_jobs,
                        SUM(service_contact_) as total_revenue,
                        AVG(service_contact_) as avg_job_value
                    FROM sales{year}
                    WHERE service_contact_ > 0
                '''
            }
        }
        
        self.cache_timestamps = {}
        self.cache_duration = 1800  # 30 นาที
        
        logger.info("🧠 HVAC Intelligent Schema Discovery system initialized")
    
    async def get_contextual_schema(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """🎯 Main function - สำหรับธุรกิจ HVAC"""
        
        # ขั้นตอนที่ 1: วิเคราะห์คำถามตามธุรกิจ HVAC
        question_analysis = self._analyze_hvac_question_deeply(question)
        logger.info(f"🔍 HVAC Question analysis: {question_analysis}")
        
        # ขั้นตอนที่ 2: เตรียมข้อมูลที่จำเป็นสำหรับ HVAC
        required_data = await self._gather_hvac_required_data(tenant_id, question_analysis)
        
        # ขั้นตอนที่ 3: สร้าง schema context สำหรับ HVAC
        contextual_schema = self._build_hvac_intelligent_context(question_analysis, required_data, tenant_id)
        
        return contextual_schema
    
    def _analyze_hvac_question_deeply(self, question: str) -> Dict[str, Any]:
        """🔍 วิเคราะห์คำถามเชิงลึกสำหรับธุรกิจ HVAC"""
        
        question_lower = question.lower()
        
        analysis_result = {
            'question_type': 'unknown',
            'hvac_entities': [],
            'target_tables': [],
            'business_context': '',
            'time_range': None,
            'customer_mentioned': None,
            'brand_mentioned': [],
            'service_type': None,
            'needs_aggregation': False,
            'confidence_level': 0.0
        }
        
        # วิเคราะห์ประเภทคำถามหลัก
        if any(word in question_lower for word in ['จำนวน', 'กี่', 'count', 'นับ']):
            analysis_result['question_type'] = 'counting'
            analysis_result['needs_aggregation'] = True
            analysis_result['confidence_level'] += 0.4
        
        elif any(word in question_lower for word in ['ราคา', 'price', 'อะไหล่', 'spare']):
            analysis_result['question_type'] = 'spare_parts_inquiry'
            analysis_result['target_tables'] = ['spare_part', 'spare_part2']
            analysis_result['confidence_level'] += 0.4
        
        elif any(word in question_lower for word in ['ประวัติ', 'history', 'ย้อนหลัง', 'มีการ']):
            analysis_result['question_type'] = 'customer_history'
            analysis_result['target_tables'] = ['sales2024', 'sales2023', 'sales2022']
            analysis_result['confidence_level'] += 0.4
        
        elif any(word in question_lower for word in ['แผนงาน', 'schedule', 'วันที่', 'งาน']):
            analysis_result['question_type'] = 'work_schedule'
            analysis_result['target_tables'] = ['work_force']
            analysis_result['confidence_level'] += 0.4
        
        elif any(word in question_lower for word in ['ยอดขาย', 'วิเคราะห์', 'รายงาน', 'สรุป']):
            analysis_result['question_type'] = 'sales_analysis'
            analysis_result['target_tables'] = ['sales2024', 'sales2023', 'sales2022']
            analysis_result['needs_aggregation'] = True
            analysis_result['confidence_level'] += 0.4
        
        # วิเคราะห์ช่วงเวลา
        year_patterns = ['2567', '2568', '2022', '2023', '2024', '2025']
        for year in year_patterns:
            if year in question:
                analysis_result['time_range'] = year
                if year in ['2567', '2568']:  # แปลงปี พ.ศ. เป็น ค.ศ.
                    analysis_result['time_range'] = str(int(year) - 543)
                break
        
        # วิเคราะห์การระบุลูกค้า
        if any(word in question_lower for word in ['บริษัท', 'company']):
            # พยายามดึงชื่อบริษัทจากคำถาม
            company_pattern = r'บริษัท\s*([^ม]+?)(?:\s|$)'
            match = re.search(company_pattern, question)
            if match:
                analysis_result['customer_mentioned'] = match.group(1).strip()
        
        # วิเคราะห์แบรนด์ที่กล่าวถึง
        for brand in self.hvac_knowledge_base['business_patterns']['brands']:
            if brand.lower() in question_lower:
                analysis_result['brand_mentioned'].append(brand)
        
        # วิเคราะห์ประเภทบริการ
        for service in self.hvac_knowledge_base['business_patterns']['service_types']:
            if service.lower() in question_lower:
                analysis_result['service_type'] = service
                break
        
        # กำหนดบริบทธุรกิจ
        if analysis_result['question_type'] == 'spare_parts_inquiry':
            analysis_result['business_context'] = 'คำถามเกี่ยวกับอะไหล่และราคา'
        elif analysis_result['question_type'] == 'customer_history':
            analysis_result['business_context'] = 'คำถามเกี่ยวกับประวัติลูกค้า'
        elif analysis_result['question_type'] == 'work_schedule':
            analysis_result['business_context'] = 'คำถามเกี่ยวกับแผนงานและทีม'
        elif analysis_result['question_type'] == 'sales_analysis':
            analysis_result['business_context'] = 'คำถามเกี่ยวกับการวิเคราะห์ยอดขาย'
        
        return analysis_result
    
    async def _gather_hvac_required_data(self, tenant_id: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """📊 รวบรวมข้อมูลที่จำเป็นสำหรับ HVAC"""
        
        required_data = {}
        
        try:
            # ดึงข้อมูลลูกค้าหากจำเป็น
            if analysis['question_type'] in ['customer_history', 'counting']:
                required_data['customers'] = await self._get_hvac_customer_data(tenant_id, analysis)
            
            # ดึงข้อมูลอะไหล่หากจำเป็น
            if analysis['question_type'] == 'spare_parts_inquiry':
                required_data['spare_parts'] = await self._get_spare_parts_data(tenant_id, analysis)
            
            # ดึงข้อมูลทีมงานหากจำเป็น
            if analysis['question_type'] == 'work_schedule':
                required_data['work_teams'] = await self._get_work_team_data(tenant_id, analysis)
            
            # ดึงข้อมูลสถิติการขายหากจำเป็น
            if analysis['question_type'] == 'sales_analysis':
                required_data['sales_stats'] = await self._get_sales_statistics(tenant_id, analysis)
        
        except Exception as e:
            logger.error(f"Failed to gather HVAC required data: {e}")
            required_data = self._get_hvac_fallback_data()
        
        return required_data
    
    async def _get_hvac_customer_data(self, tenant_id: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """👥 ดึงข้อมูลลูกค้าสำหรับ HVAC"""
        
        try:
            conn = self.db_handler.get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            # ดึงรายชื่อลูกค้าที่ไม่ซ้ำ
            cursor.execute("""
                SELECT DISTINCT customer_name 
                FROM sales2024 
                WHERE customer_name IS NOT NULL 
                AND customer_name != ''
                ORDER BY customer_name
                LIMIT 50
            """)
            
            customers = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return {
                'all_customers': customers,
                'customer_count': len(customers),
                'has_customer_data': len(customers) > 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get customer data: {e}")
            return {'all_customers': [], 'customer_count': 0, 'has_customer_data': False}
    
    async def _get_spare_parts_data(self, tenant_id: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """🔧 ดึงข้อมูลอะไหล่"""
        
        try:
            conn = self.db_handler.get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            # ดึงประเภทอะไหล่และแบรนด์
            cursor.execute("""
                SELECT DISTINCT 
                    CASE 
                        WHEN product_name ILIKE '%MOTOR%' THEN 'MOTOR'
                        WHEN product_name ILIKE '%BOARD%' THEN 'CIRCUIT_BOARD'
                        WHEN product_name ILIKE '%TRANSFORMER%' THEN 'TRANSFORMER'
                        WHEN product_name ILIKE '%FAN%' THEN 'FAN'
                        ELSE 'OTHER'
                    END as part_category,
                    product_name
                FROM spare_part 
                WHERE product_name IS NOT NULL
                LIMIT 30
            """)
            
            parts_data = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            categories = list(set([row[0] for row in parts_data]))
            sample_parts = [row[1] for row in parts_data[:10]]
            
            return {
                'part_categories': categories,
                'sample_parts': sample_parts,
                'has_parts_data': len(parts_data) > 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get spare parts data: {e}")
            return {'part_categories': [], 'sample_parts': [], 'has_parts_data': False}
    
    async def _get_work_team_data(self, tenant_id: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """👷 ดึงข้อมูลทีมงาน"""
        
        try:
            conn = self.db_handler.get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            # ดึงชื่อทีมและประเภทงาน
            cursor.execute("""
                SELECT DISTINCT service_group
                FROM work_force 
                WHERE service_group IS NOT NULL 
                AND service_group != ''
                ORDER BY service_group
                LIMIT 20
            """)
            
            teams = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return {
                'available_teams': teams,
                'team_count': len(teams),
                'has_team_data': len(teams) > 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get work team data: {e}")
            return {'available_teams': [], 'team_count': 0, 'has_team_data': False}
    
    async def _get_sales_statistics(self, tenant_id: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """📊 ดึงสถิติการขาย"""
        
        try:
            conn = self.db_handler.get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            # ดึงสถิติพื้นฐาน
            cursor.execute("""
                SELECT 
                    COUNT(*) as job_count,
                    COUNT(DISTINCT customer_name) as customer_count,
                    AVG(service_contact_::numeric) as avg_value,
                    SUM(service_contact_::numeric) as total_value
                FROM sales2024 
                WHERE service_contact_::numeric > 0
            """)
            
            stats = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return {
                'total_jobs': stats[0] if stats else 0,
                'unique_customers': stats[1] if stats else 0,
                'average_job_value': float(stats[2]) if stats and stats[2] else 0,
                'total_revenue': float(stats[3]) if stats and stats[3] else 0,
                'has_sales_data': stats and stats[0] > 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get sales statistics: {e}")
            return {'total_jobs': 0, 'unique_customers': 0, 'average_job_value': 0, 'total_revenue': 0, 'has_sales_data': False}
    
    def _get_hvac_fallback_data(self) -> Dict[str, Any]:
        """🔄 ข้อมูล fallback สำหรับ HVAC"""
        return {
            'customers': {'all_customers': [], 'has_customer_data': False},
            'spare_parts': {'part_categories': ['MOTOR', 'CIRCUIT_BOARD', 'FAN'], 'has_parts_data': True},
            'work_teams': {'available_teams': ['ทีมช่าง A', 'ทีมช่าง B'], 'has_team_data': True},
            'sales_stats': {'has_sales_data': False}
        }
    
    def _build_hvac_intelligent_context(self, analysis: Dict[str, Any], 
                                       required_data: Dict[str, Any], tenant_id: str) -> Dict[str, Any]:
        """🎯 สร้าง context ที่ชาญฉลาดสำหรับ HVAC"""
        
        context = {
            'schema_type': 'hvac_intelligent_contextual',
            'question_analysis': analysis,
            'discovered_at': datetime.now().isoformat(),
            'tenant_id': tenant_id,
            'hvac_guidance': {},
            'business_data': {},
            'query_hints': []
        }
        
        # สร้างคำแนะนำเฉพาะสำหรับแต่ละประเภทคำถาม
        if analysis['question_type'] == 'counting':
            context['hvac_guidance'] = {
                'query_type': 'COUNT aggregation for HVAC business',
                'sql_hints': [
                    'ใช้ COUNT(DISTINCT customer_name) สำหรับนับลูกค้า',
                    'ใช้ COUNT(*) สำหรับนับจำนวนงาน',
                    'ระวังข้อมูลซ้ำในตารางลูกค้า'
                ],
                'recommended_tables': ['sales2024']
            }
        
        elif analysis['question_type'] == 'spare_parts_inquiry':
            context['hvac_guidance'] = {
                'query_type': 'Spare parts search and pricing',
                'sql_hints': [
                    'ค้นหาใน product_name และ description',
                    'ใช้ ILIKE สำหรับการค้นหาที่ไม่สนใจตัวใหญ่เล็ก',
                    'รวมข้อมูลจาก spare_part และ spare_part2 หากจำเป็น'
                ],
                'recommended_tables': ['spare_part', 'spare_part2']
            }
        
        elif analysis['question_type'] == 'customer_history':
            context['hvac_guidance'] = {
                'query_type': 'Customer service history across years',
                'sql_hints': [
                    'ใช้ UNION ALL เพื่อรวมข้อมูลหลายปี',
                    'ค้นหาลูกค้าด้วย ILIKE \'%customer_name%\'',
                    'แสดง job_no, description, และ service_contact_'
                ],
                'recommended_tables': analysis['target_tables']
            }
        
        elif analysis['question_type'] == 'work_schedule':
            context['hvac_guidance'] = {
                'query_type': 'Work planning and team assignment',
                'sql_hints': [
                    'ใช้ CASE WHEN เพื่อแปลง boolean เป็นประเภทงาน',
                    'กรองตามวันที่หากระบุ',
                    'แสดงข้อมูลทีมงานและรายละเอียดงาน'
                ],
                'recommended_tables': ['work_force']
            }
        
        elif analysis['question_type'] == 'sales_analysis':
            context['hvac_guidance'] = {
                'query_type': 'Sales performance analysis',
                'sql_hints': [
                    'ใช้ SUM(service_contact_) สำหรับยอดขาย',
                    'ใช้ GROUP BY เพื่อแบ่งตามปี/เดือน',
                    'กรองข้อมูลที่ service_contact_ > 0'
                ],
                'recommended_tables': analysis['target_tables']
            }
        
        # เพิ่มข้อมูลธุรกิจที่เกี่ยวข้อง
        if 'customers' in required_data and required_data['customers']['has_customer_data']:
            context['business_data']['available_customers'] = required_data['customers']['all_customers'][:10]
        
        if 'spare_parts' in required_data and required_data['spare_parts']['has_parts_data']:
            context['business_data']['part_categories'] = required_data['spare_parts']['part_categories']
        
        if 'work_teams' in required_data and required_data['work_teams']['has_team_data']:
            context['business_data']['available_teams'] = required_data['work_teams']['available_teams']
        
        # เพิ่ม query hints เฉพาะ
        if analysis['time_range']:
            context['query_hints'].append(f"เน้นข้อมูลปี {analysis['time_range']}")
        
        if analysis['customer_mentioned']:
            context['query_hints'].append(f"เน้นลูกค้า: {analysis['customer_mentioned']}")
        
        if analysis['brand_mentioned']:
            context['query_hints'].append(f"เน้นแบรนด์: {', '.join(analysis['brand_mentioned'])}")
        
        return context


class IntelligentPromptBuilder:
    """🎨 สร้าง Prompt อัจฉริยะสำหรับ HVAC"""
    
    def __init__(self, tenant_configs):
        self.tenant_configs = tenant_configs
        logger.info("🎨 HVAC Intelligent Prompt Builder initialized")
    
    def build_contextual_prompt(self, question: str, tenant_id: str, 
                              intelligence_context: Dict[str, Any]) -> str:
        """🎯 สร้าง prompt ที่ชาญฉลาดสำหรับ HVAC"""
        
        config = self.tenant_configs.get(tenant_id)
        if not config:
            return self._create_fallback_prompt(question, tenant_id)
        
        analysis = intelligence_context.get('question_analysis', {})
        guidance = intelligence_context.get('hvac_guidance', {})
        business_data = intelligence_context.get('business_data', {})
        
        prompt_sections = []
        
        # ส่วนที่ 1: บริบทบริษัท HVAC
        prompt_sections.append(f"คุณคือ PostgreSQL Expert สำหรับธุรกิจ HVAC - {config.name}")
        prompt_sections.append("")
        
        # ส่วนที่ 2: บริบทธุรกิจ HVAC
        hvac_context = self._get_hvac_business_context(tenant_id)
        prompt_sections.append(hvac_context)
        prompt_sections.append("")
        
        # ส่วนที่ 3: โครงสร้างฐานข้อมูล HVAC
        prompt_sections.append("📊 โครงสร้างฐานข้อมูล HVAC:")
        prompt_sections.append("• sales2024, sales2023, sales2022, sales2025: ข้อมูลงานบริการรายปี")
        prompt_sections.append("  - job_no: หมายเลขงาน (SV.ปี-เดือน-ลำดับ-ประเภท)")
        prompt_sections.append("  - customer_name: ชื่อลูกค้าเต็ม") 
        prompt_sections.append("  - description: รายละเอียดงานบริการ")
        prompt_sections.append("  - service_contact_: มูลค่างาน (บาท)")
        prompt_sections.append("• spare_part, spare_part2: คลังอะไหล่")
        prompt_sections.append("  - product_code: รหัสอะไหล่, product_name: ชื่ออะไหล่")
        prompt_sections.append("  - unit_price: ราคาต่อหน่วย, balance: จำนวนคงเหลือ")
        prompt_sections.append("• work_force: การจัดทีมงาน")
        prompt_sections.append("  - date: วันที่ทำงาน, customer: ลูกค้า, service_group: ทีมช่าง")
        prompt_sections.append("")
        
        # ส่วนที่ 4: ข้อมูลธุรกิจที่เกี่ยวข้อง
        if business_data:
            prompt_sections.append("🎯 ข้อมูลธุรกิจที่เกี่ยวข้อง:")
            
            if 'available_customers' in business_data:
                customers = "', '".join(business_data['available_customers'][:5])
                prompt_sections.append(f"👥 ลูกค้าตัวอย่าง: '{customers}'")
            
            if 'part_categories' in business_data:
                categories = "', '".join(business_data['part_categories'])
                prompt_sections.append(f"🔧 ประเภทอะไหล่: '{categories}'")
            
            if 'available_teams' in business_data:
                teams = "', '".join(business_data['available_teams'][:3])
                prompt_sections.append(f"👷 ทีมงาน: '{teams}'")
            
            prompt_sections.append("")
        
        # ส่วนที่ 5: คำแนะนำเฉพาะ HVAC
        if guidance:
            prompt_sections.append("🎯 คำแนะนำเฉพาะสำหรับคำถามนี้:")
            
            if 'query_type' in guidance:
                prompt_sections.append(f"• ประเภทคำถาม: {guidance['query_type']}")
            
            if 'sql_hints' in guidance:
                for hint in guidance['sql_hints']:
                    prompt_sections.append(f"• {hint}")
            
            if 'recommended_tables' in guidance:
                tables = "', '".join(guidance['recommended_tables'])
                prompt_sections.append(f"• ตารางที่แนะนำ: '{tables}'")
            
            prompt_sections.append("")
        
        # ส่วนที่ 6: กฎสำคัญสำหรับ HVAC
        prompt_sections.append("🔧 กฎสำคัญสำหรับธุรกิจ HVAC:")
        prompt_sections.append("1. ใช้ ILIKE '%keyword%' สำหรับค้นหาลูกค้าและอะไหล่")
        prompt_sections.append("2. service_contact_ เป็นมูลค่างาน (หน่วย: บาท)")
        prompt_sections.append("3. ข้อมูลการขายแยกตามปี - เลือกตารางให้ถูกต้อง")
        prompt_sections.append("4. ใช้ UNION ALL เมื่อต้องการข้อมูลหลายปี")
        prompt_sections.append("5. product_name ในตาราง spare_part เป็นภาษาอังกฤษ")
        prompt_sections.append("6. ใช้ LIMIT 20 เสมอ")
        
        # เพิ่มกฎเฉพาะตามประเภทคำถาม
        if analysis.get('question_type') == 'counting':
            prompt_sections.append("7. ใช้ COUNT(DISTINCT customer_name) สำหรับนับลูกค้า")
        elif analysis.get('question_type') == 'spare_parts_inquiry':
            prompt_sections.append("7. ค้นหาใน product_name AND description สำหรับความครบถ้วน")
        elif analysis.get('question_type') == 'customer_history':
            prompt_sections.append("7. รวมข้อมูลจาก sales2022, sales2023, sales2024 สำหรับประวัติ")
        
        prompt_sections.append("")
        
        # ส่วนที่ 7: คำถามและคำสั่ง
        prompt_sections.append(f"❓ คำถาม: {question}")
        prompt_sections.append("")
        prompt_sections.append("สร้าง PostgreSQL query ที่เหมาะสมสำหรับธุรกิจ HVAC:")
        
        return "\n".join(prompt_sections)
    
    def _get_hvac_business_context(self, tenant_id: str) -> str:
        """🏢 Business context สำหรับ HVAC"""
        contexts = {
            'company-a': """🔧 บริบท: บริการซ่อมบำรุงระบบทำความเย็น และจำหน่ายอะไหล่
💰 ลูกค้าหลัก: โรงงานอุตสาหกรรม, โรงแรม, อาคารสำนักงาน, หน่วยงานราชการ
🛠️ บริการ: PM (Preventive Maintenance), ซ่อมแซม, เปลี่ยนชิ้นส่วน, Overhaul
🏷️ แบรนด์: Hitachi, Daikin, EuroKlimat, Toyota, Mitsubishi, York, Carrier""",

            'company-b': """🔧 บริบท: สาขาบริการ HVAC ภาคเหนือ - เชียงใหม่
💰 ลูกค้าหลัก: โรงแรมท้องถิ่น, โรงงานภาคเหนือ, ห้างสรรพสินค้า
🛠️ บริการ: PM, ซ่อมเครื่องปรับอากาศ, บริการฉุกเฉิน
🏷️ เน้นแบรนด์: Daikin, Mitsubishi, Hitachi""",

            'company-c': """🔧 บริบท: HVAC International Services - ระดับสากล  
💰 ลูกค้าหลัก: บริษัทข้ามชาติ, โรงงานอุตสาหกรรมขนาดใหญ่
🛠️ บริการ: Large scale HVAC systems, Industrial chillers
🏷️ แบรนด์ระดับสากล: York, Carrier, Trane, McQuay"""
        }
        
        return contexts.get(tenant_id, contexts['company-a'])
    
    def _create_fallback_prompt(self, question: str, tenant_id: str) -> str:
        """🔄 Fallback prompt สำหรับ HVAC"""
        
        return f"""คุณคือ PostgreSQL Expert สำหรับธุรกิจ HVAC Service & Spare Parts

🔧 บริบทธุรกิจ: บริการซ่อมบำรุงระบบทำความเย็นและจำหน่ายอะไหล่

📊 โครงสร้างฐานข้อมูล:
• sales2024, sales2023, sales2022, sales2025: ข้อมูลงานบริการ
• spare_part, spare_part2: คลังอะไหล่
• work_force: การจัดทีมงาน

🔧 กฎสำคัญ:
1. ใช้ ILIKE '%keyword%' สำหรับการค้นหา
2. service_contact_ เป็นมูลค่างาน (บาท)
3. ข้อมูลแยกตามปี - เลือกตารางให้ถูก
4. ใช้ LIMIT 20 เสมอ

คำถาม: {question}

สร้าง PostgreSQL query:"""


class EnhancedSchemaIntegration:
    """🔗 Integration class สำหรับ HVAC"""
    
    def __init__(self, database_handler, tenant_configs):
        self.schema_discovery = IntelligentSchemaDiscovery(database_handler)
        self.prompt_builder = IntelligentPromptBuilder(tenant_configs)
        logger.info("🔗 HVAC Enhanced Schema Integration initialized")
    
    async def generate_intelligent_sql_prompt(self, question: str, tenant_id: str) -> str:
        """🎯 สร้าง intelligent prompt สำหรับ HVAC"""
        
        try:
            # ขั้นตอนที่ 1: วิเคราะห์และหาข้อมูล HVAC
            hvac_context = await self.schema_discovery.get_contextual_schema(question, tenant_id)
            
            # ขั้นตอนที่ 2: สร้าง prompt สำหรับ HVAC
            hvac_prompt = self.prompt_builder.build_contextual_prompt(
                question, tenant_id, hvac_context
            )
            
            logger.info(f"✅ Generated HVAC intelligent prompt for {tenant_id}: {len(hvac_prompt)} chars")
            return hvac_prompt
            
        except Exception as e:
            logger.error(f"❌ Failed to generate HVAC intelligent prompt: {e}")
            
            # fallback สำหรับ HVAC
            return self._create_hvac_fallback_prompt(question, tenant_id)
    
    def _create_hvac_fallback_prompt(self, question: str, tenant_id: str) -> str:
        """🔄 Fallback prompt สำหรับ HVAC"""
        
        return f"""คุณคือ PostgreSQL Expert สำหรับธุรกิจ HVAC Service & Spare Parts

🔧 บริบทธุรกิจ: บริการซ่อมบำรุงระบบทำความเย็นและอะไหล่

📊 โครงสร้างฐานข้อมูล:
• sales2024: ข้อมูลงานบริการปี 2024
  - job_no: หมายเลขงาน, customer_name: ลูกค้า
  - description: รายละเอียดงาน, service_contact_: มูลค่า (บาท)
• spare_part: คลังอะไหล่
  - product_code: รหัส, product_name: ชื่อ, unit_price: ราคา
• work_force: ทีมงาน
  - date: วันที่, customer: ลูกค้า, service_group: ทีม

🔧 กฎสำคัญ:
1. ใช้ ILIKE '%keyword%' สำหรับการค้นหา
2. service_contact_ เป็นมูลค่างาน (บาท)  
3. ใช้ LIMIT 20 เสมอ
4. ข้อมูลแยกตามปี

คำถาม: {question}

สร้าง PostgreSQL query ที่เหมาะสมสำหรับธุรกิจ HVAC:"""