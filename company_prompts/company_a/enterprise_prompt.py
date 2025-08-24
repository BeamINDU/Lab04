# company_prompts/company_a/enterprise_prompt.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from company_prompts.base_prompt import BaseCompanyPrompt
from typing import Dict, Any, List
from datetime import datetime

# Import shared logger
from shared_components.logging_config import logger

class EnterprisePrompt(BaseCompanyPrompt):
    """🔧 HVAC Service & Spare Parts Business Prompt - ปรับปรุงใหม่"""
    
    def __init__(self, company_config: Dict[str, Any]):
        super().__init__(company_config)
        
        # เพิ่มความรู้เฉพาะทางสำหรับธุรกิจใหม่
        self.business_domain_knowledge = {
            'hvac_terms': {
                'PM': 'Preventive Maintenance - การบำรุงรักษาป้องกัน',
                'Chiller': 'เครื่องทำน้ำเย็น',
                'Compressor': 'คอมเพรสเซอร์',
                'Overhaul': 'การยกเครื่อง/ซ่อมแซมครั้งใหญ่',
                'Replacement': 'การเปลี่ยนชิ้นส่วน',
                'Air Cooled': 'ระบายความร้อนด้วยอากาศ',
                'Water Cooled': 'ระบายความร้อนด้วยน้ำ',
                'RCUA': 'Roof Top Unit Air Cooled',
                'Set Free': 'ระบบปรับอากาศแบบ VRF',
                'FTG': 'Floor Type Gas (แอร์แบบตั้งพื้น)',
                'EKAC': 'EuroKlimat Air Cooled model'
            },
            'service_types': {
                'PM': 'งานบำรุงรักษาตามแผน',
                'Repair': 'งานซ่อมแซม',
                'Replacement': 'งานเปลี่ยนชิ้นส่วน',
                'Overhaul': 'งานยกเครื่อง',
                'Installation': 'งานติดตั้ง',
                'Start_up': 'งานเริ่มใช้งาน'
            },
            'customer_types': {
                'Industrial': 'โรงงานอุตสาหกรรม',
                'Commercial': 'อาคารพาณิชย์',
                'Government': 'หน่วยงานราชการ',
                'Hotel': 'โรงแรม',
                'Hospital': 'โรงพยาบาล',
                'Office': 'อาคารสำนักงาน'
            },
            'brands': ['Hitachi', 'Daikin', 'EuroKlimat', 'Toyota', 'Mitsubishi', 'York', 'Carrier']
        }
        
        # Sample queries สำหรับการเรียนรู้
        self.sample_business_queries = {
            'customer_analysis': {
                'question': 'จำนวนลูกค้าทั้งหมด',
                'sql_pattern': 'SELECT COUNT(DISTINCT customer_name) FROM sales2024',
                'business_context': 'นับลูกค้าที่ไม่ซ้ำกันจากข้อมูลการขาย'
            },
            'customer_history': {
                'question': 'บริษัท XXX มีการซื้อขายย้อนหลัง 3 ปี',
                'sql_pattern': '''
                    SELECT job_no, description, service_contact_ as value
                    FROM sales2022 WHERE customer_name ILIKE '%company_name%'
                    UNION ALL
                    SELECT job_no, description, service_contact_ as value  
                    FROM sales2023 WHERE customer_name ILIKE '%company_name%'
                    UNION ALL
                    SELECT job_no, description, service_contact_ as value
                    FROM sales2024 WHERE customer_name ILIKE '%company_name%'
                ''',
                'business_context': 'หาประวัติการซื้อขายของลูกค้าเฉพาะรายข้าม 3 ปี'
            },
            'work_schedule': {
                'question': 'แผนงานวันที่ XXX มีงานอะไรบ้าง',
                'sql_pattern': '''
                    SELECT customer, detail, service_group, 
                           CASE WHEN job_description_pm THEN 'PM'
                                WHEN job_description_replacement THEN 'Replacement'
                                ELSE 'Other' END as job_type
                    FROM work_force WHERE date = 'target_date'
                ''',
                'business_context': 'ดูแผนการทำงานของทีมในวันที่กำหนด'
            },
            'spare_parts_pricing': {
                'question': 'ราคาอะไหล่เครื่องทำน้ำเย็น Hitachi',
                'sql_pattern': '''
                    SELECT product_code, product_name, unit_price, description
                    FROM spare_part 
                    WHERE product_name ILIKE '%หาคำสำคัญ%' 
                    OR description ILIKE '%Hitachi%'
                ''',
                'business_context': 'ค้นหาราคาอะไหล่ตามยี่ห้อและรุ่น'
            }
        }
        
        logger.info(f"✅ HVAC Enterprise Prompt initialized for {self.company_name}")
    
    # ✅ MAIN ENTRY POINT (Required by PromptManager)
    async def process_question(self, question: str) -> Dict[str, Any]:
        """🎯 Main processing method สำหรับธุรกิจ HVAC"""
        
        try:
            self.usage_stats['queries_processed'] += 1
            self.usage_stats['last_used'] = datetime.now().isoformat()
            
            # วิเคราะห์ประเภทคำถามตามธุรกิจใหม่
            question_type = self._analyze_hvac_question(question)
            
            if question_type == 'greeting':
                return self._create_hvac_greeting_response()
            elif question_type == 'customer_inquiry':
                return self._create_customer_response(question)
            elif question_type == 'spare_parts_inquiry':
                return self._create_spare_parts_response(question)
            elif question_type == 'service_inquiry':
                return self._create_service_response(question)
            elif question_type == 'work_schedule_inquiry':
                return self._create_work_schedule_response(question)
            else:
                return self._create_general_hvac_response(question)
                
        except Exception as e:
            logger.error(f"❌ HVAC processing failed: {e}")
            return {
                'success': False,
                'answer': f"เกิดข้อผิดพลาดในการประมวลผล: {str(e)}",
                'error': str(e),
                'tenant_id': self.company_id,
                'data_source_used': 'hvac_error'
            }
    
    def _analyze_hvac_question(self, question: str) -> str:
        """🔍 วิเคราะห์ประเภทคำถามสำหรับธุรกิจ HVAC"""
        question_lower = question.lower()
        
        # Greeting patterns
        if any(word in question_lower for word in ['สวัสดี', 'hello', 'hi', 'ช่วย']):
            return 'greeting'
        
        # Customer inquiry patterns
        if any(word in question_lower for word in ['ลูกค้า', 'บริษัท', 'customer', 'ประวัติ', 'history']):
            return 'customer_inquiry'
        
        # Spare parts inquiry patterns  
        if any(word in question_lower for word in ['อะไหล่', 'ราคา', 'spare', 'part', 'price', 'chiller', 'compressor']):
            return 'spare_parts_inquiry'
        
        # Service inquiry patterns
        if any(word in question_lower for word in ['บริการ', 'ซ่อม', 'บำรุง', 'service', 'overhaul', 'pm']):
            return 'service_inquiry'
        
        # Work schedule patterns
        if any(word in question_lower for word in ['แผนงาน', 'วันที่', 'schedule', 'งาน', 'ทีม']):
            return 'work_schedule_inquiry'
        
        return 'general'
    
    # ✅ ABSTRACT METHODS (Required by BaseCompanyPrompt)
    def generate_sql_prompt(self, question: str, schema_info: Dict[str, Any]) -> str:
        """🎯 SQL prompt generation สำหรับธุรกิจ HVAC"""
        
        # วิเคราะห์คำถามเพื่อหาความเกี่ยวข้องกับธุรกิจ
        relevant_terms = self._extract_hvac_terms(question)
        relevant_sample = self._find_relevant_sample_query(question)
        
        prompt = f"""คุณคือ PostgreSQL Expert สำหรับระบบบริหารงาน HVAC Service & Spare Parts - {self.company_name}

🔧 บริบทธุรกิจ: บริการซ่อมบำรุงระบบทำความเย็น และจำหน่ายอะไหล่
💰 ลูกค้าหลัก: โรงงานอุตสาหกรรม, โรงแรม, อาคารสำนักงาน, หน่วยงานราชการ
🛠️ บริการหลัก: PM (Preventive Maintenance), ซ่อมแซม, เปลี่ยนชิ้นส่วน, Overhaul
🏷️ แบรนด์หลัก: {', '.join(self.business_domain_knowledge['brands'])}

📊 โครงสร้างฐานข้อมูล:
• sales2024, sales2023, sales2022, sales2025: ข้อมูลการขายและบริการรายปี
  - job_no: หมายเลขงาน (รูปแบบ SV.66-XX-XXX-PM)
  - customer_name: ชื่อลูกค้า
  - description: รายละเอียดงาน
  - service_contact_: มูลค่างาน (บาท)
  - overhaul_, replacement: ประเภทงาน
  
• spare_part, spare_part2: คลังอะไหล่
  - product_code: รหัสสินค้า
  - product_name: ชื่ออะไหล่ (ภาษาอังกฤษ)
  - unit_price: ราคาต่อหน่วย
  - balance: จำนวนคงเหลือ
  - description: รายละเอียด (รุ่นเครื่อง, สถานที่)
  
• work_force: การจัดทีมงาน
  - date: วันที่ทำงาน
  - customer: ลูกค้า
  - job_description_pm, job_description_replacement: ประเภทงาน (boolean)
  - detail: รายละเอียดงาน
  - service_group: ทีมช่าง
"""

        # เพิ่มความรู้เฉพาะทางที่เกี่ยวข้อง
        if relevant_terms:
            prompt += f"\n🔍 คำศัพท์เทคนิคที่เกี่ยวข้อง:\n"
            for term, meaning in relevant_terms.items():
                prompt += f"• {term}: {meaning}\n"
        
        # เพิ่ม sample query ที่เกี่ยวข้อง
        if relevant_sample:
            prompt += f"\n💡 รูปแบบ SQL ที่คล้ายกัน:\n{relevant_sample['sql_pattern']}\n"
            prompt += f"บริบท: {relevant_sample['business_context']}\n"
        
        prompt += f"""
🔧 กฎสำคัญสำหรับธุรกิจ HVAC:
1. ใช้ ILIKE '%keyword%' สำหรับการค้นหาชื่อลูกค้าและอะไหล่
2. ข้อมูลการขายแยกตามปี - ระวังเลือกตารางที่ถูกต้อง
3. service_contact_ เป็นมูลค่างาน (หน่วย: บาท)
4. job_no มีรูปแบบ SV.ปี-เดือน-ลำดับ-ประเภท
5. product_name ในตาราง spare_part เป็นภาษาอังกฤษ
6. ใช้ UNION ALL เมื่อต้องการข้อมูลข้ามหลายปี
7. ใช้ LIMIT 20 เสมอ

คำถาม: {question}

สร้าง PostgreSQL query ที่เหมาะสมสำหรับธุรกิจ HVAC:"""

        return prompt
    
    def _extract_hvac_terms(self, question: str) -> Dict[str, str]:
        """🔍 ดึงคำศัพท์เทคนิค HVAC ที่เกี่ยวข้องจากคำถาม"""
        question_lower = question.lower()
        relevant_terms = {}
        
        # ตรวจสอบคำศัพท์เทคนิค
        for term, meaning in self.business_domain_knowledge['hvac_terms'].items():
            if term.lower() in question_lower:
                relevant_terms[term] = meaning
        
        # ตรวจสอบประเภทบริการ
        for service_type, meaning in self.business_domain_knowledge['service_types'].items():
            if service_type.lower() in question_lower:
                relevant_terms[service_type] = meaning
        
        return relevant_terms
    
    def _find_relevant_sample_query(self, question: str) -> Dict[str, str]:
        """🔍 หา sample query ที่เกี่ยวข้องกับคำถาม"""
        question_lower = question.lower()
        
        # ตรวจสอบประเภทคำถาม
        if any(word in question_lower for word in ['จำนวน', 'ลูกค้า', 'count', 'customer']):
            return self.sample_business_queries['customer_analysis']
        
        elif any(word in question_lower for word in ['ประวัติ', 'ย้อนหลัง', 'history']):
            return self.sample_business_queries['customer_history']
        
        elif any(word in question_lower for word in ['แผนงาน', 'วันที่', 'schedule']):
            return self.sample_business_queries['work_schedule']
        
        elif any(word in question_lower for word in ['ราคา', 'อะไหล่', 'price', 'spare']):
            return self.sample_business_queries['spare_parts_pricing']
        
        return None
    
    def format_response(self, question: str, results: List[Dict], metadata: Dict) -> str:
        """🎨 Format response สำหรับธุรกิจ HVAC"""
        
        if not results:
            return f"ไม่พบข้อมูลที่ตรงกับคำถาม: {question}"
        
        response = f"📊 ผลการค้นหาระบบ HVAC Service - {self.company_name}\n\n"
        
        # จัดรูปแบบตามประเภทข้อมูล
        question_lower = question.lower()
        
        if any(word in question_lower for word in ['ลูกค้า', 'customer']):
            response += self._format_customer_results(results)
        elif any(word in question_lower for word in ['อะไหล่', 'spare', 'ราคา']):
            response += self._format_spare_parts_results(results)
        elif any(word in question_lower for word in ['งาน', 'แผน', 'work']):
            response += self._format_work_results(results)
        else:
            response += self._format_general_results(results)
        
        response += f"\n\n📈 สรุป: พบข้อมูล {len(results)} รายการ"
        
        if metadata.get('tenant_id'):
            response += f" | ฐานข้อมูล: {metadata['tenant_id']}"
        
        return response
    
    def _format_customer_results(self, results: List[Dict]) -> str:
        """จัดรูปแบบผลลัพธ์ข้อมูลลูกค้า"""
        formatted = "👥 ข้อมูลลูกค้า:\n"
        for i, row in enumerate(results[:10], 1):
            customer = row.get('customer_name', row.get('customer', 'ไม่ระบุ'))
            value = row.get('service_contact_', row.get('value', 0))
            job = row.get('job_no', row.get('detail', ''))
            
            formatted += f"{i}. {customer}\n"
            if job:
                formatted += f"   งาน: {job}\n"
            if value and str(value).isdigit():
                formatted += f"   มูลค่า: {int(value):,} บาท\n"
            formatted += "\n"
        return formatted
    
    def _format_spare_parts_results(self, results: List[Dict]) -> str:
        """จัดรูปแบบผลลัพธ์อะไหล่"""
        formatted = "🔧 รายการอะไหล่:\n"
        for i, row in enumerate(results[:10], 1):
            code = row.get('product_code', '')
            name = row.get('product_name', '')
            price = row.get('unit_price', '0')
            balance = row.get('balance', 0)
            
            formatted += f"{i}. {code} - {name}\n"
            if price and price != '0':
                formatted += f"   ราคา: {price} บาท"
                if balance:
                    formatted += f" | คงเหลือ: {balance} ชิ้น"
                formatted += "\n"
            formatted += "\n"
        return formatted
    
    def _format_work_results(self, results: List[Dict]) -> str:
        """จัดรูปแบบผลลัพธ์งาน"""
        formatted = "📋 รายการงาน:\n"
        for i, row in enumerate(results[:10], 1):
            customer = row.get('customer', row.get('customer_name', ''))
            detail = row.get('detail', row.get('description', ''))
            team = row.get('service_group', '')
            date = row.get('date', '')
            
            formatted += f"{i}. {customer}\n"
            if detail:
                formatted += f"   งาน: {detail}\n"
            if team:
                formatted += f"   ทีม: {team}\n"
            if date:
                formatted += f"   วันที่: {date}\n"
            formatted += "\n"
        return formatted
    
    def _format_general_results(self, results: List[Dict]) -> str:
        """จัดรูปแบบผลลัพธ์ทั่วไป"""
        formatted = "📊 ผลลัพธ์:\n"
        for i, row in enumerate(results[:10], 1):
            formatted += f"{i}. "
            formatted += " | ".join([f"{k}: {v}" for k, v in row.items() if v is not None])
            formatted += "\n"
        return formatted
    
    # Helper methods สำหรับการตอบสนองแต่ละประเภท
    def _create_hvac_greeting_response(self) -> Dict[str, Any]:
        return {
            'success': True,
            'answer': f"""สวัสดีครับ! ผมคือ AI Assistant สำหรับระบบ HVAC Service & Spare Parts ของ {self.company_name}

🔧 ผมสามารถช่วยคุณ:
• ค้นหาข้อมูลลูกค้าและประวัติการบริการ
• สืบค้นราคาและสต็อกอะไหล่
• ดูแผนงานและการจัดทีม
• วิเคราะห์ยอดขายและรายงาน

💡 ตัวอย่างคำถาม:
- "จำนวนลูกค้าทั้งหมด"
- "ราคาอะไหล่ Hitachi chiller"
- "แผนงานวันที่ 15/06/2568"
- "ยอดขาย overhaul ปี 2567"

คุณต้องการสอบถามอะไรครับ?""",
            'tenant_id': self.company_id,
            'data_source_used': 'hvac_greeting'
        }
    
    def _create_customer_response(self, question: str) -> Dict[str, Any]:
        return {
            'success': True,
            'answer': f"กำลังค้นหาข้อมูลลูกค้าตามคำถาม: {question}\nกรุณารอสักครู่...",
            'tenant_id': self.company_id,
            'data_source_used': 'hvac_customer_query',
            'needs_sql_execution': True
        }
    
    def _create_spare_parts_response(self, question: str) -> Dict[str, Any]:
        return {
            'success': True,
            'answer': f"กำลังค้นหาข้อมูลอะไหล่ตามคำถาม: {question}\nกรุณารอสักครู่...",
            'tenant_id': self.company_id,
            'data_source_used': 'hvac_spare_parts_query',
            'needs_sql_execution': True
        }
    
    def _create_service_response(self, question: str) -> Dict[str, Any]:
        return {
            'success': True,
            'answer': f"กำลังค้นหาข้อมูลบริการตามคำถาม: {question}\nกรุณารอสักครู่...",
            'tenant_id': self.company_id,
            'data_source_used': 'hvac_service_query', 
            'needs_sql_execution': True
        }
    
    def _create_work_schedule_response(self, question: str) -> Dict[str, Any]:
        return {
            'success': True,
            'answer': f"กำลังค้นหาแผนงานตามคำถาม: {question}\nกรุณารอสักครู่...",
            'tenant_id': self.company_id,
            'data_source_used': 'hvac_work_schedule_query',
            'needs_sql_execution': True
        }
    
    def _create_general_hvac_response(self, question: str) -> Dict[str, Any]:
        return {
            'success': True,
            'answer': f"กำลังประมวลผลคำถามสำหรับระบบ HVAC: {question}\nกรุณารอสักครู่...",
            'tenant_id': self.company_id,
            'data_source_used': 'hvac_general_query',
            'needs_sql_execution': True
        }