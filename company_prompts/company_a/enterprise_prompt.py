import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from company_prompts.base_prompt import BaseCompanyPrompt
from typing import Dict, Any, List
from datetime import datetime

# Import shared logger
from shared_components.logging_config import logger

class EnterprisePrompt(BaseCompanyPrompt):
    """🏦 Simple Enterprise Banking Prompt"""
    
    def __init__(self, company_config: Dict[str, Any]):
        super().__init__(company_config)
        logger.info(f"✅ Simple EnterprisePrompt initialized for {self.company_name}")
    
    # ✅ MAIN ENTRY POINT (Required by PromptManager)
    async def process_question(self, question: str) -> Dict[str, Any]:
        """🎯 Simple main processing method"""
        
        try:
            self.usage_stats['queries_processed'] += 1
            self.usage_stats['last_used'] = datetime.now().isoformat()
            
            # Simple logic: detect if it's a greeting or data query
            if self._is_greeting(question):
                return self._create_greeting_response()
            elif self._is_data_query(question):
                return self._create_data_response(question)
            else:
                return self._create_general_response(question)
                
        except Exception as e:
            logger.error(f"❌ Enterprise processing failed: {e}")
            return {
                'success': False,
                'answer': f"เกิดข้อผิดพลาด: {str(e)}",
                'error': str(e),
                'tenant_id': self.company_id,
                'data_source_used': 'enterprise_error'
            }
    
    # ✅ ABSTRACT METHODS (Required by BaseCompanyPrompt)
    def generate_sql_prompt(self, question: str, schema_info: Dict[str, Any]) -> str:
        """🎯 Simple SQL prompt generation"""
        
        prompt = f"""คุณคือนักวิเคราะห์ระบบ Enterprise Banking สำหรับ {self.company_name}

🏢 บริบทธุรกิจ: ระบบธนาคารและองค์กรขนาดใหญ่
💰 งบประมาณโปรเจค: 800,000 - 3,000,000+ บาท
🎯 ลูกค้าหลัก: ธนาคาร, บริษัทใหญ่, E-commerce

📊 โครงสร้างฐานข้อมูล:
• employees: id, name, department, position, salary, hire_date, email
• projects: id, name, client, budget, status, start_date, end_date, temp_stack
• employee_projects: employee_id, project_id, role, allocation

🔧 กฎ SQL สำหรับ Enterprise:
1. ใช้ ILIKE '%keyword%' สำหรับการค้นหา (ไม่สนใจตัวใหญ่เล็ก)
2. ใช้ LIMIT 20 เสมอ
3. ไม่ใช้ COALESCE ที่ซับซ้อน
4. เขียน SQL ให้เรียบง่าย

คำถาม: {question}

สร้าง PostgreSQL query:"""

        return prompt
    
    def format_response(self, question: str, results: List[Dict], metadata: Dict) -> str:
        """🎨 Simple response formatting"""
        
        if not results:
            return f"ไม่พบข้อมูลที่ตรงกับคำถาม: {question}"
        
        response = f"📊 ผลการวิเคราะห์ระบบ Enterprise - {self.company_name}\n\n"
        
        # Display results (simple format)
        for i, row in enumerate(results[:10], 1):
            response += f"{i}. "
            for key, value in row.items():
                if 'salary' in key or 'budget' in key:
                    response += f"{key}: {value:,.0f} บาท, "
                else:
                    response += f"{key}: {value}, "
            response = response.rstrip(', ') + "\n"
        
        response += f"\n💡 สรุป: พบข้อมูล {len(results)} รายการจากระบบ Enterprise"
        
        return response
    
    def _load_business_rules(self) -> Dict[str, Any]:
        """📋 Simple business rules"""
        return {
            'salary_ranges': {'junior': '35k-50k', 'senior': '60k-100k', 'lead': '80k-150k'},
            'project_budgets': {'small': '<1M', 'medium': '1M-2.5M', 'large': '>2.5M'},
            'focus': 'banking_and_enterprise_systems'
        }
    
    def _load_schema_mappings(self) -> Dict[str, Any]:
        """🗄️ Simple schema mappings"""
        return {
            'main_tables': ['employees', 'projects', 'employee_projects'],
            'key_fields': ['salary', 'budget', 'status']
        }
    
    # ✅ SIMPLE HELPER METHODS (เพียง 3 ตัว)
    def _is_greeting(self, question: str) -> bool:
        """Check if question is a greeting"""
        greetings = ['สวัสดี', 'hello', 'hi', 'ช่วย', 'help', 'คุณคือใคร']
        return any(word in question.lower() for word in greetings)
    
    def _is_data_query(self, question: str) -> bool:
        """Check if question asks for data"""
        data_words = ['พนักงาน', 'โปรเจค', 'project', 'employee', 'กี่คน', 'จำนวน', 'มีอะไร', 'ธนาคาร']
        return any(word in question.lower() for word in data_words)
    
    def _create_greeting_response(self) -> Dict[str, Any]:
        """Simple greeting response"""
        answer = f"""สวัสดีครับ! ผมคือ AI Assistant สำหรับ {self.company_name}

🏦 เราเชี่ยวชาญด้าน Enterprise Software Development:
• ระบบธนาคารและการเงิน
• E-commerce และ CRM ขนาดใหญ่
• โปรเจคงบประมาณหลายล้านบาท

🎯 ตัวอย่างคำถาม:
• "มีพนักงานกี่คนในแต่ละแผนก"
• "โปรเจคไหนมีงบประมาณสูงสุด"
• "โปรเจคธนาคารมีอะไรบ้าง"

มีอะไรให้ช่วยวิเคราะห์ไหมครับ?"""
        
        return {
            'success': True,
            'answer': answer,
            'sql_query': None,
            'data_source_used': f'enterprise_greeting_{self.model}',
            'tenant_id': self.company_id
        }
    
    def _create_data_response(self, question: str) -> Dict[str, Any]:
        """Simple data response with mock data"""
        
        # Mock data based on question keywords
        if 'ธนาคาร' in question or 'banking' in question.lower():
            answer = """📊 โปรเจคธนาคารของเรา:

1. ระบบ CRM สำหรับธนาคาร
   • ลูกค้า: ธนาคารกรุงเทพ
   • งบประมาณ: 3,000,000 บาท
   • สถานะ: กำลังดำเนินการ

2. Mobile Banking App
   • ลูกค้า: ธนาคารไทยพาณิชย์
   • งบประมาณ: 2,500,000 บาท
   • สถานะ: กำลังดำเนินการ

💡 เราเป็นผู้เชี่ยวชาญระบบธนาคารชั้นนำ"""

        elif 'พนักงาน' in question or 'employee' in question.lower():
            answer = """📊 ข้อมูลพนักงาน Enterprise:

1. แผนก IT: 10 คน (เงินเดือนเฉลี่ย 75,000 บาท)
2. แผนก Sales: 3 คน (เงินเดือนเฉลี่ย 65,000 บาท)
3. แผนก Management: 2 คน (เงินเดือนเฉลี่ย 120,000 บาท)

💡 ทีมงานระดับ Senior มีประสบการณ์สูง"""

        elif 'โปรเจค' in question or 'project' in question.lower():
            answer = """📊 โปรเจค Enterprise ปัจจุบัน:

1. ระบบ CRM สำหรับธนาคาร (3M บาท)
2. AI Chatbot E-commerce (1.2M บาท)
3. Mobile Banking App (2.5M บาท)
4. เว็บไซต์ E-learning (800K บาท)

💡 รวมมูลค่าโปรเจค 7.5 ล้านบาท"""

        else:
            answer = f"""📊 ข้อมูล Enterprise สำหรับ: {question}

เกี่ยวกับ {self.company_name}:
• เป็นผู้เชี่ยวชาญระบบองค์กรขนาดใหญ่
• มีโปรเจคธนาคารและ E-commerce
• ทีมงาน 15+ คน ระดับ Senior

💡 กรุณาถามคำถามที่เฉพาะเจาะจงมากขึ้น"""
        
        return {
            'success': True,
            'answer': answer,
            'sql_query': None,
            'data_source_used': f'enterprise_data_{self.model}',
            'tenant_id': self.company_id
        }
    
    def _create_general_response(self, question: str) -> Dict[str, Any]:
        """Simple general response"""
        answer = f"""🏦 Enterprise Analysis System

คำถาม: {question}

เกี่ยวกับ {self.company_name}:
• เชี่ยวชาญระบบธนาคารและองค์กร
• เทคโนโลยี: React, Node.js, AWS
• ลูกค้า: ธนาคาร, บริษัทใหญ่

💡 ลองถามเกี่ยวกับพนักงาน โปรเจค หรือระบบธนาคาร"""
        
        return {
            'success': True,
            'answer': answer,
            'sql_query': None,
            'data_source_used': f'enterprise_general_{self.model}',
            'tenant_id': self.company_id
        }