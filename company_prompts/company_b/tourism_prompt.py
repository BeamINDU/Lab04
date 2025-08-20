import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from company_prompts.base_prompt import BaseCompanyPrompt
from typing import Dict, Any, List
from datetime import datetime
from shared_components.logging_config import logger

class SimpleTourismPrompt(BaseCompanyPrompt):
    """🏨 FIXED Tourism Prompt - Compatible with BaseCompanyPrompt"""
    
    def __init__(self, company_config: Dict[str, Any]):
        # 🔧 Initialize parent class first
        super().__init__(company_config)
        
        # 🎯 Tourism data - moved AFTER super().__init__()
        self.tourism_data = {
            'keywords': {
                'accommodation': ['โรงแรม', 'รีสอร์ท', 'hotel', 'resort', 'ที่พัก'],
                'tourism': ['ท่องเที่ยว', 'tourism', 'TAT', 'สถานที่ท่องเที่ยว'],
                'food': ['ร้านอาหาร', 'restaurant', 'อาหาร', 'ครัว'],
                'culture': ['วัฒนธรรม', 'ล้านนา', 'lanna', 'ประเพณี'],
                'regional': ['เชียงใหม่', 'ภาคเหนือ', 'northern']
            },
            'clients': [
                'โรงแรมดุสิต เชียงใหม่',
                'การท่องเที่ยวแห่งประเทศไทย', 
                'สวนพฤกษศาสตร์เชียงใหม่',
                'กลุ่มร้านอาหารล้านนา',
                'มหาวิทยาลัยเชียงใหม่'
            ],
            'culture': {
                'greeting': 'สวัสดีเจ้า',
                'foods': ['ข้าวซอย', 'แกงฮังเล', 'ไส้อั่ว', 'น้ำพริกน้ำปู'],
                'values': ['น้ำใจเหนือ', 'ความเป็นมิตร', 'การต้อนรับแบบล้านนา'],
                'festivals': ['สงกรานต์', 'ลอยกระทง', 'ยี่เป็ง', 'บุญบั้งไฟ']
            },
            'budget_ranges': {
                'small': 'budget < 400000',
                'medium': 'budget BETWEEN 400000 AND 600000', 
                'large': 'budget > 600000'
            }
        }
        
        logger.info(f"🏨 SimpleTourismPrompt initialized for {self.company_name}")
    
    # ========================================================================
    # 🎯 REQUIRED METHODS from BaseCompanyPrompt
    # ========================================================================
    
    async def process_question(self, question: str) -> Dict[str, Any]:
        """🎯 Main processing method"""
        
        try:
            self.usage_stats['queries_processed'] += 1
            self.usage_stats['last_used'] = datetime.now().isoformat()
            
            if self._is_greeting(question):
                return self._create_tourism_greeting()
            elif self._is_tourism_query(question):
                return self._create_tourism_response(question)
            else:
                return self._create_general_response(question)
                
        except Exception as e:
            logger.error(f"❌ Tourism processing failed: {e}")
            return {
                'success': False,
                'answer': f"เกิดข้อผิดพลาด: {str(e)}",
                'error': str(e),
                'tenant_id': self.company_id
            }
    
    def generate_sql_prompt(self, question: str, schema_info: Dict[str, Any]) -> str:
        """🎯 Generate tourism SQL prompt"""
        
        tourism_type = self._detect_tourism_type(question)
        cultural_hint = self._get_cultural_hint(tourism_type)
        
        prompt = f"""คุณคือนักวิเคราะห์ท่องเที่ยวเชี่ยวชาญสำหรับ {self.company_name}

🏨 บริบทธุรกิจ: เทคโนโลยีท่องเที่ยวและการต้อนรับ ภาคเหนือ
🌿 ลูกค้าหลัก: {', '.join(self.tourism_data['clients'][:3])}
💰 งบประมาณ: 300,000 - 800,000 บาท

📊 โครงสร้างฐานข้อมูล:
• employees: id, name, department, position, salary, hire_date, email
• projects: id, name, client, budget, status, start_date, end_date, temp_stack
• employee_projects: employee_id, project_id, role, allocation

🎭 บริบทวัฒนธรรม: {cultural_hint}

คำถาม: {question}

สร้าง PostgreSQL query เฉพาะท่องเที่ยวภาคเหนือ:"""

        return prompt
    
    def format_response(self, question: str, results: List[Dict], metadata: Dict) -> str:
        """🎨 Format tourism response"""
        
        if not results:
            return f"ไม่พบข้อมูลท่องเที่ยวที่เกี่ยวข้องกับ: {question}"
        
        response = f"🏨 ข้อมูลท่องเที่ยวภาคเหนือ - {self.company_name}\n\n"
        
        for i, row in enumerate(results[:10], 1):
            response += f"{i:2d}. "
            for key, value in row.items():
                if 'budget' in key.lower() and isinstance(value, (int, float)):
                    response += f"{key}: {value:,.0f} บาท, "
                elif 'client' in key.lower() and value:
                    icon = self._get_tourism_icon(value)
                    response += f"{key}: {value}{icon}, "
                else:
                    response += f"{key}: {value}, "
            response = response.rstrip(', ') + "\n"
        
        response += f"\n🌿 ข้อมูลเชิงลึก: พบ {len(results)} รายการจากระบบท่องเที่ยวภาคเหนือ"
        
        return response
    
    def _load_business_rules(self) -> Dict[str, Any]:
        """📋 Tourism business rules"""
        return {
            'focus': 'tourism_hospitality_northern_thailand',
            'budget_ranges': self.tourism_data['budget_ranges'],
            'primary_clients': self.tourism_data['clients'][:3]
        }
    
    def _load_schema_mappings(self) -> Dict[str, Any]:
        """🗄️ Tourism schema mappings"""
        return {
            'main_tables': ['employees', 'projects', 'employee_projects'],
            'tourism_keywords': self.tourism_data['keywords']
        }
    
    # ========================================================================
    # 🔧 HELPER METHODS
    # ========================================================================
    
    def _is_greeting(self, question: str) -> bool:
        greetings = ['สวัสดี', 'hello', 'hi', 'เจ้า', 'ช่วย']
        return any(word in question.lower() for word in greetings)
    
    def _is_tourism_query(self, question: str) -> bool:
        question_lower = question.lower()
        all_keywords = []
        for category in self.tourism_data['keywords'].values():
            all_keywords.extend(category)
        return any(keyword in question_lower for keyword in all_keywords)
    
    def _detect_tourism_type(self, question: str) -> str:
        question_lower = question.lower()
        for tourism_type, keywords in self.tourism_data['keywords'].items():
            if any(keyword in question_lower for keyword in keywords):
                return tourism_type
        return 'general'
    
    def _get_cultural_hint(self, tourism_type: str) -> str:
        culture_hints = {
            'accommodation': f"โรงแรมล้านนา, {self.tourism_data['culture']['values'][0]}",
            'food': f"อาหารเหนือ: {', '.join(self.tourism_data['culture']['foods'][:2])}",
            'culture': f"ประเพณีล้านนา: {', '.join(self.tourism_data['culture']['festivals'][:2])}",
            'general': f"ท่องเที่ยวภาคเหนือ, {self.tourism_data['culture']['values'][0]}"
        }
        return culture_hints.get(tourism_type, culture_hints['general'])
    
    def _get_tourism_icon(self, client_name: str) -> str:
        client_lower = client_name.lower()
        if any(word in client_lower for word in ['โรงแรม', 'hotel']):
            return ' 🏨'
        elif any(word in client_lower for word in ['ท่องเที่ยว', 'tourism']):
            return ' ✈️'
        elif any(word in client_lower for word in ['ร้านอาหาร', 'restaurant']):
            return ' 🍜'
        elif any(word in client_lower for word in ['สวน', 'garden']):
            return ' 🌿'
        return ''
    
    def _create_tourism_greeting(self) -> Dict[str, Any]:
        answer = f"""{self.tourism_data['culture']['greeting']}! ผมคือ AI Assistant สำหรับ {self.company_name}

🏨 เราเชี่ยวชาญด้านเทคโนโลยีท่องเที่ยวภาคเหนือ:
• ระบบโรงแรมและรีสอร์ท
• แอปพลิเคชันท่องเที่ยว
• ระบบร้านอาหารและ POS

🌿 ลูกค้าหลัก: {', '.join(self.tourism_data['clients'][:3])}

{self.tourism_data['culture']['values'][0]} - มีอะไรให้ช่วยไหมครับ?"""
        
        return {
            'success': True,
            'answer': answer,
            'sql_query': None,
            'data_source_used': f'tourism_greeting_{self.model}',
            'tenant_id': self.company_id
        }
    
    def _create_tourism_response(self, question: str) -> Dict[str, Any]:
        tourism_type = self._detect_tourism_type(question)
        
        responses = {
            'accommodation': f"""🏨 โปรเจคโรงแรมและที่พัก:

1. ระบบจัดการโรงแรม - โรงแรมดุสิต เชียงใหม่
   • งบประมาณ: 800,000 บาท
   • เทคโนโลยี: Vue.js, Firebase

🌿 เน้นการบริการแบบล้านนา""",

            'tourism': f"""✈️ โปรเจคท่องเที่ยว:

1. เว็บไซต์ท่องเที่ยว - การท่องเที่ยวแห่งประเทศไทย
   • งบประมาณ: 600,000 บาท  
   • เทคโนโลยี: React, Firebase

🎭 วัฒนธรรมล้านนา: {', '.join(self.tourism_data['culture']['festivals'][:2])}"""
        }
        
        answer = responses.get(tourism_type, f"""🌿 ข้อมูลท่องเที่ยวภาคเหนือสำหรับ: {question}

เกี่ยวกับ {self.company_name}: เทคโนโลยีท่องเที่ยวและการต้อนรับ""")
        
        return {
            'success': True,
            'answer': answer,
            'sql_query': None,
            'data_source_used': f'tourism_{tourism_type}_{self.model}',
            'tenant_id': self.company_id
        }
    
    def _create_general_response(self, question: str) -> Dict[str, Any]:
        answer = f"""🏨 ระบบท่องเที่ยวภาคเหนือ

คำถาม: {question}

เกี่ยวกับ {self.company_name}: เชี่ยวชาญเทคโนโลยีท่องเที่ยว

💡 ลองถามเกี่ยวกับ: โรงแรม, ท่องเที่ยว, ร้านอาหาร"""
        
        return {
            'success': True,
            'answer': answer,
            'sql_query': None,
            'data_source_used': f'tourism_general_{self.model}',
            'tenant_id': self.company_id
        }

# Create alias for compatibility  
TourismPrompt = SimpleTourismPrompt