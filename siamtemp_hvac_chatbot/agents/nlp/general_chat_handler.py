# agents/nlp/general_chat_handler.py
"""
General Chat Handler - แยกออกมาจาก intent detection
ง่ายต่อการจัดการและไม่ต้องแก้ไฟล์เดิมเยอะ
"""

import re
import random
from datetime import datetime
from typing import Dict, Optional, Tuple

class GeneralChatHandler:
    """
    Handle greetings and general questions
    แยกออกมาเพื่อไม่ให้ intent_detector.py ซับซ้อนเกินไป
    """
    
    def __init__(self):
        # Simple greeting patterns
        self.greeting_patterns = [
            r'^(สวัสดี|หวัดดี|hello|hi|hey)',
            r'^(อรุณสวัสดิ์|good\s*morning)',
            r'^(ราตรีสวัสดิ์|good\s*evening)',
            r'^(ดีครับ|ดีค่ะ|ดีจ้า)'
        ]
        
        # Help patterns
        self.help_patterns = [
            r'(ช่วย|help|ใช้งาน)',
            r'ทำอะไร.*ได้.*บ้าง',
            r'มีความสามารถ.*อะไร',
            r'วิธีใช้',
            r'คู่มือ'
        ]
        
        # Thanks patterns
        self.thanks_patterns = [
            r'(ขอบคุณ|ขอบใจ|thank|thx)',
            r'^(ได้|โอเค|ok|okay)',
            r'(เยี่ยม|ดีมาก|excellent|great)'
        ]
        
        # Goodbye patterns
        self.goodbye_patterns = [
            r'(ลาก่อน|บาย|bye|goodbye)',
            r'(เจอกัน|พบกัน.*ใหม่)',
            r'(ไว้คุย.*ใหม่)'
        ]
        
        # HVAC general questions
        self.hvac_patterns = [
            r'(hvac|แอร์|เครื่องปรับอากาศ).*คืออะไร',
            r'อธิบาย.*(hvac|ระบบปรับอากาศ)',
            r'(chiller|ชิลเลอร์).*ทำงาน',
            r'ความแตกต่าง.*(split|vrv|vrf)'
        ]
    
    def is_general_chat(self, text: str) -> Tuple[bool, Optional[str]]:
        """
        Check if this is general chat (not database query)
        Returns: (is_general, chat_type)
        """
        text_lower = text.lower().strip()
        
        # Check greeting
        for pattern in self.greeting_patterns:
            if re.search(pattern, text_lower):
                return True, 'greeting'
        
        # Check help
        for pattern in self.help_patterns:
            if re.search(pattern, text_lower):
                return True, 'help'
        
        # Check thanks
        for pattern in self.thanks_patterns:
            if re.search(pattern, text_lower):
                return True, 'thanks'
        
        # Check goodbye
        for pattern in self.goodbye_patterns:
            if re.search(pattern, text_lower):
                return True, 'goodbye'
        
        # Check HVAC general
        for pattern in self.hvac_patterns:
            if re.search(pattern, text_lower):
                return True, 'hvac_general'
        
        # Check if too short to be database query
        if len(text_lower.split()) <= 2 and not any(
            keyword in text_lower for keyword in 
            ['ราคา', 'ยอด', 'รายได้', 'งาน', 'อะไหล่', 'ลูกค้า']
        ):
            # Might be casual chat
            return True, 'casual'
        
        return False, None
    
    def get_response(self, chat_type: str, original_text: str = "") -> str:
        """Get appropriate response based on chat type"""
        
        if chat_type == 'greeting':
            return self._get_greeting_response()
        elif chat_type == 'help':
            return self._get_help_response()
        elif chat_type == 'thanks':
            return self._get_thanks_response()
        elif chat_type == 'goodbye':
            return self._get_goodbye_response()
        elif chat_type == 'hvac_general':
            return self._get_hvac_info(original_text)
        elif chat_type == 'casual':
            return self._get_casual_response()
        else:
            return "ขออภัย ไม่เข้าใจคำถาม กรุณาลองใหม่อีกครั้ง"
    
    def _get_greeting_response(self) -> str:
        """Generate greeting based on time of day"""
        hour = datetime.now().hour
        
        if 5 <= hour < 12:
            greetings = [
                "อรุณสวัสดิ์ครับ! 🌅 วันนี้มีอะไรให้ช่วย ไหมครับ?",
                "สวัสดีตอนเช้าครับ! ยินดีให้บริการข้อมูล Siamtemp ครับ"
            ]
        elif 12 <= hour < 17:
            greetings = [
                "สวัสดีครับ! ☀️ มีอะไรให้ช่วยเรื่องระบบปรับอากาศไหมครับ?",
                "สวัสดีตอนบ่ายครับ! พร้อมให้บริการข้อมูลครับ"
            ]
        elif 17 <= hour < 20:
            greetings = [
                "สวัสดีตอนเย็นครับ! 🌆 มีข้อมูลอะไรต้องการสอบถามไหมครับ?",
                "สวัสดีครับ! ยินดีให้บริการข้อมูลธุรกิจครับ"
            ]
        else:
            greetings = [
                "สวัสดีครับ! 🌙 ยังทำงานอยู่เลยนะครับ มีอะไรให้ช่วยไหมครับ?",
                "ราตรีสวัสดิ์ครับ! พร้อมให้บริการข้อมูลครับ"
            ]
        
        return random.choice(greetings)
    
    def _get_help_response(self) -> str:
        """Return help message"""
        return """📚 **คู่มือการใช้งาน Siamtemp AI**

🔍 **ข้อมูลที่สามารถถามได้:**

**1. ข้อมูลการขาย** 💰
   • "รายได้ปี 2024"
   • "ยอดขาย overhaul เดือนนี้"  
   • "Top 10 ลูกค้า"
   • "เปรียบเทียบรายได้ปี 2023-2024"

**2. ข้อมูลอะไหล่** 🔧
   • "ราคาอะไหล่ EKAC460"
   • "อะไหล่ที่ใกล้หมด"
   • "มูลค่าสินค้าคงคลัง"
   • "stock คลัง A"

**3. ข้อมูลงานบริการ** 👷
   • "งาน PM เดือนกันยายน"
   • "แผนงานวันที่ 15"
   • "งานของทีม A"
   • "อัตราความสำเร็จ"

**4. ข้อมูลลูกค้า** 🏢
   • "ประวัติ CLARION"
   • "งานซ่อมของ Stanley"
   • "ลูกค้าที่มียอดสูงสุด"

💡 **เคล็ดลับ:** ระบุช่วงเวลาและชื่อลูกค้าให้ชัดเจนจะได้ข้อมูลที่แม่นยำครับ"""
    
    def _get_thanks_response(self) -> str:
        """Return thanks response"""
        responses = [
            "ด้วยความยินดีครับ! 😊 มีอะไรให้ช่วยเพิ่มเติมบอกได้เลยครับ",
            "ยินดีที่ได้ช่วยเหลือครับ! หากต้องการข้อมูลเพิ่มเติมถามมาได้เลยครับ",
            "ไม่เป็นไรครับ ยินดีให้บริการครับ 🙏",
            "ขอบคุณที่ใช้บริการ Siamtemp HVAC AI ครับ"
        ]
        return random.choice(responses)
    
    def _get_goodbye_response(self) -> str:
        """Return goodbye response"""
        hour = datetime.now().hour
        
        if 17 <= hour <= 23:
            farewells = [
                "ขอให้มีค่ำคืนที่ดีครับ! 🌙 ยินดีให้บริการเสมอครับ",
                "ราตรีสวัสดิ์ครับ! พบกันใหม่ครับ"
            ]
        else:
            farewells = [
                "ขอให้มีวันที่ดีครับ! ☀️ ยินดีให้บริการเสมอครับ",
                "ลาก่อนครับ! หวังว่าข้อมูลจะเป็นประโยชน์นะครับ"
            ]
        
        return random.choice(farewells)
    
    def _get_casual_response(self) -> str:
        """Return response for casual/unclear input"""
        return """ขออภัยครับ ไม่แน่ใจว่าต้องการข้อมูลอะไร 🤔

💡 ลองถามแบบนี้ดูครับ:
• "รายได้ปี 2024"
• "ราคาอะไหล่ [รหัสสินค้า]"
• "งาน PM เดือนนี้"

หรือพิมพ์ "ช่วย" เพื่อดูคำแนะนำทั้งหมดครับ"""
    
    def _get_hvac_info(self, question: str) -> str:
        """Return HVAC general information"""
        question_lower = question.lower()
        
        if 'hvac' in question_lower and 'คืออะไร' in question_lower:
            return """**HVAC (Heating, Ventilation, and Air Conditioning)** 
คือระบบปรับอากาศและระบายอากาศในอาคาร ประกอบด้วย:

🔥 **Heating** - ระบบทำความร้อน
❄️ **Ventilation** - ระบบระบายอากาศ  
🌡️ **Air Conditioning** - ระบบปรับอากาศ

Siamtemp เชี่ยวชาญด้าน HVAC มากว่า 30 ปี พร้อมให้บริการ:
• ติดตั้งระบบแอร์ขนาดใหญ่
• บำรุงรักษา (PM)
• ซ่อมแซม Overhaul
• จำหน่ายอะไหล่"""
        
        elif 'chiller' in question_lower or 'ชิลเลอร์' in question_lower:
            return """**Chiller (เครื่องทำน้ำเย็น)** 
คือเครื่องจักรขนาดใหญ่ที่ทำน้ำเย็นสำหรับระบบปรับอากาศส่วนกลาง

ประเภทหลัก:
• **Air-Cooled** - ระบายความร้อนด้วยอากาศ
• **Water-Cooled** - ระบายความร้อนด้วยน้ำ

Siamtemp ให้บริการ Chiller ทุกยี่ห้อ ทั้ง Overhaul และ Replacement"""
        
        else:
            return """มีคำถามเกี่ยวกับระบบ HVAC ใช่ไหมครับ?

ผมสามารถให้ข้อมูล:
• ข้อมูลธุรกิจ (รายได้, ยอดขาย, ลูกค้า)
• ราคาอะไหล่และสินค้าคงคลัง
• แผนงานบริการและซ่อมบำรุง

ลองถามเจาะจงมากขึ้นดูครับ 😊"""

