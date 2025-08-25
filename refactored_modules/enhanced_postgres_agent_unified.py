# refactored_modules/enhanced_postgres_agent_unified.py
# 🔧 ปรับปรุงสำหรับธุรกิจ HVAC Service & Spare Parts

import os
import time
import re
import json
import asyncio
import aiohttp
import psycopg2
from decimal import Decimal
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Tuple, AsyncGenerator
import logging
from .intelligent_schema_discovery import EnhancedSchemaIntegration
# Import configs only
from .tenant_config import TenantConfigManager, TenantConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UnifiedEnhancedPostgresOllamaAgent:
    """🤖 Enhanced PostgreSQL Agent สำหรับธุรกิจ HVAC Service & Spare Parts"""
    
    def __init__(self):
        """🏗️ Initialize unified agent สำหรับ HVAC"""
        
        # 🔧 Configuration
        self.config_manager = TenantConfigManager()
        self.tenant_configs = self.config_manager.tenant_configs
        
        # 🌐 Ollama Configuration
        self.ollama_base_url = os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434')
        self.request_timeout = int(os.getenv('AI_REQUEST_TIMEOUT', '90'))
        self.max_retries = int(os.getenv('AI_MAX_RETRIES', '3'))
        
        # 🆕 AI Response Configuration
        self.enable_ai_responses = os.getenv('ENABLE_AI_RESPONSES', 'true').lower() == 'true'
        self.ai_response_temperature = float(os.getenv('AI_RESPONSE_TEMPERATURE', '0.3'))
        self.fallback_to_hardcode = os.getenv('FALLBACK_TO_HARDCODE', 'true').lower() == 'true'
        
        # 📊 Performance tracking
        self.stats = {
            'total_queries': 0,
            'hvac_queries': 0,
            'customer_queries': 0,
            'spare_parts_queries': 0,
            'service_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'ai_responses_used': 0,
            'hardcode_responses_used': 0,
            'avg_response_time': 0.0
        }
        
        # 🧠 Schema cache
        self.schema_cache = {}
        self.cache_ttl = 3600  # 1 hour
        
        # 🎯 HVAC Intent detection keywords
        self.hvac_indicators = {
            'customer_queries': ['ลูกค้า', 'customer', 'บริษัท', 'จำนวน', 'count', 'ประวัติ', 'history'],
            'spare_parts_queries': ['อะไหล่', 'spare', 'part', 'ราคา', 'price', 'สต็อก', 'stock', 'คลัง'],
            'service_queries': ['บริการ', 'service', 'ซ่อม', 'repair', 'บำรุง', 'maintenance', 'pm', 'overhaul'],
            'work_schedule_queries': ['แผนงาน', 'schedule', 'วันที่', 'date', 'ทีม', 'team', 'ช่าง', 'tempnician'],
            'sales_analysis': ['ยอดขาย', 'sales', 'วิเคราะห์', 'analysis', 'รายงาน', 'report', 'สรุป', 'summary']
        }
        
        self.conversational_indicators = {
            'greetings': ['สวัสดี', 'hello', 'hi', 'ช่วย', 'help'],
            'general_info': ['คุณคือใคร', 'เกี่ยวกับ', 'about', 'what are you'],
            'capabilities': ['ทำอะไรได้', 'ช่วยอะไร', 'what can you do']
        }
        
        # HVAC Business Knowledge (แทน metadata ในฐานข้อมูล)
        self.hvac_business_knowledge = {
            'table_info': {
                'sales2024': 'ข้อมูลงานบริการปี 2024',
                'sales2023': 'ข้อมูลงานบริการปี 2023',
                'sales2022': 'ข้อมูลงานบริการปี 2022',
                'sales2025': 'ข้อมูลงานบริการปี 2025',
                'spare_part': 'คลังอะไหล่หลัก',
                'spare_part2': 'คลังอะไหล่สำรอง',
                'work_force': 'การจัดทีมงานและแผนการทำงาน'
            },
            'technical_terms': {
                'PM': 'Preventive Maintenance',
                'Chiller': 'เครื่องทำน้ำเย็น',
                'Compressor': 'คอมเพรสเซอร์',
                'Overhaul': 'การยกเครื่อง',
                'Air Cooled': 'ระบายความร้อนด้วยอากาศ',
                'Water Cooled': 'ระบายความร้อนด้วยน้ำ'
            },
            'brands': ['Hitachi', 'Daikin', 'EuroKlimat', 'Toyota', 'Mitsubishi', 'York', 'Carrier']
        }
        
        try:
            from .intelligent_schema_discovery import EnhancedSchemaIntegration
            self.schema_integration = EnhancedSchemaIntegration(
                database_handler=self,  # ส่ง self เพราะมี method get_database_connection
                tenant_configs=self.tenant_configs
            )
            self.intelligent_schema_available = True
            logger.info("🧠 HVAC Intelligent Schema Discovery integrated successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize HVAC Intelligent Schema Discovery: {e}")
            self.schema_integration = None
            self.intelligent_schema_available = False
            logger.warning("⚠️ Falling back to basic HVAC schema discovery")
        
        logger.info("🤖 HVAC Enhanced PostgreSQL Agent initialized")
        logger.info(f"🌐 Ollama: {self.ollama_base_url}")
        logger.info(f"🎨 AI Responses: {'Enabled' if self.enable_ai_responses else 'Disabled'}")
        logger.info(f"🏢 Tenants: {list(self.tenant_configs.keys())}")
    
    # ========================================================================
    # 🎯 MAIN ENTRY POINT
    # ========================================================================
    
    async def process_enhanced_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """🎯 MAIN: Process questions สำหรับธุรกิจ HVAC"""
        
        if tenant_id not in self.tenant_configs:
            return self._create_error_response(f"Unknown tenant: {tenant_id}", tenant_id)
        
        start_time = time.time()
        self.stats['total_queries'] += 1
        
        try:
            logger.info(f"🎯 Processing HVAC question for {tenant_id}: {question[:100]}...")
            
            # วิเคราะห์ intent สำหรับธุรกิจ HVAC
            intent_result = self._detect_hvac_intent(question)
            logger.info(f"🔍 HVAC Intent detected: {intent_result}")
            
            # ตัดสินใจประเภทการตอบสนอง
            if intent_result['is_conversational']:
                self.stats['conversational_queries'] += 1
                response = await self._handle_hvac_conversational_query(question, tenant_id, intent_result)
            else:
                # เป็นคำถามที่ต้องการข้อมูลจากฐานข้อมูล
                response = await self._handle_hvac_data_query(question, tenant_id, intent_result)
            
            # Update statistics
            processing_time = time.time() - start_time
            self._update_stats(tenant_id, True, processing_time)
            
            return response
            
        except Exception as e:
            logger.error(f"❌ HVAC Enhanced question processing failed: {e}")
            processing_time = time.time() - start_time
            self._update_stats(tenant_id, False, processing_time)
            return self._create_error_response(str(e), tenant_id)
    
    def _detect_hvac_intent(self, question: str) -> Dict[str, Any]:
        """🔍 วิเคราะห์ intent สำหรับธุรกิจ HVAC"""
        
        question_lower = question.lower()
        
        intent_result = {
            'is_conversational': False,
            'hvac_category': 'general',
            'confidence': 0.0,
            'keywords_found': [],
            'requires_database': True,
            'suggested_tables': []
        }
        
        # ตรวจสอบว่าเป็นคำถามสนทนาหรือไม่
        conversational_score = 0
        for category, keywords in self.conversational_indicators.items():
            for keyword in keywords:
                if keyword in question_lower:
                    conversational_score += 1
                    intent_result['keywords_found'].append(keyword)
        
        if conversational_score > 0:
            intent_result['is_conversational'] = True
            intent_result['requires_database'] = False
            return intent_result
        
        # วิเคราะห์ประเภทคำถาม HVAC
        max_score = 0
        best_category = 'general'
        
        for category, keywords in self.hvac_indicators.items():
            score = 0
            category_keywords = []
            
            for keyword in keywords:
                if keyword in question_lower:
                    score += 1
                    category_keywords.append(keyword)
            
            if score > max_score:
                max_score = score
                best_category = category
                intent_result['keywords_found'] = category_keywords
        
        intent_result['hvac_category'] = best_category
        intent_result['confidence'] = min(max_score / 3.0, 1.0)  # normalize to 0-1
        
        # กำหนดตารางที่แนะนำ
        table_mapping = {
            'customer_queries': ['sales2024', 'sales2023', 'sales2022'],
            'spare_parts_queries': ['spare_part', 'spare_part2'],
            'service_queries': ['sales2024', 'work_force'],
            'work_schedule_queries': ['work_force'],
            'sales_analysis': ['sales2024', 'sales2023', 'sales2022']
        }
        
        intent_result['suggested_tables'] = table_mapping.get(best_category, ['sales2024'])
        
        return intent_result
    
    async def _handle_hvac_conversational_query(self, question: str, tenant_id: str, 
                                              intent_result: Dict[str, Any]) -> Dict[str, Any]:
        """💬 จัดการคำถามสนทนาสำหรับ HVAC"""
        
        config = self.tenant_configs[tenant_id]
        business_emoji = self._get_hvac_business_emoji(tenant_id)
        
        # สร้างคำตอบสนทนาสำหรับธุรกิจ HVAC
        if any(word in question.lower() for word in ['สวัสดี', 'hello', 'hi']):
            answer = self._create_hvac_greeting_response(config, business_emoji)
        elif any(word in question.lower() for word in ['คุณคือใคร', 'what are you']):
            answer = self._create_hvac_identity_response(config, business_emoji)
        elif any(word in question.lower() for word in ['ช่วยอะไร', 'what can you do']):
            answer = self._create_hvac_capabilities_response(config, business_emoji)
        else:
            answer = self._create_hvac_general_response(question, config, business_emoji)
        
        return {
            "answer": answer,
            "success": True,
            "data_source_used": f"hvac_conversational_{config.model_name}",
            "sql_query": None,
            "tenant_id": tenant_id,
            "system_used": "hvac_conversational_ai",
            "intent_detected": intent_result
        }
    
    async def _handle_hvac_data_query(self, question: str, tenant_id: str, 
                                    intent_result: Dict[str, Any]) -> Dict[str, Any]:
        """📊 จัดการคำถามข้อมูลสำหรับ HVAC"""
        
        try:
            # อัปเดตสถิติตามประเภทคำถาม
            category = intent_result['hvac_category']
            if category in self.stats:
                self.stats[category] += 1
            
            # ใช้ Intelligent Schema Discovery หากมี
            if self.intelligent_schema_available and self.schema_integration:
                sql_prompt = await self.schema_integration.generate_intelligent_sql_prompt(question, tenant_id)
            else:
                # ใช้ fallback prompt สำหรับ HVAC
                sql_prompt = self._generate_hvac_fallback_prompt(question, tenant_id, intent_result)
            
            # สร้าง SQL จาก AI
            ai_response = await self._call_ollama_api(sql_prompt, tenant_id)
            
            # ดึง SQL query จากการตอบสนองของ AI
            sql_query = self._extract_sql_unified(ai_response, question)
            
            if not sql_query:
                return self._create_sql_error_response(question, tenant_id, "ไม่สามารถสร้าง SQL query ได้")
            
            # Execute SQL
            results = await self._execute_sql_unified(sql_query, tenant_id)
            
            # สร้างคำตอบสำหรับธุรกิจ HVAC
            if self.enable_ai_responses and results:
                answer = await self._generate_hvac_ai_response(question, results, tenant_id, intent_result)
                self.stats['ai_responses_used'] += 1
            else:
                answer = self._format_hvac_results(question, results, tenant_id, intent_result)
                self.stats['hardcode_responses_used'] += 1
            
            return {
                "answer": answer,
                "success": True,
                "data_source_used": f"hvac_database_{self.tenant_configs[tenant_id].model_name}",
                "sql_query": sql_query,
                "results_count": len(results),
                "tenant_id": tenant_id,
                "system_used": "hvac_intelligent_agent",
                "intent_detected": intent_result
            }
            
        except Exception as e:
            logger.error(f"❌ HVAC data query failed: {e}")
            return self._create_sql_error_response(question, tenant_id, str(e))
    
    # ========================================================================
    # 🎨 HVAC RESPONSE FORMATTING
    # ========================================================================
    
    def _create_hvac_greeting_response(self, config: TenantConfig, business_emoji: str) -> str:
        return f"""สวัสดีครับ! {business_emoji} ผมคือ AI Assistant สำหรับระบบ HVAC Service & Spare Parts ของ {config.name}

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

คุณต้องการสอบถามอะไรครับ?"""
    
    def _create_hvac_identity_response(self, config: TenantConfig, business_emoji: str) -> str:
        return f"""{business_emoji} ผมคือ AI Assistant สำหรับธุรกิจ HVAC Service & Spare Parts

🏢 บริษัท: {config.name}
🔧 ธุรกิจ: บริการซ่อมบำรุงระบบทำความเย็นและจำหน่ายอะไหล่
🛠️ บริการหลัก: PM (Preventive Maintenance), ซ่อมแซม, เปลี่ยนชิ้นส่วน, Overhaul
🏷️ แบรนด์ที่รองรับ: Hitachi, Daikin, EuroKlimat, Toyota, Mitsubishi

💡 ผมสามารถช่วยวิเคราะห์ข้อมูลและตอบคำถามเกี่ยวกับ:
• ลูกค้าและประวัติการบริการ
• อะไหล่และราคา  
• แผนงานและทีมช่าง
• ยอดขายและรายงาน"""
    
    def _create_hvac_capabilities_response(self, config: TenantConfig, business_emoji: str) -> str:
        return f"""{business_emoji} ความสามารถของระบบ HVAC AI Assistant

🔍 การค้นหาและวิเคราะห์:
• ข้อมูลลูกค้า: "บริษัท ABC มีประวัติการบริการอะไรบ้าง"
• อะไหล่และราคา: "ราคาอะไหล่ Compressor Hitachi"
• แผนงาน: "แผนงานเดือนนี้มีงานอะไรบ้าง"
• ยอดขาย: "วิเคราะห์ยอดขายปี 2567"

📊 รายงานและสถิติ:
• นับจำนวนลูกค้า ประเภทบริการ
• วิเคราะห์ประสิทธิภาพทีม
• สรุปยอดขายและกำไร
• รายงานสต็อกอะไหล่

🛠️ เทคโนโลยีที่ใช้:
• AI Model: {config.model_name}
• Database: PostgreSQL HVAC System  
• Language: {config.language}"""
    
    def _create_hvac_general_response(self, question: str, config: TenantConfig, business_emoji: str) -> str:
        return f"""{business_emoji} ระบบ HVAC AI สำหรับ {config.name}

คำถาม: {question}

💡 คำแนะนำสำหรับคำถามที่เฉพาะเจาะจง:
🔍 ข้อมูลลูกค้า: "จำนวนลูกค้าทั้งหมด" หรือ "บริษัท [ชื่อ] มีประวัติอะไรบ้าง"
🔧 อะไหล่: "ราคาอะไหล่ [ยี่ห้อ] [รุ่น]" หรือ "สต็อกอะไหล่ Chiller"
👷 แผนงาน: "แผนงานวันที่ [วันที่]" หรือ "ทีมช่างมีใครบ้าง"
📊 ยอดขาย: "ยอดขายปี [ปี]" หรือ "รายงานบริการ PM"

ลองถามคำถามที่เฉพาะเจาะจงมากขึ้นนะครับ!"""
    
    def _format_hvac_results(self, question: str, results: List[Dict], tenant_id: str, 
                           intent_result: Dict[str, Any]) -> str:
        """🎨 จัดรูปแบบผลลัพธ์สำหรับ HVAC"""
        
        if not results:
            return f"ไม่พบข้อมูลที่ตรงกับคำถาม: {question}"
        
        config = self.tenant_configs[tenant_id]
        business_emoji = self._get_hvac_business_emoji(tenant_id)
        category = intent_result.get('hvac_category', 'general')
        
        response = f"{business_emoji} ผลการค้นหาระบบ HVAC - {config.name}\n\n"
        
        # จัดรูปแบบตามประเภทคำถาม HVAC
        if category == 'customer_queries':
            response += self._format_hvac_customer_results(results)
        elif category == 'spare_parts_queries':
            response += self._format_hvac_spare_parts_results(results)
        elif category == 'service_queries':
            response += self._format_hvac_service_results(results)
        elif category == 'work_schedule_queries':
            response += self._format_hvac_work_schedule_results(results)
        elif category == 'sales_analysis':
            response += self._format_hvac_sales_analysis_results(results)
        else:
            response += self._format_hvac_general_results(results)
        
        response += f"\n\n📈 สรุป: พบข้อมูล {len(results)} รายการ"
        return response
    
    def _format_hvac_customer_results(self, results: List[Dict]) -> str:
        """จัดรูปแบบผลลัพธ์ลูกค้า HVAC"""
        formatted = "👥 ข้อมูลลูกค้า HVAC:\n"
        for i, row in enumerate(results[:10], 1):
            customer = row.get('customer_name', row.get('customer', 'ไม่ระบุ'))
            value = row.get('service_contact_', row.get('value', 0))
            job = row.get('job_no', row.get('description', ''))
            
            formatted += f"{i}. {customer}\n"
            if job:
                formatted += f"   งาน: {job}\n"
            if value and str(value).replace('.', '').isdigit():
                formatted += f"   มูลค่า: {float(value):,.0f} บาท\n"
            formatted += "\n"
        return formatted
    
    def _format_hvac_spare_parts_results(self, results: List[Dict]) -> str:
        """จัดรูปแบบผลลัพธ์อะไหล่ HVAC"""
        formatted = "🔧 รายการอะไหล่ HVAC:\n"
        for i, row in enumerate(results[:10], 1):
            code = row.get('product_code', '')
            name = row.get('product_name', '')
            price = row.get('unit_price', '0')
            balance = row.get('balance', 0)
            description = row.get('description', '')
            
            formatted += f"{i}. {code} - {name}\n"
            if price and str(price).replace('.', '').replace(',', '').isdigit():
                formatted += f"   ราคา: {float(str(price).replace(',', '')):,.0f} บาท"
                if balance:
                    formatted += f" | คงเหลือ: {balance} ชิ้น"
                formatted += "\n"
            if description:
                formatted += f"   รายละเอียด: {description[:80]}...\n"
            formatted += "\n"
        return formatted
    
    def _format_hvac_service_results(self, results: List[Dict]) -> str:
        """จัดรูปแบบผลลัพธ์บริการ HVAC"""
        formatted = "🛠️ รายการบริการ HVAC:\n"
        for i, row in enumerate(results[:10], 1):
            customer = row.get('customer_name', row.get('customer', ''))
            description = row.get('description', row.get('detail', ''))
            job_no = row.get('job_no', '')
            value = row.get('service_contact_', 0)
            
            formatted += f"{i}. {customer}\n"
            if job_no:
                formatted += f"   เลขที่งาน: {job_no}\n"
            if description:
                formatted += f"   บริการ: {description[:80]}...\n"
            if value and str(value).replace('.', '').isdigit():
                formatted += f"   มูลค่า: {float(value):,.0f} บาท\n"
            formatted += "\n"
        return formatted
    
    def _format_hvac_work_schedule_results(self, results: List[Dict]) -> str:
        """จัดรูปแบบผลลัพธ์แผนงาน HVAC"""
        formatted = "📋 แผนงานทีม HVAC:\n"
        for i, row in enumerate(results[:10], 1):
            customer = row.get('customer', '')
            detail = row.get('detail', '')
            team = row.get('service_group', '')
            date = row.get('date', '')
            job_type = row.get('job_type', '')
            
            formatted += f"{i}. ลูกค้า: {customer}\n"
            if date:
                formatted += f"   วันที่: {date}\n"
            if detail:
                formatted += f"   งาน: {detail}\n"
            if team:
                formatted += f"   ทีมช่าง: {team}\n"
            if job_type:
                formatted += f"   ประเภท: {job_type}\n"
            formatted += "\n"
        return formatted
    
    def _format_hvac_sales_analysis_results(self, results: List[Dict]) -> str:
        """จัดรูปแบบผลลัพธ์วิเคราะห์ยอดขาย HVAC"""
        formatted = "📊 วิเคราะห์ยอดขาย HVAC:\n"
        for i, row in enumerate(results[:10], 1):
            # ตรวจสอบคีย์ที่เป็นไปได้สำหรับข้อมูลสถิติ
            total_jobs = row.get('total_jobs', row.get('job_count', row.get('count', 0)))
            total_revenue = row.get('total_revenue', row.get('total_value', row.get('sum', 0)))
            avg_value = row.get('avg_job_value', row.get('avg_value', row.get('avg', 0)))
            
            if total_jobs:
                formatted += f"{i}. จำนวนงาน: {total_jobs} งาน\n"
            if total_revenue and str(total_revenue).replace('.', '').isdigit():
                formatted += f"   รายได้รวม: {float(total_revenue):,.0f} บาท\n"
            if avg_value and str(avg_value).replace('.', '').isdigit():
                formatted += f"   มูลค่าเฉลี่ย: {float(avg_value):,.0f} บาท/งาน\n"
            formatted += "\n"
        return formatted
    
    def _format_hvac_general_results(self, results: List[Dict]) -> str:
        """จัดรูปแบบผลลัพธ์ทั่วไป HVAC"""
        formatted = "📊 ผลลัพธ์ HVAC:\n"
        for i, row in enumerate(results[:10], 1):
            formatted += f"{i}. "
            formatted += " | ".join([f"{k}: {v}" for k, v in row.items() if v is not None])
            formatted += "\n"
        return formatted
    
    # ========================================================================
    # 🤖 AI RESPONSE GENERATION
    # ========================================================================
    
    async def _generate_hvac_ai_response(self, question: str, results: List[Dict], 
                                       tenant_id: str, intent_result: Dict[str, Any]) -> str:
        """🤖 สร้างคำตอบ AI สำหรับ HVAC"""
        
        config = self.tenant_configs[tenant_id]
        category = intent_result.get('hvac_category', 'general')
        
        # สร้าง context สำหรับ AI
        context_prompt = f"""คุณคือ AI Assistant สำหรับธุรกิจ HVAC Service & Spare Parts ของ {config.name}

🔧 บริบทธุรกิจ: บริการซ่อมบำรุงระบบทำความเย็นและจำหน่ายอะไหล่
🏷️ แบรนด์หลัก: Hitachi, Daikin, EuroKlimat, Toyota, Mitsubishi
📊 ประเภทคำถาม: {category}

คำถาม: {question}

ข้อมูลที่พบ: {json.dumps(results[:5], ensure_ascii=False, indent=2)}

กรุณาสร้างคำตอบที่:
1. เป็นมิตรและเป็นประโยชน์
2. ใช้ศัพท์เทคนิค HVAC ที่เหมาะสม
3. จัดรูปแบบให้อ่านง่าย
4. เน้นข้อมูลที่สำคัญ
5. ใส่หน่วยเงิน (บาท) หากเกี่ยวกับราคา"""

        try:
            ai_response = await self._call_ollama_api(context_prompt, tenant_id)
            return ai_response
        except Exception as e:
            logger.error(f"❌ AI response generation failed: {e}")
            # Fallback to formatted response
            return self._format_hvac_results(question, results, tenant_id, intent_result)
    
    # ========================================================================
    # 🔧 HVAC UTILITY METHODS
    # ========================================================================
    
    def _get_hvac_business_emoji(self, tenant_id: str) -> str:
        """🎨 Business emoji สำหรับ HVAC"""
        emoji_mapping = {
            'company-a': '🔧',  # HVAC Service Main
            'company-b': '❄️',  # HVAC Regional  
            'company-c': '🌍'   # HVAC International
        }
        return emoji_mapping.get(tenant_id, '🔧')
    
    def _generate_hvac_fallback_prompt(self, question: str, tenant_id: str, 
                                     intent_result: Dict[str, Any]) -> str:
        """🔄 Fallback prompt สำหรับ HVAC"""
        
        config = self.tenant_configs[tenant_id]
        category = intent_result.get('hvac_category', 'general')
        suggested_tables = intent_result.get('suggested_tables', ['sales2024'])
        
        return f"""คุณคือ PostgreSQL Expert สำหรับธุรกิจ HVAC Service & Spare Parts - {config.name}

🔧 บริบทธุรกิจ: บริการซ่อมบำรุงระบบทำความเย็นและจำหน่ายอะไหล่
💰 ลูกค้าหลัก: โรงงานอุตสาหกรรม, โรงแรม, อาคารสำนักงาน
🛠️ บริการหลัก: PM (Preventive Maintenance), ซ่อมแซม, เปลี่ยนชิ้นส่วน, Overhaul

📊 โครงสร้างฐานข้อมูล:
• sales2024, sales2023, sales2022: ข้อมูลงานบริการรายปี
  - job_no: หมายเลขงาน (SV.ปี-เดือน-ลำดับ-ประเภท)
  - customer_name: ชื่อลูกค้าเต็ม
  - description: รายละเอียดงานบริการ  
  - service_contact_: มูลค่างาน (บาท)
• spare_part, spare_part2: คลังอะไหล่
  - product_code: รหัสอะไหล่, product_name: ชื่ออะไหล่ (ภาษาอังกฤษ)
  - unit_price: ราคาต่อหน่วย, balance: จำนวนคงเหลือ
• work_force: การจัดทีมงาน
  - date: วันที่ทำงาน, customer: ลูกค้า, service_group: ทีมช่าง

🎯 ประเภทคำถาม: {category}
📋 ตารางที่แนะนำ: {', '.join(suggested_tables)}

🔧 กฎสำคัญสำหรับ HVAC:
1. ใช้ ILIKE '%keyword%' สำหรับค้นหาลูกค้าและอะไหล่
2. service_contact_ เป็นมูลค่างาน (บาท) - ใช้ CAST เป็น numeric หากจำเป็น
3. ข้อมูลการขายแยกตามปี - เลือกตารางให้ถูกต้อง
4. ใช้ UNION ALL เมื่อต้องการข้อมูลหลายปี
5. product_name ในตาราง spare_part เป็นภาษาอังกฤษ
6. ใช้ LIMIT 20 เสมอ

คำถาม: {question}

สร้าง PostgreSQL query ที่เหมาะสมสำหรับธุรกิจ HVAC:"""
    
    # ========================================================================
    # 🌐 OLLAMA API INTEGRATION
    # ========================================================================
    
    async def _call_ollama_api(self, prompt: str, tenant_id: str) -> str:
        """🌐 Call Ollama API with retry logic"""
        
        config = self.tenant_configs[tenant_id]
        
        payload = {
            "model": config.model_name,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": self.ai_response_temperature,
                "top_p": 0.9,
                "max_tokens": 2000
            }
        }
        
        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.request_timeout)) as session:
                    async with session.post(f"{self.ollama_base_url}/api/generate", json=payload) as response:
                        if response.status == 200:
                            result = await response.json()
                            return result.get('response', '').strip()
                        else:
                            logger.warning(f"🔄 Ollama API returned {response.status}, attempt {attempt + 1}")
                            if attempt < self.max_retries - 1:
                                await asyncio.sleep(2 ** attempt)
                                
            except asyncio.TimeoutError:
                logger.warning(f"🔄 AI request timeout attempt {attempt + 1}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    
            except Exception as e:
                logger.warning(f"🔄 AI API error attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
        
        raise Exception(f"All {self.max_retries} AI API attempts failed")
    
    # ========================================================================
    # 🗄️ DATABASE OPERATIONS
    # ========================================================================
    
    def get_database_connection(self, tenant_id: str) -> psycopg2.extensions.connection:
        """🔌 Get database connection (public method for schema discovery)"""
        
        config = self.tenant_configs[tenant_id]
        
        try:
            conn = psycopg2.connect(
                host=config.db_host,
                port=config.db_port,
                database=config.db_name,
                user=config.db_user,
                password=config.db_password,
                connect_timeout=10
            )
            conn.set_session(autocommit=True)
            return conn
            
        except Exception as e:
            logger.error(f"❌ Database connection failed for {tenant_id}: {e}")
            raise
    
    async def _execute_sql_unified(self, sql_query: str, tenant_id: str) -> List[Dict[str, Any]]:
        """🗄️ Execute SQL query สำหรับ HVAC"""
        
        try:
            logger.info(f"🗄️ Executing HVAC SQL for {tenant_id}: {sql_query[:100]}...")
            
            conn = self.get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            cursor.execute(sql_query)
            
            # Get results
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            
            # Process results
            results = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                processed_row = self._process_row_data(row_dict)
                results.append(processed_row)
            
            cursor.close()
            conn.close()
            
            logger.info(f"✅ HVAC SQL executed successfully: {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"❌ HVAC SQL execution failed: {e}")
            logger.error(f"❌ Failed SQL: {sql_query}")
            return []
    
    def _process_row_data(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """🔧 Process row data สำหรับ HVAC"""
        
        processed_row = {}
        
        for key, value in row_dict.items():
            if isinstance(value, Decimal):
                processed_row[key] = float(value)
            elif isinstance(value, (date, datetime)):
                processed_row[key] = value.isoformat()
            elif value is None:
                processed_row[key] = None
            elif isinstance(value, str):
                processed_row[key] = value.strip()
            else:
                processed_row[key] = value
        
        return processed_row
    
    # ========================================================================
    # 🔍 SQL EXTRACTION
    # ========================================================================
    
    def _extract_sql_unified(self, ai_response: str, question: str) -> str:
        """🔍 Extract SQL from AI response"""
        
        logger.info(f"🔍 Extracting SQL from response (length: {len(ai_response)})")
        
        # Method 1: Look for SQL blocks
        sql_patterns = [
            r'```sql\s*(.*?)\s*```',
            r'```\s*(SELECT.*?);?\s*```',
            r'Query:\s*(SELECT.*?);?',
            r'SQL:\s*(SELECT.*?);?',
            r'(SELECT\s+.*?FROM\s+.*?(?:WHERE.*?)?(?:GROUP BY.*?)?(?:ORDER BY.*?)?(?:LIMIT.*?)?);?'
        ]
        
        for pattern in sql_patterns:
            matches = re.findall(pattern, ai_response, re.DOTALL | re.IGNORECASE)
            if matches:
                sql = matches[0].strip()
                if self._is_valid_sql_structure(sql):
                    logger.info(f"✅ Extracted SQL: {sql[:100]}...")
                    return sql
        
        # Method 2: Look for any SELECT statement
        select_pattern = r'(SELECT\s+[^;]*(?:FROM\s+[^;]*)?(?:WHERE\s+[^;]*)?(?:LIMIT\s+\d+)?)'
        matches = re.findall(select_pattern, ai_response, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            sql = match.strip()
            if len(sql) > 20 and 'FROM' in sql.upper():
                logger.info(f"✅ Found SELECT statement: {sql[:100]}...")
                return sql
        
        logger.warning("❌ No valid SQL found in AI response")
        return None
    
    def _is_valid_sql_structure(self, sql: str) -> bool:
        """✅ Validate SQL structure"""
        
        sql_upper = sql.upper()
        
        # Must have SELECT and FROM
        if 'SELECT' not in sql_upper or 'FROM' not in sql_upper:
            return False
        
        # Should not have dangerous keywords
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE']
        if any(keyword in sql_upper for keyword in dangerous_keywords):
            return False
        
        # Basic structure check
        if sql.strip().startswith('SELECT') and len(sql.strip()) > 20:
            return True
        
        return False
    
    # ========================================================================
    # ❌ ERROR HANDLING
    # ========================================================================
    
    def _create_error_response(self, error_message: str, tenant_id: str) -> Dict[str, Any]:
        return {
            "answer": f"เกิดข้อผิดพลาดในระบบ HVAC: {error_message}",
            "success": False,
            "data_source_used": "hvac_error",
            "sql_query": None,
            "tenant_id": tenant_id,
            "system_used": "hvac_error_handler",
            "error": error_message
        }
    
    def _create_sql_error_response(self, question: str, tenant_id: str, error_message: str) -> Dict[str, Any]:
        config = self.tenant_configs[tenant_id]
        business_emoji = self._get_hvac_business_emoji(tenant_id)
        
        answer = f"""{business_emoji} ไม่สามารถประมวลผลคำถาม HVAC ได้

คำถาม: {question}

⚠️ ปัญหา: {error_message}

💡 คำแนะนำสำหรับคำถาม HVAC:
• ลูกค้า: "จำนวนลูกค้าทั้งหมด" หรือ "บริษัท ABC มีประวัติอะไรบ้าง"
• อะไหล่: "ราคาอะไหล่ Hitachi chiller" หรือ "สต็อกอะไหล่ทั้งหมด"
• แผนงาน: "แผนงานวันที่ 15/06/2568" หรือ "ทีมช่างมีใครบ้าง"
• ยอดขาย: "ยอดขายปี 2567" หรือ "วิเคราะห์บริการ PM"

หรือลองถามเกี่ยวกับข้อมูลทั่วไปของธุรกิจ HVAC"""
        
        return {
            "answer": answer,
            "success": False,
            "data_source_used": f"hvac_sql_error_{config.model_name}",
            "sql_query": None,
            "tenant_id": tenant_id,
            "system_used": "hvac_sql_error_handler",
            "error_reason": error_message
        }
    
    # ========================================================================
    # 📊 STATISTICS
    # ========================================================================
    
    def _update_stats(self, tenant_id: str, success: bool, processing_time: float):
        """Update system statistics"""
        
        if success:
            self.stats['successful_queries'] += 1
        else:
            self.stats['failed_queries'] += 1
        
        # Update average response time
        total_queries = self.stats['total_queries']
        current_avg = self.stats['avg_response_time']
        new_avg = ((current_avg * (total_queries - 1)) + processing_time) / total_queries
        self.stats['avg_response_time'] = new_avg
    
    async def get_intelligent_schema_stats(self) -> Dict[str, Any]:
        """📊 ดูสถิติของระบบ HVAC Intelligent Schema Discovery"""
        
        if self.intelligent_schema_available and self.schema_integration:
            try:
                cache_stats = self.schema_integration.schema_discovery.get_cache_statistics()
                return {
                    'hvac_intelligent_schema_system': 'active',
                    'cache_statistics': cache_stats,
                    'hvac_features': [
                        'contextual_hvac_schema_discovery',
                        'intelligent_hvac_prompt_building',
                        'hvac_question_analysis',
                        'hvac_business_data_extraction',
                        'smart_caching'
                    ]
                }
            except Exception as e:
                return {'error': f'Failed to get HVAC stats: {str(e)}'}
        else:
            return {'hvac_intelligent_schema_system': 'not_available'}

    def clear_schema_cache(self, tenant_id: Optional[str] = None):
        """🗑️ ล้าง cache ของ HVAC schema discovery"""
        
        if self.intelligent_schema_available and self.schema_integration:
            try:
                self.schema_integration.schema_discovery.clear_cache(tenant_id)
                logger.info(f"🗑️ HVAC Schema cache cleared for {tenant_id if tenant_id else 'all tenants'}")
            except Exception as e:
                logger.error(f"❌ Failed to clear HVAC cache: {e}")
        else:
            logger.warning("⚠️ HVAC Intelligent Schema Discovery not available")
    
    # ========================================================================
    # 🔄 COMPATIBILITY METHODS
    # ========================================================================
    
    async def process_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Compatibility method"""
        return await self.process_enhanced_question(question, tenant_id)


# Export for compatibility
UnifiedEnhancedPostgresOllamaAgentWithAIResponse = UnifiedEnhancedPostgresOllamaAgent
EnhancedPostgresOllamaAgent = UnifiedEnhancedPostgresOllamaAgent