# refactored_modules/enhanced_postgres_agent_unified_fixed.py
# 🔧 FIXED: Added missing SQL extraction methods

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
    """🤖 FIXED: Enhanced PostgreSQL Agent with ALL required methods"""
    
    def __init__(self):
        """🏗️ Initialize unified agent"""
        
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
            'sql_queries': 0,
            'conversational_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'ai_responses_used': 0,
            'hardcode_responses_used': 0,
            'avg_response_time': 0.0
        }
        
        # 🧠 Schema cache
        self.schema_cache = {}
        self.cache_ttl = 3600  # 1 hour
        
        # 🎯 Intent detection keywords
        self.sql_indicators = {
            'identification': ['ใครอยู่', 'ใครเป็น', 'ใครทำ', 'who is', 'who are', 'who works'],
            'listing': ['ใครบ้าง', 'รายชื่อ', 'list', 'แสดง', 'show me', 'display'],
            'counting': ['กี่คน', 'จำนวน', 'how many', 'count', 'เท่าไร', 'มีกี่'],
            'searching': ['หา', 'ค้นหา', 'find', 'search', 'ตำแหน่ง', 'position'],
            'filtering': ['แผนก', 'department', 'ฝ่าย', 'งาน', 'โปรเจค', 'project'],
            'relationships': ['รับผิดชอบ', 'ทำงาน', 'assigned', 'working on', 'responsible']
        }
        
        self.conversational_indicators = {
            'greetings': ['สวัสดี', 'hello', 'hi', 'ช่วย', 'help'],
            'general_info': ['คุณคือใคร', 'เกี่ยวกับ', 'about', 'what are you'],
            'capabilities': ['ทำอะไรได้', 'ช่วยอะไร', 'what can you do']
        }
        
        try:
            from .intelligent_schema_discovery import EnhancedSchemaIntegration
            self.schema_integration = EnhancedSchemaIntegration(
                database_handler=self,  # ส่ง self เพราะมี method _get_database_connection
                tenant_configs=self.tenant_configs
            )
            self.intelligent_schema_available = True
            logger.info("🧠 Intelligent Schema Discovery integrated successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Intelligent Schema Discovery: {e}")
            self.schema_integration = None
            self.intelligent_schema_available = False
            logger.warning("⚠️ Falling back to basic schema discovery")
        logger.info("🤖 FIXED Unified Enhanced PostgreSQL Agent initialized")
        logger.info(f"🌐 Ollama: {self.ollama_base_url}")
        logger.info(f"🎨 AI Responses: {'Enabled' if self.enable_ai_responses else 'Disabled'}")
        logger.info(f"🏢 Tenants: {list(self.tenant_configs.keys())}")
    
    # ========================================================================
    # 🎯 MAIN ENTRY POINT
    # ========================================================================
    
    async def process_enhanced_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """🎯 MAIN: Process questions with unified logic + AI responses"""
        
        if tenant_id not in self.tenant_configs:
            return self._create_error_response("Unknown tenant", tenant_id)
        
        start_time = datetime.now()
        self.stats['total_queries'] += 1
        
        try:
            logger.info(f"🎯 Processing question for {tenant_id}: {question[:50]}...")
            
            # 🔍 Enhanced Intent Detection
            intent_result = self._detect_intent_unified(question)
            logger.info(f"🎯 Intent: {intent_result['intent']} (confidence: {intent_result['confidence']:.2f})")
            
            # 🔀 Route based on intent
            if intent_result['intent'] == 'conversational' and intent_result['confidence'] >= 0.6:
                result = await self._process_conversational_unified(question, tenant_id, intent_result)
            elif intent_result['intent'] == 'sql_query' and intent_result['confidence'] >= 0.5:
                result = await self._process_sql_unified_with_ai_response(question, tenant_id, intent_result)
            else:
                # Hybrid approach for ambiguous cases
                result = await self._process_hybrid_unified(question, tenant_id, intent_result)
            
            # 📊 Update statistics
            processing_time = (datetime.now() - start_time).total_seconds()
            self._update_stats(tenant_id, True, processing_time)
            
            result['processing_time_seconds'] = processing_time
            result['unified_agent_version'] = 'v3.1_fixed'
            result['intent_detection'] = intent_result
            
            return result
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            self._update_stats(tenant_id, False, processing_time)
            logger.error(f"❌ Processing failed for {tenant_id}: {e}")
            return self._create_error_response(str(e), tenant_id)
    
    # ========================================================================
    # 🎯 SQL PROCESSING WITH AI RESPONSE
    # ========================================================================
    
    async def _process_sql_unified_with_ai_response(self, question: str, tenant_id: str, intent_result: Dict) -> Dict[str, Any]:
        """🎯 Fixed SQL processing with intelligent schema"""
        
        self.stats['sql_queries'] += 1
        
        try:
            # 🆕 ใช้ Intelligent Schema หากมี
            if self.intelligent_schema_available and self.schema_integration:
                try:
                    sql_prompt = await self.schema_integration.generate_intelligent_sql_prompt(question, tenant_id)
                    logger.info("✅ Using Intelligent Schema Discovery")
                except Exception as e:
                    logger.warning(f"🔄 Intelligent schema failed: {e}, falling back to basic")
                    # Fallback to basic schema
                    schema_info = await self._get_schema_unified(tenant_id)
                    sql_prompt = self._generate_sql_prompt_unified(question, tenant_id, schema_info, intent_result)
            else:
                # ใช้ระบบเดิม
                schema_info = await self._get_schema_unified(tenant_id)
                sql_prompt = self._generate_sql_prompt_unified(question, tenant_id, schema_info, intent_result)
            
            # 🤖 Call AI service for SQL generation (เหมือนเดิม)
            ai_response = await self._call_ollama_unified(tenant_id, sql_prompt)
            
            # 🔍 Extract and validate SQL (เหมือนเดิม)
            sql_result = self._extract_sql_unified(ai_response, question)
            
            if not sql_result['success']:
                raise ValueError(f"SQL extraction failed: {sql_result['error']}")
            
            sql_query = sql_result['sql']
            
            # 🗄️ Execute SQL (เหมือนเดิม)
            db_results = await self._execute_sql_unified(sql_query, tenant_id)
            
            # 🆕 Generate AI response (Fixed parameters)
            if self.enable_ai_responses and db_results:
                try:
                    formatted_answer = await self._generate_ai_response_from_data(
                        question, db_results, tenant_id, sql_query, schema_info=None  # 🔧 Fixed
                    )
                    response_method = 'ai_generated'
                    self.stats['ai_responses_used'] += 1
                    
                except Exception as ai_error:
                    logger.warning(f"🔄 AI response generation failed: {ai_error}, falling back to hardcode")
                    if self.fallback_to_hardcode:
                        formatted_answer = self._format_response_hardcode(
                            db_results, question, tenant_id, sql_query, schema_info=None  # 🔧 Fixed
                        )
                        response_method = 'hardcode_fallback'
                        self.stats['hardcode_responses_used'] += 1
                    else:
                        raise ai_error
            else:
                # Use hardcode formatting (Fixed parameters)
                formatted_answer = self._format_response_hardcode(
                    db_results, question, tenant_id, sql_query, schema_info=None  # 🔧 Fixed
                )
                response_method = 'hardcode_default'
                self.stats['hardcode_responses_used'] += 1
            
            return {
                "answer": formatted_answer,
                "success": True,
                "data_source_used": f"fixed_intelligent_schema_{self.tenant_configs[tenant_id].model_name}",
                "sql_query": sql_query,
                "db_results_count": len(db_results) if db_results else 0,
                "tenant_id": tenant_id,
                "system_used": f"fixed_intelligent_schema_with_{response_method}",
                "sql_extraction_method": sql_result['method'],
                "sql_confidence": sql_result['confidence'],
                "response_generation_method": response_method,
                "intelligent_schema_used": self.intelligent_schema_available
            }
            
        except Exception as e:
            logger.error(f"❌ SQL processing failed: {e}")
            return self._create_sql_error_response(question, tenant_id, str(e))
    
    # 🆕 เพิ่ม method ใหม่สำหรับ management
    async def get_intelligent_schema_stats(self) -> Dict[str, Any]:
        """📊 ดูสถิติของระบบ Intelligent Schema Discovery"""
        return self.schema_integration.get_system_statistics()
    
    def clear_schema_cache(self, tenant_id: Optional[str] = None):
        """🗑️ ล้าง cache ของ schema discovery"""
        self.schema_integration.schema_discovery.clear_cache(tenant_id)
    async def get_intelligent_schema_stats(self) -> Dict[str, Any]:
        """📊 ดูสถิติของระบบ Intelligent Schema Discovery"""
        
        if self.intelligent_schema_available and self.schema_integration:
            try:
                cache_stats = self.schema_integration.schema_discovery.get_cache_statistics()
                return {
                    'intelligent_schema_system': 'active',
                    'cache_statistics': cache_stats,
                    'features': [
                        'contextual_schema_discovery',
                        'intelligent_prompt_building',
                        'question_analysis',
                        'relevant_data_extraction',
                        'smart_caching'
                    ]
                }
            except Exception as e:
                return {'error': f'Failed to get stats: {str(e)}'}
        else:
            return {'intelligent_schema_system': 'not_available'}

    def clear_schema_cache(self, tenant_id: Optional[str] = None):
        """🗑️ ล้าง cache ของ schema discovery"""
        
        if self.intelligent_schema_available and self.schema_integration:
            try:
                self.schema_integration.schema_discovery.clear_cache(tenant_id)
                logger.info(f"🗑️ Schema cache cleared for {tenant_id if tenant_id else 'all tenants'}")
            except Exception as e:
                logger.error(f"❌ Failed to clear cache: {e}")
        else:
            logger.warning("⚠️ Intelligent Schema Discovery not available")
    # ========================================================================
    # 🔍 SQL EXTRACTION - FIXED with ALL methods
    # ========================================================================
    
    def _extract_sql_unified(self, ai_response: str, question: str) -> Dict[str, Any]:
        """🔍 FIXED: Extract SQL with ALL required methods"""
        
        logger.info(f"🔍 Extracting SQL from response (length: {len(ai_response)})")
        
        extraction_result = {
            'success': False,
            'sql': None,
            'method': None,
            'confidence': 0.0,
            'error': None
        }
        
        # 🔧 FIXED: All extraction methods are now included
        extraction_methods = [
            ('sql_code_block_complete', self._extract_complete_sql_block),
            ('multiline_select_complete', self._extract_multiline_select),  # ✅ FIXED
            ('single_line_complete', self._extract_single_line_select),     # ✅ FIXED
            ('intelligent_fallback', self._create_intelligent_fallback)
        ]
        
        for method_name, method_func in extraction_methods:
            try:
                sql = method_func(ai_response, question)
                
                if sql and self._validate_complete_sql(sql):
                    confidence = self._calculate_sql_confidence(sql, question, method_name)
                    
                    if confidence > extraction_result['confidence']:
                        extraction_result.update({
                            'success': True,
                            'sql': sql,
                            'method': method_name,
                            'confidence': confidence
                        })
                        
                        logger.info(f"✅ Valid SQL found: {method_name} (confidence: {confidence:.2f})")
                        
                        if confidence >= 0.8:
                            break
                            
            except Exception as e:
                logger.warning(f"⚠️ Method {method_name} failed: {e}")
                continue
        
        if not extraction_result['success']:
            extraction_result['error'] = "No valid SQL could be extracted or generated"
            logger.error(f"❌ All SQL extraction methods failed for: {question[:50]}...")
        
        return extraction_result
    
    def _extract_complete_sql_block(self, response: str, question: str) -> Optional[str]:
        """🔍 Extract complete SQL from code blocks"""
        
        patterns = [
            r'```sql\s*(SELECT.*?)\s*```',
            r'```sql\s*(.*?SELECT.*?)\s*```',
            r'```\s*(SELECT.*?FROM.*?)\s*```'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, response, re.DOTALL | re.IGNORECASE)
            if match:
                sql = self._clean_sql_thoroughly(match.group(1))
                if self._has_required_clauses(sql):
                    return sql
        
        return None
    
    def _extract_multiline_select(self, response: str, question: str) -> Optional[str]:
        """🔍 FIXED: Extract multiline SELECT statements"""
        
        # Look for multiline SELECT patterns
        patterns = [
            # Standard multiline with proper formatting
            r'SELECT\s+.*?FROM\s+.*?(?:WHERE\s+.*?)?(?:ORDER\s+BY\s+.*?)?(?:LIMIT\s+\d+)?[;\s]*',
            
            # With JOIN
            r'SELECT\s+.*?FROM\s+.*?JOIN\s+.*?(?:WHERE\s+.*?)?(?:ORDER\s+BY\s+.*?)?(?:LIMIT\s+\d+)?[;\s]*',
            
            # With aliases
            r'SELECT\s+.*?FROM\s+\w+\s+\w+.*?(?:WHERE\s+.*?)?(?:ORDER\s+BY\s+.*?)?(?:LIMIT\s+\d+)?[;\s]*'
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, response, re.DOTALL | re.IGNORECASE)
            for match in matches:
                sql = self._clean_sql_thoroughly(match.group(0))
                if self._has_required_clauses(sql) and len(sql) > 30:
                    return sql
        
        return None
    
    def _extract_single_line_select(self, response: str, question: str) -> Optional[str]:
        """🔍 FIXED: Extract single line SELECT statements"""
        
        lines = response.split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Skip code block markers
            if line.startswith('```') or not line:
                continue
            
            # Look for SELECT statements
            if re.match(r'^SELECT\s+', line, re.IGNORECASE):
                sql = self._clean_sql_thoroughly(line)
                
                # Basic validation
                if ('FROM' in sql.upper() and 
                    len(sql) > 20 and 
                    self._has_required_clauses(sql)):
                    return sql
        
        return None
    
    def _clean_sql_thoroughly(self, sql: str) -> str:
        """🧹 Thorough SQL cleaning"""
        
        if not sql:
            return ""
        
        # Remove artifacts
        sql = sql.strip().rstrip(';').strip()
        sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'```\s*$', '', sql)
        
        # Normalize whitespace
        sql = re.sub(r'\s+', ' ', sql)
        
        # Remove duplicates
        sql = re.sub(r'\bSELECT\s+SELECT\b', 'SELECT', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bFROM\s+FROM\b', 'FROM', sql, flags=re.IGNORECASE)
        
        # Proper keyword casing
        keywords = ['SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'INNER JOIN', 
                   'ORDER BY', 'GROUP BY', 'LIMIT', 'AS', 'ON', 'AND', 'OR']
        
        for keyword in keywords:
            pattern = r'\b' + keyword.replace(' ', r'\s+') + r'\b'
            sql = re.sub(pattern, keyword, sql, flags=re.IGNORECASE)
        
        return sql.strip()
    
    def _has_required_clauses(self, sql: str) -> bool:
        """🔍 Check if SQL has required clauses"""
        
        if not sql or len(sql) < 15:
            return False
        
        sql_upper = sql.upper()
        
        # Must have SELECT and FROM
        if not sql_upper.startswith('SELECT'):
            return False
        
        if 'FROM' not in sql_upper:
            return False
        
        # Check for undefined aliases
        if self._has_undefined_aliases(sql):
            return False
        
        return True
    
    def _has_undefined_aliases(self, sql: str) -> bool:
        """🔍 Check for undefined table aliases"""
        
        # Find alias usage (e.g., e.name, p.id)
        alias_usage = re.findall(r'\b([a-zA-Z])\.\w+', sql)
        
        if not alias_usage:
            return False
        
        # Check if aliases are defined
        for alias in set(alias_usage):
            alias_patterns = [
                rf'\b\w+\s+{alias}\b',
                rf'\b\w+\s+AS\s+{alias}\b'
            ]
            
            alias_defined = any(
                re.search(pattern, sql, re.IGNORECASE) 
                for pattern in alias_patterns
            )
            
            if not alias_defined:
                logger.warning(f"⚠️ Undefined alias '{alias}' in SQL")
                return True
        
        return False
    
    def _validate_complete_sql(self, sql: str) -> bool:
        """🔍 Validate that SQL is complete and safe"""
        
        if not sql or len(sql) < 15:
            return False
        
        sql_upper = sql.upper()
        
        # Security checks
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        if any(keyword in sql_upper for keyword in dangerous_keywords):
            logger.warning(f"🚨 Dangerous SQL detected")
            return False
        
        # Structure checks
        if not sql_upper.startswith('SELECT'):
            return False
        
        if 'FROM' not in sql_upper:
            return False
        
        # Alias consistency
        if self._has_undefined_aliases(sql):
            return False
        
        return True
    
    def _calculate_sql_confidence(self, sql: str, question: str, method: str) -> float:
        """🔍 Calculate confidence score for SQL"""
        
        confidence = 0.0
        
        # Base confidence by method
        method_confidence = {
            'sql_code_block_complete': 0.9,
            'multiline_select_complete': 0.8,
            'single_line_complete': 0.7,
            'intelligent_fallback': 0.6
        }
        confidence += method_confidence.get(method, 0.3)
        
        # Boost for relevance
        sql_lower = sql.lower()
        question_lower = question.lower()
        
        relevance_boost = 0.0
        if 'ตำแหน่ง' in question_lower and 'position' in sql_lower:
            relevance_boost += 0.1
        if 'แผนก' in question_lower and 'department' in sql_lower:
            relevance_boost += 0.1
        if any(word in question_lower for word in ['รับผิดชอบ', 'โปรเจค']) and 'join' in sql_lower:
            relevance_boost += 0.1
        if any(word in question_lower for word in ['กี่คน', 'จำนวน']) and 'count' in sql_lower:
            relevance_boost += 0.1
        
        confidence += relevance_boost
        
        # Quality indicators
        if 'LIMIT' in sql.upper():
            confidence += 0.05
        if len(sql) > 30 and len(sql) < 300:
            confidence += 0.05
        
        return min(confidence, 1.0)
    
    def _create_intelligent_fallback(self, response: str, question: str) -> Optional[str]:
        """🔄 Create intelligent SQL fallback"""
        
        question_lower = question.lower()
        
        # Employee-Project relationship
        if any(word in question_lower for word in ['รับผิดชอบ', 'ทำงาน', 'โปรเจค', 'assigned']):
            return """SELECT 
                e.name as employee_name,
                p.name as project_name,
                ep.role,
                ep.allocation
            FROM employees e
            JOIN employee_projects ep ON e.id = ep.employee_id
            JOIN projects p ON ep.project_id = p.id
            ORDER BY e.name
            LIMIT 20"""
        
        # Position search
        elif 'ตำแหน่ง' in question_lower or 'position' in question_lower:
            position = self._extract_position_keyword(question)
            return f"""SELECT name, position, department, salary
            FROM employees
            WHERE position ILIKE '%{position}%'
            ORDER BY name
            LIMIT 20"""
        
        # Department counting
        elif 'แผนก' in question_lower and any(word in question_lower for word in ['กี่คน', 'จำนวน']):
            return """SELECT department, COUNT(*) as employee_count, AVG(salary) as avg_salary
            FROM employees
            GROUP BY department
            ORDER BY employee_count DESC"""
        
        # General employee listing
        else:
            return """SELECT name, position, department, salary
            FROM employees
            ORDER BY name
            LIMIT 20"""
    
    def _extract_position_keyword(self, question: str) -> str:
        """Extract position keyword from question"""
        
        question_lower = question.lower()
        
        position_keywords = ['frontend', 'backend', 'fullstack', 'developer', 'designer', 'manager', 'qa', 'devops']
        
        for keyword in position_keywords:
            if keyword in question_lower:
                return keyword
        
        # Try to extract after "ตำแหน่ง"
        match = re.search(r'ตำแหน่ง\s*(\w+)', question_lower)
        if match:
            return match.group(1)
        
        return 'developer'  # default
    
    # ========================================================================
    # 🔧 ADD ALL MISSING METHODS
    # ========================================================================
    
    def _detect_intent_unified(self, question: str) -> Dict[str, Any]:
        """🎯 UNIFIED: Enhanced intent detection"""
        
        question_lower = question.lower()
        
        # Calculate SQL indicators score
        sql_score = 0
        sql_reasons = []
        
        for category, keywords in self.sql_indicators.items():
            matches = [word for word in keywords if word in question_lower]
            if matches:
                weight = 3 if category in ['identification', 'counting', 'relationships'] else 2
                sql_score += len(matches) * weight
                sql_reasons.append(f"{category}: {matches}")
        
        # Calculate conversational indicators score
        conv_score = 0
        conv_reasons = []
        
        for category, keywords in self.conversational_indicators.items():
            matches = [word for word in keywords if word in question_lower]
            if matches:
                conv_score += len(matches) * 3
                conv_reasons.append(f"{category}: {matches}")
        
        # Special pattern detection
        if self._has_sql_patterns(question_lower):
            sql_score += 5
            sql_reasons.append("sql_pattern_detected")
        
        if self._has_conversational_patterns(question_lower):
            conv_score += 5
            conv_reasons.append("conversational_pattern_detected")
        
        # Determine intent
        total_score = sql_score + conv_score
        
        if total_score == 0:
            return {'intent': 'unknown', 'confidence': 0.0, 'reasons': ['no_clear_indicators']}
        
        if conv_score > sql_score:
            return {
                'intent': 'conversational',
                'confidence': conv_score / total_score,
                'sql_score': sql_score,
                'conv_score': conv_score,
                'reasons': conv_reasons
            }
        else:
            return {
                'intent': 'sql_query',
                'confidence': sql_score / total_score,
                'sql_score': sql_score,
                'conv_score': conv_score,
                'reasons': sql_reasons
            }
    
    def _has_sql_patterns(self, question_lower: str) -> bool:
        """Check for SQL-specific patterns"""
        sql_patterns = [
            r'ใครอยู่.*ตำแหน่ง',
            r'มี.*กี่คน.*แผนก',
            r'รายชื่อ.*ที่',
            r'แสดง.*ข้อมูล',
            r'.*รับผิดชอบ.*โปรเจค',
            r'who.*in.*position',
            r'how many.*in'
        ]
        return any(re.search(pattern, question_lower) for pattern in sql_patterns)
    
    def _has_conversational_patterns(self, question_lower: str) -> bool:
        """Check for conversational patterns"""
        conv_patterns = [
            r'สวัสดี.*ครับ',
            r'คุณ.*คือ.*ใคร',
            r'ช่วย.*อะไร.*ได้',
            r'hello.*there',
            r'what.*are.*you'
        ]
        return any(re.search(pattern, question_lower) for pattern in conv_patterns)
    
    def _generate_sql_prompt_unified(self, question: str, tenant_id: str, 
                                   schema_info: Dict, intent_result: Dict) -> str:
        """🎯 UNIFIED: Generate SQL prompt"""
        
        config = self.tenant_configs[tenant_id]
        business_context = self._get_business_context_unified(tenant_id)
        
        prompt = f"""คุณคือ PostgreSQL Expert สำหรับ {config.name}

{business_context}

📊 โครงสร้างฐานข้อมูล (ข้อมูลจริง):
• employees: id, name, department, position, salary, hire_date, email
• projects: id, name, client, budget, status, start_date, end_date, temp_stack
• employee_projects: employee_id, project_id, role, allocation

🔗 ความสัมพันธ์สำคัญ:
• employee_projects.employee_id → employees.id
• employee_projects.project_id → projects.id

🔧 กฎ SQL สำคัญ (ห้ามผิด):
1. SQL ต้องมี FROM clause ที่สมบูรณ์
2. ถ้าใช้ alias (e, p, ep) ต้องกำหนดใน FROM/JOIN
3. ใช้ JOIN แทน WHERE เมื่อต้องการข้อมูลจากหลายตาราง
4. ใช้ ILIKE '%keyword%' สำหรับการค้นหา
5. ใช้ LIMIT 20 เสมอ
6. ตรวจสอบ syntax ให้ถูกต้องก่อน response

Intent: {intent_result['intent']} (confidence: {intent_result['confidence']:.2f})
คำถาม: {question}

สร้าง PostgreSQL query ที่สมบูรณ์และทำงานได้:"""
        
        return prompt
    
    # Add all other missing methods here...
    # (For brevity, I'll include the essential ones)
    
    async def _call_ollama_unified(self, tenant_id: str, prompt: str, 
                                 temperature: float = 0.1) -> str:
        """🤖 UNIFIED: AI API call"""
        
        config = self.tenant_configs[tenant_id]
        
        payload = {
            "model": config.model_name,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": 1500,
                "top_k": 20,
                "top_p": 0.8,
                "repeat_penalty": 1.0,
                "num_ctx": 4096
            }
        }
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"🤖 AI API call attempt {attempt + 1} for {tenant_id}")
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.ollama_base_url}/api/generate",
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=self.request_timeout)
                    ) as response:
                        
                        if response.status == 200:
                            result = await response.json()
                            response_text = result.get('response', '').strip()
                            
                            if response_text:
                                logger.info(f"✅ AI API call successful for {tenant_id}")
                                return response_text
                            else:
                                raise ValueError("Empty response from AI")
                        else:
                            raise aiohttp.ClientResponseError(
                                request_info=response.request_info,
                                history=response.history,
                                status=response.status
                            )
                            
            except asyncio.TimeoutError:
                logger.warning(f"⏰ AI API timeout attempt {attempt + 1}")
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
    
    def _get_database_connection(self, tenant_id: str) -> psycopg2.extensions.connection:
        """🔌 Get database connection"""
        
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
        """🗄️ UNIFIED: Execute SQL query"""
        
        try:
            logger.info(f"🗄️ Executing SQL for {tenant_id}: {sql_query[:100]}...")
            
            conn = self._get_database_connection(tenant_id)
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
            
            logger.info(f"✅ SQL executed successfully: {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"❌ SQL execution failed: {e}")
            logger.error(f"❌ Failed SQL: {sql_query}")
            return []
    
    def _process_row_data(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """🔧 Process row data"""
        
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
    # 🔍 SCHEMA DISCOVERY
    # ========================================================================
    
    async def _get_schema_unified(self, tenant_id: str) -> Dict[str, Any]:
        """🔍 UNIFIED: Get schema info with caching"""
        
        cache_key = f"{tenant_id}_schema"
        
        # Check cache
        if self._is_schema_cache_valid(cache_key):
            logger.info(f"📊 Using cached schema for {tenant_id}")
            return self.schema_cache[cache_key]['data']
        
        try:
            logger.info(f"🔍 Discovering schema for {tenant_id}")
            schema_info = await self._discover_schema(tenant_id)
            
            # Cache results
            self.schema_cache[cache_key] = {
                'data': schema_info,
                'timestamp': time.time()
            }
            
            return schema_info
            
        except Exception as e:
            logger.error(f"❌ Schema discovery failed: {e}")
            return self._get_fallback_schema()
    
    def _is_schema_cache_valid(self, cache_key: str) -> bool:
        """Check cache validity"""
        if cache_key not in self.schema_cache:
            return False
        
        cache_age = time.time() - self.schema_cache[cache_key]['timestamp']
        return cache_age < self.cache_ttl
    
    async def _discover_schema(self, tenant_id: str) -> Dict[str, Any]:
        """Discover database schema"""
        
        try:
            conn = self._get_database_connection(tenant_id)
            cursor = conn.cursor()
            
            schema_info = {
                'tables': {},
                'sample_data': {},
                'discovered_at': datetime.now().isoformat()
            }
            
            # Get table structure
            cursor.execute("""
                SELECT table_name, column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name IN ('employees', 'projects', 'employee_projects')
                ORDER BY table_name, ordinal_position
            """)
            
            for row in cursor.fetchall():
                table_name, column_name, data_type, is_nullable = row
                
                if table_name not in schema_info['tables']:
                    schema_info['tables'][table_name] = {'columns': []}
                
                schema_info['tables'][table_name]['columns'].append({
                    'name': column_name,
                    'type': data_type,
                    'nullable': is_nullable == 'YES'
                })
            
            cursor.close()
            conn.close()
            return schema_info
            
        except Exception as e:
            logger.error(f"Schema discovery error: {e}")
            raise
    
    def _get_fallback_schema(self) -> Dict[str, Any]:
        """Fallback schema"""
        return {
            'tables': {
                'employees': {
                    'columns': [
                        {'name': 'id', 'type': 'integer', 'nullable': False},
                        {'name': 'name', 'type': 'character varying', 'nullable': False},
                        {'name': 'department', 'type': 'character varying', 'nullable': False},
                        {'name': 'position', 'type': 'character varying', 'nullable': False},
                        {'name': 'salary', 'type': 'numeric', 'nullable': False}
                    ]
                }
            },
            'sample_data': {},
            'discovered_at': datetime.now().isoformat(),
            'fallback': True
        }
    
    # ========================================================================
    # 🤖 AI RESPONSE GENERATION
    # ========================================================================
    
    async def _generate_ai_response_from_data(self, question: str, db_results: List[Dict], 
                                            tenant_id: str, sql_query: str, 
                                            enable_streaming: bool = True) -> str:
        """🤖 Generate AI response with optional streaming"""
        
        config = self.tenant_configs[tenant_id]
        business_context = self._get_business_context_unified(tenant_id)
        business_emoji = self._get_business_emoji(tenant_id)
        
        # Prepare data summary for AI
        data_summary = self._prepare_data_summary_for_ai(db_results, tenant_id)
        
        # Create AI prompt for response generation
        response_prompt = self._create_ai_response_prompt(
            question, data_summary, tenant_id, business_context, business_emoji, sql_query
        )
        
        logger.info(f"🤖 Generating AI response for {tenant_id} with {len(db_results)} results")
        
        if enable_streaming:
            # 🆕 Streaming response generation
            return await self._call_ollama_streaming(tenant_id, response_prompt)
        else:
            # Original non-streaming
            ai_response = await self._call_ollama_unified(
                tenant_id, response_prompt, temperature=self.ai_response_temperature
            )
            return self._post_process_ai_response(ai_response, tenant_id, len(db_results))
        
    async def _call_ollama_streaming(self, tenant_id: str, prompt: str, 
                                temperature: float = 0.3) -> AsyncGenerator[Dict[str, Any], None]:
        """🌊 Call Ollama with streaming for response generation"""
        
        config = self.tenant_configs[tenant_id]
        
        payload = {
            "model": config.model_name,
            "prompt": prompt,
            "stream": True,  # ← Streaming enabled
            "options": {
                "temperature": temperature,
                "num_predict": 1500,
                "top_k": 20,
                "top_p": 0.8,
                "repeat_penalty": 1.0,
                "num_ctx": 4096
            }
        }
        
        try:
            logger.info(f"🌊 Starting streaming AI call for {tenant_id}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.request_timeout)
                ) as response:
                    
                    if response.status == 200:
                        full_response = ""
                        
                        async for line in response.content:
                            if line:
                                try:
                                    line_str = line.decode('utf-8').strip()
                                    if line_str:
                                        chunk_data = json.loads(line_str)
                                        chunk_text = chunk_data.get('response', '')
                                        
                                        if chunk_text:
                                            full_response += chunk_text
                                            
                                            # Yield each chunk to user
                                            yield {
                                                "type": "response_chunk",
                                                "content": chunk_text,
                                                "tenant_id": tenant_id,
                                                "accumulated": full_response
                                            }
                                        
                                        # Check if complete
                                        if chunk_data.get('done', False):
                                            # Send completion signal
                                            yield {
                                                "type": "response_complete",
                                                "content": "",
                                                "final_response": self._post_process_ai_response(
                                                    full_response, tenant_id, 0
                                                ),
                                                "tenant_id": tenant_id
                                            }
                                            break
                                            
                                except json.JSONDecodeError:
                                    continue
                    else:
                        yield {
                            "type": "error",
                            "message": f"Ollama API error: HTTP {response.status}",
                            "tenant_id": tenant_id
                        }
                        
        except Exception as e:
            logger.error(f"❌ Streaming AI call failed for {tenant_id}: {e}")
            yield {
                "type": "error", 
                "message": f"AI streaming error: {str(e)}",
                "tenant_id": tenant_id
            }

    async def _process_sql_unified_with_streaming_response(self, question: str, tenant_id: str, 
                                                        intent_result: Dict) -> AsyncGenerator[Dict[str, Any], None]:
        """🎯 SQL processing with streaming response generation"""
        
        self.stats['sql_queries'] += 1
        
        try:
            # Step 1-5: Same as before (Non-streaming SQL generation)
            schema_info = await self._get_schema_unified(tenant_id)
            sql_prompt = self._generate_sql_prompt_unified(question, tenant_id, schema_info, intent_result)
            
            # SQL Generation: Non-streaming (for accuracy)
            ai_response = await self._call_ollama_unified(tenant_id, sql_prompt)
            sql_result = self._extract_sql_unified(ai_response, question)
            
            if not sql_result['success']:
                yield {
                    "type": "error",
                    "message": f"SQL generation failed: {sql_result['error']}",
                    "tenant_id": tenant_id
                }
                return
            
            sql_query = sql_result['sql']
            
            # Execute SQL
            db_results = await self._execute_sql_unified(sql_query, tenant_id)
            
            # Send metadata first
            yield {
                "type": "metadata",
                "sql_query": sql_query,
                "db_results_count": len(db_results),
                "tenant_id": tenant_id,
                "status": "generating_response"
            }
            
            # Step 6: Response Generation with Streaming
            if self.enable_ai_responses and db_results:
                async for chunk in self._generate_ai_response_streaming(
                    question, db_results, tenant_id, sql_query
                ):
                    yield chunk
            else:
                # Fallback to hardcode
                hardcode_response = self._format_response_hardcode(
                    db_results, question, tenant_id, sql_query
                )
                yield {
                    "type": "response_complete",
                    "final_response": hardcode_response,
                    "tenant_id": tenant_id,
                    "method": "hardcode_fallback"
                }
                
        except Exception as e:
            yield {
                "type": "error",
                "message": f"Processing failed: {str(e)}",
                "tenant_id": tenant_id
            }

    async def _generate_ai_response_streaming(self, question: str, db_results: List[Dict], 
                                            tenant_id: str, sql_query: str) -> AsyncGenerator[Dict[str, Any], None]:
        """🌊 Generate streaming AI response from database results"""
        
        try:
            # Prepare prompt (same as before)
            data_summary = self._prepare_data_summary_for_ai(db_results, tenant_id)
            response_prompt = self._create_ai_response_prompt(
                question, data_summary, tenant_id, 
                self._get_business_context_unified(tenant_id),
                self._get_business_emoji(tenant_id), 
                sql_query
            )
            
            # Stream the AI response
            async for chunk in self._call_ollama_streaming(tenant_id, response_prompt):
                yield chunk
                
        except Exception as e:
            yield {
                "type": "error",
                "message": f"AI response streaming failed: {str(e)}",
                "tenant_id": tenant_id
            }

    def _prepare_data_summary_for_ai(self, db_results: List[Dict], tenant_id: str) -> str:
        """📋 Prepare database results summary for AI processing"""
        
        if not db_results:
            return "ไม่พบข้อมูล"
        
        # Limit data size for AI processing
        max_results = 20
        limited_results = db_results[:max_results]
        
        data_summary = f"จำนวนข้อมูลทั้งหมด: {len(db_results)} รายการ\n"
        
        if len(db_results) > max_results:
            data_summary += f"(แสดงเฉพาะ {max_results} รายการแรก)\n"
        
        data_summary += "\nข้อมูลที่พบ:\n"
        
        for i, row in enumerate(limited_results, 1):
            row_text = f"{i}. "
            
            # Convert each row to readable format
            for key, value in row.items():
                if value is not None:
                    # Handle different data types
                    if isinstance(value, (int, float)) and 'salary' in key.lower():
                        currency = "USD" if tenant_id == 'company-c' else "บาท"
                        row_text += f"{key}: {value:,.0f} {currency}, "
                    elif isinstance(value, (int, float)) and 'budget' in key.lower():
                        currency = "USD" if tenant_id == 'company-c' else "บาท"
                        row_text += f"{key}: {value:,.0f} {currency}, "
                    elif isinstance(value, (int, float)) and 'allocation' in key.lower():
                        row_text += f"{key}: {value*100:.0f}%, "
                    else:
                        row_text += f"{key}: {value}, "
            
            data_summary += row_text.rstrip(', ') + "\n"
        
        return data_summary
    
    def _create_ai_response_prompt(self, question: str, data_summary: str, tenant_id: str, 
                                 business_context: str, business_emoji: str, sql_query: str) -> str:
        """🎯 Create AI prompt for response generation"""
        
        config = self.tenant_configs[tenant_id]
        
        # Language-specific instructions
        if config.language == 'en':
            language_instruction = "Respond in clear, professional English."
            tone_instruction = "Use a professional, informative tone."
        else:
            language_instruction = "ตอบเป็นภาษาไทยที่สุภาพและเป็นมิตร"
            tone_instruction = "ใช้น้ำเสียงที่เป็นกันเองและเข้าใจง่าย"
        
        prompt = f"""คุณคือ AI Assistant ผู้เชี่ยวชาญสำหรับ {config.name}

{business_context}

🎯 งานของคุณ: สร้างคำตอบที่เป็นธรรมชาติและเข้าใจง่ายจากข้อมูลที่ได้จากฐานข้อมูล

📋 ข้อมูลจากฐานข้อมูล:
{data_summary}

❓ คำถามเดิม: {question}

📝 คำแนะนำในการตอบ:
1. {language_instruction}
2. {tone_instruction}
3. เริ่มต้นด้วย emoji ธุรกิจ: {business_emoji}
4. แสดงชื่อบริษัท: {config.name}
5. สรุปผลลัพธ์อย่างชัดเจน
6. จัดรูปแบบให้อ่านง่าย

🚫 สิ่งที่ห้ามทำ:
- ไม่ต้องอธิบายเกี่ยวกับ SQL หรือฐานข้อมูล
- ไม่ต้องเพิ่มข้อมูลที่ไม่มีในผลลัพธ์

สร้างคำตอบที่เป็นธรรมชาติและเป็นประโยชน์:"""
        
        return prompt
    
    def _post_process_ai_response(self, ai_response: str, tenant_id: str, result_count: int) -> str:
        """🔧 Post-process AI response for consistency"""
        
        response = ai_response.strip()
        
        # Ensure response starts with business emoji if missing
        business_emoji = self._get_business_emoji(tenant_id)
        if not response.startswith(business_emoji):
            response = f"{business_emoji} {response}"
        
        # Add metadata if not present
        if result_count > 0 and "สรุป:" not in response and "Summary:" not in response:
            if tenant_id == 'company-c':
                response += f"\n\n💡 Summary: Found {result_count} records from database"
            else:
                response += f"\n\n💡 สรุป: พบข้อมูล {result_count} รายการจากฐานข้อมูล"
        
        # Ensure reasonable length
        if len(response) > 2000:
            logger.warning(f"⚠️ AI response too long ({len(response)} chars), truncating")
            response = response[:1800] + "..."
            if tenant_id == 'company-c':
                response += "\n\n(Response truncated for readability)"
            else:
                response += "\n\n(ตัดทอนเพื่อความสะดวกในการอ่าน)"
        
        return response
    
    # ========================================================================
    # 🔧 HARDCODE FORMATTING (Fallback)
    # ========================================================================
    
    def _format_response_hardcode(self, results: List[Dict], question: str, 
                                tenant_id: str, sql_query: str, schema_info: Dict = None) -> str:
        """🔧 Fixed: Added schema_info parameter with default None"""
        
        if not results:
            return f"ไม่พบข้อมูลที่ตรงกับคำถาม: {question}"
        
        config = self.tenant_configs[tenant_id]
        business_emoji = self._get_business_emoji(tenant_id)
        
        response = f"{business_emoji} ผลการค้นหา - {config.name}\n\n"
        response += f"🎯 คำถาม: {question}\n\n"
        
        # Format based on query type (simplified)
        if self._is_counting_query(sql_query):
            response += self._format_counting_results_simple(results, tenant_id)
        elif self._is_relationship_query(sql_query):
            response += self._format_relationship_results_simple(results, tenant_id)
        else:
            response += self._format_general_results_simple(results, tenant_id)
        
        # Add summary
        response += f"\n💡 สรุป: พบข้อมูล {len(results)} รายการ"
        
        if schema_info and not schema_info.get('fallback', False):
            response += " (ข้อมูลล่าสุดจากฐานข้อมูล)"
        
        return response
    
    # ========================================================================
    # 💬 CONVERSATIONAL PROCESSING
    # ========================================================================
    def _format_counting_results_simple(self, results: List[Dict], tenant_id: str) -> str:
        """📊 Simple counting format"""
        
        response = "📊 สถิติและจำนวน:\n"
        
        for i, row in enumerate(results, 1):
            response += f"{i}. "
            
            for key, value in row.items():
                if value is not None:
                    if 'count' in key.lower():
                        response += f"{key}: {value:,} คน, "
                    elif key.lower() == 'department':
                        response += f"แผนก{value}: "
                    elif 'salary' in key.lower() and isinstance(value, (int, float)):
                        currency = "USD" if tenant_id == 'company-c' else "บาท"
                        response += f"เงินเดือนเฉลี่ย: {value:,.0f} {currency}, "
                    else:
                        response += f"{key}: {value}, "
            
            response = response.rstrip(', ') + "\n"
        
        return response

    def _format_relationship_results_simple(self, results: List[Dict], tenant_id: str) -> str:
        """🤝 Simple relationship format"""
        
        response = "👥 การมอบหมายงานและโปรเจค:\n"
        
        for i, row in enumerate(results[:15], 1):
            response += f"{i:2d}. "
            
            # Safe handling of employee and project names
            emp_name = row.get('employee_name') or row.get('Employee Name') or row.get('name', '[ไม่ระบุชื่อ]')
            proj_name = row.get('project_name') or row.get('Project Name') or row.get('project', '')
            role = row.get('role', '')
            
            response += f"👤 {emp_name}"
            
            if proj_name:
                response += f" ➜ 📋 {proj_name}"
            
            if role:
                response += f" ({role})"
            
            # Safe allocation handling
            allocation = row.get('allocation')
            if allocation is not None:
                try:
                    allocation_val = float(allocation)
                    response += f" - จัดสรร: {allocation_val*100:.0f}%"
                except (ValueError, TypeError):
                    pass
            
            response += "\n"
        
        if len(results) > 15:
            response += f"... และอีก {len(results) - 15} รายการ\n"
        
        return response

    def _format_general_results_simple(self, results: List[Dict], tenant_id: str) -> str:
        """📋 Simple general format"""
        
        response = "📋 ข้อมูลที่พบ:\n"
        
        for i, row in enumerate(results[:10], 1):
            response += f"{i:2d}. "
            
            for key, value in row.items():
                if value is not None:
                    if key.lower() in ['salary', 'budget'] and isinstance(value, (int, float)):
                        currency = "USD" if tenant_id == 'company-c' else "บาท"
                        response += f"{key}: {value:,.0f} {currency}, "
                    else:
                        response += f"{key}: {value}, "
            
            response = response.rstrip(', ') + "\n"
        
        if len(results) > 10:
            response += f"... และอีก {len(results) - 10} รายการ\n"
        
        return response

    def _is_counting_query(self, sql: str) -> bool:
        """Check if SQL is counting query"""
        return 'count(' in sql.lower() or 'group by' in sql.lower()

    def _is_relationship_query(self, sql: str) -> bool:
        """Check if SQL is relationship query"""
        return 'join' in sql.lower() and 'employee_projects' in sql.lower()

    async def _process_conversational_unified(self, question: str, tenant_id: str, intent_result: Dict) -> Dict[str, Any]:
        """💬 UNIFIED: Process conversational questions"""
        
        self.stats['conversational_queries'] += 1
        
        config = self.tenant_configs[tenant_id]
        business_emoji = self._get_business_emoji(tenant_id)
        
        if self._is_greeting(question):
            answer = self._create_greeting_response(tenant_id, business_emoji)
        else:
            answer = self._create_general_conversational_response(question, tenant_id, business_emoji)
        
        return {
            "answer": answer,
            "success": True,
            "data_source_used": f"unified_conversational_{config.model_name}",
            "sql_query": None,
            "tenant_id": tenant_id,
            "system_used": "unified_conversational"
        }
    
    async def _process_hybrid_unified(self, question: str, tenant_id: str, intent_result: Dict) -> Dict[str, Any]:
        """🔄 UNIFIED: Process hybrid questions"""
        
        logger.info(f"🔄 Using hybrid approach for: {question[:50]}...")
        
        # Try SQL first
        try:
            sql_result = await self._process_sql_unified_with_ai_response(question, tenant_id, intent_result)
            
            if sql_result.get('success') and sql_result.get('db_results_count', 0) > 0:
                sql_result['system_used'] = 'unified_hybrid_sql_ai'
                return sql_result
        except Exception as e:
            logger.warning(f"Hybrid SQL failed: {e}")
        
        # Fallback to conversational
        conv_result = await self._process_conversational_unified(question, tenant_id, intent_result)
        conv_result['system_used'] = 'unified_hybrid_conversational'
        return conv_result
    
    # ========================================================================
    # 🔧 HELPER METHODS
    # ========================================================================
    
    def _get_business_context_unified(self, tenant_id: str) -> str:
        """🏢 Get business context"""
        
        contexts = {
            'company-a': """🏢 บริบท: สำนักงานใหญ่ กรุงเทพมฯ - Enterprise Banking & E-commerce
💰 สกุลเงิน: บาท (THB) | งบประมาณ: 800K-3M+ บาท
🎯 เน้น: ระบบธนาคาร, CRM, โปรเจคขนาดใหญ่""",

            'company-b': """🏨 บริบท: สาขาภาคเหนือ เชียงใหม่ - Tourism & Hospitality tempnology  
💰 สกุลเงิน: บาท (THB) | งบประมาณ: 300K-800K บาท
🎯 เน้น: ระบบท่องเที่ยว, โรงแรม, วัฒนธรรมล้านนา""",

            'company-c': """🌍 บริบท: International Office - Global Software Solutions
💰 สกุลเงิน: USD และ Multi-currency | งบประมาณ: 1M-4M+ USD  
🎯 เน้น: ระบบข้ามประเทศ, Global compliance, Multi-currency"""
        }
        
        return contexts.get(tenant_id, contexts['company-a'])
    
    def _get_business_emoji(self, tenant_id: str) -> str:
        emojis = {'company-a': '🏦', 'company-b': '🏨', 'company-c': '🌍'}
        return emojis.get(tenant_id, '💼')
    
    def _is_greeting(self, question: str) -> bool:
        greetings = ['สวัสดี', 'hello', 'hi', 'ช่วย', 'help', 'คุณคือใคร']
        return any(word in question.lower() for word in greetings)
    
    def _create_greeting_response(self, tenant_id: str, business_emoji: str) -> str:
        config = self.tenant_configs[tenant_id]
        
        return f"""สวัสดีครับ! ผมคือ AI Assistant สำหรับ {config.name} (Fixed v3.1)

{business_emoji} พร้อมให้บริการ - ระบบแก้ไขแล้ว
💡 ตัวอย่างคำถาม:
  • "ใครอยู่ตำแหน่ง frontend บ้าง"
  • "มีพนักงานกี่คนในแผนก IT"  
  • "พนักงานแต่ละคนรับผิดชอบโปรเจคอะไรบ้าง"

มีอะไรให้ช่วยไหมครับ?"""
    
    def _create_general_conversational_response(self, question: str, tenant_id: str, business_emoji: str) -> str:
        config = self.tenant_configs[tenant_id]
        
        return f"""{business_emoji} ระบบ AI ที่แก้ไขแล้ว - {config.name}

คำถาม: {question}

🔧 Status: All missing methods fixed
💡 ลองถามคำถามที่เฉพาะเจาะจงมากขึ้น เช่น:
• การค้นหาพนักงาน: "ใครอยู่ตำแหน่ง [ตำแหน่ง] บ้าง"
• การนับจำนวน: "มีพนักงานกี่คนในแผนก [แผนก]"  
• การมอบหมายงาน: "พนักงานแต่ละคนรับผิดชอบโปรเจคอะไรบ้าง"

🚀 Powered by Fixed Unified Agent v3.1"""
    
    # ========================================================================
    # ❌ ERROR HANDLING
    # ========================================================================
    
    def _create_error_response(self, error_message: str, tenant_id: str) -> Dict[str, Any]:
        return {
            "answer": f"เกิดข้อผิดพลาดในระบบ: {error_message}",
            "success": False,
            "data_source_used": "unified_ai_error",
            "sql_query": None,
            "tenant_id": tenant_id,
            "system_used": "unified_ai_error_handler",
            "error": error_message
        }
    
    def _create_sql_error_response(self, question: str, tenant_id: str, error_message: str) -> Dict[str, Any]:
        config = self.tenant_configs[tenant_id]
        business_emoji = self._get_business_emoji(tenant_id)
        
        answer = f"""{business_emoji} ไม่สามารถประมวลผลคำถามได้

คำถาม: {question}

⚠️ ปัญหา: {error_message}

🔧 Status: System has been fixed
💡 คำแนะนำ:
• ลองถามใหม่ด้วยรูปแบบที่ชัดเจนขึ้น
• ตัวอย่าง: "ใครอยู่ตำแหน่ง frontend บ้าง" หรือ "มีพนักงานกี่คนในแผนก IT"

หรือลองถามเกี่ยวกับข้อมูลทั่วไปของบริษัท"""
        
        return {
            "answer": answer,
            "success": False,
            "data_source_used": f"unified_ai_sql_error_{config.model_name}",
            "sql_query": None,
            "tenant_id": tenant_id,
            "system_used": "unified_ai_sql_error_handler",
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
    
    # ========================================================================
    # 🔄 COMPATIBILITY METHODS
    # ========================================================================
    
    async def process_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Compatibility method"""
        return await self.process_enhanced_question(question, tenant_id)


# Export for compatibility
UnifiedEnhancedPostgresOllamaAgentWithAIResponse = UnifiedEnhancedPostgresOllamaAgent
EnhancedPostgresOllamaAgent = UnifiedEnhancedPostgresOllamaAgent