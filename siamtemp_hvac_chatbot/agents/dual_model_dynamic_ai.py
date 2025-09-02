"""
dual_model_dynamic_ai.py - Ultimate Version
============================================
Complete AI System with all professional features integrated
"""

import os
import re
import json
import asyncio
import time
import logging
import hashlib
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, date
from decimal import Decimal
from collections import deque, defaultdict
import aiohttp
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# =============================================================================
# CONVERSATION MEMORY SYSTEM
# =============================================================================

class ConversationMemory:
    """Advanced conversation memory with context tracking"""
    
    def __init__(self, max_history: int = 20):
        self.conversations = defaultdict(lambda: deque(maxlen=max_history))
        self.user_preferences = defaultdict(dict)
        self.successful_patterns = defaultdict(list)
        self.context_cache = {}
        
    def add_conversation(self, user_id: str, query: str, response: Dict[str, Any]):
        """Store conversation with rich metadata"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'query': query,
            'intent': response.get('intent', 'unknown'),
            'entities': response.get('entities', {}),
            'success': response.get('success', False),
            'sql_query': response.get('sql_query'),
            'results_count': response.get('results_count', 0),
            'processing_time': response.get('processing_time', 0)
        }
        
        self.conversations[user_id].append(entry)
        
        # Track successful patterns
        if entry['success'] and entry['sql_query']:
            pattern_key = f"{entry['intent']}_{json.dumps(entry['entities'], sort_keys=True)}"
            self.successful_patterns[pattern_key].append(entry['sql_query'])
            
        logger.debug(f"📝 Stored conversation for user {user_id}")
    
    def get_context(self, user_id: str, current_query: str) -> Dict[str, Any]:
        """Get relevant context from conversation history"""
        recent = list(self.conversations[user_id])[-5:]
        
        context = {
            'conversation_count': len(self.conversations[user_id]),
            'recent_queries': [c['query'] for c in recent],
            'recent_intents': [c['intent'] for c in recent],
            'recent_entities': self._merge_recent_entities(recent),
            'has_history': len(recent) > 0,
            'continuation_signals': self._detect_continuation(current_query, recent)
        }
        
        # Check for patterns
        if recent:
            context['dominant_intent'] = max(set([c['intent'] for c in recent]), 
                                            key=[c['intent'] for c in recent].count)
        
        return context
    
    def _merge_recent_entities(self, conversations: List[Dict]) -> Dict:
        """Merge entities from recent conversations"""
        merged = defaultdict(set)
        for conv in conversations:
            for key, value in conv.get('entities', {}).items():
                if isinstance(value, list):
                    merged[key].update(value)
                else:
                    merged[key].add(value)
        return {k: list(v) for k, v in merged.items()}
    
    def _detect_continuation(self, current_query: str, recent: List[Dict]) -> List[str]:
        """Detect if current query continues previous conversation"""
        signals = []
        
        # Check for pronouns referring to previous context
        pronouns = ['นั้น', 'นี้', 'เดิม', 'เมื่อกี้', 'ต่อ', 'เพิ่ม', 'อีก']
        for pronoun in pronouns:
            if pronoun in current_query:
                signals.append(f"pronoun:{pronoun}")
        
        # Check for incomplete questions
        if not any(q in current_query for q in ['อะไร', 'เท่าไหร่', 'กี่', 'ใคร', 'ที่ไหน']):
            if len(current_query.split()) < 3:
                signals.append("incomplete_question")
        
        return signals

# =============================================================================
# PARALLEL PROCESSING ENGINE
# =============================================================================

class ParallelProcessingEngine:
    """Execute multiple analysis tasks in parallel"""
    
    def __init__(self):
        self.performance_stats = defaultdict(list)
        
    async def parallel_analyze(self, question: str, context: Dict) -> Dict[str, Any]:
        """Run multiple analysis tasks concurrently"""
        start_time = time.time()
        
        # Define tasks
        tasks = {
            'intent': self._analyze_intent(question, context),
            'entities': self._extract_entities(question),
            'thai_conversion': self._convert_thai_text(question),
            'query_complexity': self._analyze_complexity(question),
            'business_context': self._extract_business_context(question)
        }
        
        # Execute all tasks in parallel
        try:
            results = await asyncio.gather(
                *tasks.values(),
                return_exceptions=True
            )
            
            # Map results back to task names
            task_names = list(tasks.keys())
            processed_results = {}
            
            for name, result in zip(task_names, results):
                if isinstance(result, Exception):
                    logger.warning(f"Task {name} failed: {result}")
                    processed_results[name] = self._get_task_fallback(name)
                else:
                    processed_results[name] = result
            
            # Calculate performance
            processing_time = time.time() - start_time
            self.performance_stats['parallel_times'].append(processing_time)
            
            processed_results['parallel_efficiency'] = {
                'tasks_completed': len([r for r in results if not isinstance(r, Exception)]),
                'total_tasks': len(tasks),
                'processing_time': processing_time
            }
            
            return processed_results
            
        except Exception as e:
            logger.error(f"Parallel processing failed: {e}")
            return self._get_fallback_analysis(question)
    
    async def _analyze_intent(self, question: str, context: Dict) -> Dict[str, Any]:
        """Enhanced intent analysis with business-specific patterns"""
        await asyncio.sleep(0.01)  # Simulate async work
        
        question_lower = question.lower()
        
        # Enhanced intent patterns for HVAC business
        intent_patterns = {
            'revenue_analysis': ['ยอดขาย', 'รายได้', 'revenue', 'sales', 'ยอดรวม', 'มูลค่า'],
            'overhaul_analysis': ['overhaul', 'โอเวอร์ฮอล', 'oh', 'compressor'],
            'customer_ranking': ['ลูกค้า', 'อันดับ', 'top', 'สูงสุด', 'customer', 'บริษัท'],
            'customer_history': ['ประวัติ', 'ย้อนหลัง', 'เคย', 'ซื้อขาย', 'การซ่อม', 'history'],
            'spare_parts': ['อะไหล่', 'spare', 'part', 'คลัง', 'stock', 'ราคา'],
            'spare_price': ['ราคาอะไหล่', 'ราคา', 'price', 'model', 'รุ่น'],
            'team_analysis': ['ทีม', 'team', 'งาน', 'กลุ่ม', 'service_group'],
            'work_plan': ['แผน', 'วางแผน', 'plan', 'schedule', 'จะทำ'],
            'work_summary': ['สรุปงาน', 'งานที่ทำ', 'summary', 'report'],
            'comparison': ['เปรียบเทียบ', 'compare', 'ระหว่าง', 'กับ', 'วิเคราะห์'],
            'counting': ['จำนวน', 'count', 'กี่', 'นับ', 'ทั้งหมด'],
            'quotation': ['เสนอราคา', 'quotation', 'standard', 'มาตรฐาน'],
            'monthly_analysis': ['เดือน', 'month', 'รายเดือน', 'monthly'],
            'yearly_analysis': ['ปี', 'year', 'รายปี', 'yearly', 'annual'],
            'listing': ['แสดง', 'list', 'รายการ', 'ดู', 'มีอะไรบ้าง']
        }
        
        detected_intents = []
        intent_scores = {}
        
        # Score each intent based on keyword matches
        for intent, keywords in intent_patterns.items():
            score = sum(2 if keyword in question_lower else 0 for keyword in keywords)
            if score > 0:
                intent_scores[intent] = score
                detected_intents.append(intent)
        
        # Sort by score
        detected_intents.sort(key=lambda x: intent_scores.get(x, 0), reverse=True)
        
        # Use context to refine intent
        if context.get('continuation_signals'):
            if context.get('dominant_intent'):
                detected_intents.insert(0, context['dominant_intent'])
        
        primary_intent = detected_intents[0] if detected_intents else 'general_query'
        
        return {
            'primary': primary_intent,
            'secondary': detected_intents[1:3] if len(detected_intents) > 1 else [],
            'confidence': min(0.9, 0.3 + (intent_scores.get(primary_intent, 0) * 0.1)),
            'scores': intent_scores
        }
    
    async def _extract_entities(self, question: str) -> Dict[str, List]:
        """Enhanced entity extraction with business-specific patterns"""
        await asyncio.sleep(0.01)
        
        entities = defaultdict(list)
        
        # Extract years (Thai and Gregorian)
        year_patterns = [
            (r'ปี\s*(\d{4})', 'year_marker'),
            (r'(256[5-9]|257[0-9])', 'buddhist'),
            (r'(202[2-9]|203[0-9])', 'gregorian'),
            (r'ย้อนหลัง\s*(\d+)\s*ปี', 'years_back')
        ]
        
        for pattern, year_type in year_patterns:
            matches = re.findall(pattern, question)
            for match in matches:
                if year_type == 'buddhist':
                    entities['years'].append(str(int(match) - 543))
                elif year_type == 'years_back':
                    # Calculate years back from current year
                    current_year = 2024
                    for i in range(int(match)):
                        entities['years'].append(str(current_year - i))
                elif year_type == 'year_marker':
                    year = int(match)
                    if year > 2500:  # Thai year
                        entities['years'].append(str(year - 543))
                    else:
                        entities['years'].append(match)
                else:
                    entities['years'].append(match)
        
        # Extract months (Thai and English)
        thai_months = {
            'มกราคม': '01', 'กุมภาพันธ์': '02', 'มีนาคม': '03', 
            'เมษายน': '04', 'พฤษภาคม': '05', 'มิถุนายน': '06',
            'กรกฎาคม': '07', 'สิงหาคม': '08', 'กันยายน': '09',
            'ตุลาคม': '10', 'พฤศจิกายน': '11', 'ธันวาคม': '12'
        }
        
        for thai_month, month_num in thai_months.items():
            if thai_month in question:
                entities['months'].append(month_num)
                entities['month_names'].append(thai_month)
        
        # Extract model numbers and part codes
        model_patterns = [
            r'model\s+([A-Z0-9]+(?:\s+[A-Z0-9]+)*)',
            r'รุ่น\s+([A-Z0-9]+(?:\s+[A-Z0-9]+)*)',
            r'(RCUG\d+\s*[A-Z0-9]*)',
            r'(EKAC\d+)',
            r'(EK\s+model\s+[A-Z0-9]+)',
            r'([A-Z]{2,}\d{3,})'  # General pattern for model codes
        ]
        
        for pattern in model_patterns:
            matches = re.findall(pattern, question, re.IGNORECASE)
            if matches:
                entities['models'].extend(matches)
        
        # Extract company/customer names
        company_patterns = [
            r'บริษัท\s*([^\s]+(?:\s+[^\s]+)*?)(?:\s+จำกัด)?',
            r'company\s+([^\s]+(?:\s+[^\s]+)*)',
            r'ลูกค้า\s*([^\s]+(?:\s+[^\s]+)*)',
        ]
        
        for pattern in company_patterns:
            matches = re.findall(pattern, question, re.IGNORECASE)
            if matches:
                entities['companies'].extend(matches)
        
        # Extract date ranges
        date_range_pattern = r'(\w+)\s*-\s*(\w+)'
        matches = re.findall(date_range_pattern, question)
        for start, end in matches:
            # Check if they are months
            start_month = thai_months.get(start)
            end_month = thai_months.get(end)
            if start_month and end_month:
                entities['date_range'].append({
                    'start_month': start_month,
                    'end_month': end_month
                })
        
        # Extract numbers for counting/limits
        number_patterns = [
            (r'(\d+)\s*อันดับ', 'ranking'),
            (r'top\s*(\d+)', 'ranking'),
            (r'(\d+)\s*รายการ', 'items'),
            (r'จำนวน\s*(\d+)', 'count')
        ]
        
        for pattern, num_type in number_patterns:
            matches = re.findall(pattern, question, re.IGNORECASE)
            for match in matches:
                entities['numbers'].append({'value': match, 'type': num_type})
        
        # Extract job types
        job_types = ['standard', 'มาตรฐาน', 'pm', 'overhaul', 'replacement', 'emergency']
        for job_type in job_types:
            if job_type in question.lower():
                entities['job_types'].append(job_type)
        
        # Clean up duplicates
        for key in entities:
            if isinstance(entities[key], list):
                if key == 'models':
                    # Keep models as is (might have variations)
                    entities[key] = list(set(entities[key]))
                elif key not in ['date_range', 'numbers']:
                    entities[key] = list(set(entities[key]))
        
        return dict(entities)
    
    async def _convert_thai_text(self, question: str) -> str:
        """Convert Thai years and normalize text"""
        await asyncio.sleep(0.01)
        
        converted = question
        
        # Convert Thai years to Gregorian
        thai_year_pattern = r'(256[5-9]|257[0-9])'
        matches = re.findall(thai_year_pattern, converted)
        for match in matches:
            gregorian = str(int(match) - 543)
            converted = converted.replace(match, gregorian)
        
        # Normalize common variations
        replacements = {
            'โอเวอร์ฮอล': 'overhaul',
            'พีเอ็ม': 'PM',
            'เซอร์วิส': 'service'
        }
        
        for thai, eng in replacements.items():
            converted = converted.replace(thai, eng)
        
        return converted
    
    async def _analyze_complexity(self, question: str) -> str:
        """Determine query complexity"""
        await asyncio.sleep(0.01)
        
        # Simple heuristics
        word_count = len(question.split())
        has_comparison = any(word in question for word in ['เปรียบเทียบ', 'compare', 'ระหว่าง'])
        has_aggregation = any(word in question for word in ['รวม', 'sum', 'total', 'นับ'])
        has_grouping = any(word in question for word in ['แต่ละ', 'กลุ่ม', 'group', 'by'])
        
        if has_comparison or (has_aggregation and has_grouping):
            return 'complex'
        elif has_aggregation or has_grouping or word_count > 10:
            return 'moderate'
        else:
            return 'simple'
    
    async def _extract_business_context(self, question: str) -> Dict:
        """Extract HVAC business-specific context"""
        await asyncio.sleep(0.01)
        
        context = {
            'service_types': [],
            'equipment_brands': [],
            'time_urgency': None
        }
        
        # Service types
        service_keywords = {
            'PM': ['pm', 'preventive', 'maintenance', 'บำรุงรักษา'],
            'Overhaul': ['overhaul', 'โอเวอร์ฮอล', 'oh'],
            'Replacement': ['replacement', 'เปลี่ยน', 'replace'],
            'Emergency': ['emergency', 'ฉุกเฉิน', 'urgent']
        }
        
        for service_type, keywords in service_keywords.items():
            if any(kw in question.lower() for kw in keywords):
                context['service_types'].append(service_type)
        
        # Equipment brands
        brands = ['hitachi', 'daikin', 'carrier', 'trane', 'york', 'mitsubishi']
        for brand in brands:
            if brand in question.lower():
                context['equipment_brands'].append(brand)
        
        # Time urgency
        if any(word in question for word in ['ด่วน', 'urgent', 'เร่ง', 'ทันที']):
            context['time_urgency'] = 'high'
        
        return context
    
    def _get_task_fallback(self, task_name: str) -> Any:
        """Get fallback value for failed task"""
        fallbacks = {
            'intent': {'primary': 'general_query', 'secondary': [], 'confidence': 0.3},
            'entities': {},
            'thai_conversion': '',
            'query_complexity': 'simple',
            'business_context': {}
        }
        return fallbacks.get(task_name, {})
    
    def _get_fallback_analysis(self, question: str) -> Dict:
        """Complete fallback when parallel processing fails"""
        return {
            'intent': {'primary': 'general_query', 'secondary': [], 'confidence': 0.3},
            'entities': {},
            'thai_conversion': question,
            'query_complexity': 'simple',
            'business_context': {},
            'parallel_efficiency': {'error': 'parallel_processing_failed'}
        }

# =============================================================================
# DATA CLEANING ENGINE
# =============================================================================

class DataCleaningEngine:
    """Real-time data cleaning and validation"""
    
    def __init__(self):
        self.numeric_fields = {
            'overhaul_', 'replacement', 'service_contact_',
            'parts_all_', 'product_all', 'solution_',
            'unit_price', 'balance'
        }
        self.boolean_fields = {
            'job_description_pm', 'job_description_replacement',
            'job_description_overhaul', 'job_description_start_up'
        }
        self.cleaning_stats = defaultdict(int)
    
    def clean_query_results(self, results: List[Dict], sql_query: str = None) -> List[Dict]:
        """Clean and validate query results"""
        if not results:
            return results
        
        cleaned = []
        for row in results:
            cleaned_row = self._clean_row(row)
            cleaned.append(cleaned_row)
        
        # Log statistics
        logger.debug(f"🧹 Cleaned {len(cleaned)} rows, stats: {dict(self.cleaning_stats)}")
        
        return cleaned
    
    def _clean_row(self, row: Dict) -> Dict:
        """Clean individual row"""
        cleaned = {}
        
        for key, value in row.items():
            if key in self.numeric_fields:
                cleaned[key] = self._clean_numeric(value, key)
            elif key in self.boolean_fields:
                cleaned[key] = self._clean_boolean(value, key)
            else:
                cleaned[key] = self._clean_text(value, key)
        
        return cleaned
    
    def _clean_numeric(self, value: Any, field_name: str) -> float:
        """Convert to numeric, handle NULL"""
        if value is None or value == 'NULL' or value == '':
            self.cleaning_stats['nulls_converted'] += 1
            return 0.0
        
        if isinstance(value, (int, float, Decimal)):
            return float(value)
        
        if isinstance(value, str):
            # Remove formatting
            cleaned = value.replace(',', '').replace(' ', '').strip()
            cleaned = cleaned.replace('฿', '').replace('$', '')
            
            if not cleaned or cleaned.lower() == 'null':
                self.cleaning_stats['nulls_converted'] += 1
                return 0.0
            
            try:
                result = float(cleaned)
                self.cleaning_stats['numeric_converted'] += 1
                return result
            except ValueError:
                logger.debug(f"Could not convert '{value}' to numeric in field {field_name}")
                self.cleaning_stats['conversion_errors'] += 1
                return 0.0
        
        return 0.0
    
    def _clean_boolean(self, value: Any, field_name: str) -> bool:
        """Convert to boolean"""
        if value is None or value == 'NULL':
            return False
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ['true', 't', 'yes', 'y', '1']
        
        if isinstance(value, (int, float)):
            return bool(value)
        
        return False
    
    def _clean_text(self, value: Any, field_name: str) -> str:
        """Clean text fields, fix encoding"""
        if value is None or value == 'NULL':
            return ''
        
        if not isinstance(value, str):
            return str(value)
        
        # Fix Thai encoding issues
        cleaned = self._fix_thai_encoding(value)
        
        # Remove excessive whitespace
        cleaned = ' '.join(cleaned.split())
        
        return cleaned
    
    def _fix_thai_encoding(self, text: str) -> str:
        """Fix common Thai encoding issues"""
        if not text:
            return text
        
        # Check for mojibake
        if 'à¸' in text or 'à¹' in text:
            try:
                fixed = text.encode('latin-1').decode('utf-8', errors='ignore')
                self.cleaning_stats['encoding_fixed'] += 1
                return fixed
            except:
                pass
        
        return text
    
    def get_cleaning_stats(self) -> Dict[str, int]:
        """Get cleaning statistics"""
        return dict(self.cleaning_stats)
    
    def reset_stats(self):
        """Reset cleaning statistics"""
        self.cleaning_stats.clear()

# =============================================================================
# DATABASE HANDLER
# =============================================================================

class SimplifiedDatabaseHandler:
    """Database connection and query execution"""
    
    def __init__(self):
        self.connection_cache = {}
        self.connection_configs = {
            'company-a': {
                'host': os.getenv('POSTGRES_HOST_COMPANY_A', 'postgres-company-a'),
                'port': int(os.getenv('POSTGRES_PORT_COMPANY_A', '5432')),
                'database': os.getenv('POSTGRES_DB_COMPANY_A', 'siamtemp_company_a'),
                'user': os.getenv('POSTGRES_USER_COMPANY_A', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD_COMPANY_A', 'password123')
            }
        }
    
    def get_connection(self, tenant_id: str = 'company-a'):
        """Get database connection with caching"""
        if tenant_id not in self.connection_cache:
            config = self.connection_configs.get(tenant_id, self.connection_configs['company-a'])
            try:
                conn = psycopg2.connect(**config, cursor_factory=RealDictCursor)
                self.connection_cache[tenant_id] = conn
                logger.info(f"✅ Database connected for tenant {tenant_id}")
            except Exception as e:
                logger.error(f"Database connection failed: {e}")
                raise
        
        return self.connection_cache[tenant_id]
    
    async def execute_query(self, sql: str, tenant_id: str = 'company-a') -> List[Dict]:
        """Execute SQL query asynchronously"""
        try:
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(None, self._execute_sync, sql, tenant_id)
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []
    
    def _execute_sync(self, sql: str, tenant_id: str) -> List[Dict]:
        """Synchronous query execution"""
        conn = self.get_connection(tenant_id)
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                if columns:
                    return cursor.fetchall()
                else:
                    conn.commit()
                    return []
        except Exception as e:
            conn.rollback()
            raise e
    
    def close_connections(self):
        """Close all database connections"""
        for conn in self.connection_cache.values():
            if conn and not conn.closed:
                conn.close()
        self.connection_cache.clear()

# =============================================================================
# OLLAMA CLIENT
# =============================================================================

class SimplifiedOllamaClient:
    """Ollama API client for LLM interactions"""
    
    def __init__(self):
        self.base_url = os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434')
        self.timeout = int(os.getenv('OLLAMA_TIMEOUT', '30'))
    
    async def generate_response(self, model: str, prompt: str, temperature: float = 0.7) -> str:
        """Generate response from Ollama model"""
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": 2000
            }
        }
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.post(f"{self.base_url}/api/generate", json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get('response', '')
                    else:
                        logger.error(f"Ollama API error: {response.status}")
                        return ""
        except asyncio.TimeoutError:
            logger.error(f"Ollama request timed out after {self.timeout}s")
            return ""
        except Exception as e:
            logger.error(f"Ollama request failed: {e}")
            return ""

# =============================================================================
# MAIN DUAL MODEL DYNAMIC AI SYSTEM
# =============================================================================

class DualModelDynamicAISystem:
    """Ultimate Dual-Model AI System with all features integrated"""
    
    def __init__(self):
        # Initialize all components
        self.db_handler = SimplifiedDatabaseHandler()
        self.ollama_client = SimplifiedOllamaClient()
        self.conversation_memory = ConversationMemory()
        self.parallel_processor = ParallelProcessingEngine()
        self.data_cleaner = DataCleaningEngine()
        
        # Model configuration
        self.SQL_MODEL = "mannix/defog-llama3-sqlcoder-8b:latest"
        self.NL_MODEL = "llama3.1:8b"
        
        # Feature flags
        self.enable_conversation_memory = True
        self.enable_parallel_processing = True
        self.enable_data_cleaning = True
        self.enable_sql_validation = True
        
        # Caches
        self.sql_cache = {}
        self.schema_cache = {}
        
        # Statistics
        self.stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'cache_hits': 0,
            'avg_response_time': 0
        }
        
        # Load SQL examples
        self.sql_examples = self._load_sql_examples()
        
        logger.info("🚀 Ultimate Dual-Model Dynamic AI System initialized")
    
    async def process_any_question(self, question: str, tenant_id: str = 'company-a', 
                                   user_id: str = 'default') -> Dict[str, Any]:
        """Main entry point for processing questions"""
        start_time = time.time()
        self.stats['total_queries'] += 1
        
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"🎯 Processing: {question}")
            logger.info(f"👤 User: {user_id} | 🏢 Tenant: {tenant_id}")
            
            # 1. Get conversation context
            context = {}
            if self.enable_conversation_memory:
                context = self.conversation_memory.get_context(user_id, question)
                logger.info(f"💭 Context: {context['conversation_count']} previous conversations")
            
            # 2. Parallel analysis of question
            if self.enable_parallel_processing:
                analysis = await self.parallel_processor.parallel_analyze(question, context)
            else:
                analysis = await self._sequential_analysis(question, context)
            
            intent = analysis.get('intent', {}).get('primary', 'general')
            entities = analysis.get('entities', {})
            thai_converted = analysis.get('thai_conversion', question)
            complexity = analysis.get('query_complexity', 'simple')
            
            logger.info(f"📊 Analysis: intent={intent}, complexity={complexity}")
            logger.info(f"🔍 Entities: {entities}")
            
            # 3. Generate and validate SQL
            sql_query = await self._generate_enhanced_sql(
                thai_converted or question,
                intent,
                entities,
                context,
                complexity
            )
            
            if not sql_query:
                raise ValueError("Failed to generate valid SQL query")
            
            logger.info(f"📝 SQL Generated: {sql_query[:100]}...")
            
            # 4. Execute query with optional cleaning
            raw_results = await self.db_handler.execute_query(sql_query, tenant_id)
            
            if self.enable_data_cleaning:
                results = self.data_cleaner.clean_query_results(raw_results, sql_query)
                cleaning_stats = self.data_cleaner.get_cleaning_stats()
                self.data_cleaner.reset_stats()
            else:
                results = raw_results
                cleaning_stats = {}
            
            logger.info(f"📊 Results: {len(results)} rows")
            if cleaning_stats:
                logger.info(f"🧹 Cleaning: {cleaning_stats}")
            
            # 5. Generate natural language response
            answer = await self._generate_natural_response(
                question,
                results,
                sql_query,
                intent,
                cleaning_stats
            )
            
            # 6. Prepare response
            processing_time = time.time() - start_time
            response = {
                'answer': answer,
                'success': True,
                'sql_query': sql_query,
                'results_count': len(results),
                'tenant_id': tenant_id,
                'user_id': user_id,
                'intent': intent,
                'entities': entities,
                'processing_time': processing_time,
                'ai_system': 'dual_model_dynamic',
                'features_used': {
                    'conversation_memory': self.enable_conversation_memory,
                    'parallel_processing': self.enable_parallel_processing,
                    'data_cleaning': self.enable_data_cleaning,
                    'sql_validation': self.enable_sql_validation
                }
            }
            
            if cleaning_stats:
                response['data_quality'] = cleaning_stats
            
            # 7. Update conversation memory
            if self.enable_conversation_memory:
                self.conversation_memory.add_conversation(user_id, question, response)
            
            # 8. Update statistics
            self.stats['successful_queries'] += 1
            self._update_avg_response_time(processing_time)
            
            logger.info(f"✅ Completed in {processing_time:.2f}s")
            logger.info(f"{'='*60}\n")
            
            return response
            
        except Exception as e:
            logger.error(f"❌ Processing failed: {e}")
            processing_time = time.time() - start_time
            
            return {
                'answer': f"ขออภัย เกิดข้อผิดพลาดในการประมวลผล: {str(e)}",
                'success': False,
                'error': str(e),
                'processing_time': processing_time,
                'ai_system': 'dual_model_dynamic'
            }
    
    async def _generate_enhanced_sql(self, question: str, intent: str, entities: Dict,
                                    context: Dict, complexity: str) -> str:
        """Generate SQL with validation and optimization"""
        
        # Check cache
        cache_key = hashlib.md5(f"{intent}_{entities}_{question}".encode()).hexdigest()
        if cache_key in self.sql_cache:
            self.stats['cache_hits'] += 1
            logger.info("📋 Using cached SQL")
            return self.sql_cache[cache_key]
        
        # Check for successful patterns in memory
        if self.enable_conversation_memory:
            pattern_key = f"{intent}_{json.dumps(entities, sort_keys=True)}"
            if pattern_key in self.conversation_memory.successful_patterns:
                patterns = self.conversation_memory.successful_patterns[pattern_key]
                if patterns:
                    logger.info("📋 Using successful pattern from memory")
                    return patterns[-1]  # Use most recent successful pattern
        
        # Generate new SQL with retry logic
        max_attempts = 2
        for attempt in range(max_attempts):
            try:
                # Build comprehensive prompt
                prompt = self._build_enhanced_sql_prompt(
                    question, intent, entities, context, complexity, attempt
                )
                
                # Generate SQL
                sql_response = await self.ollama_client.generate_response(
                    self.SQL_MODEL,
                    prompt,
                    temperature=0.1  # Low temperature for consistency
                )
                
                sql_query = self._clean_sql_response(sql_response)
                
                # Validate if enabled
                if self.enable_sql_validation:
                    is_valid, error_msg = self._validate_sql_query(sql_query)
                    if is_valid:
                        # Cache successful SQL
                        self.sql_cache[cache_key] = sql_query
                        return sql_query
                    else:
                        logger.warning(f"SQL validation failed (attempt {attempt + 1}): {error_msg}")
                        if attempt < max_attempts - 1:
                            continue
                else:
                    return sql_query
                    
            except Exception as e:
                logger.error(f"SQL generation attempt {attempt + 1} failed: {e}")
        
        # Fallback to safe SQL
        logger.warning("Using fallback SQL")
        return self._get_fallback_sql(intent, entities)
    
    def _build_enhanced_sql_prompt(self, question: str, intent: str, entities: Dict,
                                  context: Dict, complexity: str, attempt: int) -> str:
        """Build comprehensive prompt for SQL generation with entity replacement"""
        
        # Find relevant examples
        examples = self._find_relevant_examples(intent, question)
        
        # Replace placeholders in examples with actual entities
        processed_examples = []
        for example in examples[:3]:
            sql = example['sql']
            
            # Replace customer placeholders
            if entities.get('companies'):
                sql = sql.replace('%X%', f"%{entities['companies'][0]}%")
                sql = sql.replace('X%', f"{entities['companies'][0]}%")
            
            # Replace model placeholders
            if entities.get('models'):
                sql = sql.replace('%RCUG120%', f"%{entities['models'][0]}%")
                sql = sql.replace('%Hitachi%', f"%{entities['models'][0].split()[0]}%")
            
            # Replace date placeholders
            if entities.get('months') and entities.get('years'):
                year = entities['years'][0]
                month = entities['months'][0]
                year_short = year[-2:] if len(year) == 4 else year
                sql = sql.replace('%25-06-%', f"%{year_short}-{month}-%")
                sql = sql.replace('%08/2025%', f"%{month}/{year}%")
            
            processed_examples.append({
                'question': example['question'],
                'sql': sql
            })
        
        prompt = f"""You are an expert PostgreSQL developer for an HVAC business database.

=== DATABASE SCHEMA ===
Tables available:
- sales2022, sales2023, sales2024, sales2025: Sales and service records
  Columns: id, job_no (pattern: SVyy-mm-xxx where yy=year last 2 digits, mm=month),
           customer_name, description,
           overhaul_ (TEXT - needs CAST), replacement (TEXT - needs CAST),
           service_contact_ (TEXT - needs CAST), parts_all_ (TEXT), product_all (TEXT)
           
- spare_part, spare_part2: Spare parts inventory
  Columns: id, product_code, product_name, unit, 
           balance (TEXT - needs CAST), unit_price (TEXT - needs CAST), description
           
- work_force: Work team management
  Columns: id, date, customer, project, service_group, detail,
           job_description_pm (BOOLEAN), job_description_overhaul (BOOLEAN),
           job_description_replacement (BOOLEAN)

=== CRITICAL RULES ===
1. Revenue fields (overhaul_, replacement, service_contact_, parts_all_, product_all) are TEXT
   ALWAYS use: COALESCE(CAST(field AS NUMERIC), 0) for calculations
   
2. For boolean fields: Use field = true/false
   For counting booleans: SUM(CASE WHEN field = true THEN 1 ELSE 0 END)
   
3. Job number pattern: 'SVyy-mm-xxx' or 'JAEyy-mm-xxx'
   Example: SV24-06-001 = June 2024 job #001
   
4. Text search: Use ILIKE for case-insensitive matching
5. Always include meaningful column aliases
6. Handle NULL values with COALESCE"""

        # Add examples if available
        if processed_examples:
            prompt += "\n\n=== SIMILAR QUERY EXAMPLES ==="
            for i, example in enumerate(processed_examples, 1):
                prompt += f"\n\nExample {i}:"
                prompt += f"\nQuestion: {example['question']}"
                prompt += f"\nSQL:\n{example['sql']}"
        
        # Add entity information
        if entities:
            prompt += "\n\n=== EXTRACTED ENTITIES ==="
            if entities.get('years'):
                prompt += f"\nYears: {entities['years']} (tables: sales{entities['years'][0]} etc.)"
            if entities.get('months'):
                prompt += f"\nMonths: {entities['months']} (use in job_no pattern)"
            if entities.get('companies'):
                prompt += f"\nCompanies: {entities['companies']} (use ILIKE for matching)"
            if entities.get('models'):
                prompt += f"\nModels: {entities['models']} (search in product_name/description)"
            if entities.get('date_range'):
                prompt += f"\nDate range: {entities['date_range']}"
            if entities.get('job_types'):
                prompt += f"\nJob types: {entities['job_types']}"
        
        # Add context if available
        if context.get('recent_queries'):
            prompt += f"\n\n=== CONVERSATION CONTEXT ==="
            prompt += f"\nRecent queries: {context['recent_queries'][-2:]}"
            prompt += f"\nThis might be a follow-up question."
        
        prompt += f"""

=== YOUR TASK ===
Question: {question}
Intent: {intent}
Complexity: {complexity}

Instructions:
1. Generate PostgreSQL query that correctly answers the question
2. Use proper CAST and COALESCE for TEXT numeric fields
3. Include all relevant columns in SELECT
4. Use appropriate JOINs if needed
5. Return meaningful column names

Return ONLY the SQL query, no explanations or markdown."""
        
        if attempt > 0:
            prompt += f"""

IMPORTANT: Previous attempt failed validation.
Common issues to fix:
- Missing CAST for numeric TEXT fields in calculations
- Incorrect boolean syntax (use = true/false)
- Missing COALESCE for NULL handling
- Unbalanced parentheses"""
        
        return prompt
    
    def _validate_sql_query(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Validate generated SQL query"""
        if not sql:
            return False, "Empty SQL query"
        
        sql_upper = sql.upper()
        
        # Check basic structure
        if 'SELECT' not in sql_upper:
            return False, "Missing SELECT statement"
        if 'FROM' not in sql_upper:
            return False, "Missing FROM clause"
        
        # Check for proper CAST usage with numeric TEXT fields
        numeric_fields = ['overhaul_', 'replacement', 'service_contact_', 'balance', 'unit_price']
        for field in numeric_fields:
            if field in sql.lower():
                # Check if used in aggregation without CAST
                patterns = [f'SUM({field})', f'AVG({field})', f'MAX({field})', f'MIN({field})']
                for pattern in patterns:
                    if pattern.upper() in sql_upper:
                        if f'CAST({field}' not in sql:
                            return False, f"Field {field} needs CAST for aggregation"
        
        # Check for SQL injection attempts
        dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE']
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                return False, f"Dangerous keyword detected: {keyword}"
        
        # Check parentheses balance
        if sql.count('(') != sql.count(')'):
            return False, "Unbalanced parentheses"
        
        return True, None
    
    def _clean_sql_response(self, response: str) -> str:
        """Clean SQL response from LLM"""
        if not response:
            return ""
        
        sql = response.strip()
        
        # Remove markdown code blocks
        sql = re.sub(r'```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'```\s*', '', sql)
        
        # Find the SQL query
        lines = sql.split('\n')
        sql_lines = []
        in_sql = False
        
        for line in lines:
            line_stripped = line.strip()
            if line_stripped.upper().startswith('SELECT'):
                in_sql = True
            
            if in_sql:
                sql_lines.append(line)
                if ';' in line:
                    break
        
        sql = '\n'.join(sql_lines).strip().rstrip(';')
        
        # Final cleanup
        sql = ' '.join(sql.split())
        
        return sql
    
    def _get_fallback_sql(self, intent: str, entities: Dict) -> str:
        """Get safe fallback SQL based on intent"""
        year = entities.get('years', ['2024'])[0] if entities.get('years') else '2024'
        table = f"sales{year}"
        
        fallback_queries = {
            'revenue_analysis': f"""
                SELECT 
                    COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) as overhaul_total,
                    COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) as replacement_total,
                    COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) as service_total
                FROM {table}
            """,
            'customer_ranking': f"""
                SELECT 
                    customer_name,
                    COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) as total_revenue
                FROM {table}
                WHERE customer_name IS NOT NULL
                GROUP BY customer_name
                ORDER BY total_revenue DESC
                LIMIT 5
            """,
            'spare_parts': """
                SELECT 
                    product_code,
                    product_name,
                    CAST(balance AS NUMERIC) as stock_balance
                FROM spare_part
                WHERE balance IS NOT NULL
                ORDER BY CAST(balance AS NUMERIC) DESC
                LIMIT 10
            """,
            'team_analysis': """
                SELECT 
                    service_group,
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN job_description_pm = true THEN 1 ELSE 0 END) as pm_jobs
                FROM work_force
                WHERE service_group IS NOT NULL
                GROUP BY service_group
                ORDER BY total_jobs DESC
                LIMIT 10
            """
        }
        
        # Return appropriate fallback or generic query
        return fallback_queries.get(intent, f"SELECT COUNT(*) as total FROM {table}")
    
    async def _generate_natural_response(self, question: str, results: List[Dict],
                                        sql_query: str, intent: str,
                                        cleaning_stats: Dict) -> str:
        """Generate natural language response from results"""
        
        if not results:
            return "ไม่พบข้อมูลตามที่ต้องการ กรุณาลองเปลี่ยนเงื่อนไขการค้นหา"
        
        # For simple queries, use template-based response
        if len(results) == 1 and len(results[0]) <= 3:
            return self._generate_simple_response(results[0], question, cleaning_stats)
        
        # For complex results, optionally use LLM
        use_llm_response = os.getenv('USE_LLM_RESPONSE', 'false').lower() == 'true'
        
        if use_llm_response:
            try:
                prompt = f"""Based on this data, answer the question in Thai:
                
Question: {question}
Data: {json.dumps(results[:10], ensure_ascii=False, default=str)}
                
Provide a clear, concise answer in Thai. Include numbers and key insights."""
                
                response = await self.ollama_client.generate_response(
                    self.NL_MODEL,
                    prompt,
                    temperature=0.3
                )
                
                if response:
                    return response
                    
            except Exception as e:
                logger.warning(f"LLM response generation failed: {e}")
        
        # Fallback to structured response
        return self._generate_structured_response(results, intent, question, cleaning_stats)
    
    def _generate_simple_response(self, result: Dict, question: str, cleaning_stats: Dict) -> str:
        """Generate response for simple aggregate queries"""
        
        # Extract values
        values = {}
        for key, value in result.items():
            if isinstance(value, (int, float)):
                values[key] = value
            elif isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                values[key] = float(value)
        
        # Build response based on column names
        response_parts = []
        
        if 'overhaul_total' in values:
            response_parts.append(f"💰 ยอด Overhaul: {values['overhaul_total']:,.2f} บาท")
        
        if 'replacement_total' in values:
            response_parts.append(f"🔧 ยอด Replacement: {values['replacement_total']:,.2f} บาท")
        
        if 'service_total' in values:
            response_parts.append(f"🛠️ ยอด Service: {values['service_total']:,.2f} บาท")
        
        if 'total_revenue' in values:
            response_parts.append(f"💵 ยอดรวม: {values['total_revenue']:,.2f} บาท")
        
        if 'count' in values or 'total' in values:
            count = values.get('count', values.get('total', 0))
            response_parts.append(f"📊 จำนวน: {int(count)} รายการ")
        
        if response_parts:
            response = '\n'.join(response_parts)
            
            if cleaning_stats.get('nulls_converted', 0) > 0:
                response += f"\n\n✅ หมายเหตุ: แปลงค่า NULL เป็น 0 จำนวน {cleaning_stats['nulls_converted']} รายการ"
            
            return response
        
        # Generic response
        return f"พบข้อมูล: {json.dumps(result, ensure_ascii=False, default=str)}"
    
    def _generate_structured_response(self, results: List[Dict], intent: str,
                                     question: str, cleaning_stats: Dict) -> str:
        """Generate structured response for complex results"""
        
        response = f"📊 พบข้อมูล {len(results)} รายการ\n\n"
        
        # Show top results based on intent
        if 'ranking' in intent or 'top' in question.lower():
            for i, row in enumerate(results[:5], 1):
                # Find the main identifier
                name = (row.get('customer_name') or row.get('product_name') or 
                       row.get('service_group') or f"Item {i}")
                
                # Find the main value
                value = (row.get('total_revenue') or row.get('total') or 
                        row.get('balance') or 0)
                
                response += f"{i}. {name}: {value:,.2f}\n"
        
        elif 'spare' in intent or 'part' in question.lower():
            for i, row in enumerate(results[:10], 1):
                product = row.get('product_name', 'Unknown')
                balance = row.get('balance', 0)
                response += f"{i}. {product}: {balance} units\n"
        
        else:
            # Generic table display
            if results:
                # Show first 3 rows
                for i, row in enumerate(results[:3], 1):
                    response += f"\nรายการ {i}:\n"
                    for key, value in row.items():
                        if value is not None and value != '':
                            response += f"  - {key}: {value}\n"
        
        if cleaning_stats:
            response += f"\n📈 Data Quality: {cleaning_stats}"
        
        return response
    
    def _find_relevant_examples(self, intent: str, question: str) -> List[Dict]:
        """Find relevant SQL examples based on intent and question"""
        relevant = []
        question_lower = question.lower()
        
        for example in self.sql_examples:
            score = 0
            
            # Check intent match
            if example.get('intent') == intent:
                score += 3
            
            # Check keyword overlap
            example_keywords = example.get('keywords', [])
            for keyword in example_keywords:
                if keyword in question_lower:
                    score += 1
            
            if score > 0:
                relevant.append((score, example))
        
        # Sort by relevance and return top examples
        relevant.sort(key=lambda x: x[0], reverse=True)
        return [ex[1] for ex in relevant[:3]]
    
    def _load_sql_examples(self) -> List[Dict]:
        """Comprehensive SQL examples for HVAC business queries"""
        return [
            # Basic aggregations
            {
                'intent': 'revenue_analysis',
                'keywords': ['ยอด', 'รวม', 'total', 'sum', 'มูลค่า'],
                'question': 'ยอดรวม overhaul ปี 2024',
                'sql': """SELECT 
                    COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) as overhaul_total,
                    COUNT(CASE WHEN overhaul_ IS NOT NULL AND overhaul_ != '' THEN 1 END) as job_count
                FROM sales2024
WHERE overhaul_ IS NOT NULL"""
            },
            # Customer analysis
            {
                'intent': 'customer_ranking',
                'keywords': ['ลูกค้า', 'อันดับ', 'top', 'สูงสุด'],
                'question': 'ลูกค้า 5 อันดับแรกที่มียอดสูงสุด',
                'sql': """SELECT 
                    customer_name,
                    COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) + 
                    COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) + 
                    COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) as total_revenue
                FROM sales2024
                WHERE customer_name IS NOT NULL
                GROUP BY customer_name
                ORDER BY total_revenue DESC
                LIMIT 5"""
            },
            # Customer history
            {
                'intent': 'customer_history',
                'keywords': ['ประวัติ', 'ย้อนหลัง', 'ซื้อขาย', 'การซ่อม'],
                'question': 'บริษัท X มีประวัติการซ่อมอะไรบ้าง',
                'sql': """SELECT 
                    job_no,
                    customer_name,
                    description,
                    COALESCE(CAST(service_contact_ AS NUMERIC), 0) as service_amount,
                    COALESCE(CAST(overhaul_ AS NUMERIC), 0) as overhaul_amount,
                    COALESCE(CAST(replacement AS NUMERIC), 0) as replacement_amount
                FROM sales2024
                WHERE customer_name ILIKE '%X%'
                ORDER BY job_no DESC"""
            },
            # Multi-year customer history
            {
                'intent': 'customer_history',
                'keywords': ['ย้อนหลัง', '3 ปี', 'ประวัติ'],
                'question': 'บริษัท X มีการซื้อขายย้อนหลัง 3 ปี',
                'sql': """SELECT 
                    '2024' as year,
                    COUNT(*) as job_count,
                    COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) + 
                    COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) + 
                    COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) as total_amount
                FROM sales2024
                WHERE customer_name ILIKE '%X%'
                UNION ALL
                SELECT 
                    '2023' as year,
                    COUNT(*) as job_count,
                    COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) + 
                    COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) + 
                    COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) as total_amount
                FROM sales2023
                WHERE customer_name ILIKE '%X%'
                UNION ALL
                SELECT 
                    '2022' as year,
                    COUNT(*) as job_count,
                    COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) + 
                    COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) + 
                    COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) as total_amount
                FROM sales2022
                WHERE customer_name ILIKE '%X%'
                ORDER BY year DESC"""
            },
            # Spare parts pricing
            {
                'intent': 'spare_price',
                'keywords': ['ราคา', 'อะไหล่', 'model', 'รุ่น'],
                'question': 'ราคาอะไหล่ Hitachi model RCUG120',
                'sql': """SELECT 
                    product_code,
                    product_name,
                    CAST(unit_price AS NUMERIC) as price,
                    CAST(balance AS NUMERIC) as stock_balance,
                    unit
                FROM spare_part
                WHERE product_name ILIKE '%RCUG120%'
                   OR product_name ILIKE '%Hitachi%'
                   OR description ILIKE '%RCUG120%'
                ORDER BY price DESC"""
            },
            # Work planning
            {
                'intent': 'work_plan',
                'keywords': ['แผน', 'วางแผน', 'งาน'],
                'question': 'แผนงานวันที่ X มีงานอะไรบ้าง',
                'sql': """SELECT 
                    date,
                    customer,
                    project,
                    detail,
                    service_group,
                    CASE 
                        WHEN job_description_pm = true THEN 'PM'
                        WHEN job_description_overhaul = true THEN 'Overhaul'
                        WHEN job_description_replacement = true THEN 'Replacement'
                        ELSE 'Other'
                    END as job_type
                FROM work_force
                WHERE date ILIKE '%X%'
                ORDER BY date"""
            },
            # Monthly work summary
            {
                'intent': 'monthly_analysis',
                'keywords': ['เดือน', 'สรุปงาน', 'monthly'],
                'question': 'สรุปงานที่ทำของเดือนมิถุนายน 2568',
                'sql': """SELECT 
                    customer_name,
                    COUNT(*) as job_count,
                    COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) as service_total,
                    COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) as overhaul_total,
                    COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) as replacement_total
                FROM sales2025
                WHERE job_no LIKE '%25-06-%'
                GROUP BY customer_name
                ORDER BY job_count DESC"""
            },
            # Quotation summary
            {
                'intent': 'quotation',
                'keywords': ['เสนอราคา', 'quotation', 'standard'],
                'question': 'สรุปเสนอราคางาน Standard ทั้งหมด',
                'sql': """SELECT 
                    job_no,
                    customer_name,
                    description,
                    COALESCE(CAST(service_contact_ AS NUMERIC), 0) as quotation_amount
                FROM sales2024
                WHERE description ILIKE '%standard%'
                   OR description ILIKE '%มาตรฐาน%'
                   OR job_no LIKE '%-S-%'
                ORDER BY quotation_amount DESC"""
            },
            # Overhaul comparison
            {
                'intent': 'comparison',
                'keywords': ['เปรียบเทียบ', 'compare', 'overhaul'],
                'question': 'รายงานยอดขาย overhaul compressor ปี 2567-2568',
                'sql': """SELECT 
                    '2024' as year,
                    COUNT(CASE WHEN description ILIKE '%compressor%' THEN 1 END) as compressor_jobs,
                    COALESCE(SUM(CASE WHEN description ILIKE '%compressor%' 
                        THEN CAST(overhaul_ AS NUMERIC) ELSE 0 END), 0) as compressor_amount,
                    COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) as total_overhaul
                FROM sales2024
                WHERE overhaul_ IS NOT NULL
                UNION ALL
                SELECT 
                    '2025' as year,
                    COUNT(CASE WHEN description ILIKE '%compressor%' THEN 1 END) as compressor_jobs,
                    COALESCE(SUM(CASE WHEN description ILIKE '%compressor%' 
                        THEN CAST(overhaul_ AS NUMERIC) ELSE 0 END), 0) as compressor_amount,
                    COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) as total_overhaul
                FROM sales2025
                WHERE overhaul_ IS NOT NULL
                ORDER BY year"""
            },
            # Team performance
            {
                'intent': 'team_analysis',
                'keywords': ['ทีม', 'team', 'งาน', 'pm'],
                'question': 'ทีมที่ทำงาน PM มากที่สุด',
                'sql': """SELECT 
                    service_group,
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN job_description_pm = true THEN 1 ELSE 0 END) as pm_jobs,
                    SUM(CASE WHEN job_description_overhaul = true THEN 1 ELSE 0 END) as overhaul_jobs,
                    SUM(CASE WHEN job_description_replacement = true THEN 1 ELSE 0 END) as replacement_jobs
                FROM work_force
                WHERE service_group IS NOT NULL
                GROUP BY service_group
                ORDER BY pm_jobs DESC
                LIMIT 10"""
            },
            # Spare parts inventory
            {
                'intent': 'spare_parts',
                'keywords': ['อะไหล่', 'คงเหลือ', 'stock'],
                'question': 'อะไหล่ที่มีจำนวนคงเหลือมากที่สุด',
                'sql': """SELECT 
                    product_code,
                    product_name,
                    CAST(balance AS NUMERIC) as stock_balance,
                    CAST(unit_price AS NUMERIC) as unit_price,
                    unit,
                    CAST(balance AS NUMERIC) * CAST(unit_price AS NUMERIC) as total_value
                FROM spare_part
                WHERE balance IS NOT NULL AND balance != '0'
                ORDER BY CAST(balance AS NUMERIC) DESC
                LIMIT 10"""
            },
            # Count all customers
            {
                'intent': 'counting',
                'keywords': ['จำนวน', 'ลูกค้า', 'ทั้งหมด'],
                'question': 'จำนวนลูกค้าทั้งหมด',
                'sql': """SELECT 
                    COUNT(DISTINCT customer_name) as total_customers,
                    COUNT(*) as total_transactions
                FROM (
                    SELECT customer_name FROM sales2024
                    UNION ALL
                    SELECT customer_name FROM sales2023
                    UNION ALL
                    SELECT customer_name FROM sales2022
                ) all_customers
                WHERE customer_name IS NOT NULL"""
            },
            # Date range analysis
            {
                'intent': 'work_plan',
                'keywords': ['วางแผน', 'เดือน', 'สิงหาคม', 'กันยายน'],
                'question': 'งานที่วางแผนของเดือนสิงหาคม-กันยายน 2568',
                'sql': """SELECT 
                    date,
                    customer,
                    project,
                    detail,
                    service_group,
                    CASE 
                        WHEN job_description_pm = true THEN 'PM'
                        WHEN job_description_overhaul = true THEN 'Overhaul'
                        WHEN job_description_replacement = true THEN 'Replacement'
                        ELSE 'Other'
                    END as job_type
                FROM work_force
                WHERE (date LIKE '%08/2025%' OR date LIKE '%09/2025%'
                    OR date LIKE '%08/25%' OR date LIKE '%09/25%')
                ORDER BY date"""
            },
            # Yearly analysis with details
            {
                'intent': 'yearly_analysis',
                'keywords': ['วิเคราะห์', 'การขาย', 'ปี'],
                'question': 'วิเคราะห์การขายของปี 2567-2568',
                'sql': """SELECT 
                    year,
                    SUM(service_total) as service_revenue,
                    SUM(overhaul_total) as overhaul_revenue,
                    SUM(replacement_total) as replacement_revenue,
                    SUM(grand_total) as total_revenue,
                    SUM(job_count) as total_jobs,
                    COUNT(DISTINCT customer_name) as unique_customers
                FROM (
                    SELECT 
                        '2024' as year,
                        customer_name,
                        COUNT(*) as job_count,
                        COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) as service_total,
                        COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) as overhaul_total,
                        COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) as replacement_total,
                        COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) + 
                        COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) + 
                        COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) as grand_total
                    FROM sales2024
                    GROUP BY customer_name
                    
                    UNION ALL
                    
                    SELECT 
                        '2025' as year,
                        customer_name,
                        COUNT(*) as job_count,
                        COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) as service_total,
                        COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) as overhaul_total,
                        COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) as replacement_total,
                        COALESCE(SUM(CAST(service_contact_ AS NUMERIC)), 0) + 
                        COALESCE(SUM(CAST(overhaul_ AS NUMERIC)), 0) + 
                        COALESCE(SUM(CAST(replacement AS NUMERIC)), 0) as grand_total
                    FROM sales2025
                    GROUP BY customer_name
                ) yearly_data
                GROUP BY year
                ORDER BY year"""
            }
        ]
    
    async def _sequential_analysis(self, question: str, context: Dict) -> Dict:
        """Fallback sequential analysis when parallel processing is disabled"""
        return {
            'intent': await self.parallel_processor._analyze_intent(question, context),
            'entities': await self.parallel_processor._extract_entities(question),
            'thai_conversion': await self.parallel_processor._convert_thai_text(question),
            'query_complexity': await self.parallel_processor._analyze_complexity(question),
            'business_context': await self.parallel_processor._extract_business_context(question)
        }
    
    def _update_avg_response_time(self, new_time: float):
        """Update average response time"""
        total = self.stats['avg_response_time'] * (self.stats['successful_queries'] - 1)
        self.stats['avg_response_time'] = (total + new_time) / self.stats['successful_queries']
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get comprehensive system statistics"""
        cache_hit_rate = 0
        if self.stats['total_queries'] > 0:
            cache_hit_rate = (self.stats['cache_hits'] / self.stats['total_queries']) * 100
        
        return {
            'performance': {
                'total_queries': self.stats['total_queries'],
                'successful_queries': self.stats['successful_queries'],
                'success_rate': (self.stats['successful_queries'] / max(self.stats['total_queries'], 1)) * 100,
                'cache_hit_rate': cache_hit_rate,
                'avg_response_time': self.stats['avg_response_time']
            },
            'features': {
                'conversation_memory': self.enable_conversation_memory,
                'parallel_processing': self.enable_parallel_processing,
                'data_cleaning': self.enable_data_cleaning,
                'sql_validation': self.enable_sql_validation
            },
            'models': {
                'sql_generation': self.SQL_MODEL,
                'response_generation': self.NL_MODEL
            }
        }

# =============================================================================
# UNIFIED AGENT CLASS (for compatibility)
# =============================================================================

class UnifiedEnhancedPostgresOllamaAgent:
    """Unified agent class for backward compatibility"""
    
    def __init__(self):
        self.dual_model_ai = DualModelDynamicAISystem()
        logger.info("✅ Unified Enhanced PostgreSQL Ollama Agent initialized")
    
    async def process_any_question(self, question: str, tenant_id: str = 'company-a',
                                   user_id: str = 'default') -> Dict[str, Any]:
        """Process question using dual model system"""
        return await self.dual_model_ai.process_any_question(question, tenant_id, user_id)
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get system statistics"""
        return self.dual_model_ai.get_system_stats()

# Aliases for compatibility
EnhancedUnifiedPostgresOllamaAgent = UnifiedEnhancedPostgresOllamaAgent