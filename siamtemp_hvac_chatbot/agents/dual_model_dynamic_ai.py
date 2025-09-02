"""
dual_model_dynamic_ai.py - Ultimate Version with Enhanced Data Cleaning
========================================================================
Complete AI System with Enhanced Real-time Data Cleaner integrated
"""

import os
import re
import json
import asyncio
import time
import logging
import hashlib
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, date, timedelta
from decimal import Decimal
from collections import deque, defaultdict
import aiohttp
import psycopg2
from psycopg2.extras import RealDictCursor

# Import Enhanced Data Cleaner (make sure realtime_data_cleaner.py is in same directory)
try:
    from realtime_data_cleaner import EnhancedRealTimeDataCleaner, EnhancedSQLGenerator, ResponseValidator
    ENHANCED_CLEANER_AVAILABLE = True
except ImportError:
    ENHANCED_CLEANER_AVAILABLE = False
    print("Warning: Enhanced cleaner not available, using fallback")

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
        
        # Intent patterns with Thai keywords - ตรวจสอบ intent ที่เฉพาะเจาะจงก่อน
        
        # Job/Work summary (ต้องตรวจสอบก่อน spare_parts)
        if any(word in question_lower for word in ['สรุปงาน', 'งานที่ทำ', 'เสนอราคา', 'quotation', 'standard']):
            primary = 'job_quotation'
        # Revenue/Sales analysis
        elif any(word in question_lower for word in ['ยอด', 'รายได้', 'รายรับ', 'เงิน', 'มูลค่า', 'revenue', 'income', 'วิเคราะห์การขาย', 'วิเคราะห์']):
            primary = 'revenue_analysis'
        # Customer analysis
        elif any(word in question_lower for word in ['ลูกค้า', 'บริษัท', 'customer', 'client']):
            primary = 'customer_analysis'
        # Spare parts (ตรวจสอบหลังจาก job เพื่อไม่ให้สับสน)
        elif any(word in question_lower for word in ['อะไหล่', 'spare', 'part', 'ราคาอะไหล่', 'motor', 'chiller']):
            primary = 'spare_parts'
        # Work force
        elif any(word in question_lower for word in ['ช่าง', 'ทีม', 'พนักงาน', 'work', 'team', 'technician']):
            primary = 'work_force'
        # Service analysis
        elif any(word in question_lower for word in ['บริการ', 'pm', 'maintenance', 'service', 'ซ่อม', 'บำรุง']):
            primary = 'service_analysis'
        else:
            primary = 'general_query'
        
        # Check continuation from context
        if context.get('has_history') and context.get('continuation_signals'):
            primary = context.get('dominant_intent', primary)
        
        confidence = 0.9 if primary != 'general_query' else 0.5
        
        return {
            'primary': primary,
            'secondary': [],
            'confidence': confidence
        }
    
    async def _extract_entities(self, question: str) -> Dict[str, List]:
        """Extract business entities from question"""
        await asyncio.sleep(0.01)
        
        entities = {}
        question_lower = question.lower()
        
        # Extract years (including Buddhist Era)
        year_patterns = [r'25\d{2}', r'20\d{2}', r'ปี\s*(\d{2,4})', r'\d{4}']
        for pattern in year_patterns:
            matches = re.findall(pattern, question)
            if matches:
                # Convert Buddhist Era to Anno Domini
                converted_years = []
                for year in matches:
                    year_str = year.replace('ปี', '').strip()
                    converted = self._convert_year_be_to_ad(year_str)
                    converted_years.append(converted)
                entities['years'] = converted_years
                break
        
        # Extract months
        thai_months = {
            'มกราคม': 1, 'กุมภาพันธ์': 2, 'มีนาคม': 3, 'เมษายน': 4,
            'พฤษภาคม': 5, 'มิถุนายน': 6, 'กรกฎาคม': 7, 'สิงหาคม': 8,
            'กันยายน': 9, 'ตุลาคม': 10, 'พฤศจิกายน': 11, 'ธันวาคม': 12
        }
        
        for month_name, month_num in thai_months.items():
            if month_name in question:
                entities.setdefault('months', []).append(month_num)
        
        # Extract company names
        company_patterns = [
            r'บริษัท\s*([^\s]+)', r'COMPANY\s+([A-Z]+)', 
            'AGC', 'HONDA', 'CLARION', 'STANLEY'
        ]
        companies = []
        for pattern in company_patterns:
            if isinstance(pattern, str) and pattern in question.upper():
                companies.append(pattern)
            else:
                matches = re.findall(pattern, question, re.IGNORECASE)
                companies.extend(matches)
        
        if companies:
            entities['companies'] = list(set(companies))
        
        return entities
    
    async def _convert_thai_text(self, question: str) -> str:
        """Convert Thai text patterns"""
        await asyncio.sleep(0.01)
        return question  # Placeholder for Thai conversion
    
    async def _analyze_complexity(self, question: str) -> str:
        """Analyze query complexity"""
        await asyncio.sleep(0.01)
        
        complexity_indicators = {
            'complex': ['เปรียบเทียบ', 'compare', 'trend', 'แนวโน้ม', 'ทั้งหมด', 'รวม'],
            'medium': ['กี่', 'เท่าไหร่', 'how many', 'how much'],
            'simple': ['อะไร', 'what', 'ใคร', 'who']
        }
        
        for level, indicators in complexity_indicators.items():
            if any(ind in question.lower() for ind in indicators):
                return level
        
        return 'simple'
    
    async def _extract_business_context(self, question: str) -> Dict:
        """Extract business-specific context"""
        await asyncio.sleep(0.01)
        
        context = {}
        
        # Check for aggregation needs
        if any(word in question.lower() for word in ['รวม', 'ทั้งหมด', 'total', 'sum']):
            context['needs_aggregation'] = True
        
        # Check for comparison
        if any(word in question.lower() for word in ['เทียบ', 'compare', 'vs']):
            context['needs_comparison'] = True
        
        # Check for ranking
        if any(word in question.lower() for word in ['สูงสุด', 'top', 'มากที่สุด', 'highest']):
            context['needs_ranking'] = True
            
        return context
    
    def _convert_year_be_to_ad(self, year_str: str) -> str:
        """Convert Buddhist Era (BE) year to Anno Domini (AD)"""
        # Year mapping for Thai Buddhist Era
        year_mappings = {
            # Buddhist Era -> Anno Domini
            '2565': '2022', '2566': '2023', '2567': '2024', '2568': '2025',
            '65': '2022', '66': '2023', '67': '2024', '68': '2025',
            # Anno Domini (keep as is)
            '2022': '2022', '2023': '2023', '2024': '2024', '2025': '2025',
            '22': '2022', '23': '2023', '24': '2024', '25': '2025'
        }
        
        # Check mapping first
        if year_str in year_mappings:
            return year_mappings[year_str]
        
        try:
            year_int = int(year_str)
            
            # Full Buddhist Era year (2500+)
            if year_int >= 2500 and year_int <= 2600:
                return str(year_int - 543)
            
            # Short Buddhist Era year (60-99)
            elif year_int >= 60 and year_int <= 99:
                be_year = 2500 + year_int
                return str(be_year - 543)
            
            # Anno Domini year (1900-2100)
            elif year_int >= 1900 and year_int <= 2100:
                return str(year_int)
            
            # Short Anno Domini year (00-99)
            elif year_int >= 0 and year_int <= 99:
                if year_int <= 30:  # Assume 2000s
                    return str(2000 + year_int)
                else:  # Assume 1900s
                    return str(1900 + year_int)
        except ValueError:
            pass
        
        # Default fallback to current year
        return '2024'
    
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
# ENHANCED DATA CLEANING ENGINE
# =============================================================================

class DataCleaningEngine:
    """Enhanced data cleaning engine using EnhancedRealTimeDataCleaner"""
    
    def __init__(self):
        if ENHANCED_CLEANER_AVAILABLE:
            # Use enhanced cleaner
            self.cleaner = EnhancedRealTimeDataCleaner()
            self.sql_generator = EnhancedSQLGenerator(self.cleaner)
            self.validator = ResponseValidator(self.cleaner)
            logger.info("🧹 Using Enhanced Data Cleaning Engine v2.0")
        else:
            # Fallback to basic cleaning
            self.cleaner = None
            self.sql_generator = None
            self.validator = None
            logger.warning("⚠️ Using basic data cleaning (enhanced cleaner not available)")
        
        # Year converter for Buddhist Era
        self.year_converter = self._create_year_converter()
        
        # Maintain compatibility
        self.numeric_fields = {
            'overhaul_', 'replacement', 'service_contact_',
            'parts_all_', 'product_all', 'solution_',
            'unit_price', 'balance', 'unit', 'total', 'received'
        }
        self.boolean_fields = {
            'job_description_pm', 'job_description_replacement',
            'job_description_overhaul', 'job_description_start_up',
            'job_description_support_all', 'job_description_cpa',
            'success', 'unsuccessful', 
            'report_kpi_2_days', 'report_over_kpi_2_days'
        }
        self.cleaning_stats = defaultdict(int)
    
    def _create_year_converter(self) -> callable:
        """Create year converter function"""
        def convert(year_str: str) -> str:
            mappings = {
                '2565': '2022', '2566': '2023', '2567': '2024', '2568': '2025',
                '65': '2022', '66': '2023', '67': '2024', '68': '2025'
            }
            return mappings.get(year_str, year_str)
        return convert
    
    def clean_query_results(self, results: List[Dict], sql_query: str = None) -> List[Dict]:
        """Clean and validate query results with enhanced logic"""
        if not results:
            return results
        
        if self.cleaner:
            # Use enhanced cleaner with table detection
            table_hint = self._detect_table_from_query(sql_query)
            cleaned = self.cleaner.clean_query_results(
                results, 
                query=sql_query,
                table_hint=table_hint
            )
            # Update stats for compatibility
            self.cleaning_stats = self.cleaner.get_statistics()
        else:
            # Fallback to basic cleaning
            cleaned = []
            for row in results:
                cleaned_row = self._basic_clean_row(row)
                cleaned.append(cleaned_row)
        
        # Log statistics
        logger.debug(f"🧹 Cleaned {len(cleaned)} rows, stats: {dict(self.cleaning_stats)}")
        
        return cleaned
    
    def generate_safe_sql(self, intent: str, entities: Dict, table_info: Dict = None) -> Optional[str]:
        """Generate safe SQL using enhanced templates"""
        # Convert Buddhist Era years if present
        if entities.get('years'):
            converted_years = []
            for year in entities['years']:
                converted = self.year_converter(str(year))
                # Validate year is in available range
                if converted in ['2022', '2023', '2024', '2025']:
                    converted_years.append(converted)
                else:
                    # Map to closest available year
                    try:
                        year_int = int(converted)
                        if year_int < 2022:
                            converted_years.append('2022')
                        elif year_int > 2025:
                            converted_years.append('2025')
                        else:
                            converted_years.append(str(year_int))
                    except:
                        converted_years.append('2024')  # Default
            entities['years'] = converted_years
        
        if self.cleaner:
            try:
                return self.cleaner.generate_safe_sql(intent, entities, table_info)
            except Exception as e:
                logger.warning(f"Enhanced SQL generation failed: {e}")
        return None
    
    def validate_response(self, response: str, sql_results: List[Dict]) -> Dict:
        """Validate AI response against data"""
        if self.validator:
            return self.validator.validate_response(response, sql_results)
        return {'is_valid': True, 'confidence': 1.0, 'warnings': [], 'cleaned_response': response}
    
    def _detect_table_from_query(self, query: str) -> Optional[str]:
        """Extract table hint from SQL query"""
        if not query:
            return None
        
        query_lower = query.lower()
        
        # Check for table names
        tables = ['sales2022', 'sales2023', 'sales2024', 'sales2025', 
                 'spare_part', 'spare_part2', 'work_force']
        
        for table in tables:
            if table in query_lower:
                return table
        
        return None
    
    def _basic_clean_row(self, row: Dict) -> Dict:
        """Basic fallback cleaning when enhanced cleaner not available"""
        cleaned = {}
        
        for key, value in row.items():
            if key in self.numeric_fields:
                cleaned[key] = self._basic_clean_numeric(value, key)
            elif key in self.boolean_fields:
                cleaned[key] = self._basic_clean_boolean(value, key)
            else:
                cleaned[key] = self._basic_clean_text(value, key)
        
        return cleaned
    
    def _basic_clean_numeric(self, value: Any, field_name: str) -> float:
        """Basic numeric cleaning"""
        self.cleaning_stats['numeric_processed'] += 1
        
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
    
    def _basic_clean_boolean(self, value: Any, field_name: str) -> bool:
        """Basic boolean cleaning"""
        self.cleaning_stats['boolean_processed'] += 1
        
        if value is None or value == 'NULL':
            return False
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ['true', 't', 'yes', 'y', '1', 'checked', 'x']
        
        if isinstance(value, (int, float)):
            return bool(value)
        
        return False
    
    def _basic_clean_text(self, value: Any, field_name: str) -> str:
        """Basic text cleaning with Thai encoding fix"""
        if value is None or value == 'NULL':
            return ''
        
        if not isinstance(value, str):
            return str(value)
        
        # Fix Thai encoding issues
        cleaned = self._basic_fix_thai_encoding(value)
        
        # Remove excessive whitespace
        cleaned = ' '.join(cleaned.split())
        
        return cleaned
    
    def _basic_fix_thai_encoding(self, text: str) -> str:
        """Basic Thai encoding fix"""
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
        """Reset statistics"""
        if self.cleaner:
            self.cleaner.reset_statistics()
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
            },
            'company-b': {
                'host': os.getenv('POSTGRES_HOST_COMPANY_B', 'postgres-company-b'),
                'port': int(os.getenv('POSTGRES_PORT_COMPANY_B', '5433')),
                'database': os.getenv('POSTGRES_DB_COMPANY_B', 'siamtemp_company_b'),
                'user': os.getenv('POSTGRES_USER_COMPANY_B', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD_COMPANY_B', 'password123')
            },
            'company-c': {
                'host': os.getenv('POSTGRES_HOST_COMPANY_C', 'postgres-company-c'),
                'port': int(os.getenv('POSTGRES_PORT_COMPANY_C', '5434')),
                'database': os.getenv('POSTGRES_DB_COMPANY_C', 'siamtemp_company_c'),
                'user': os.getenv('POSTGRES_USER_COMPANY_C', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD_COMPANY_C', 'password123')
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
            # Run in executor to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._execute_sync, sql, tenant_id)
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def _execute_sync(self, sql: str, tenant_id: str) -> List[Dict]:
        """Synchronous query execution"""
        conn = self.get_connection(tenant_id)
        
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                
                # Check if it's a SELECT query
                if cursor.description:
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
                else:
                    conn.commit()
                    return []
                    
        except Exception as e:
            conn.rollback()
            raise e
    
    def close_connections(self):
        """Close all database connections"""
        for conn in self.connection_cache.values():
            try:
                conn.close()
            except:
                pass
        self.connection_cache.clear()
        logger.info("Database connections closed")

# =============================================================================
# OLLAMA CLIENT
# =============================================================================

class SimplifiedOllamaClient:
    """Async Ollama API client"""
    
    def __init__(self):
        # ใช้ Remote Ollama server แทน localhost
        self.base_url = os.getenv('OLLAMA_API_URL', 'http://52.74.36.160:12434')
        self.timeout = 30
        logger.info(f"🔗 Ollama Client initialized with: {self.base_url}")
        
    async def generate(self, model: str, prompt: str, temperature: float = 0.7) -> str:
        """Generate text using Ollama"""
        async with aiohttp.ClientSession() as session:
            payload = {
                'model': model,
                'prompt': prompt,
                'temperature': temperature,
                'stream': False
            }
            
            try:
                logger.debug(f"Calling Ollama at {self.base_url}/api/generate with model {model}")
                async with session.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
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
        self.SQL_MODEL = os.getenv('SQL_MODEL', 'mannix/defog-llama3-sqlcoder-8b:latest')
        self.NL_MODEL = os.getenv('NL_MODEL', 'llama3.1:8b')
        
        # Feature flags
        self.enable_conversation_memory = True
        self.enable_parallel_processing = True
        self.enable_data_cleaning = True
        self.enable_sql_validation = True
        self.enable_few_shot_learning = True  # New flag for Few-Shot Learning
        
        # Caches
        self.sql_cache = {}
        self.schema_cache = {}
        
        # Statistics
        self.stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'cache_hits': 0,
            'avg_response_time': 0,
            'few_shot_examples_used': 0,
            'dynamic_examples_added': 0
        }
        
        # Load SQL examples (enhanced for Few-Shot Learning)
        self.sql_examples = self._load_sql_examples()
        
        # Dynamic example storage for learning from successful queries
        self.dynamic_examples = []
        self.max_dynamic_examples = 100
        
        logger.info("🚀 Ultimate Dual-Model Dynamic AI System initialized")
        logger.info(f"✨ Enhanced Cleaner Available: {ENHANCED_CLEANER_AVAILABLE}")
        logger.info(f"📚 Few-Shot Learning: {len(self.sql_examples)} examples loaded")
    
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
            # Store original question for fallback SQL generation
            if entities:
                entities['original_question'] = question
            
            sql_query = await self._generate_enhanced_sql(
                thai_converted or question,
                intent,
                entities,
                context,
                complexity
            )
            
            if not sql_query:
                raise ValueError("Failed to generate valid SQL query")
            
            logger.info(f"📝 SQL Generated: {sql_query[:200]}...")
            
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
            
            # 6. Validate response if enhanced cleaner available
            if self.enable_data_cleaning and results:
                validation = self.data_cleaner.validate_response(answer, results)
                if validation.get('warnings'):
                    logger.warning(f"⚠️ Response validation warnings: {validation['warnings']}")
                    answer = validation.get('cleaned_response', answer)
            
            # 7. Prepare response
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
                'ai_system': 'dual_model_dynamic_enhanced',
                'features_used': {
                    'conversation_memory': self.enable_conversation_memory,
                    'parallel_processing': self.enable_parallel_processing,
                    'data_cleaning': self.enable_data_cleaning,
                    'sql_validation': self.enable_sql_validation,
                    'enhanced_cleaner': ENHANCED_CLEANER_AVAILABLE
                }
            }
            
            if cleaning_stats:
                response['data_quality'] = cleaning_stats
            
            # 8. Update conversation memory
            if self.enable_conversation_memory:
                self.conversation_memory.add_conversation(user_id, question, response)
            
            # 9. Add to dynamic examples if successful (for Few-Shot Learning)
            if self.enable_few_shot_learning and response['success'] and len(results) > 0:
                self._add_dynamic_example(question, sql_query, intent, entities, results)
            
            # 10. Update statistics
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
                'ai_system': 'dual_model_dynamic_enhanced'
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
        
        # Try enhanced SQL generation first if available
        if self.enable_data_cleaning and entities:
            safe_sql = self.data_cleaner.generate_safe_sql(intent, entities)
            if safe_sql:
                logger.info("✅ Using Enhanced SQL Generator")
                self.sql_cache[cache_key] = safe_sql
                return safe_sql
        
        # Check for successful patterns in memory
        if self.enable_conversation_memory:
            pattern_key = f"{intent}_{json.dumps(entities, sort_keys=True)}"
            if pattern_key in self.conversation_memory.successful_patterns:
                patterns = self.conversation_memory.successful_patterns[pattern_key]
                if patterns:
                    logger.info("📋 Using successful pattern from memory")
                    sql = patterns[-1]  # Use most recent successful pattern
                    self.sql_cache[cache_key] = sql
                    return sql
        
        # Generate SQL using Ollama
        max_attempts = 3 if complexity == 'complex' else 2
        
        for attempt in range(max_attempts):
            prompt = self._build_sql_prompt(question, intent, entities, context, complexity, attempt)
            
            try:
                sql = await self.ollama_client.generate(
                    self.SQL_MODEL,
                    prompt,
                    temperature=0.1
                )
                
                sql = self._clean_sql_response(sql)
                
                if self.enable_sql_validation:
                    is_valid, error = self._validate_sql(sql, entities)
                    if is_valid:
                        self.sql_cache[cache_key] = sql
                        return sql
                    else:
                        logger.warning(f"SQL validation failed (attempt {attempt + 1}): {error}")
                else:
                    self.sql_cache[cache_key] = sql
                    return sql
                    
            except Exception as e:
                logger.error(f"SQL generation attempt {attempt + 1} failed: {e}")
        
        # Last resort: use fallback SQL
        return self._generate_fallback_sql(intent, entities)
    
    def _build_sql_prompt(self, question: str, intent: str, entities: Dict,
                         context: Dict, complexity: str, attempt: int) -> str:
        """Build comprehensive prompt for SQL generation with enhanced Few-Shot Learning"""
        
        # Find relevant examples (get more for better few-shot)
        examples = self._find_relevant_examples(intent, question)
        
        # Dynamically replace entities in examples to match current question
        processed_examples = self._process_examples_with_entities(examples, entities)
        
        prompt = f"""You are an expert PostgreSQL developer for an HVAC business database in Thailand.

=== DATABASE SCHEMA ===
Tables available:
- sales2022, sales2023, sales2024, sales2025: Sales and service records
  Columns: id (SERIAL PRIMARY KEY),
           job_no (VARCHAR - pattern: SVyy-mm-xxx or JAEyy-mm-xxx where yy=year, mm=month),
           customer_name (VARCHAR - company names),
           description (TEXT - job description),
           overhaul_ (TEXT - CAST to NUMERIC for calculations),
           replacement (TEXT - CAST to NUMERIC),
           service_contact_ (TEXT - CAST to NUMERIC),
           parts_all_ (TEXT - CAST to NUMERIC),
           product_all (TEXT - CAST to NUMERIC),
           solution_ (TEXT - CAST to NUMERIC)
           
- spare_part, spare_part2: Spare parts inventory
  Columns: id (SERIAL PRIMARY KEY),
           wh (VARCHAR - warehouse location),
           product_code (VARCHAR),
           product_name (VARCHAR),
           unit (NUMERIC),
           balance (NUMERIC or TEXT - needs CAST),
           unit_price (NUMERIC or TEXT - needs CAST),
           total (NUMERIC),
           description (TEXT)
           
- work_force: Work team management
  Columns: id (SERIAL PRIMARY KEY),
           date (VARCHAR/DATE),
           customer (VARCHAR),
           project (VARCHAR),
           service_group (VARCHAR - team name),
           detail (TEXT),
           job_description_pm (VARCHAR - NOT BOOLEAN, use = 'true' or IS NOT NULL),
           job_description_overhaul (VARCHAR - NOT BOOLEAN),
           job_description_replacement (VARCHAR - NOT BOOLEAN),
           success (VARCHAR - NOT BOOLEAN),
           report_kpi_2_days (VARCHAR - NOT BOOLEAN)

=== CRITICAL RULES ===
1. Revenue fields (overhaul_, replacement, service_contact_, etc.) are TEXT type
   ALWAYS use: CAST(field AS NUMERIC) for calculations
   Filter out empty strings with: WHERE field IS NOT NULL AND field != ''
   
2. For NULL handling: Use COALESCE after CAST
   
3. Boolean-like fields in work_force are VARCHAR, NOT BOOLEAN!
   WRONG: WHERE job_description_pm = true
   CORRECT: WHERE job_description_pm = 'true' OR WHERE job_description_pm IS NOT NULL
   
4. Job number pattern: 'SVyy-mm-xxx' or 'JAEyy-mm-xxx'
   Example: SV24-06-001 = June 2024 job #001
   
5. For counting work_force boolean-like fields: 
   SUM(CASE WHEN field = 'true' THEN 1 ELSE 0 END) or
   COUNT(CASE WHEN field IS NOT NULL THEN 1 END)

6. Always include ORDER BY for meaningful results

7. Use appropriate LIMIT for large result sets

=== FEW-SHOT EXAMPLES ({len(processed_examples)} most relevant) ===
"""
        
        # Add processed examples with better formatting
        for i, example in enumerate(processed_examples[:5], 1):  # Use up to 5 examples
            prompt += f"""
--- Example {i} ---
User Question: {example['question']}
SQL Query:
{example['sql']}
"""
        
        # Add context from conversation history
        if context.get('has_history') and context.get('recent_queries'):
            prompt += f"""
=== CONVERSATION CONTEXT ===
Recent queries: {', '.join(context['recent_queries'][:3])}
Dominant intent: {context.get('dominant_intent', 'general')}
"""
        
        prompt += f"""
=== YOUR TASK ===
User Question: {question}
Detected Intent: {intent}
Extracted Entities: {json.dumps(entities, ensure_ascii=False)}
Query Complexity: {complexity}

Based on the examples above, generate a PostgreSQL query that:
1. Follows the same patterns as the examples
2. Uses proper CAST for all numeric fields
3. Handles NULL and empty strings correctly
4. For work_force table, treats boolean fields as VARCHAR
5. Returns meaningful, well-formatted results
6. Includes appropriate GROUP BY, ORDER BY, and LIMIT clauses

Generate ONLY the SQL query without any explanation or markdown:
"""
        
        return prompt
    
    def _process_examples_with_entities(self, examples: List[Dict], entities: Dict) -> List[Dict]:
        """Process examples by replacing placeholders with actual entities"""
        processed = []
        
        for example in examples:
            processed_example = example.copy()
            sql = example['sql']
            
            # Replace year placeholders if we have year entities
            if entities.get('years') and len(entities['years']) > 0:
                year = entities['years'][0]
                # Replace table names
                sql = sql.replace('sales2024', f'sales{year}')
                sql = sql.replace('sales2023', f'sales{entities["years"][-1] if len(entities["years"]) > 1 else year}')
                # Replace year patterns in LIKE clauses
                year_short = year[-2:] if len(year) == 4 else year
                sql = sql.replace('%24-', f'%{year_short}-')
                sql = sql.replace('SV24-', f'SV{year_short}-')
                sql = sql.replace('JAE24-', f'JAE{year_short}-')
            
            # Replace month placeholders if we have month entities
            if entities.get('months') and len(entities['months']) > 0:
                month = entities['months'][0]
                sql = sql.replace('-06-', f'-{month:02d}-')
            
            # Replace customer placeholders if we have company entities
            if entities.get('companies') and len(entities['companies']) > 0:
                company = entities['companies'][0]
                sql = sql.replace('%AGC%', f'%{company}%')
                sql = sql.replace("'AGC'", f"'{company}'")
            
            processed_example['sql'] = sql
            processed.append(processed_example)
        
        return processed
    
    def _find_relevant_examples(self, intent: str, question: str) -> List[Dict]:
        """Find relevant SQL examples based on intent and question keywords"""
        relevant = []
        question_lower = question.lower()
        
        # Score each example based on relevance
        scored_examples = []
        
        for example in self.sql_examples:
            score = 0
            example_text = example['question'].lower()
            
            # Category match (highest priority)
            if example.get('category') == intent:
                score += 10
            
            # Intent match
            if example.get('intent') == intent:
                score += 8
            
            # Keyword matching
            keywords_in_question = {
                'ยอด': ['ยอด', 'รายได้', 'รวม'],
                'เดือน': ['เดือน', 'month'],
                'ปี': ['ปี', 'year'],
                'ลูกค้า': ['ลูกค้า', 'customer', 'บริษัท'],
                'อะไหล่': ['อะไหล่', 'spare', 'part', 'คลัง'],
                'ช่าง': ['ช่าง', 'ทีม', 'work', 'team'],
                'เปรียบเทียบ': ['เปรียบเทียบ', 'compare', 'กับ'],
                'สูงสุด': ['สูงสุด', 'top', 'highest', 'มากที่สุด'],
                'pm': ['pm', 'maintenance', 'บำรุง'],
                'service': ['service', 'บริการ'],
                'overhaul': ['overhaul', 'ซ่อมใหญ่'],
                'replacement': ['replacement', 'เปลี่ยน']
            }
            
            for key_group, keywords in keywords_in_question.items():
                if any(kw in question_lower for kw in keywords):
                    if any(kw in example_text for kw in keywords):
                        score += 5
            
            # Direct word overlap
            question_words = set(question_lower.split())
            example_words = set(example_text.split())
            overlap = len(question_words & example_words)
            score += overlap * 2
            
            # Add to scored list
            if score > 0:
                scored_examples.append((score, example))
        
        # Sort by score and get top examples
        scored_examples.sort(key=lambda x: x[0], reverse=True)
        
        # Get top 5 examples (for better few-shot learning)
        for score, example in scored_examples[:5]:
            relevant.append(example)
        
        # If no relevant examples found, use category-based fallback
        if not relevant:
            # Try to find examples by category
            category_map = {
                'revenue_analysis': 'revenue',
                'customer_analysis': 'customer',
                'spare_parts': 'spare_parts',
                'work_force': 'work_force'
            }
            
            target_category = category_map.get(intent, 'revenue')
            relevant = [ex for ex in self.sql_examples if ex.get('category') == target_category][:3]
        
        # Final fallback: use first 3 examples
        if not relevant:
            relevant = self.sql_examples[:3]
        
        return relevant
    
    def _clean_sql_response(self, sql: str) -> str:
        """Clean SQL response from Ollama"""
        # Remove markdown code blocks
        sql = re.sub(r'```sql?\s*', '', sql)
        sql = re.sub(r'```', '', sql)
        
        # Remove explanations
        lines = sql.split('\n')
        clean_lines = []
        
        for line in lines:
            # Skip comment lines
            if line.strip().startswith('--') or line.strip().startswith('#'):
                continue
            # Skip explanation lines
            if any(word in line.lower() for word in ['explanation:', 'note:', 'this query']):
                break
            clean_lines.append(line)
        
        sql = '\n'.join(clean_lines).strip()
        
        # Ensure it ends with semicolon
        if sql and not sql.endswith(';'):
            sql += ';'
        
        return sql
    
    def _validate_sql(self, sql: str, entities: Dict) -> Tuple[bool, Optional[str]]:
        """Validate SQL query"""
        if not sql or len(sql) < 10:
            return False, "SQL too short"
        
        sql_lower = sql.lower()
        
        # Check for required keywords
        if 'select' not in sql_lower:
            return False, "Missing SELECT statement"
        
        if 'from' not in sql_lower:
            return False, "Missing FROM clause"
        
        # Check for dangerous operations
        dangerous = ['drop', 'delete', 'truncate', 'update', 'insert', 'alter']
        for word in dangerous:
            if word in sql_lower:
                return False, f"Dangerous operation: {word}"
        
        # Check table names
        valid_tables = ['sales2022', 'sales2023', 'sales2024', 'sales2025',
                       'spare_part', 'spare_part2', 'work_force']
        
        has_valid_table = any(table in sql_lower for table in valid_tables)
        if not has_valid_table:
            return False, "No valid table referenced"
        
        return True, None
    
    def _generate_fallback_sql(self, intent: str, entities: Dict) -> str:
        """Generate fallback SQL when all else fails"""
        # Determine table with year validation
        years = []
        if entities.get('years'):
            for raw_year in entities['years']:
                year = str(raw_year)
                # Validate year is in available range
                if year in ['2022', '2023', '2024', '2025']:
                    years.append(year)
                else:
                    # Try to map from BE or invalid year
                    years.append(self._map_to_available_year(year))
        
        if not years:
            years = ['2024']  # Default
        
        # Special handling for job_quotation with month
        if intent == 'job_quotation' and entities.get('months'):
            year = years[0]
            month = entities['months'][0]
            year_short = year[-2:]
            
            sql = (
                f"SELECT job_no, customer_name, description, "
                f"CAST(service_contact_ AS NUMERIC) as service_amount, "
                f"CAST(overhaul_ AS NUMERIC) as overhaul_amount, "
                f"CAST(replacement AS NUMERIC) as replacement_amount "
                f"FROM sales{year} "
                f"WHERE job_no LIKE '%{year_short}-{month:02d}-%' "
                f"AND (service_contact_ IS NOT NULL AND service_contact_ != '' "
                f"OR overhaul_ IS NOT NULL AND overhaul_ != '' "
                f"OR replacement IS NOT NULL AND replacement != '') "
                f"ORDER BY job_no;"
            )
            return sql
        
        # Generate based on intent and years
        if len(years) > 1:
            # Multiple years - create UNION query
            if intent == 'revenue_analysis' or 'วิเคราะห์' in entities.get('original_question', ''):
                union_parts = []
                for year in years:
                    # Use direct CAST with WHERE filter
                    union_part = (
                        f"SELECT '{year}' as year, "
                        f"COUNT(*) as total_jobs, "
                        f"SUM(CAST(service_contact_ AS NUMERIC)) as service_total, "
                        f"SUM(CAST(overhaul_ AS NUMERIC)) as overhaul_total, "
                        f"SUM(CAST(replacement AS NUMERIC)) as replacement_total, "
                        f"SUM(COALESCE(CAST(overhaul_ AS NUMERIC), 0) + "
                        f"COALESCE(CAST(service_contact_ AS NUMERIC), 0) + "
                        f"COALESCE(CAST(replacement AS NUMERIC), 0)) as grand_total "
                        f"FROM sales{year} "
                        f"WHERE (overhaul_ IS NOT NULL AND overhaul_ != '') OR "
                        f"(service_contact_ IS NOT NULL AND service_contact_ != '') OR "
                        f"(replacement IS NOT NULL AND replacement != '')"
                    )
                    union_parts.append(union_part)
                
                sql = " UNION ALL ".join(union_parts) + " ORDER BY year;"
                logger.info(f"Generated multi-year SQL using direct CAST")
                return sql
            else:
                # Simple multi-year query
                table = f"sales{years[0]}"
                return f"SELECT * FROM {table} LIMIT 20;"
        
        # Single year query
        year = years[0]
        table = f"sales{year}"
        
        # Generate based on intent
        if intent == 'revenue_analysis' or 'รายได้' in entities.get('original_question', ''):
            sql = (
                f"SELECT COUNT(*) as total_jobs, "
                f"SUM(CAST(service_contact_ AS NUMERIC)) as service_total, "
                f"SUM(CAST(overhaul_ AS NUMERIC)) as overhaul_total, "
                f"SUM(CAST(replacement AS NUMERIC)) as replacement_total, "
                f"SUM(COALESCE(CAST(service_contact_ AS NUMERIC), 0) + "
                f"COALESCE(CAST(overhaul_ AS NUMERIC), 0) + "
                f"COALESCE(CAST(replacement AS NUMERIC), 0)) as grand_total "
                f"FROM {table} "
                f"WHERE (service_contact_ IS NOT NULL AND service_contact_ != '') OR "
                f"(overhaul_ IS NOT NULL AND overhaul_ != '') OR "
                f"(replacement IS NOT NULL AND replacement != '');"
            )
            return sql
        elif intent == 'job_quotation':
            # Standard job quotation query
            sql = (
                f"SELECT job_no, customer_name, description, "
                f"CAST(service_contact_ AS NUMERIC) as service_amount, "
                f"CAST(overhaul_ AS NUMERIC) as overhaul_amount, "
                f"CAST(replacement AS NUMERIC) as replacement_amount "
                f"FROM {table} "
                f"WHERE (service_contact_ IS NOT NULL AND service_contact_ != '' "
                f"OR overhaul_ IS NOT NULL AND overhaul_ != '' "
                f"OR replacement IS NOT NULL AND replacement != '') "
                f"ORDER BY job_no DESC LIMIT 20;"
            )
            return sql
        elif intent == 'spare_parts':
            sql = (
                "SELECT product_code, product_name, "
                "CAST(balance AS NUMERIC) as balance, "
                "CAST(unit_price AS NUMERIC) as price "
                "FROM spare_part "
                "WHERE balance IS NOT NULL AND CAST(balance AS TEXT) != '' "
                "ORDER BY CAST(balance AS NUMERIC) DESC LIMIT 20;"
            )
            return sql
        elif intent == 'work_force':
            sql = (
                "SELECT service_group, COUNT(*) as total_jobs, "
                "SUM(CASE WHEN job_description_pm = 'true' THEN 1 ELSE 0 END) as pm_jobs, "
                "SUM(CASE WHEN success = 'true' THEN 1 ELSE 0 END) as successful "
                "FROM work_force "
                "WHERE service_group IS NOT NULL "
                "GROUP BY service_group ORDER BY total_jobs DESC;"
            )
            return sql
        else:
            # Default safe query
            sql = f"SELECT * FROM {table} LIMIT 10;"
            return sql
        
        # Generate based on intent
        if intent == 'revenue_analysis':
            return f"""
                SELECT 
                    COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as service_total,
                    COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as overhaul_total,
                    COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as replacement_total,
                    COALESCE(SUM(
                        CAST(NULLIF(service_contact_, '') AS NUMERIC) +
                        CAST(NULLIF(overhaul_, '') AS NUMERIC) +
                        CAST(NULLIF(replacement, '') AS NUMERIC)
                    ), 0) as grand_total
                FROM {table}
                WHERE 1=1;
            """
        elif intent == 'spare_parts':
            return """
                SELECT 
                    product_code,
                    product_name,
                    CAST(NULLIF(balance::text, '') AS NUMERIC) as balance,
                    CAST(NULLIF(unit_price::text, '') AS NUMERIC) as price
                FROM spare_part
                WHERE CAST(NULLIF(balance::text, '') AS NUMERIC) > 0
                ORDER BY balance DESC
                LIMIT 20;
            """
        elif intent == 'work_force':
            return """
                SELECT 
                    service_group,
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN job_description_pm = true THEN 1 ELSE 0 END) as pm_jobs,
                    SUM(CASE WHEN success = true THEN 1 ELSE 0 END) as successful
                FROM work_force
                WHERE service_group IS NOT NULL
                GROUP BY service_group
                ORDER BY total_jobs DESC;
            """
        else:
            return f"SELECT * FROM {table} LIMIT 10;"
    
    def _map_to_available_year(self, year_str: str) -> str:
        """Map any year to available years in database (2022-2025)"""
        try:
            year_int = int(year_str)
            
            # If year is too old, use oldest available
            if year_int < 2022:
                return '2022'
            # If year is too new, use newest available
            elif year_int > 2025:
                return '2025'
            # Year is in range
            else:
                return str(year_int)
        except ValueError:
            # Can't parse year, use default
            return '2024'
    
    def _generate_fallback_response(self, results: List[Dict], intent: str, question: str) -> str:
        """Generate response without Ollama"""
        try:
            # Check if results contain year comparison
            if 'year' in results[0]:
                # Multi-year analysis
                response = "📊 ผลการวิเคราะห์ข้อมูลการขาย:\n\n"
                total_grand = 0
                
                for row in results:
                    year = row.get('year', 'N/A')
                    total_jobs = row.get('total_jobs', 0)
                    service = row.get('service_total', 0)
                    overhaul = row.get('overhaul_total', 0)
                    replacement = row.get('replacement_total', 0)
                    grand = row.get('grand_total', 0)
                    total_grand += grand
                    
                    response += f"**ปี {year}:**\n"
                    response += f"- จำนวนงาน: {total_jobs:,} งาน\n"
                    response += f"- รายได้จากบริการ: ฿{service:,.2f}\n"
                    response += f"- รายได้จาก Overhaul: ฿{overhaul:,.2f}\n"
                    response += f"- รายได้จากการเปลี่ยนอะไหล่: ฿{replacement:,.2f}\n"
                    response += f"- รายได้รวม: ฿{grand:,.2f}\n\n"
                
                response += f"**รายได้รวมทั้งหมด:** ฿{total_grand:,.2f}"
                return response
            
            # Single result analysis
            elif len(results) == 1:
                row = results[0]
                response = "📋 ข้อมูลที่พบ:\n\n"
                
                # Format each field nicely
                for key, value in row.items():
                    if value is not None and value != '':
                        # Skip id field
                        if key == 'id':
                            continue
                        
                        # Format field name
                        field_name = key.replace('_', ' ').title()
                        
                        # Format value based on type
                        if isinstance(value, (int, float)):
                            if any(word in key.lower() for word in ['price', 'total', 'cost', 'overhaul', 'replacement', 'service']):
                                response += f"- {field_name}: ฿{value:,.2f}\n"
                            else:
                                response += f"- {field_name}: {value:,}\n"
                        else:
                            response += f"- {field_name}: {value}\n"
                
                return response
            
            # Multiple results - show summary
            else:
                response = f"📊 พบข้อมูล {len(results)} รายการ:\n\n"
                
                # Show first 5 items
                for i, row in enumerate(results[:5], 1):
                    # Try to find a good identifier
                    identifier = (row.get('customer_name') or 
                                row.get('product_name') or 
                                row.get('job_no') or 
                                row.get('service_group') or 
                                f"รายการที่ {i}")
                    
                    response += f"{i}. {identifier}\n"
                    
                    # Show key numeric values
                    for key in ['service_contact_', 'overhaul_', 'replacement', 'grand_total', 'balance', 'unit_price']:
                        if key in row and row[key]:
                            value = row[key]
                            if isinstance(value, (int, float)) and value > 0:
                                response += f"   - {key.replace('_', ' ').title()}: ฿{value:,.2f}\n"
                
                if len(results) > 5:
                    response += f"\n... และอีก {len(results) - 5} รายการ"
                
                return response
                
        except Exception as e:
            logger.error(f"Fallback response generation failed: {e}")
            return f"พบข้อมูล {len(results)} รายการ (ไม่สามารถแสดงรายละเอียดได้)"
    
    async def _generate_natural_response(self, question: str, results: List[Dict],
                                        sql_query: str, intent: str, 
                                        cleaning_stats: Dict) -> str:
        """Generate natural language response from results"""
        if not results:
            return "ขออภัย ไม่พบข้อมูลที่ตรงกับคำถามของคุณ กรุณาลองใช้คำค้นหาอื่น"
        
        # Try Ollama first
        try:
            # Build prompt for natural language generation
            prompt = f"""You are a helpful HVAC business assistant responding in Thai.

Question: {question}
Intent: {intent}
Number of results: {len(results)}

Data (first 5 rows):
{json.dumps(results[:5], ensure_ascii=False, indent=2)}

Generate a natural, conversational response in Thai that:
1. Directly answers the user's question
2. Includes specific numbers from the data
3. Is concise but informative
4. Uses Thai currency format (฿) for money values

Response (in Thai):
"""
            
            response = await self.ollama_client.generate(
                self.NL_MODEL,
                prompt,
                temperature=0.7
            )
            
            if response and response.strip():
                return response
        except Exception as e:
            logger.warning(f"Ollama response generation failed: {e}")
        
        # Fallback: Generate response without Ollama
        return self._generate_fallback_response(results, intent, question)
    
    def _load_sql_examples(self) -> List[Dict]:
        """Load comprehensive SQL examples for enhanced Few-Shot Learning"""
        return [
            # ========== REVENUE ANALYSIS EXAMPLES ==========
            {
                'category': 'revenue',
                'question': 'ยอดรวมของเดือนมิถุนายน 2024',
                'intent': 'revenue_analysis',
                'sql': """SELECT 
                    'มิถุนายน 2024' as period,
                    COUNT(*) as total_jobs,
                    COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as service_revenue,
                    COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as overhaul_revenue,
                    COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as replacement_revenue,
                    COALESCE(SUM(
                        CAST(NULLIF(service_contact_, '') AS NUMERIC) +
                        CAST(NULLIF(overhaul_, '') AS NUMERIC) +
                        CAST(NULLIF(replacement, '') AS NUMERIC)
                    ), 0) as total_revenue
                    FROM sales2024 
                    WHERE job_no LIKE '%24-06-%'"""
            },
            {
                'category': 'revenue',
                'question': 'รายได้ทั้งหมดในปี 2567',
                'intent': 'revenue_analysis',
                'sql': """SELECT 
                    '2024' as year,
                    COUNT(*) as total_jobs,
                    COUNT(DISTINCT customer_name) as total_customers,
                    COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as service_total,
                    COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as overhaul_total,
                    COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as replacement_total,
                    COALESCE(SUM(
                        CAST(NULLIF(service_contact_, '') AS NUMERIC) +
                        CAST(NULLIF(overhaul_, '') AS NUMERIC) +
                        CAST(NULLIF(replacement, '') AS NUMERIC) +
                        CAST(NULLIF(parts_all_, '') AS NUMERIC) +
                        CAST(NULLIF(product_all, '') AS NUMERIC)
                    ), 0) as grand_total
                    FROM sales2024"""
            },
            
            # ========== CUSTOMER ANALYSIS EXAMPLES ==========
            {
                'category': 'customer',
                'question': 'ลูกค้าที่มียอดสูงสุด 10 อันดับแรก',
                'intent': 'customer_analysis',
                'sql': """SELECT 
                    customer_name,
                    COUNT(*) as job_count,
                    COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as service_total,
                    COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as overhaul_total,
                    COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as replacement_total,
                    COALESCE(SUM(
                        CAST(NULLIF(service_contact_, '') AS NUMERIC) +
                        CAST(NULLIF(overhaul_, '') AS NUMERIC) +
                        CAST(NULLIF(replacement, '') AS NUMERIC)
                    ), 0) as total_revenue
                    FROM sales2024
                    WHERE customer_name IS NOT NULL AND customer_name != ''
                    GROUP BY customer_name
                    ORDER BY total_revenue DESC
                    LIMIT 10"""
            },
            {
                'category': 'customer',
                'question': 'งานของบริษัท AGC ในปี 2024',
                'intent': 'customer_analysis',
                'sql': """SELECT 
                    job_no,
                    customer_name,
                    description,
                    COALESCE(CAST(NULLIF(service_contact_, '') AS NUMERIC), 0) as service_amount,
                    COALESCE(CAST(NULLIF(overhaul_, '') AS NUMERIC), 0) as overhaul_amount,
                    COALESCE(CAST(NULLIF(replacement, '') AS NUMERIC), 0) as replacement_amount
                    FROM sales2024
                    WHERE UPPER(customer_name) LIKE '%AGC%'
                    ORDER BY job_no"""
            },
            
            # ========== SPARE PARTS EXAMPLES ==========
            {
                'category': 'spare_parts',
                'question': 'รายการอะไหล่ที่มีในคลัง',
                'intent': 'spare_parts',
                'sql': """SELECT 
                    product_code,
                    product_name,
                    CAST(NULLIF(balance::text, '') AS NUMERIC) as current_stock,
                    CAST(NULLIF(unit_price::text, '') AS NUMERIC) as price_per_unit,
                    CAST(NULLIF(balance::text, '') AS NUMERIC) * 
                    CAST(NULLIF(unit_price::text, '') AS NUMERIC) as total_value
                    FROM spare_part 
                    WHERE CAST(NULLIF(balance::text, '') AS NUMERIC) > 0
                    ORDER BY total_value DESC
                    LIMIT 20"""
            },
            {
                'category': 'spare_parts',
                'question': 'มูลค่ารวมของอะไหล่ในคลังกลาง',
                'intent': 'spare_parts',
                'sql': """SELECT 
                    wh as warehouse,
                    COUNT(*) as total_items,
                    COALESCE(SUM(CAST(NULLIF(balance::text, '') AS NUMERIC)), 0) as total_units,
                    COALESCE(SUM(
                        CAST(NULLIF(balance::text, '') AS NUMERIC) * 
                        CAST(NULLIF(unit_price::text, '') AS NUMERIC)
                    ), 0) as total_value
                    FROM spare_part
                    WHERE wh LIKE '%คลัง%'
                    GROUP BY wh"""
            },
            
            # ========== WORK FORCE EXAMPLES ==========
            {
                'category': 'work_force',
                'question': 'สรุปงานของทีมช่างแต่ละกลุ่ม',
                'intent': 'work_force',
                'sql': """SELECT 
                    service_group,
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN job_description_pm = true THEN 1 ELSE 0 END) as pm_jobs,
                    SUM(CASE WHEN job_description_replacement = true THEN 1 ELSE 0 END) as replacement_jobs,
                    SUM(CASE WHEN job_description_overhaul = true THEN 1 ELSE 0 END) as overhaul_jobs,
                    SUM(CASE WHEN success = true THEN 1 ELSE 0 END) as successful_jobs,
                    ROUND(100.0 * SUM(CASE WHEN success = true THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
                    FROM work_force
                    WHERE service_group IS NOT NULL
                    GROUP BY service_group
                    ORDER BY total_jobs DESC"""
            },
            {
                'category': 'work_force',
                'question': 'งาน PM ที่ทำสำเร็จในเดือนนี้',
                'intent': 'work_force',
                'sql': """SELECT 
                    date,
                    customer,
                    project,
                    service_group,
                    detail
                    FROM work_force
                    WHERE job_description_pm = true 
                    AND success = true
                    AND date >= DATE_TRUNC('month', CURRENT_DATE)
                    ORDER BY date DESC"""
            },
            
            # ========== COMPARISON EXAMPLES ==========
            {
                'category': 'comparison',
                'question': 'เปรียบเทียบยอดขายปี 2023 กับ 2024',
                'intent': 'revenue_analysis',
                'sql': """SELECT 
                    year,
                    SUM(job_count) as total_jobs,
                    SUM(service_total) as service_revenue,
                    SUM(overhaul_total) as overhaul_revenue,
                    SUM(replacement_total) as replacement_revenue,
                    SUM(grand_total) as total_revenue
                    FROM (
                        SELECT 
                            '2023' as year,
                            COUNT(*) as job_count,
                            COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as service_total,
                            COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as overhaul_total,
                            COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as replacement_total,
                            COALESCE(SUM(
                                CAST(NULLIF(service_contact_, '') AS NUMERIC) +
                                CAST(NULLIF(overhaul_, '') AS NUMERIC) +
                                CAST(NULLIF(replacement, '') AS NUMERIC)
                            ), 0) as grand_total
                        FROM sales2023
                        
                        UNION ALL
                        
                        SELECT 
                            '2024' as year,
                            COUNT(*) as job_count,
                            COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as service_total,
                            COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as overhaul_total,
                            COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as replacement_total,
                            COALESCE(SUM(
                                CAST(NULLIF(service_contact_, '') AS NUMERIC) +
                                CAST(NULLIF(overhaul_, '') AS NUMERIC) +
                                CAST(NULLIF(replacement, '') AS NUMERIC)
                            ), 0) as grand_total
                        FROM sales2024
                    ) yearly_data
                    GROUP BY year
                    ORDER BY year"""
            },
            
            # ========== MONTHLY ANALYSIS EXAMPLES ==========
            {
                'category': 'time_based',
                'question': 'ยอดขายรายเดือนของปี 2024',
                'intent': 'revenue_analysis',
                'sql': """SELECT 
                    SUBSTRING(job_no, 4, 2) as month,
                    COUNT(*) as job_count,
                    COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as service_total,
                    COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as overhaul_total,
                    COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as replacement_total,
                    COALESCE(SUM(
                        CAST(NULLIF(service_contact_, '') AS NUMERIC) +
                        CAST(NULLIF(overhaul_, '') AS NUMERIC) +
                        CAST(NULLIF(replacement, '') AS NUMERIC)
                    ), 0) as total_revenue
                    FROM sales2024
                    WHERE job_no LIKE 'SV24-%' OR job_no LIKE 'JAE24-%'
                    GROUP BY month
                    ORDER BY month"""
            },
            
            # ========== SERVICE TYPE ANALYSIS ==========
            {
                'category': 'service_analysis',
                'question': 'สัดส่วนประเภทงานบริการ',
                'intent': 'service_analysis',
                'sql': """SELECT 
                    'Service Contract' as service_type,
                    COUNT(*) as count,
                    COALESCE(SUM(CAST(NULLIF(service_contact_, '') AS NUMERIC)), 0) as total_amount
                    FROM sales2024
                    WHERE CAST(NULLIF(service_contact_, '') AS NUMERIC) > 0
                    
                    UNION ALL
                    
                    SELECT 
                    'Overhaul' as service_type,
                    COUNT(*) as count,
                    COALESCE(SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)), 0) as total_amount
                    FROM sales2024
                    WHERE CAST(NULLIF(overhaul_, '') AS NUMERIC) > 0
                    
                    UNION ALL
                    
                    SELECT 
                    'Replacement' as service_type,
                    COUNT(*) as count,
                    COALESCE(SUM(CAST(NULLIF(replacement, '') AS NUMERIC)), 0) as total_amount
                    FROM sales2024
                    WHERE CAST(NULLIF(replacement, '') AS NUMERIC) > 0
                    
                    ORDER BY total_amount DESC"""
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
                'avg_response_time': self.stats['avg_response_time'],
                'few_shot_examples': len(self.sql_examples),
                'dynamic_examples': len(self.dynamic_examples)
            },
            'features': {
                'conversation_memory': self.enable_conversation_memory,
                'parallel_processing': self.enable_parallel_processing,
                'data_cleaning': self.enable_data_cleaning,
                'sql_validation': self.enable_sql_validation,
                'enhanced_cleaner': ENHANCED_CLEANER_AVAILABLE,
                'few_shot_learning': self.enable_few_shot_learning
            },
            'models': {
                'sql_generation': self.SQL_MODEL,
                'response_generation': self.NL_MODEL
            }
        }
    
    def _add_dynamic_example(self, question: str, sql: str, intent: str, 
                            entities: Dict, results: List[Dict]):
        """Add successful query to dynamic examples for Few-Shot Learning"""
        try:
            # Create new example
            new_example = {
                'category': 'dynamic',
                'question': question,
                'intent': intent,
                'sql': sql,
                'entities': entities,
                'result_count': len(results),
                'timestamp': datetime.now().isoformat(),
                'success_score': 1.0  # Can be adjusted based on user feedback
            }
            
            # Add to dynamic examples
            self.dynamic_examples.append(new_example)
            self.stats['dynamic_examples_added'] += 1
            
            # Maintain size limit (FIFO)
            if len(self.dynamic_examples) > self.max_dynamic_examples:
                self.dynamic_examples.pop(0)
            
            # Optionally add to main examples if very successful
            if len(results) > 5:  # Good indicator of useful query
                # Check if similar example doesn't already exist
                similar_exists = any(
                    ex.get('question', '').lower() == question.lower() 
                    for ex in self.sql_examples
                )
                
                if not similar_exists:
                    self.sql_examples.append(new_example)
                    logger.info(f"📚 Added new example to Few-Shot Learning: {question[:50]}...")
            
        except Exception as e:
            logger.warning(f"Failed to add dynamic example: {e}")

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

# =============================================================================
# TESTING AND EXAMPLES
# =============================================================================

if __name__ == "__main__":
    import asyncio
    
    async def test_system():
        """Test the enhanced system"""
        system = DualModelDynamicAISystem()
        
        test_questions = [
            "ยอดขายเดือนมิถุนายน 2024 เป็นเท่าไหร่",
            "มีอะไหล่อะไรบ้างในคลังกลาง",
            "ทีมช่างทำงาน PM กี่งานในเดือนนี้",
            "บริษัท AGC มียอดรวมเท่าไหร่ในปี 2024"
        ]
        
        for question in test_questions:
            print(f"\n{'='*60}")
            print(f"❓ Question: {question}")
            print(f"{'='*60}")
            
            result = await system.process_any_question(question)
            
            print(f"✅ Success: {result.get('success')}")
            print(f"📝 Answer: {result.get('answer')}")
            print(f"📊 Results: {result.get('results_count')} rows")
            print(f"⏱️ Time: {result.get('processing_time', 0):.2f}s")
            
            if result.get('data_quality'):
                print(f"🧹 Cleaning Stats: {result['data_quality']}")
            
            if result.get('sql_query'):
                print(f"📋 SQL: {result['sql_query'][:200]}...")
        
        # Show system stats
        stats = system.get_system_stats()
        print(f"\n{'='*60}")
        print("📊 System Statistics:")
        print(json.dumps(stats, indent=2))
    
    # Run test
    asyncio.run(test_system())