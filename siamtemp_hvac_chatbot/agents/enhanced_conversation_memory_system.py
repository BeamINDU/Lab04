import os
import re
import json
import asyncio
import time
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date
from decimal import Decimal
from collections import deque, defaultdict
import hashlib

logger = logging.getLogger(__name__)

class ConversationMemory:
    """Advanced conversation memory system for contextual understanding"""
    
    def __init__(self, max_history: int = 20):
        self.conversations = defaultdict(lambda: deque(maxlen=max_history))
        self.user_preferences = defaultdict(dict)
        self.session_context = defaultdict(dict)
        self.intent_patterns = defaultdict(list)
        
    def add_conversation(self, user_id: str, query: str, response: Dict[str, Any]):
        """Add conversation to memory with rich context"""
        conversation_entry = {
            'timestamp': datetime.now().isoformat(),
            'query': query,
            'intent': response.get('question_analysis', {}).get('intent', 'unknown'),
            'success': response.get('success', False),
            'sql_query': response.get('sql_query'),
            'results_count': response.get('results_count', 0),
            'processing_time': response.get('processing_time', 0),
            'keywords_extracted': self._extract_keywords(query),
            'entities_extracted': self._extract_entities(query)
        }
        
        self.conversations[user_id].append(conversation_entry)
        self._update_user_preferences(user_id, conversation_entry)
        self._update_session_context(user_id, conversation_entry)
        
        logger.info(f"📝 Added conversation to memory for user {user_id}")
    
    def get_context(self, user_id: str, current_query: str) -> Dict[str, Any]:
        """Get relevant context for current query"""
        recent_conversations = list(self.conversations[user_id])[-5:]  # Last 5 conversations
        session_ctx = self.session_context.get(user_id, {})
        user_prefs = self.user_preferences.get(user_id, {})
        
        # Analyze current query against recent context
        context = {
            'recent_intents': [conv['intent'] for conv in recent_conversations],
            'recent_keywords': self._get_recent_keywords(recent_conversations),
            'recent_entities': self._get_recent_entities(recent_conversations),
            'session_focus': session_ctx.get('current_focus'),
            'user_preferences': user_prefs,
            'continuation_signals': self._detect_continuation_signals(current_query, recent_conversations),
            'implicit_context': self._extract_implicit_context(current_query, recent_conversations)
        }
        
        return context
    
    def _extract_keywords(self, query: str) -> List[str]:
        """Extract important keywords from query"""
        hvac_keywords = [
            'overhaul', 'ยกเครื่อง', 'compressor', 'chiller', 'hitachi', 'daikin',
            'ยอดขาย', 'รายได้', 'ลูกค้า', 'อะไหล่', 'spare', 'parts',
            '2565', '2566', '2567', '2568', '2022', '2023', '2024', '2025',
            'มกราคม', 'กุมภาพันธ์', 'มีนาคม', 'เมษายน', 'พฤษภาคม', 'มิถุนายน'
        ]
        
        found_keywords = []
        query_lower = query.lower()
        for keyword in hvac_keywords:
            if keyword in query_lower:
                found_keywords.append(keyword)
        
        return found_keywords
    
    def _extract_entities(self, query: str) -> Dict[str, List[str]]:
        """Extract entities like years, companies, amounts"""
        entities = {
            'years': re.findall(r'(\d{4}|256[5-8])', query),
            'companies': re.findall(r'(บริษัท[^,.\n]+|[A-Z][A-Z\s]+CO[.,\s]|CLARION|HITACHI|DAIKIN)', query),
            'amounts': re.findall(r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:บาท|ล้าน|แสน)', query),
            'months': re.findall(r'(มกราคม|กุมภาพันธ์|มีนาคม|เมษายน|พฤษภาคม|มิถุนายน|กรกฎาคม|สิงหาคม|กันยายน|ตุลาคม|พฤศจิกายน|ธันวาคม)', query)
        }
        
        # Clean empty lists
        return {k: v for k, v in entities.items() if v}
    
    def _detect_continuation_signals(self, current_query: str, recent_conversations: List[Dict]) -> List[str]:
        """Detect if current query is continuation of previous conversation"""
        continuation_signals = []
        
        # Check for pronouns and relative references
        relative_words = ['แล้ว', 'ล่ะ', 'เปรียบเทียบ', 'ดูเพิ่ม', 'รายละเอียด', 'ต่อ']
        for word in relative_words:
            if word in current_query:
                continuation_signals.append(f"relative_reference_{word}")
        
        # Check for incomplete context (short queries that need previous context)
        if len(current_query.split()) < 3 and recent_conversations:
            continuation_signals.append("incomplete_context")
        
        # Check for comparison words without explicit comparison target
        comparison_words = ['เปรียบเทียบ', 'มากกว่า', 'น้อยกว่า', 'ต่าง', 'เพิ่ม', 'ลด']
        if any(word in current_query for word in comparison_words):
            continuation_signals.append("implicit_comparison")
        
        return continuation_signals
    
    def _extract_implicit_context(self, current_query: str, recent_conversations: List[Dict]) -> Dict[str, Any]:
        """Extract implicit context that should be carried forward"""
        if not recent_conversations:
            return {}
        
        last_conv = recent_conversations[-1]
        implicit_context = {}
        
        # If current query has continuation signals, inherit context
        if any(signal in current_query for signal in ['แล้ว', 'ล่ะ', 'เปรียบเทียบ']):
            implicit_context['inherit_intent'] = last_conv['intent']
            implicit_context['inherit_keywords'] = last_conv['keywords_extracted']
            implicit_context['inherit_entities'] = last_conv['entities_extracted']
            
            # Special handling for year/time comparisons
            if 'เปรียบเทียบ' in current_query or 'ปีก่อน' in current_query:
                years = last_conv.get('entities_extracted', {}).get('years', [])
                if years:
                    last_year = max(years)  # Get most recent year mentioned
                    try:
                        prev_year = str(int(last_year) - 1)
                        implicit_context['comparison_years'] = [last_year, prev_year]
                        implicit_context['comparison_type'] = 'year_over_year'
                    except (ValueError, TypeError):
                        pass
        
        return implicit_context
    
    def _update_user_preferences(self, user_id: str, conversation: Dict[str, Any]):
        """Learn and update user preferences"""
        prefs = self.user_preferences[user_id]
        
        # Track preferred response detail level
        if conversation['results_count'] > 10 and conversation['success']:
            prefs['prefers_detailed'] = prefs.get('prefers_detailed', 0) + 1
        elif conversation['results_count'] <= 5 and conversation['success']:
            prefs['prefers_summary'] = prefs.get('prefers_summary', 0) + 1
        
        # Track frequent intents
        intent = conversation['intent']
        prefs['frequent_intents'] = prefs.get('frequent_intents', defaultdict(int))
        prefs['frequent_intents'][intent] += 1
        
        # Track response time sensitivity
        if conversation['processing_time'] > 10:
            prefs['time_sensitive'] = prefs.get('time_sensitive', 0) + 1
    
    def _update_session_context(self, user_id: str, conversation: Dict[str, Any]):
        """Update current session context"""
        session = self.session_context[user_id]
        
        # Track current focus area
        intent = conversation['intent']
        session['current_focus'] = intent
        session['focus_depth'] = session.get('focus_depth', 0) + 1
        
        # Reset focus if switching to different area
        recent_intents = [conv['intent'] for conv in list(self.conversations[user_id])[-3:]]
        if len(set(recent_intents)) > 1:  # Multiple different intents recently
            session['focus_depth'] = 1
    
    def _get_recent_keywords(self, conversations: List[Dict]) -> List[str]:
        """Get all keywords from recent conversations"""
        all_keywords = []
        for conv in conversations:
            all_keywords.extend(conv.get('keywords_extracted', []))
        return list(set(all_keywords))
    
    def _get_recent_entities(self, conversations: List[Dict]) -> Dict[str, List[str]]:
        """Get all entities from recent conversations"""
        all_entities = defaultdict(list)
        for conv in conversations:
            entities = conv.get('entities_extracted', {})
            for entity_type, values in entities.items():
                all_entities[entity_type].extend(values)
        
        # Remove duplicates
        return {k: list(set(v)) for k, v in all_entities.items()}

class ParallelProcessingEngine:
    """Advanced parallel processing for multiple AI tasks"""
    
    def __init__(self, ollama_client):
        self.ollama_client = ollama_client
        self.task_cache = {}
        self.performance_stats = defaultdict(list)
    
    async def parallel_process_query(self, question: str, context: Dict[str, Any], tenant_id: str) -> Dict[str, Any]:
        """Process query with parallel execution of multiple tasks"""
        start_time = time.time()
        
        logger.info(f"🚀 Starting parallel processing for: {question[:50]}...")
        
        try:
            # Phase 1: Launch all independent tasks simultaneously
            phase1_tasks = {
                'intent_analysis': self._analyze_intent_with_context(question, context),
                'entity_extraction': self._extract_enhanced_entities(question, context),
                'query_complexity': self._analyze_query_complexity(question, context),
                'context_enrichment': self._enrich_context(question, context),
                'thai_processing': self._process_thai_elements(question)
            }
            
            # Execute Phase 1 in parallel
            phase1_results = await self._execute_parallel_tasks(phase1_tasks, timeout=5)
            
            # Phase 2: Use Phase 1 results for SQL generation and business context
            enhanced_context = self._merge_analysis_results(phase1_results, context)
            
            phase2_tasks = {
                'sql_generation': self._generate_contextual_sql(question, enhanced_context, tenant_id),
                'business_context': self._prepare_business_context(question, enhanced_context, tenant_id),
                'response_strategy': self._determine_response_strategy(enhanced_context)
            }
            
            # Execute Phase 2 in parallel
            phase2_results = await self._execute_parallel_tasks(phase2_tasks, timeout=15)
            
            # Combine all results
            processing_time = time.time() - start_time
            final_result = {
                **phase1_results,
                **phase2_results,
                'processing_time': processing_time,
                'parallel_efficiency': self._calculate_efficiency(phase1_results, phase2_results)
            }
            
            logger.info(f"✅ Parallel processing completed in {processing_time:.2f}s")
            return final_result
            
        except Exception as e:
            logger.error(f"❌ Parallel processing failed: {e}")
            return {'error': str(e), 'processing_time': time.time() - start_time}
    
    async def _execute_parallel_tasks(self, tasks: Dict[str, Any], timeout: int = 10) -> Dict[str, Any]:
        """Execute multiple async tasks in parallel with timeout"""
        try:
            # Start all tasks simultaneously
            task_futures = {name: asyncio.create_task(task) for name, task in tasks.items()}
            
            # Wait for all tasks with timeout
            results = {}
            completed_tasks, pending_tasks = await asyncio.wait(
                task_futures.values(), 
                timeout=timeout,
                return_when=asyncio.ALL_COMPLETED
            )
            
            # Collect results from completed tasks
            for name, future in task_futures.items():
                if future in completed_tasks:
                    try:
                        results[name] = await future
                    except Exception as e:
                        logger.error(f"Task {name} failed: {e}")
                        results[name] = {'error': str(e)}
                else:
                    logger.warning(f"Task {name} timed out")
                    future.cancel()
                    results[name] = {'error': 'timeout'}
            
            return results
            
        except Exception as e:
            logger.error(f"Parallel execution failed: {e}")
            return {name: {'error': str(e)} for name in tasks.keys()}
    
    async def _analyze_intent_with_context(self, question: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced intent analysis using conversation context"""
        try:
            # Basic intent classification
            basic_intent = self._classify_basic_intent(question)
            
            # Context-enhanced intent
            context_clues = context.get('implicit_context', {})
            if context_clues.get('inherit_intent') and len(question.split()) < 5:
                # Short query likely continues previous conversation
                enhanced_intent = context_clues['inherit_intent']
                confidence = 0.8
                logger.info(f"🧠 Inherited intent from context: {enhanced_intent}")
            else:
                enhanced_intent = basic_intent
                confidence = 0.9
            
            # Handle comparison queries with context
            if context_clues.get('comparison_type') == 'year_over_year':
                enhanced_intent = f"{enhanced_intent}_comparison"
                years = context_clues.get('comparison_years', [])
                logger.info(f"📊 Detected year comparison: {years}")
            
            return {
                'basic_intent': basic_intent,
                'enhanced_intent': enhanced_intent,
                'confidence': confidence,
                'context_used': bool(context_clues),
                'reasoning': f"Intent enhanced with context: {list(context_clues.keys())}"
            }
            
        except Exception as e:
            return {'error': str(e), 'basic_intent': 'general'}
    
    async def _extract_enhanced_entities(self, question: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract entities with context awareness"""
        try:
            # Extract entities from current query
            current_entities = self._extract_entities_from_text(question)
            
            # Inherit missing entities from context
            context_entities = context.get('implicit_context', {}).get('inherit_entities', {})
            
            # Merge entities intelligently
            merged_entities = current_entities.copy()
            for entity_type, values in context_entities.items():
                if entity_type not in merged_entities or not merged_entities[entity_type]:
                    merged_entities[entity_type] = values
                    logger.info(f"🔗 Inherited {entity_type}: {values}")
            
            return {
                'current_entities': current_entities,
                'inherited_entities': context_entities,
                'merged_entities': merged_entities,
                'entity_sources': self._track_entity_sources(current_entities, context_entities)
            }
            
        except Exception as e:
            return {'error': str(e), 'merged_entities': {}}
    
    async def _analyze_query_complexity(self, question: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze query complexity for optimization"""
        try:
            complexity_factors = {
                'word_count': len(question.split()),
                'has_comparison': any(word in question.lower() for word in ['เปรียบเทียบ', 'ต่าง', 'กับ']),
                'multiple_years': len(re.findall(r'\d{4}', question)) > 1,
                'multiple_entities': sum(len(v) for v in context.get('recent_entities', {}).values()) > 5,
                'context_dependent': len(context.get('continuation_signals', [])) > 0
            }
            
            # Calculate complexity score
            complexity_score = (
                complexity_factors['word_count'] * 0.1 +
                complexity_factors['has_comparison'] * 2 +
                complexity_factors['multiple_years'] * 3 +
                complexity_factors['multiple_entities'] * 1.5 +
                complexity_factors['context_dependent'] * 2
            )
            
            if complexity_score < 5:
                complexity_level = 'simple'
            elif complexity_score < 10:
                complexity_level = 'moderate'
            else:
                complexity_level = 'complex'
            
            return {
                'complexity_score': complexity_score,
                'complexity_level': complexity_level,
                'factors': complexity_factors,
                'estimated_processing_time': complexity_score * 0.8 + 2,
                'recommended_strategy': self._get_processing_strategy(complexity_level)
            }
            
        except Exception as e:
            return {'error': str(e), 'complexity_level': 'moderate'}
    
    async def _enrich_context(self, question: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich context with additional business intelligence"""
        try:
            enriched = context.copy()
            
            # Add business context based on question
            if 'overhaul' in question.lower() or 'ยกเครื่อง' in question.lower():
                enriched['business_focus'] = 'high_value_service'
                enriched['expected_data_volume'] = 'medium'
                enriched['priority_level'] = 'high'
            
            elif 'อะไหล่' in question.lower() or 'spare' in question.lower():
                enriched['business_focus'] = 'inventory_management'
                enriched['expected_data_volume'] = 'high'
                enriched['priority_level'] = 'medium'
            
            # Add seasonal context
            current_month = datetime.now().month
            if current_month in [3, 4, 5]:  # High season
                enriched['seasonal_context'] = 'peak_season'
            else:
                enriched['seasonal_context'] = 'normal_season'
            
            return enriched
            
        except Exception as e:
            return context
    
    async def _process_thai_elements(self, question: str) -> Dict[str, Any]:
        """Process Thai-specific elements in parallel"""
        try:
            thai_years = re.findall(r'256[5-8]', question)
            thai_months = re.findall(r'(มกราคม|กุมภาพันธ์|มีนาคม|เมษายน|พฤษภาคม|มิถุนายน|กรกฎาคม|สิงหาคม|กันยายน|ตุลาคม|พฤศจิกายน|ธันวาคม)', question)
            
            # Convert Thai years to international
            year_mappings = {'2565': '2022', '2566': '2023', '2567': '2024', '2568': '2025'}
            converted_years = [year_mappings.get(year, year) for year in thai_years]
            
            # Convert Thai months to numbers
            month_mappings = {
                'มกราคม': '01', 'กุมภาพันธ์': '02', 'มีนาคม': '03', 'เมษายน': '04',
                'พฤษภาคม': '05', 'มิถุนายน': '06', 'กรกฎาคม': '07', 'สิงหาคม': '08',
                'กันยายน': '09', 'ตุลาคม': '10', 'พฤศจิกายน': '11', 'ธันวาคม': '12'
            }
            converted_months = [month_mappings.get(month, month) for month in thai_months]
            
            return {
                'thai_years': thai_years,
                'converted_years': converted_years,
                'thai_months': thai_months,
                'converted_months': converted_months,
                'has_thai_elements': bool(thai_years or thai_months)
            }
            
        except Exception as e:
            return {'error': str(e), 'has_thai_elements': False}
    
    async def _generate_contextual_sql(self, question: str, enhanced_context: Dict[str, Any], tenant_id: str) -> Dict[str, Any]:
        """Generate SQL with full context awareness"""
        try:
            # Get processed elements
            intent_info = enhanced_context.get('intent_analysis', {})
            entity_info = enhanced_context.get('entity_extraction', {})
            thai_info = enhanced_context.get('thai_processing', {})
            
            # Build enhanced prompt with all context
            context_prompt = self._build_contextual_sql_prompt(
                question, 
                intent_info.get('enhanced_intent', 'general'),
                entity_info.get('merged_entities', {}),
                thai_info,
                enhanced_context
            )
            
            # Generate SQL using context-aware prompt
            sql_response = await self.ollama_client.generate_response(
                model="mannix/defog-llama3-sqlcoder-8b:latest",
                prompt=context_prompt,
                temperature=0.1
            )
            
            cleaned_sql = self._clean_sql_response(sql_response)
            
            return {
                'sql_query': cleaned_sql,
                'context_used': True,
                'prompt_length': len(context_prompt),
                'generation_method': 'contextual_enhanced'
            }
            
        except Exception as e:
            logger.error(f"Contextual SQL generation failed: {e}")
            return {'error': str(e), 'sql_query': None}
    
    def _build_contextual_sql_prompt(self, question: str, intent: str, entities: Dict, thai_info: Dict, context: Dict) -> str:
        """Build comprehensive SQL prompt with all available context"""
        
        base_prompt = """Generate PostgreSQL query for HVAC business data analysis.

DATABASE SCHEMA:
- sales2022,2023,2024,2025: id, job_no, customer_name, description, overhaul_, replacement, service_contact_
- spare_part, spare_part2: id, wh, product_code, product_name, unit, balance, unit_price, description
- work_force: id, date, customer, project, detail, service_group

IMPORTANT RULES:
- overhaul_, replacement, service_contact_, unit_price are TEXT fields - ALWAYS use CAST(field AS NUMERIC)
- Use COALESCE for NULL handling in revenue calculations
- Use ILIKE for case-insensitive text search"""
        
        # Add context-specific information
        context_info = []
        
        if entities.get('years'):
            context_info.append(f"YEARS MENTIONED: {entities['years']} (focus on these years)")
        
        if entities.get('companies'):
            context_info.append(f"COMPANIES: {entities['companies']} (filter by these)")
        
        if thai_info.get('converted_years'):
            context_info.append(f"THAI YEARS CONVERTED: {thai_info['thai_years']} → {thai_info['converted_years']}")
        
        if intent.endswith('_comparison'):
            context_info.append("COMPARISON QUERY: Use UNION ALL to compare multiple periods")
        
        # Add conversation context
        implicit_context = context.get('implicit_context', {})
        if implicit_context.get('comparison_years'):
            years = implicit_context['comparison_years']
            context_info.append(f"IMPLICIT COMPARISON: Compare {years[0]} vs {years[1]}")
        
        if context_info:
            context_section = "\nCONTEXT INFORMATION:\n" + "\n".join(f"- {info}" for info in context_info)
        else:
            context_section = ""
        
        return f"{base_prompt}{context_section}\n\nQuestion: {question}\nIntent: {intent}\n\nGenerate the most appropriate SQL query:"
    
    def _clean_sql_response(self, sql_response: str) -> str:
        """Clean up SQL response from AI model"""
        sql_query = sql_response.strip()
        
        # Remove markdown code blocks
        if sql_query.startswith('```sql'):
            sql_query = sql_query[6:]
        if sql_query.startswith('```'):
            sql_query = sql_query[3:]
        if sql_query.endswith('```'):
            sql_query = sql_query[:-3]
        
        return sql_query.strip()
    
    def _merge_analysis_results(self, phase1_results: Dict, original_context: Dict) -> Dict[str, Any]:
        """Merge all analysis results into enhanced context"""
        enhanced = original_context.copy()
        
        for task_name, result in phase1_results.items():
            if not result.get('error'):
                enhanced[task_name] = result
        
        return enhanced
    
    def _classify_basic_intent(self, question: str) -> str:
        """Basic intent classification (same as before but isolated)"""
        question_lower = question.lower()
        
        if any(keyword in question_lower for keyword in ['overhaul', 'ยกเครื่อง']):
            return 'overhaul_analysis'
        elif any(keyword in question_lower for keyword in ['ยอดขาย', 'วิเคราะห์']):
            return 'sales_analysis'
        elif any(keyword in question_lower for keyword in ['อะไหล่', 'spare', 'parts']):
            return 'spare_parts'
        else:
            return 'general'
    
    def _extract_entities_from_text(self, text: str) -> Dict[str, List[str]]:
        """Extract entities from text"""
        entities = {
            'years': re.findall(r'(\d{4}|256[5-8])', text),
            'companies': re.findall(r'(บริษัท[^,.\n]+|[A-Z][A-Z\s]+)', text),
            'amounts': re.findall(r'(\d{1,3}(?:,\d{3})*)', text)
        }
        return {k: v for k, v in entities.items() if v}
    
    def _track_entity_sources(self, current: Dict, inherited: Dict) -> Dict[str, str]:
        """Track where each entity came from"""
        sources = {}
        for entity_type in set(list(current.keys()) + list(inherited.keys())):
            if entity_type in current and current[entity_type]:
                sources[entity_type] = 'current_query'
            elif entity_type in inherited and inherited[entity_type]:
                sources[entity_type] = 'inherited_context'
        return sources
    
    def _get_processing_strategy(self, complexity_level: str) -> str:
        """Get recommended processing strategy based on complexity"""
        strategies = {
            'simple': 'fast_track_with_cache',
            'moderate': 'standard_parallel_processing',
            'complex': 'full_context_analysis'
        }
        return strategies.get(complexity_level, 'standard_parallel_processing')
    
    def _calculate_efficiency(self, phase1: Dict, phase2: Dict) -> Dict[str, Any]:
        """Calculate parallel processing efficiency"""
        total_tasks = len(phase1) + len(phase2)
        successful_tasks = sum(1 for result in list(phase1.values()) + list(phase2.values()) if not result.get('error'))
        
        return {
            'total_tasks': total_tasks,
            'successful_tasks': successful_tasks,
            'success_rate': successful_tasks / total_tasks if total_tasks > 0 else 0,
            'efficiency_score': successful_tasks / total_tasks * 100 if total_tasks > 0 else 0
        }

class SimplifiedDatabaseHandler:
    """Enhanced database handler with connection pooling"""
    
    def __init__(self):
        self.connection_cache = {}
        self.connection_pool = {}
    
    def get_database_connection(self, tenant_id: str):
        """Get database connection for tenant"""
        try:
            import psycopg2
            
            db_configs = {
                'company-a': {
                    'host': os.getenv('POSTGRES_HOST_COMPANY_A', 'postgres-company-a'),
                    'port': int(os.getenv('POSTGRES_PORT_COMPANY_A', '5432')),
                    'database': os.getenv('POSTGRES_DB_COMPANY_A', 'siamtemp_company_a'),
                    'user': os.getenv('POSTGRES_USER_COMPANY_A', 'postgres'),
                    'password': os.getenv('POSTGRES_PASSWORD_COMPANY_A', 'password123')
                },
                'company-b': {
                    'host': os.getenv('POSTGRES_HOST_COMPANY_B', 'postgres-company-b'),
                    'port': int(os.getenv('POSTGRES_PORT_COMPANY_B', '5432')),
                    'database': os.getenv('POSTGRES_DB_COMPANY_B', 'siamtemp_company_b'),
                    'user': os.getenv('POSTGRES_USER_COMPANY_B', 'postgres'),
                    'password': os.getenv('POSTGRES_PASSWORD_COMPANY_B', 'password123')
                },
                'company-c': {
                    'host': os.getenv('POSTGRES_HOST_COMPANY_C', 'postgres-company-c'),
                    'port': int(os.getenv('POSTGRES_PORT_COMPANY_C', '5432')),
                    'database': os.getenv('POSTGRES_DB_COMPANY_C', 'siamtemp_company_c'),
                    'user': os.getenv('POSTGRES_USER_COMPANY_C', 'postgres'),
                    'password': os.getenv('POSTGRES_PASSWORD_COMPANY_C', 'password123')
                }
            }
            
            config = db_configs.get(tenant_id, db_configs['company-a'])
            connection = psycopg2.connect(**config)
            return connection
            
        except Exception as e:
            logger.error(f"Database connection failed for {tenant_id}: {e}")
            return None
    
    async def execute_query(self, sql: str, tenant_id: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results"""
        try:
            conn = self.get_database_connection(tenant_id)
            if not conn:
                return []
            
            with conn.cursor() as cursor:
                cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                results = []
                for row in rows:
                    result = {}
                    for i, value in enumerate(row):
                        if isinstance(value, Decimal):
                            result[columns[i]] = float(value)
                        elif isinstance(value, (date, datetime)):
                            result[columns[i]] = str(value)
                        else:
                            result[columns[i]] = value
                    results.append(result)
                
                conn.close()
                return results
                
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []

class SimplifiedOllamaClient:
    """Enhanced Ollama client with retry logic"""
    
    def __init__(self):
        self.base_url = os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434')
        self.timeout = int(os.getenv('OLLAMA_TIMEOUT', '60'))
        self.max_retries = 2
    
    async def generate_response(self, model: str, prompt: str, temperature: float = 0.7, max_retries: int = None) -> str:
        """Generate response from Ollama model with retry logic"""
        if max_retries is None:
            max_retries = self.max_retries
            
        for attempt in range(max_retries + 1):
            try:
                import aiohttp
                
                payload = {
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "num_predict": 2000
                    }
                }
                
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                    async with session.post(f"{self.base_url}/api/generate", json=payload) as response:
                        if response.status == 200:
                            result = await response.json()
                            return result.get('response', '')
                        else:
                            logger.warning(f"Ollama API error {response.status} on attempt {attempt + 1}")
                            if attempt == max_retries:
                                return ""
                            await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                            
            except Exception as e:
                logger.error(f"Ollama request failed on attempt {attempt + 1}: {e}")
                if attempt == max_retries:
                    return ""
                await asyncio.sleep(1 * (attempt + 1))
        
        return ""

class EnhancedDualModelDynamicAISystem:
    """Enhanced Dual-Model AI System with Conversation Memory and Parallel Processing"""
    
    def __init__(self):
        # Initialize core components
        self.db_handler = SimplifiedDatabaseHandler()
        self.ollama_client = SimplifiedOllamaClient()
        self.conversation_memory = ConversationMemory(max_history=20)
        self.parallel_engine = ParallelProcessingEngine(self.ollama_client)
        
        # Model configuration
        self.SQL_MODEL = "mannix/defog-llama3-sqlcoder-8b:latest"
        self.NL_MODEL = "llama3.1:8b"
        
        # Performance settings
        self.sql_cache = {}
        self.semantic_cache = {}
        self.enable_parallel = True
        self.enable_conversation_memory = True
        
        # Enhanced stats tracking
        self.stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'sql_generated': 0,
            'nl_responses': 0,
            'cache_hits': 0,
            'context_usage': 0,
            'parallel_efficiency': [],
            'avg_response_time': 0.0,
            'conversation_continuations': 0
        }
        
        # HVAC Business Knowledge (enhanced)
        self.hvac_context = {
            'tables': {
                'sales2024': 'ข้อมูลงานบริการและการขายปี 2024',
                'sales2023': 'ข้อมูลงานบริการและการขายปี 2023',  
                'sales2022': 'ข้อมูลงานบริการและการขายปี 2022',
                'sales2025': 'ข้อมูลงานบริการและการขายปี 2025',
                'spare_part': 'คลังอะไหล่หลัก',
                'spare_part2': 'คลังอะไหล่สำรอง',
                'work_force': 'การจัดทีมงานและแผนการทำงาน'
            },
            'business_rules': {
                'overhaul_priority': 'งาน overhaul มีความสำคัญสูงสุดและมูลค่าสูง',
                'seasonal_patterns': 'เดือน มี.ค.-พ.ค. เป็นช่วง high season',
                'customer_tiers': 'ลูกค้าญี่ปุ่นมักเป็น premium customers',
                'service_types': ['PM', 'Overhaul', 'Replacement', 'Emergency']
            }
        }
        
        logger.info("🚀 Enhanced Dual-Model Dynamic AI System with Conversation Memory & Parallel Processing initialized")
    
    async def process_any_question(self, question: str, tenant_id: str, user_id: str = 'default') -> Dict[str, Any]:
        """Main entry point with enhanced conversation memory and parallel processing"""
        start_time = time.time()
        self.stats['total_queries'] += 1
        
        try:
            logger.info(f"🎯 Processing enhanced query for user {user_id}: {question[:50]}...")
            
            # Step 1: Get conversation context
            conversation_context = {}
            if self.enable_conversation_memory:
                conversation_context = self.conversation_memory.get_context(user_id, question)
                if conversation_context.get('continuation_signals'):
                    self.stats['conversation_continuations'] += 1
                    logger.info(f"🔗 Detected conversation continuation: {conversation_context['continuation_signals']}")
            
            # Step 2: Parallel processing with context
            if self.enable_parallel:
                parallel_results = await self.parallel_engine.parallel_process_query(question, conversation_context, tenant_id)
                
                # Track parallel efficiency
                efficiency = parallel_results.get('parallel_efficiency', {})
                if efficiency:
                    self.stats['parallel_efficiency'].append(efficiency.get('efficiency_score', 0))
                
                # Extract SQL from parallel results
                sql_info = parallel_results.get('sql_generation', {})
                sql_query = sql_info.get('sql_query')
                
                if not sql_query or sql_info.get('error'):
                    logger.warning("Parallel SQL generation failed, using fallback")
                    sql_query = self._get_fallback_sql_with_context(
                        parallel_results.get('intent_analysis', {}).get('enhanced_intent', 'general'),
                        parallel_results.get('entity_extraction', {}).get('merged_entities', {})
                    )
                
            else:
                # Fallback to sequential processing
                logger.info("Using sequential processing")
                intent = self._classify_question_intent(question)
                sql_query = await self._generate_sql_query(question, intent, tenant_id)
                parallel_results = {'intent_analysis': {'enhanced_intent': intent}}
            
            # Step 3: Execute SQL query
            sql_results = await self.db_handler.execute_query(sql_query, tenant_id)
            logger.info(f"📊 SQL execution returned {len(sql_results)} results")
            
            # Step 4: Generate enhanced response
            nl_response = await self._generate_enhanced_natural_response(
                question, 
                sql_results, 
                sql_query, 
                tenant_id,
                conversation_context,
                parallel_results
            )
            
            # Step 5: Prepare comprehensive response
            processing_time = time.time() - start_time
            success = len(sql_results) > 0
            
            if success:
                self.stats['successful_queries'] += 1
            
            response = {
                'answer': nl_response,
                'success': success,
                'sql_query': sql_query,
                'results_count': len(sql_results),
                'processing_time': processing_time,
                'ai_system_used': 'enhanced_dual_model_parallel',
                'question_analysis': {
                    'intent': parallel_results.get('intent_analysis', {}).get('enhanced_intent', 'unknown'),
                    'tenant_id': tenant_id,
                    'user_id': user_id,
                    'context_used': self.enable_conversation_memory,
                    'parallel_processing': self.enable_parallel,
                    'conversation_continuation': len(conversation_context.get('continuation_signals', [])) > 0,
                    'entities_extracted': parallel_results.get('entity_extraction', {}).get('merged_entities', {}),
                    'thai_elements': parallel_results.get('thai_processing', {}).get('has_thai_elements', False)
                },
                'models_used': {
                    'sql_model': self.SQL_MODEL,
                    'nl_model': self.NL_MODEL
                },
                'performance_stats': {
                    'parallel_efficiency': efficiency.get('efficiency_score', 0) if 'efficiency' in locals() else 0,
                    'context_usage_rate': self.stats['context_usage'] / max(self.stats['total_queries'], 1) * 100,
                    'conversation_continuation_rate': self.stats['conversation_continuations'] / max(self.stats['total_queries'], 1) * 100,
                    'avg_parallel_efficiency': sum(self.stats['parallel_efficiency']) / len(self.stats['parallel_efficiency']) if self.stats['parallel_efficiency'] else 0
                },
                'enhancement_features': {
                    'conversation_memory': self.enable_conversation_memory,
                    'parallel_processing': self.enable_parallel,
                    'context_inheritance': bool(conversation_context.get('implicit_context')),
                    'semantic_caching': len(self.semantic_cache) > 0
                }
            }
            
            # Step 6: Store conversation in memory
            if self.enable_conversation_memory:
                self.conversation_memory.add_conversation(user_id, question, response)
                self.stats['context_usage'] += 1
            
            # Update average response time
            total_time = self.stats['avg_response_time'] * (self.stats['total_queries'] - 1) + processing_time
            self.stats['avg_response_time'] = total_time / self.stats['total_queries']
            
            logger.info(f"✅ Enhanced processing completed in {processing_time:.2f}s")
            return response
            
        except Exception as e:
            logger.error(f"❌ Enhanced processing failed: {e}")
            processing_time = time.time() - start_time
            
            error_response = {
                'answer': f"เกิดข้อผิดพลาดในการประมวลผล: {str(e)}\n\nระบบกำลังพัฒนาฟีเจอร์ใหม่ กรุณาลองใหม่อีกครั้ง",
                'success': False,
                'sql_query': None,
                'results_count': 0,
                'processing_time': processing_time,
                'ai_system_used': 'enhanced_dual_model_error',
                'error': str(e),
                'enhancement_features': {
                    'conversation_memory': self.enable_conversation_memory,
                    'parallel_processing': self.enable_parallel,
                    'error_recovery': True
                }
            }
            
            # Still try to store conversation for learning
            if self.enable_conversation_memory:
                try:
                    self.conversation_memory.add_conversation(user_id, question, error_response)
                except:
                    pass
                    
            return error_response
    
    async def _generate_enhanced_natural_response(self, question: str, sql_results: List[Dict], 
                                                 sql_query: str, tenant_id: str, 
                                                 conversation_context: Dict, parallel_results: Dict) -> str:
        """Generate enhanced natural language response with context awareness"""
        try:
            self.stats['nl_responses'] += 1
            
            if not sql_results:
                # Enhanced no-data response with context
                if conversation_context.get('continuation_signals'):
                    return f"ไม่พบข้อมูลสำหรับคำถามต่อเนื่อง: {question}\n\nอาจเป็นเพราะข้อมูลในช่วงที่สนใจไม่มีหรือเปลี่ยนแปลงไป"
                else:
                    return f"ไม่พบข้อมูลสำหรับคำถาม: {question}"
            
            # Smart response strategy
            if len(sql_results) > 20:
                logger.info("Large dataset detected, using enhanced structured response")
                return self._get_enhanced_structured_response(question, sql_results, conversation_context, parallel_results)
            
            # Create context-aware data summary
            data_summary = self._create_contextual_summary(sql_results, question, conversation_context)
            
            # Build enhanced prompt with conversation context
            nl_prompt = self._build_contextual_nl_prompt(question, data_summary, conversation_context, parallel_results)
            
            # Generate response with retry logic
            nl_response = await asyncio.wait_for(
                self.ollama_client.generate_response(
                    model=self.NL_MODEL,
                    prompt=nl_prompt,
                    temperature=0.5,
                    max_retries=2
                ),
                timeout=8  # Slightly longer timeout for enhanced features
            )
            
            if nl_response:
                # Post-process response with context
                enhanced_response = self._post_process_response(nl_response, conversation_context)
                return enhanced_response
            else:
                return self._get_enhanced_structured_response(question, sql_results, conversation_context, parallel_results)
            
        except asyncio.TimeoutError:
            logger.info("Enhanced NL generation timeout, using structured response")
            return self._get_enhanced_structured_response(question, sql_results, conversation_context, parallel_results)
        except Exception as e:
            logger.error(f"Enhanced NL response generation failed: {e}")
            return self._get_enhanced_structured_response(question, sql_results, conversation_context, parallel_results)
    
    def _build_contextual_nl_prompt(self, question: str, data_summary: str, 
                                   conversation_context: Dict, parallel_results: Dict) -> str:
        """Build enhanced NL prompt with conversation context"""
        
        base_prompt = f"คำถาม: {question}\nข้อมูล: {data_summary}"
        
        # Add conversation context
        context_additions = []
        
        if conversation_context.get('continuation_signals'):
            context_additions.append("หมายเหตุ: นี่เป็นคำถามต่อเนื่องจากการสนทนาก่อนหน้า")
        
        implicit_context = conversation_context.get('implicit_context', {})
        if implicit_context.get('comparison_type') == 'year_over_year':
            years = implicit_context.get('comparison_years', [])
            context_additions.append(f"บริบท: กำลังเปรียบเทียบข้อมูลปี {years[0]} กับ {years[1]}")
        
        recent_focus = conversation_context.get('session_focus')
        if recent_focus:
            context_additions.append(f"โฟกัสปัจจุบัน: {recent_focus}")
        
        # Add business intelligence
        intent_info = parallel_results.get('intent_analysis', {})
        if intent_info.get('enhanced_intent') == 'overhaul_analysis':
            context_additions.append("ธุรกิจ: งาน overhaul เป็นบริการมูลค่าสูงที่ต้องความเชี่ยวชาญ")
        
        if context_additions:
            context_section = "\n" + "\n".join(context_additions)
        else:
            context_section = ""
        
        return f"{base_prompt}{context_section}\n\nตอบในภาษาไทยแบบเป็นธรรมชาติ เน้นความถูกต้องและเป็นประโยชน์:"
    
    def _create_contextual_summary(self, results: List[Dict], question: str, conversation_context: Dict) -> str:
        """Create context-aware data summary"""
        if not results:
            return "ไม่พบข้อมูล"
        
        # Check for conversation continuity
        if conversation_context.get('continuation_signals'):
            focus_area = conversation_context.get('session_focus', 'general')
            if focus_area == 'overhaul_analysis':
                return f"ข้อมูล overhaul ต่อเนื่อง: {len(results)} records, ตัวอย่าง: {results[:2]}"
            elif focus_area == 'sales_analysis':
                return f"ข้อมูลยอดขายต่อเนื่อง: {len(results)} records, ตัวอย่าง: {results[:2]}"
        
        # Standard contextual summary
        if any(word in question.lower() for word in ['เปรียบเทียบ', 'ต่าง', 'กับ']):
            return f"ข้อมูลเปรียบเทียบ: {results}"
        elif len(results) <= 5:
            return f"ข้อมูลรายละเอียด: {results}"
        else:
            return f"ข้อมูล {len(results)} รายการ, ตัวอย่าง: {results[:3]}"
    
    def _post_process_response(self, response: str, conversation_context: Dict) -> str:
        """Post-process AI response with context enhancements"""
        enhanced_response = response
        
        # Add continuity markers
        if conversation_context.get('continuation_signals'):
            if not any(marker in response for marker in ['ต่อจาก', 'เพิ่มเติม', 'สืบเนื่อง']):
                enhanced_response = f"ต่อจากที่สอบถามไว้ {enhanced_response}"
        
        # Add follow-up suggestions based on context
        session_focus = conversation_context.get('session_focus')
        if session_focus == 'overhaul_analysis' and 'overhaul' in enhanced_response.lower():
            enhanced_response += "\n\n💡 คำแนะนำ: คุณสามารถถามเพิ่มเติมเกี่ยวกับลูกค้าที่ใช้บริการ overhaul หรือเทรนด์ราคา"
        elif session_focus == 'sales_analysis' and any(word in enhanced_response for word in ['ยอด', 'รายได้']):
            enhanced_response += "\n\n📊 คำแนะนำ: สามารถขอดูการวิเคราะห์เชิงลึกหรือการพยากรณ์ได้"
        
        return enhanced_response
    
    def _get_enhanced_structured_response(self, question: str, results: List[Dict], 
                                        conversation_context: Dict, parallel_results: Dict) -> str:
        """Generate enhanced structured response with conversation awareness"""
        if not results:
            return f"ไม่พบข้อมูลสำหรับคำถาม: {question}"
        
        # Get context information
        intent_info = parallel_results.get('intent_analysis', {})
        enhanced_intent = intent_info.get('enhanced_intent', 'general')
        
        # Add conversation context to response
        context_prefix = ""
        if conversation_context.get('continuation_signals'):
            context_prefix = "ต่อจากคำถามก่อนหน้า:\n\n"
        
        # Use appropriate formatter based on enhanced intent
        if enhanced_intent.startswith('overhaul'):
            formatted_response = self._format_overhaul_response_enhanced(question, results, conversation_context)
        elif enhanced_intent.startswith('sales'):
            formatted_response = self._format_sales_response_enhanced(question, results, conversation_context)
        elif enhanced_intent == 'spare_parts':
            formatted_response = self._format_parts_response_enhanced(question, results, conversation_context)
        else:
            formatted_response = self._format_generic_response_enhanced(question, results, conversation_context)
        
        return context_prefix + formatted_response
    
    def _format_overhaul_response_enhanced(self, question: str, results: List[Dict], context: Dict) -> str:
        """Enhanced overhaul response formatting with context"""
        response = f"📋 รายงานงาน Overhaul: {question}\n\nพบ {len(results)} รายการ\n\n"
        
        total_value = 0
        overhaul_count = 0
        
        for i, result in enumerate(results, 1):
            job_no = result.get('job_no', 'N/A')
            customer = result.get('customer_name', 'N/A')
            description = result.get('description', 'N/A')
            
            # Enhanced value detection
            overhaul_value = result.get('overhaul_revenue') or result.get('overhaul_') or 0
            service_value = result.get('service_revenue') or result.get('service_contact_') or 0
            
            if overhaul_value:
                total_value += float(overhaul_value)
                overhaul_count += 1
                response += f"{i}. 🔧 {job_no}\n"
                response += f"   ลูกค้า: {customer}\n"
                response += f"   งาน: {description}\n"
                response += f"   มูลค่า Overhaul: {float(overhaul_value):,.0f} บาท\n"
                if service_value:
                    response += f"   บริการเพิ่มเติม: {float(service_value):,.0f} บาท\n"
                response += "\n"
        
        # Enhanced summary with business insights
        if total_value > 0:
            avg_value = total_value / overhaul_count if overhaul_count > 0 else 0
            response += f"💰 สรุป:\n"
            response += f"• มูลค่า Overhaul รวม: {total_value:,.0f} บาท\n"
            response += f"• จำนวนงาน: {overhaul_count} งาน\n"
            response += f"• มูลค่าเฉลี่ย: {avg_value:,.0f} บาท/งาน\n\n"
            
            # Add business insights
            if avg_value > 200000:
                response += "📈 การวิเคราะห์: งาน Overhaul มีมูลค่าสูง แสดงถึงลูกค้าระดับ premium"
            elif overhaul_count > 5:
                response += "📊 การวิเคราะห์: ปริมาณงาน Overhaul สูง แสดงถึงความต้องการบริการเชิงลึก"
        
        # Add contextual follow-ups
        if context.get('session_focus') == 'overhaul_analysis':
            response += "\n\n🔍 คำถามที่เกี่ยวข้อง:\n• ลูกค้าไหนใช้บริการ Overhaul บ่อยที่สุด?\n• เทรนด์ราคา Overhaul เป็นอย่างไร?\n• อะไหล่ที่ใช้ในงาน Overhaul"
        
        return response
    
    def _format_sales_response_enhanced(self, question: str, results: List[Dict], context: Dict) -> str:
        """Enhanced sales response formatting with context"""
        response = f"📊 การวิเคราะห์ยอดขาย: {question}\n\n"
        
        total_jobs = 0
        total_revenue = 0
        yearly_data = []
        
        for result in results:
            year = result.get('year', 'N/A')
            jobs = result.get('jobs', 0) or result.get('total_jobs', 0)
            revenue = (result.get('total_revenue') or 
                      result.get('overhaul_revenue', 0) + result.get('service_revenue', 0) or 0)
            
            if jobs or revenue:
                yearly_data.append({'year': year, 'jobs': jobs, 'revenue': revenue})
                total_jobs += jobs
                total_revenue += revenue
        
        # Display yearly breakdown
        for data in yearly_data:
            year, jobs, revenue = data['year'], data['jobs'], data['revenue']
            avg_per_job = revenue / jobs if jobs > 0 else 0
            
            response += f"📅 ปี {year}:\n"
            response += f"  • งานทั้งหมด: {jobs:,} งาน\n"
            if revenue > 0:
                response += f"  • รายได้รวม: {revenue:,.0f} บาท\n"
                response += f"  • เฉลี่ยต่องาน: {avg_per_job:,.0f} บาท\n"
            response += "\n"
        
        # Enhanced comparative analysis
        if len(yearly_data) > 1:
            current_year = max(yearly_data, key=lambda x: int(x['year']) if x['year'].isdigit() else 0)
            prev_year = min(yearly_data, key=lambda x: int(x['year']) if x['year'].isdigit() else 0)
            
            if current_year['revenue'] and prev_year['revenue']:
                growth_rate = ((current_year['revenue'] - prev_year['revenue']) / prev_year['revenue']) * 100
                
                response += f"📈 การเปรียบเทียบ:\n"
                if growth_rate > 0:
                    response += f"• รายได้เติบโต {growth_rate:+.1f}% จากปีก่อน\n"
                else:
                    response += f"• รายได้ลดลง {abs(growth_rate):.1f}% จากปีก่อน\n"
                
                job_growth = ((current_year['jobs'] - prev_year['jobs']) / prev_year['jobs'] * 100) if prev_year['jobs'] > 0 else 0
                if job_growth != 0:
                    response += f"• จำนวนงานเปลี่ยนแปลง {job_growth:+.1f}%\n"
        
        # Business insights
        response += f"\n💡 สรุปภาพรวม:\n"
        response += f"• รายได้รวมทั้งหมด: {total_revenue:,.0f} บาท\n"
        response += f"• จำนวนงานรวม: {total_jobs:,} งาน\n"
        
        if total_jobs > 0:
            overall_avg = total_revenue / total_jobs
            response += f"• มูลค่าเฉลี่ยต่องาน: {overall_avg:,.0f} บาท\n"
        
        return response
    
    def _format_parts_response_enhanced(self, question: str, results: List[Dict], context: Dict) -> str:
        """Enhanced parts response formatting with context"""
        response = f"🔧 ผลการค้นหาอะไหล่: {question}\n\nพบ {len(results)} รายการ\n\n"
        
        display_count = min(10, len(results))
        total_value = 0
        available_count = 0
        
        for i, result in enumerate(results[:display_count], 1):
            name = result.get('product_name', 'N/A')
            code = result.get('product_code', 'N/A')
            price = result.get('price') or result.get('unit_price', 0)
            balance = result.get('balance', 0)
            wh = result.get('wh', '')
            
            response += f"{i}. 📦 {name}\n"
            response += f"   รหัส: {code}"
            
            if wh and wh.strip():
                response += f" | 🏪 {wh}"
            response += "\n"
            
            if price and float(price) > 0:
                price_val = float(price)
                response += f"   💰 ราคา: {price_val:,.0f} บาท"
                total_value += price_val
                
            if balance and int(balance) > 0:
                balance_val = int(balance)
                response += f" | 📊 คงเหลือ: {balance_val} ชิ้น"
                available_count += 1
                
            response += "\n\n"
        
        if len(results) > display_count:
            response += f"... และอีก {len(results) - display_count} รายการ\n\n"
        
        # Enhanced summary
        response += f"📋 สรุป:\n"
        response += f"• จำนวนรายการ: {len(results)} รายการ\n"
        if available_count > 0:
            response += f"• มีสินค้าพร้อมส่ง: {available_count} รายการ\n"
        if total_value > 0:
            response += f"• มูลค่าโดยประมาณ: {total_value:,.0f} บาท\n"
        
        return response
    
    def _format_generic_response_enhanced(self, question: str, results: List[Dict], context: Dict) -> str:
        """Enhanced generic response formatting with context"""
        response = f"🔍 ผลการค้นหา: {question}\n\nพบ {len(results)} รายการ\n\n"
        
        display_count = min(5, len(results))
        
        for i, result in enumerate(results[:display_count], 1):
            response += f"{i}. "
            items = list(result.items())[:4]  # Show first 4 fields
            response += " | ".join(f"{k}: {v}" for k, v in items)
            response += "\n"
        
        if len(results) > display_count:
            response += f"\n... และอีก {len(results) - display_count} รายการ"
        
        return response
    
    def _get_fallback_sql_with_context(self, intent: str, entities: Dict[str, Any]) -> str:
        """Get fallback SQL with entity context"""
        if intent == 'overhaul_analysis':
            return """SELECT '2024' as year, COUNT(*) as jobs, 
                     SUM(CAST(overhaul_ AS NUMERIC)) as overhaul_revenue,
                     SUM(CAST(service_contact_ AS NUMERIC)) as service_revenue,
                     SUM(COALESCE(CAST(overhaul_ AS NUMERIC), 0) + COALESCE(CAST(service_contact_ AS NUMERIC), 0)) as total_revenue
                     FROM sales2024 WHERE LOWER(description) LIKE '%overhaul%'
                     UNION ALL
                     SELECT '2025' as year, COUNT(*) as jobs,
                     SUM(CAST(overhaul_ AS NUMERIC)) as overhaul_revenue,
                     SUM(CAST(service_contact_ AS NUMERIC)) as service_revenue,
                     SUM(COALESCE(CAST(overhaul_ AS NUMERIC), 0) + COALESCE(CAST(service_contact_ AS NUMERIC), 0)) as total_revenue
                     FROM sales2025 WHERE LOWER(description) LIKE '%overhaul%'
                     ORDER BY year"""
        
        elif intent == 'sales_analysis':
            return """SELECT '2024' as year, COUNT(*) as jobs,
                     SUM(COALESCE(CAST(overhaul_ AS NUMERIC), 0) + COALESCE(CAST(service_contact_ AS NUMERIC), 0)) as total_revenue
                     FROM sales2024 WHERE service_contact_ IS NOT NULL OR overhaul_ IS NOT NULL
                     UNION ALL
                     SELECT '2025' as year, COUNT(*) as jobs,
                     SUM(COALESCE(CAST(overhaul_ AS NUMERIC), 0) + COALESCE(CAST(service_contact_ AS NUMERIC), 0)) as total_revenue
                     FROM sales2025 WHERE service_contact_ IS NOT NULL OR overhaul_ IS NOT NULL
                     ORDER BY year"""
        
        else:
            return """SELECT product_code, product_name, CAST(unit_price AS NUMERIC) as price, balance
                     FROM spare_part WHERE product_name IS NOT NULL
                     UNION ALL
                     SELECT product_code, product_name, CAST(unit_price AS NUMERIC) as price, balance
                     FROM spare_part2 WHERE product_name IS NOT NULL
                     ORDER BY price DESC NULLS LAST LIMIT 10"""
    
    def _classify_question_intent(self, question: str) -> str:
        """Basic intent classification for fallback"""
        question_lower = question.lower()
        
        if any(keyword in question_lower for keyword in ['overhaul', 'ยกเครื่อง']):
            return 'overhaul_analysis'
        elif any(keyword in question_lower for keyword in ['ยอดขาย', 'วิเคราะห์']):
            return 'sales_analysis'
        elif any(keyword in question_lower for keyword in ['อะไหล่', 'spare', 'parts']):
            return 'spare_parts'
        else:
            return 'general'
    
    async def _generate_sql_query(self, question: str, intent: str, tenant_id: str) -> str:
        """Fallback SQL generation method"""
        try:
            # Simple SQL generation for fallback
            basic_prompt = f"""Generate PostgreSQL query for: {question}
Intent: {intent}

Tables: sales2024, sales2025, spare_part, spare_part2
Use CAST for numeric fields.

Query:"""
            
            response = await self.ollama_client.generate_response(
                model=self.SQL_MODEL,
                prompt=basic_prompt,
                temperature=0.1
            )
            
            return self._clean_sql_response(response) if response else self._get_fallback_sql_with_context(intent, {})
            
        except Exception as e:
            logger.error(f"Fallback SQL generation failed: {e}")
            return self._get_fallback_sql_with_context(intent, {})
    
    def _clean_sql_response(self, sql_response: str) -> str:
        """Clean up SQL response from AI model"""
        sql_query = sql_response.strip()
        
        if sql_query.startswith('```sql'):
            sql_query = sql_query[6:]
        if sql_query.startswith('```'):
            sql_query = sql_query[3:]
        if sql_query.endswith('```'):
            sql_query = sql_query[:-3]
        
        return sql_query.strip()
    
    def get_enhanced_stats(self) -> Dict[str, Any]:
        """Get comprehensive enhanced statistics"""
        return {
            'basic_stats': self.stats,
            'conversation_stats': {
                'total_users': len(self.conversation_memory.conversations),
                'avg_conversation_length': sum(len(conv) for conv in self.conversation_memory.conversations.values()) / max(len(self.conversation_memory.conversations), 1),
                'context_usage_rate': self.stats['context_usage'] / max(self.stats['total_queries'], 1) * 100,
                'continuation_rate': self.stats['conversation_continuations'] / max(self.stats['total_queries'], 1) * 100
            },
            'parallel_processing_stats': {
                'enabled': self.enable_parallel,
                'avg_efficiency': sum(self.stats['parallel_efficiency']) / len(self.stats['parallel_efficiency']) if self.stats['parallel_efficiency'] else 0,
                'efficiency_trend': self.stats['parallel_efficiency'][-5:] if self.stats['parallel_efficiency'] else []
            },
            'enhancement_features': {
                'conversation_memory': self.enable_conversation_memory,
                'parallel_processing': self.enable_parallel,
                'contextual_sql_generation': True,
                'enhanced_response_formatting': True,
                'business_intelligence': True
            }
        }

class EnhancedUnifiedPostgresOllamaAgent:
    """Enhanced main agent class with conversation memory and parallel processing"""
    
    def __init__(self):
        try:
            self.dual_model_ai = EnhancedDualModelDynamicAISystem()
            
            self.stats = {
                'total_queries': 0,
                'successful_queries': 0,
                'failed_queries': 0,
                'avg_response_time': 0.0
            }
            
            logger.info("🚀 Enhanced Unified PostgreSQL Ollama Agent with Conversation Memory & Parallel Processing initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Enhanced Agent: {e}")
            raise
    
    async def process_any_question(self, question: str, tenant_id: str, user_id: str = 'default') -> Dict[str, Any]:
        """Enhanced main entry point with user session support"""
        try:
            self.stats['total_queries'] += 1
            
            # Use enhanced processing with conversation memory and parallel processing
            result = await self.dual_model_ai.process_any_question(question, tenant_id, user_id)
            
            if result.get('success'):
                self.stats['successful_queries'] += 1
            else:
                self.stats['failed_queries'] += 1
            
            if 'processing_time' in result:
                total_time = self.stats['avg_response_time'] * (self.stats['total_queries'] - 1) + result['processing_time']
                self.stats['avg_response_time'] = total_time / self.stats['total_queries']
            
            return result
            
        except Exception as e:
            self.stats['failed_queries'] += 1
            logger.error(f"Enhanced question processing failed: {e}")
            
            return {
                'answer': f"เกิดข้อผิดพลาดในการประมวลผลคำถาม: {str(e)}\n\nระบบใหม่กำลังเรียนรู้ กรุณาลองใหม่อีกครั้ง",
                'success': False,
                'sql_query': None,
                'results_count': 0,
                'ai_system_used': 'enhanced_error_handler',
                'error': str(e),
                'enhancement_features': {
                    'conversation_memory': True,
                    'parallel_processing': True,
                    'error_recovery': True
                }
            }
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get comprehensive system statistics"""
        return {
            'agent_stats': self.stats,
            'enhanced_ai_stats': self.dual_model_ai.get_enhanced_stats(),
            'system_capabilities': {
                'conversation_memory': self.dual_model_ai.enable_conversation_memory,
                'parallel_processing': self.dual_model_ai.enable_parallel,
                'contextual_understanding': True,
                'business_intelligence': True,
                'multi_user_support': True
            }
        }
    
    def clear_conversation_memory(self, user_id: str = None):
        """Clear conversation memory for specific user or all users"""
        if user_id:
            if user_id in self.dual_model_ai.conversation_memory.conversations:
                self.dual_model_ai.conversation_memory.conversations[user_id].clear()
                logger.info(f"Cleared conversation memory for user: {user_id}")
        else:
            self.dual_model_ai.conversation_memory.conversations.clear()
            logger.info("Cleared all conversation memory")

# Export for backwards compatibility
UnifiedEnhancedPostgresOllamaAgent = EnhancedUnifiedPostgresOllamaAgent
DualModelDynamicAgent = EnhancedUnifiedPostgresOllamaAgent