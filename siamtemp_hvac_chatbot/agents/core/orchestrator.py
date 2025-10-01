# agents/core/orchestrator.py
"""
Refactored ImprovedDualModelDynamicAISystem
Clean architecture with separated concerns
"""

from collections import defaultdict
import time
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from ..storage.redis_memory import ScalableStorageAdapter
from ..storage.scalable_database import ScalableDatabaseHandler
from ..storage.database import SimplifiedDatabaseHandler
from .context_handler import ContextHandler, ConversationTurn, ConversationState
from collections import defaultdict
from agents.nlp.general_chat_handler import GeneralChatHandler
from ..utils.table_formatter import format_results_as_table_response
logger = logging.getLogger(__name__)

# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class QueryContext:
    """Context for query processing"""
    question: str
    tenant_id: str
    user_id: str
    intent: Optional[str] = None
    entities: Optional[Dict] = None
    confidence: float = 0.0
    previous_intent: Optional[str] = None
    
@dataclass
class ProcessingResult:
    """Result of query processing"""
    success: bool
    answer: str
    sql_query: Optional[str] = None
    results_count: int = 0
    insights: Optional[Dict] = None
    processing_time: float = 0.0
    error: Optional[str] = None

# =============================================================================
# MAIN ORCHESTRATOR - REFACTORED
# =============================================================================

class ImprovedDualModelDynamicAISystem:
    """
    Refactored orchestrator with clean architecture
    Each method has single responsibility
    """
    
    def __init__(self):
        # Initialize components
        self._initialize_components()
        self._initialize_features()
        self._initialize_stats()
        # self.conversation_memory = ScalableStorageAdapter()
        # self.db_handler = ScalableDatabaseHandler()
        # self.db_handler = SimplifiedDatabaseHandler()
        self.context_handler = ContextHandler()
        self.conversation_turns = defaultdict(list) 
        self.general_chat = GeneralChatHandler()
        logger.info("🚀 Refactored System initialized")
    
    # =========================================================================
    # INITIALIZATION METHODS
    # =========================================================================

    def _initialize_components(self):
        """Initialize all system components"""
        from ..nlp.prompt_manager import PromptManager
        from ..sql.validator import SQLValidator
        from ..nlp.intent_detector import ImprovedIntentDetector
        from ..data.cleaner import DataCleaningEngine
        from ..clients.ollama import SimplifiedOllamaClient
        from ..storage.database import SimplifiedDatabaseHandler
        
        # ✅ ใช้ SimplifiedDatabaseHandler เสมอ (ซึ่งทำงานได้ดีอยู่แล้ว)
        self.db_handler = SimplifiedDatabaseHandler()
        
        # Redis storage (optional)
        try:
            from ..storage.redis_memory import ScalableStorageAdapter
            self.conversation_memory = ScalableStorageAdapter()
            logger.info("✅ Using Redis-based conversation memory")
        except Exception as e:
            from ..storage.memory import ConversationMemory
            logger.warning(f"Redis unavailable: {e}, using in-memory storage")
            self.conversation_memory = ConversationMemory()
        
        # Other components
        self.prompt_manager = PromptManager()
        self.sql_validator = SQLValidator(self.prompt_manager)
        self.intent_detector = ImprovedIntentDetector()
        self.data_cleaner = DataCleaningEngine()
        self.ollama_client = SimplifiedOllamaClient()
    
    def _initialize_features(self):
        """Initialize feature flags"""
        self.enable_conversation_memory = True
        self.enable_parallel_processing = True
        self.enable_data_cleaning = True
        self.enable_sql_validation = True
        self.enable_few_shot_learning = True
    
    def _initialize_stats(self):
        """Initialize statistics tracking"""
        self.stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'validation_fixes': 0,
            'avg_confidence': 0.0,
            'avg_response_time': 0.0
        }
        self.dynamic_examples = []
        self.max_dynamic_examples = 100
    
    # =========================================================================
    # MAIN PROCESSING METHOD - REFACTORED
    # =========================================================================
    
    async def process_any_question(self, question: str, 
                                tenant_id: str = 'company-a',
                                user_id: str = 'default') -> Dict[str, Any]:
        """
        Main processing pipeline - clean and modular
        """
        start_time = time.time()
        context = QueryContext(question, tenant_id, user_id)
        
        try:
            # Step 1: Preparation (MUST BE FIRST - loads conversation context)
            await self._prepare_processing(context)
            await self._detect_intent(context)
            is_general, chat_type = self.general_chat.is_general_chat(question)
            if is_general:
                response = self.general_chat.get_response(chat_type, question)
                return {
                    'answer': response,
                    'success': True,
                    'intent': f'general_{chat_type}',
                    'entities': {},
                    'confidence': 1.0,
                    'processing_time': time.time() - start_time,
                    'tenant_id': tenant_id,
                    'user_id': user_id
                }
            # Check for continuation query (AFTER context is loaded)
            continuation_result = await self.handle_continuation_query(context)
            if continuation_result:
                return continuation_result
            
            # Step 2: Intent Detection
            await self._detect_intent(context)
            
            # Step 3: Check if clarification needed
            if await self._needs_clarification(context):
                return self._create_clarification_response(context)
            
            # Step 4: SQL Generation
            sql_query = await self._generate_sql(context)
            
            # Step 5: Query Execution
            results = await self._execute_query(sql_query)
            
            # Step 6: Data Processing
            processed_data = await self._process_results(results, context)
            
            # Step 7: Response Generation
            response = await self._generate_response(
                context, sql_query, processed_data
            )
            
            # Step 8: Finalization
            return self._finalize_response(
                response, context, start_time
            )
            
        except Exception as e:
            return self._handle_error(e, context, start_time)
    
    async def handle_continuation_query(self, context: QueryContext) -> Optional[Dict[str, Any]]:
        """
        Handle continuation/pagination queries
        Returns None if not a continuation query
        """
        # Get conversation context (already loaded in _prepare_processing)
        user_memory = self.conversation_memory.get_context(
            context.user_id, 
            context.question
        )
        
        # Check if this is a continuation query
        if not user_memory.get('is_continuation'):
            return None  # Not a continuation, proceed normally
        
        # Rest of continuation handling logic...
        # (code from the artifact I provided earlier)   

    # =========================================================================
    # STEP 1: PREPARATION
    # =========================================================================
    
    async def _prepare_processing(self, context: QueryContext):
        """Prepare for processing"""
        self.stats['total_queries'] += 1
        
        # Ensure Ollama connection
        await self.ollama_client.test_connection()
        
        # Get conversation context if enabled
        if self.enable_conversation_memory:
            conv_context = self.conversation_memory.get_context(
                context.user_id, context.question
            )
            context.previous_intent = conv_context.get('recent_intents', [None])[-1] if conv_context.get('recent_intents') else None
        
        logger.info(f"Processing: {context.question[:100]}...")
    
    # =========================================================================
    # STEP 2: INTENT DETECTION
    # =========================================================================
    
    async def _detect_intent(self, context: QueryContext):
        """Detect intent and extract entities"""
        detection_result = self.intent_detector.detect_intent_and_entities(
            context.question, 
            context.previous_intent
        )
        
        context.intent = detection_result.get('intent', 'unknown')
        context.entities = detection_result.get('entities', {})
        context.confidence = detection_result.get('confidence', 0.0)
        
        # ===== เพิ่ม detailed logging =====
        logger.info(f"Intent: {context.intent} (confidence: {context.confidence:.2f})")
        
        # Log entities ที่จับได้
        logger.info("="*60)
        logger.info("DETECTED ENTITIES:")
        for entity_type, values in context.entities.items():
            if values:  # แสดงเฉพาะที่มีค่า
                logger.info(f"  {entity_type}: {values}")
        
        # เน้นปัญหา customer detection
        if not context.entities.get('customers'):
            logger.warning("  ⚠️ No customers detected!")
            # ถ้ามีคำว่าคลีนิค แต่จับไม่ได้
            if 'คลีนิค' in context.question:
                logger.warning("  ⚠️ Found 'คลีนิค' in question but not extracted!")
        
        logger.info("="*60)
        
        # Update average confidence
        self._update_confidence_stats(context.confidence)
    
    # =========================================================================
    # STEP 3: CLARIFICATION CHECK
    # =========================================================================
    
    async def _needs_clarification(self, context: QueryContext) -> bool:
        """Check if clarification is needed"""
        if context.confidence >= 0.4:
            return False
            
        missing_info = self._identify_missing_info(context)
        context.missing_info = missing_info
        return bool(missing_info)
    
    def _identify_missing_info(self, context: QueryContext) -> List[str]:
        """Identify what information is missing"""
        missing = []
        question = context.question.lower()
        entities = context.entities
        
        # Check for missing temporal info
        if any(word in question for word in ['เดือน', 'ปี', 'วันที่']):
            if not any(entities.get(k) for k in ['years', 'months', 'dates']):
                missing.append('ระบุเดือน/ปี ที่ต้องการค้นหา')
        
        # Check for missing customer info
        if 'บริษัท' in question and not entities.get('customers'):
            missing.append('ชื่อบริษัทที่ต้องการค้นหา')
        
        # Check for missing product info
        if any(word in question for word in ['อะไหล่', 'เครื่อง', 'model']):
            if not entities.get('products'):
                missing.append('รหัสสินค้าหรือ model')
        
        return missing
    
    def _create_clarification_response(self, context: QueryContext) -> Dict:
        """Create clarification response"""
        clarification = self.prompt_manager.build_clarification_prompt(
            context.question, 
            context.missing_info
        )
        
        return {
            'answer': clarification,
            'success': True,
            'needs_clarification': True,
            'missing_info': context.missing_info,
            'confidence': context.confidence
        }
    
    # =========================================================================
    # STEP 4: SQL GENERATION
    # =========================================================================
    
    async def _generate_sql(self, context: QueryContext) -> str:
        """Generate and validate SQL query"""
        logger.info(f"🔍 _generate_sql entities: {context.entities}")
        # Build SQL prompt
        prompt = self.prompt_manager.build_sql_prompt(
            question=context.question,
            intent=context.intent,
            entities=context.entities
        )
        
        # Generate SQL
        sql = await self.ollama_client.generate(prompt, self.SQL_MODEL)
        sql = self._clean_sql_response(sql)
        
        # Validate and fix if enabled
        if self.enable_sql_validation:
            is_valid, fixed_sql, issues = self.sql_validator.validate_and_fix(sql)
            if issues:
                self.stats['validation_fixes'] += len(issues)
                logger.info(f"SQL fixes applied: {len(issues)}")
            sql = fixed_sql
        
        logger.info(f"Generated SQL:\n{sql}") 
        return sql
    
    # =========================================================================
    # STEP 5: QUERY EXECUTION
    # =========================================================================
    
    async def _execute_query(self, sql: str) -> List[Dict]:
        """Execute SQL query"""
        try:
            results = await self.db_handler.execute_query(sql)
            logger.info(f"Query returned {len(results)} results")
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    # =========================================================================
    # STEP 6: DATA PROCESSING
    # =========================================================================
    
    async def _process_results(self, results: List[Dict], 
                              context: QueryContext) -> Dict:
        """Process and clean results"""
        processed = {
            'results': results,
            'insights': {},
            'cleaning_stats': {}
        }
        
        if not results:
            return processed
        
        # Clean data if enabled
        if self.enable_data_cleaning:
            cleaned_results, cleaning_stats = self.data_cleaner.clean_results(results, context.intent)
            processed['results'] = cleaned_results  # ← ใช้ cleaned
        else:
            processed['results'] = results
        
        return processed
    
    # =========================================================================
    # STEP 7: RESPONSE GENERATION
    # =========================================================================
    
    async def _generate_response(self, context: QueryContext, 
                                sql_query: str,
                                processed_data: Dict) -> str:
        results = processed_data['results']
        
        if not results:
            return self._generate_no_results_response(context)
        
        # ใช้ Table Formatter แทน LLM
        return format_results_as_table_response(results, context.question)
    
    # =========================================================================
    # STEP 8: FINALIZATION
    # =========================================================================
    
    def _finalize_response(self, answer: str, context: QueryContext, 
                          start_time: float) -> Dict[str, Any]:
        """Finalize and return response"""
        processing_time = time.time() - start_time
        
        # Update stats
        self.stats['successful_queries'] += 1
        self._update_response_time_stats(processing_time)
        
        # Add to conversation memory if enabled
        if self.enable_conversation_memory:
            self.conversation_memory.add_conversation(
                context.user_id,
                context.question,
                {'intent': context.intent, 'success': True}
            )
        
        return {
            'answer': answer,
            'success': True,
            'intent': context.intent,
            'entities': context.entities,
            'confidence': context.confidence,
            'processing_time': processing_time,
            'tenant_id': context.tenant_id,
            'user_id': context.user_id
        }
    
    # =========================================================================
    # ERROR HANDLING
    # =========================================================================
    
    def _handle_error(self, error: Exception, context: QueryContext, 
                     start_time: float) -> Dict[str, Any]:
        """Handle processing errors"""
        self.stats['failed_queries'] += 1
        processing_time = time.time() - start_time
        
        logger.error(f"Processing failed: {error}")
        
        return {
            'answer': self._generate_error_response(str(error)),
            'success': False,
            'error': str(error),
            'processing_time': processing_time,
            'intent': context.intent,
            'confidence': context.confidence
        }
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def _clean_sql_response(self, sql: str) -> str:
        """Clean SQL response from model"""
        import re
        if not sql:
            return ""
        sql = re.sub(r'```sql?\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'```', '', sql)
        # Remove comments and explanations
        lines = []
        for line in sql.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            lines.append(line)
        sql = ' '.join(lines)
        if sql and not sql.rstrip().endswith(';'):
            sql += ';'
        return sql.strip()
    
    def _update_confidence_stats(self, confidence: float):
        """Update confidence statistics"""
        total = self.stats['total_queries']
        if total > 0:  # เพิ่มการ check
            self.stats['avg_confidence'] = (
                (self.stats['avg_confidence'] * (total - 1) + confidence) / total
            )
        else:
            self.stats['avg_confidence'] = confidence
    
    def _update_response_time_stats(self, response_time: float):
        """Update response time statistics"""
        total = self.stats['successful_queries']
        self.stats['avg_response_time'] = (
            (self.stats['avg_response_time'] * (total - 1) + response_time) / total
        )
    
    def _generate_no_results_response(self, context: QueryContext) -> str:
        """Generate response when no results found"""
        return f"ไม่พบข้อมูลที่ตรงกับคำถาม: {context.question}"
    
    def _generate_template_response(self, results: List[Dict], 
                                   context: QueryContext,
                                   insights: Dict) -> str:
        """Generate template-based response"""
        count = len(results)
        response = f"พบข้อมูล {count:,} รายการ"
        
        if insights.get('summary'):
            # Add key insights
            for key, value in list(insights['summary'].items())[:3]:
                response += f"\n• {key}: {value}"
        
        return response
    
    def _generate_error_response(self, error_msg: str) -> str:
        """Generate user-friendly error response"""
        if 'column' in error_msg.lower():
            return "ขออภัย เกิดข้อผิดพลาดในการเข้าถึงข้อมูล กรุณาลองใหม่"
        elif 'timeout' in error_msg.lower():
            return "ขออภัย การประมวลผลใช้เวลานานเกินไป กรุณาลองใหม่"
        else:
            return "ขออภัย เกิดข้อผิดพลาดในการประมวลผล กรุณาลองใหม่"
    
    # =========================================================================
    # PUBLIC METHODS
    # =========================================================================
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get system statistics"""
        return {
            'performance': self.stats,
            'features': {
                'conversation_memory': self.enable_conversation_memory,
                'parallel_processing': self.enable_parallel_processing,
                'data_cleaning': self.enable_data_cleaning,
                'sql_validation': self.enable_sql_validation
            },
            'models': {
                'sql_generation': getattr(self, 'SQL_MODEL', 'default'),
                'response_generation': getattr(self, 'NL_MODEL', 'default')
            }
        }
    
    # Model configurations
    SQL_MODEL = 'llama3.1:8b'
    NL_MODEL = 'llama3.2-vision:11b-instruct-q4_K_M'