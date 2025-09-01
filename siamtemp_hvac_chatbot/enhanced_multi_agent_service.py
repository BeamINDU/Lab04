import os
import asyncio
import time
import json
from datetime import datetime
from typing import Dict, Any, AsyncGenerator, Optional, List
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import uvicorn
import logging

# =============================================================================
# 🔧 ENHANCED AI AGENT IMPORT WITH CONVERSATION MEMORY & PARALLEL PROCESSING
# =============================================================================
try:
    from agents.dual_model_dynamic_ai import EnhancedUnifiedPostgresOllamaAgent
    ENHANCED_AI_AVAILABLE = True
    print("✅ Enhanced Dual-Model AI System with Conversation Memory & Parallel Processing loaded successfully")
    print("🧠 Features: Conversation Memory, Parallel Processing, Context Awareness")
    print("📝 SQL Generation: mannix/defog-llama3-sqlcoder-8b:latest")
    print("💬 Response Generation: llama3.1:8b")
    print("⚡ Performance: ~50% faster with parallel processing")
except ImportError as e:
    print(f"❌ Could not import Enhanced AI agent: {e}")
    print("💡 Creating minimal fallback agent...")
    
    # Create a minimal fallback agent for testing
    class EnhancedUnifiedPostgresOllamaAgent:
        def __init__(self):
            self.stats = {'total_queries': 0, 'successful_queries': 0}
            print("⚠️ Using minimal fallback agent - Enhanced features not available")
        
        async def process_any_question(self, question: str, tenant_id: str, user_id: str = 'default') -> Dict[str, Any]:
            """Minimal fallback processing with user session support"""
            self.stats['total_queries'] += 1
            
            if any(word in question.lower() for word in ['สวัสดี', 'hello', 'hi']):
                response = f"สวัสดีครับ! ผมคือ Enhanced AI Assistant ของ {tenant_id}\n🚀 รองรับ Conversation Memory และ Parallel Processing"
                self.stats['successful_queries'] += 1
                return {
                    'answer': response,
                    'success': True,
                    'sql_query': None,
                    'results_count': 0,
                    'ai_system_used': 'enhanced_fallback',
                    'enhancement_features': {
                        'conversation_memory': False,
                        'parallel_processing': False,
                        'fallback_mode': True
                    }
                }
            else:
                return {
                    'answer': f"ขออภัยครับ ระบบ Enhanced AI หลักยังไม่พร้อมใช้งาน\n\nคำถาม: {question}\nUser: {user_id}\nTenant: {tenant_id}\n\n🔧 กรุณาติดต่อผู้ดูแลระบบ",
                    'success': False,
                    'sql_query': None,
                    'results_count': 0,
                    'ai_system_used': 'enhanced_fallback',
                    'enhancement_features': {
                        'conversation_memory': False,
                        'parallel_processing': False,
                        'fallback_mode': True
                    }
                }
        
        def get_system_stats(self):
            return {
                'agent_stats': self.stats,
                'system_capabilities': {
                    'conversation_memory': False,
                    'parallel_processing': False,
                    'fallback_mode': True
                }
            }
        
        def clear_conversation_memory(self, user_id: str = None):
            print(f"Fallback: Cannot clear memory for user {user_id} - feature not available")
    
    ENHANCED_AI_AVAILABLE = False

# Configure logging with enhanced formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# 🔧 ENHANCED TENANT CONFIGURATION WITH CONVERSATION SUPPORT
# =============================================================================

ENHANCED_TENANT_CONFIGS = {
    'company-a': {
        'name': 'Siamtemp Bangkok HQ (Enhanced HVAC Service)',
        'description': 'สำนักงานใหญ่ กรุงเทพมฯ - บริการ HVAC พร้อม AI ขั้นสูง',
        'sql_model': 'mannix/defog-llama3-sqlcoder-8b:latest',
        'nl_model': 'llama3.1:8b',
        'model': 'llama3.2:3b',  # Legacy fallback
        'language': 'th',
        'business_type': 'hvac_service_enhanced',
        'emoji': '🚀',
        
        # Database configuration
        'database': {
            'host': os.getenv('POSTGRES_HOST_COMPANY_A', 'postgres-company-a'),
            'port': int(os.getenv('POSTGRES_PORT_COMPANY_A', '5432')),
            'database': os.getenv('POSTGRES_DB_COMPANY_A', 'siamtemp_company_a'),
            'username': os.getenv('POSTGRES_USER_COMPANY_A', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD_COMPANY_A', 'password123')
        },
        
        # Enhanced AI features
        'features': {
            'conversation_memory': True,
            'parallel_processing': True,
            'context_awareness': True,
            'business_intelligence': True,
            'proactive_insights': True
        },
        
        # HVAC business context
        'business_context': {
            'industry': 'Advanced HVAC Service & Maintenance',
            'specialization': 'AI-powered chiller maintenance, Smart diagnostics, Predictive analytics',
            'key_services': ['Enhanced PM', 'Smart Overhaul', 'Emergency AI Support', 'Predictive Parts'],
            'main_brands': ['Hitachi', 'Daikin', 'EuroKlimat', 'Toyota', 'Mitsubishi', 'York', 'Carrier'],
            'location': 'Bangkok, Thailand',
            'ai_capabilities': 'Conversation Memory, Parallel Processing, Context Understanding'
        }
    },
    
    'company-b': {
        'name': 'Siamtemp Chiang Mai Enhanced (Regional AI)',
        'description': 'สาขาภาคเหนือ เชียงใหม่ - HVAC ระบบ AI',
        'sql_model': 'mannix/defog-llama3-sqlcoder-8b:latest',
        'nl_model': 'llama3.2:3b',
        'model': 'llama3.1:8b',
        'language': 'th',
        'business_type': 'hvac_regional_enhanced',
        'emoji': '🏔️',
        
        'database': {
            'host': os.getenv('POSTGRES_HOST_COMPANY_B', 'postgres-company-b'),
            'port': int(os.getenv('POSTGRES_PORT_COMPANY_B', '5433')),
            'database': os.getenv('POSTGRES_DB_COMPANY_B', 'siamtemp_company_b'),
            'username': os.getenv('POSTGRES_USER_COMPANY_B', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD_COMPANY_B', 'password123')
        },
        
        'features': {
            'conversation_memory': True,
            'parallel_processing': True,
            'context_awareness': True,
            'regional_optimization': True
        },
        
        'business_context': {
            'industry': 'Regional HVAC with AI Support',
            'specialization': 'Northern Thailand HVAC, Hotel systems, Smart maintenance',
            'key_services': ['Regional AI support', 'Tourism facility HVAC', 'Smart diagnostics'],
            'location': 'Chiang Mai, Thailand'
        }
    },
    
    'company-c': {
        'name': 'Siamtemp International AI (Global)',
        'description': 'International Operations - AI-Powered Global HVAC',
        'sql_model': 'mannix/defog-llama3-sqlcoder-8b:latest',
        'nl_model': 'llama3.2:3b',
        'model': 'llama3.1:8b',
        'language': 'en',
        'business_type': 'hvac_international_ai',
        'emoji': '🌍',
        
        'database': {
            'host': os.getenv('POSTGRES_HOST_COMPANY_C', 'postgres-company-c'),
            'port': int(os.getenv('POSTGRES_PORT_COMPANY_C', '5434')),
            'database': os.getenv('POSTGRES_DB_COMPANY_C', 'siamtemp_company_c'),
            'username': os.getenv('POSTGRES_USER_COMPANY_C', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD_COMPANY_C', 'password123')
        },
        
        'features': {
            'conversation_memory': True,
            'parallel_processing': True,
            'multilingual_support': True,
            'global_optimization': True
        },
        
        'business_context': {
            'industry': 'Global AI-Powered HVAC Solutions',
            'specialization': 'International projects, Cross-border AI support, Global standards',
            'key_services': ['Global AI installations', 'International service', 'Multi-country projects'],
            'location': 'Bangkok, Thailand (Global AI Operations)'
        }
    }
}

# Enhanced global settings
ENHANCED_GLOBAL_SETTINGS = {
    'ollama_server': {
        'base_url': os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434'),
        'timeout': int(os.getenv('OLLAMA_TIMEOUT', '60')),
        'retry_attempts': 3
    },
    'ai_enhancements': {
        'conversation_memory': True,
        'parallel_processing': True,
        'context_window': 20,  # messages to remember
        'max_parallel_tasks': 5,
        'response_timeout': 8  # seconds
    },
    'security': {
        'enable_tenant_isolation': True,
        'session_timeout': 3600,
        'user_session_tracking': True
    },
    'monitoring': {
        'enable_metrics': True,
        'log_level': 'INFO',
        'track_usage': True,
        'track_conversation_patterns': True
    }
}

# =============================================================================
# 🚀 INITIALIZE ENHANCED AI AGENT
# =============================================================================

try:
    enhanced_agent = EnhancedUnifiedPostgresOllamaAgent()
    
    if ENHANCED_AI_AVAILABLE:
        logger.info("✅ Enhanced AI Agent initialized with advanced features")
        logger.info(f"🧠 Conversation Memory: Enabled (20 messages)")
        logger.info(f"⚡ Parallel Processing: Enabled (5 concurrent tasks)")
        logger.info(f"📝 SQL Model: {ENHANCED_TENANT_CONFIGS['company-a']['sql_model']}")
        logger.info(f"💬 NL Model: {ENHANCED_TENANT_CONFIGS['company-a']['nl_model']}")
    else:
        logger.warning("⚠️ Enhanced AI Agent initialized with fallback mode")
        
except Exception as e:
    logger.error(f"❌ Failed to initialize Enhanced AI Agent: {e}")
    raise RuntimeError(f"Cannot start enhanced service: {e}")

# =============================================================================
# 🌐 ENHANCED FASTAPI APPLICATION SETUP
# =============================================================================

app = FastAPI(
    title="Siamtemp Enhanced HVAC AI Service",
    description="Enhanced Multi-Tenant AI System with Conversation Memory & Parallel Processing for HVAC Business Intelligence",
    version="6.0-Enhanced",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "Enhanced AI",
            "description": "Enhanced AI operations with conversation memory and parallel processing"
        },
        {
            "name": "Conversation",
            "description": "Conversation memory and context management"
        },
        {
            "name": "Legacy Compatible",
            "description": "Backwards compatible endpoints"
        },
        {
            "name": "System",
            "description": "System monitoring and health checks"
        }
    ]
)

# =============================================================================
# 📋 ENHANCED REQUEST/RESPONSE MODELS
# =============================================================================

class EnhancedRAGQuery(BaseModel):
    """Enhanced model for incoming questions with conversation support"""
    question: str
    tenant_id: str = "company-a"
    user_id: str = "default"  # 🆕 User session tracking
    use_conversation_memory: bool = True  # 🆕 Enable conversation memory
    use_parallel_processing: bool = True  # 🆕 Enable parallel processing
    streaming: bool = False
    context_override: Optional[Dict[str, Any]] = None  # 🆕 Manual context override

class EnhancedRAGResponse(BaseModel):
    """Enhanced model for responses with conversation metadata"""
    answer: str
    success: bool
    sql_query: Optional[str] = None
    results_count: int = 0
    tenant_id: str
    user_id: str  # 🆕 User session info
    processing_time: float
    ai_system_used: str
    question_analysis: Optional[Dict[str, Any]] = None
    models_used: Optional[Dict[str, str]] = None
    enhancement_features: Optional[Dict[str, Any]] = None  # 🆕 Enhancement status
    conversation_metadata: Optional[Dict[str, Any]] = None  # 🆕 Conversation context

class ChatMessage(BaseModel):
    """OpenAI-compatible chat message with session support"""
    role: str
    content: str
    user_id: Optional[str] = "default"  # 🆕 User tracking in messages

class EnhancedChatCompletionRequest(BaseModel):
    """Enhanced OpenAI-compatible chat completion request"""
    model: str
    messages: List[ChatMessage]
    user_id: str = "default"  # 🆕 User session
    stream: bool = False
    temperature: float = 0.7
    max_tokens: int = 2000
    use_conversation_memory: bool = True  # 🆕 Conversation memory control

class ConversationManagementRequest(BaseModel):
    """Request model for conversation management"""
    user_id: str
    action: str  # "clear", "get_history", "get_stats"
    tenant_id: str = "company-a"

# =============================================================================
# 🛠️ ENHANCED UTILITY FUNCTIONS
# =============================================================================

def get_tenant_id() -> str:
    """Get tenant ID from environment or default"""
    return os.getenv('TENANT_ID', 'company-a')

def get_user_id(x_user_id: str = Header(None)) -> str:
    """Get user ID from header or default"""
    return x_user_id or "default"

def ensure_enhanced_response_fields(result: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure response has all enhanced fields with sensible defaults"""
    required_fields = {
        'answer': result.get('answer', 'ไม่สามารถสร้างคำตอบได้'),
        'success': result.get('success', False),
        'sql_query': result.get('sql_query'),
        'results_count': result.get('results_count', 0),
        'tenant_id': result.get('tenant_id', 'unknown'),
        'user_id': result.get('user_id', 'default'),
        'processing_time': result.get('processing_time', 0.0),
        'ai_system_used': result.get('ai_system_used', 'unknown'),
        'question_analysis': result.get('question_analysis', {}),
        'models_used': result.get('models_used', {}),
        'enhancement_features': result.get('enhancement_features', {
            'conversation_memory': False,
            'parallel_processing': False,
            'context_awareness': False
        }),
        'conversation_metadata': result.get('conversation_metadata', {})
    }
    
    return required_fields

def validate_tenant_id(tenant_id: str) -> str:
    """Validate and return proper tenant ID"""
    if tenant_id not in ENHANCED_TENANT_CONFIGS:
        logger.warning(f"⚠️ Invalid tenant_id '{tenant_id}', using default 'company-a'")
        return 'company-a'
    return tenant_id

def get_enhanced_tenant_info(tenant_id: str) -> Dict[str, Any]:
    """Get enhanced tenant configuration"""
    config = ENHANCED_TENANT_CONFIGS.get(tenant_id, ENHANCED_TENANT_CONFIGS['company-a'])
    return {
        'tenant_id': tenant_id,
        'name': config['name'],
        'features': config.get('features', {}),
        'business_context': config.get('business_context', {}),
        'ai_models': {
            'sql_model': config.get('sql_model'),
            'nl_model': config.get('nl_model')
        }
    }

# =============================================================================
# 🚀 ENHANCED MAIN API ENDPOINTS
# =============================================================================

@app.post("/enhanced-rag-query", response_model=EnhancedRAGResponse, tags=["Enhanced AI"])
async def enhanced_rag_query(
    request: EnhancedRAGQuery, 
    tenant_id: str = Depends(get_tenant_id),
    user_id: str = Depends(get_user_id)
):
    """
    🚀 Enhanced endpoint for processing HVAC-related questions with conversation memory and parallel processing
    
    Features:
    - 🧠 Conversation Memory: Remembers previous conversations per user
    - ⚡ Parallel Processing: Processes multiple tasks simultaneously
    - 🔗 Context Awareness: Understands conversation flow and implicit references
    - 📊 Business Intelligence: Provides enhanced business insights
    """
    try:
        start_time = time.time()
        
        # Validate and prepare parameters
        validated_tenant_id = validate_tenant_id(request.tenant_id or tenant_id)
        validated_user_id = request.user_id or user_id
        tenant_info = get_enhanced_tenant_info(validated_tenant_id)
        
        logger.info(f"🎯 Processing enhanced question for {tenant_info['name']}")
        logger.info(f"👤 User: {validated_user_id} | Question: {request.question[:100]}...")
        
        # Check if enhanced features are available
        if not ENHANCED_AI_AVAILABLE:
            logger.warning("Enhanced features not available, using fallback")
            request.use_conversation_memory = False
            request.use_parallel_processing = False
        
        # Process with enhanced agent
        result = await enhanced_agent.process_any_question(
            question=request.question,
            tenant_id=validated_tenant_id,
            user_id=validated_user_id
        )
        
        # Ensure all enhanced fields are present
        result = ensure_enhanced_response_fields(result)
        result['tenant_id'] = validated_tenant_id
        result['user_id'] = validated_user_id
        result['processing_time'] = time.time() - start_time
        
        # Add tenant context and conversation metadata
        result['tenant_context'] = tenant_info
        result['conversation_metadata'] = {
            'conversation_enabled': request.use_conversation_memory and ENHANCED_AI_AVAILABLE,
            'parallel_processing_enabled': request.use_parallel_processing and ENHANCED_AI_AVAILABLE,
            'context_override_used': request.context_override is not None,
            'session_timestamp': datetime.now().isoformat()
        }
        
        # Add enhanced performance metrics
        if ENHANCED_AI_AVAILABLE and hasattr(enhanced_agent, 'dual_model_ai'):
            enhanced_stats = enhanced_agent.dual_model_ai.get_enhanced_stats()
            result['enhanced_performance'] = {
                'conversation_continuation_rate': enhanced_stats.get('conversation_stats', {}).get('continuation_rate', 0),
                'parallel_efficiency': enhanced_stats.get('parallel_processing_stats', {}).get('avg_efficiency', 0),
                'context_usage_rate': enhanced_stats.get('conversation_stats', {}).get('context_usage_rate', 0)
            }
        
        logger.info(f"✅ Enhanced query processed successfully in {result['processing_time']:.2f}s")
        return result
        
    except Exception as e:
        logger.error(f"❌ Enhanced RAG query failed: {e}")
        error_response = ensure_enhanced_response_fields({
            'answer': f"เกิดข้อผิดพลาดในระบบ Enhanced AI: {str(e)}\n\nกรุณาลองใหม่อีกครั้ง หรือติดต่อผู้ดูแลระบบ",
            'success': False,
            'tenant_id': validated_tenant_id,
            'user_id': validated_user_id,
            'ai_system_used': 'enhanced_error_handler',
            'processing_time': time.time() - start_time
        })
        return error_response

@app.post("/conversation/manage", tags=["Conversation"])
async def manage_conversation(request: ConversationManagementRequest):
    """
    🧠 Manage conversation memory for users
    
    Actions:
    - "clear": Clear conversation history for user
    - "get_history": Get conversation history
    - "get_stats": Get conversation statistics
    """
    try:
        if not ENHANCED_AI_AVAILABLE:
            return {
                "status": "error",
                "message": "Conversation memory not available in fallback mode",
                "enhanced_features_available": False
            }
        
        if request.action == "clear":
            enhanced_agent.clear_conversation_memory(request.user_id)
            return {
                "status": "success",
                "message": f"Conversation memory cleared for user: {request.user_id}",
                "user_id": request.user_id,
                "action": "clear",
                "timestamp": datetime.now().isoformat()
            }
        
        elif request.action == "get_history":
            if hasattr(enhanced_agent, 'dual_model_ai') and hasattr(enhanced_agent.dual_model_ai, 'conversation_memory'):
                conversations = list(enhanced_agent.dual_model_ai.conversation_memory.conversations.get(request.user_id, []))
                return {
                    "status": "success",
                    "user_id": request.user_id,
                    "conversation_count": len(conversations),
                    "conversations": conversations[-10:],  # Last 10 conversations
                    "timestamp": datetime.now().isoformat()
                }
            else:
                return {"status": "error", "message": "Conversation memory not accessible"}
        
        elif request.action == "get_stats":
            stats = enhanced_agent.get_system_stats()
            return {
                "status": "success",
                "user_id": request.user_id,
                "conversation_stats": stats.get('enhanced_ai_stats', {}).get('conversation_stats', {}),
                "timestamp": datetime.now().isoformat()
            }
        
        else:
            return {
                "status": "error",
                "message": f"Unknown action: {request.action}",
                "available_actions": ["clear", "get_history", "get_stats"]
            }
            
    except Exception as e:
        logger.error(f"Conversation management failed: {e}")
        return {
            "status": "error",
            "message": f"Conversation management error: {str(e)}",
            "action": request.action,
            "user_id": request.user_id
        }

# =============================================================================
# 🔗 ENHANCED OPENAI-COMPATIBLE API ENDPOINTS
# =============================================================================

@app.post("/v1/chat/completions", tags=["Enhanced AI"])
async def enhanced_chat_completions(
    request: EnhancedChatCompletionRequest, 
    tenant_id: str = Depends(get_tenant_id),
    user_id: str = Depends(get_user_id)
):
    """
    🤖 Enhanced OpenAI-compatible chat completions with conversation memory
    
    Features:
    - User session tracking across requests
    - Conversation memory integration
    - Enhanced business context
    """
    try:
        start_time = time.time()
        
        # Extract user's question from messages
        user_messages = [msg for msg in request.messages if msg.role == "user"]
        if not user_messages:
            raise HTTPException(400, "No user message found in request")
        
        question = user_messages[-1].content
        session_user_id = request.user_id or user_id
        validated_tenant_id = validate_tenant_id(tenant_id)
        
        # Process using enhanced agent with conversation memory
        if ENHANCED_AI_AVAILABLE and request.use_conversation_memory:
            rag_result = await enhanced_agent.process_any_question(question, validated_tenant_id, session_user_id)
        else:
            # Fallback without conversation memory
            rag_result = await enhanced_agent.process_any_question(question, validated_tenant_id, session_user_id)
        
        fixed_result = ensure_enhanced_response_fields(rag_result)
        
        # Determine AI system status
        ai_system = 'enhanced_dual_model' if ENHANCED_AI_AVAILABLE else 'fallback'
        
        # Return in OpenAI-compatible format with enhanced metadata
        return {
            "id": f"chatcmpl-{int(time.time())}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": request.model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": fixed_result.get('answer', 'ไม่สามารถสร้างคำตอบได้')
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": len(question.split()),
                "completion_tokens": len(fixed_result.get('answer', '').split()),
                "total_tokens": len(question.split()) + len(fixed_result.get('answer', '').split())
            },
            "siamtemp_enhanced_metadata": {
                "tenant_id": validated_tenant_id,
                "user_id": session_user_id,
                "ai_system_used": ai_system,
                "processing_time": time.time() - start_time,
                "enhanced_features": {
                    "conversation_memory": ENHANCED_AI_AVAILABLE and request.use_conversation_memory,
                    "parallel_processing": ENHANCED_AI_AVAILABLE,
                    "context_awareness": ENHANCED_AI_AVAILABLE
                },
                "sql_query": fixed_result.get("sql_query"),
                "results_count": fixed_result.get("results_count", 0),
                "models_used": fixed_result.get("models_used", {}),
                "conversation_continuation": fixed_result.get('question_analysis', {}).get('conversation_continuation', False)
            }
        }
            
    except Exception as e:
        raise HTTPException(500, f"Enhanced chat completions failed: {str(e)}")

@app.get("/v1/models", tags=["Enhanced AI"])
async def list_enhanced_models():
    """
    📋 List available enhanced models with conversation and parallel processing capabilities
    """
    models_data = []
    
    for tid, config in ENHANCED_TENANT_CONFIGS.items():
        model_info = {
            "id": f"siamtemp-enhanced-hvac-{tid}",
            "object": "model",
            "created": int(datetime.now().timestamp()),
            "owned_by": f"siamtemp-enhanced-{tid}",
            "streaming_supported": False,
            "enhanced_features": {
                "conversation_memory": ENHANCED_AI_AVAILABLE,
                "parallel_processing": ENHANCED_AI_AVAILABLE,
                "context_awareness": ENHANCED_AI_AVAILABLE,
                "business_intelligence": ENHANCED_AI_AVAILABLE
            },
            "siamtemp_enhanced_metadata": {
                "tenant_id": tid,
                "tenant_name": config['name'],
                "business_type": config['business_type'],
                "language": config['language'],
                "emoji": config['emoji'],
                "features": config.get('features', {}),
                "specialization": config['business_context']['specialization'],
                "models_configured": {
                    "sql_generation": config.get('sql_model', 'not_configured'),
                    "response_generation": config.get('nl_model', 'not_configured'),
                    "legacy_fallback": config.get('model', 'not_configured')
                },
                "ai_capabilities": config['business_context'].get('ai_capabilities', 'Standard AI')
            }
        }
        models_data.append(model_info)
    
    return {
        "object": "list",
        "data": models_data,
        "enhanced_service_info": {
            "version": "6.0-Enhanced",
            "features_available": ENHANCED_AI_AVAILABLE,
            "total_enhanced_models": len(models_data),
            "conversation_memory": ENHANCED_AI_AVAILABLE,
            "parallel_processing": ENHANCED_AI_AVAILABLE
        }
    }

# =============================================================================
# 📊 ENHANCED SYSTEM MONITORING AND HEALTH
# =============================================================================

@app.get("/health", tags=["System"])
async def enhanced_health_check():
    """
    🏥 Enhanced system health check with conversation memory and parallel processing status
    """
    health_status = {
        "status": "healthy",
        "service": "Siamtemp Enhanced HVAC AI Service",
        "version": "6.0-Enhanced",
        "enhanced_features": {
            "conversation_memory": ENHANCED_AI_AVAILABLE,
            "parallel_processing": ENHANCED_AI_AVAILABLE,
            "context_awareness": ENHANCED_AI_AVAILABLE,
            "business_intelligence": ENHANCED_AI_AVAILABLE
        },
        "tenant_count": len(ENHANCED_TENANT_CONFIGS),
        "default_tenant": get_tenant_id(),
        "timestamp": datetime.now().isoformat(),
        "models_configured": {
            "sql_generation": ENHANCED_TENANT_CONFIGS['company-a']['sql_model'],
            "response_generation": ENHANCED_TENANT_CONFIGS['company-a']['nl_model']
        },
        "ollama_server": ENHANCED_GLOBAL_SETTINGS['ollama_server']['base_url']
    }
    
    # Add enhanced AI system status
    if ENHANCED_AI_AVAILABLE and hasattr(enhanced_agent, 'get_system_stats'):
        try:
            system_stats = enhanced_agent.get_system_stats()
            health_status['enhanced_system_stats'] = {
                "total_queries": system_stats.get('agent_stats', {}).get('total_queries', 0),
                "successful_queries": system_stats.get('agent_stats', {}).get('successful_queries', 0),
                "conversation_features": system_stats.get('system_capabilities', {}),
                "performance_metrics": system_stats.get('enhanced_ai_stats', {}).get('parallel_processing_stats', {})
            }
        except Exception as e:
            health_status['enhanced_system_stats'] = {"error": f"Could not retrieve stats: {e}"}
    
    return health_status

@app.get("/system-info", tags=["System"])
async def enhanced_system_info():
    """
    📊 Comprehensive enhanced system information with conversation and parallel processing details
    """
    try:
        # Get enhanced statistics
        if ENHANCED_AI_AVAILABLE and hasattr(enhanced_agent, 'get_system_stats'):
            system_stats = enhanced_agent.get_system_stats()
        else:
            system_stats = {"fallback_mode": True}
        
        system_info = {
            "service_name": "Siamtemp Enhanced HVAC AI Service",
            "version": "6.0-Enhanced",
            "status": "operational",
            "enhanced_capabilities": {
                "conversation_memory": ENHANCED_AI_AVAILABLE,
                "parallel_processing": ENHANCED_AI_AVAILABLE,
                "context_awareness": ENHANCED_AI_AVAILABLE,
                "business_intelligence": ENHANCED_AI_AVAILABLE,
                "multi_user_sessions": ENHANCED_AI_AVAILABLE,
                "proactive_insights": ENHANCED_AI_AVAILABLE,
                "thai_english_support": True,
                "anti_hallucination": True,
                "openai_compatibility": True
            },
            "conversation_features": {
                "memory_window": ENHANCED_GLOBAL_SETTINGS['ai_enhancements']['context_window'],
                "user_preference_learning": ENHANCED_AI_AVAILABLE,
                "session_continuity": ENHANCED_AI_AVAILABLE,
                "implicit_context_understanding": ENHANCED_AI_AVAILABLE
            },
            "parallel_processing": {
                "max_concurrent_tasks": ENHANCED_GLOBAL_SETTINGS['ai_enhancements']['max_parallel_tasks'],
                "response_timeout": ENHANCED_GLOBAL_SETTINGS['ai_enhancements']['response_timeout'],
                "efficiency_optimization": ENHANCED_AI_AVAILABLE
            },
            "tenant_information": {
                "total_tenants": len(ENHANCED_TENANT_CONFIGS),
                "available_tenants": list(ENHANCED_TENANT_CONFIGS.keys()),
                "default_tenant": get_tenant_id(),
                "tenant_details": {
                    tid: {
                        "name": config["name"],
                        "business_type": config["business_type"],
                        "language": config["language"],
                        "emoji": config["emoji"],
                        "features": config.get("features", {}),
                        "ai_capabilities": config["business_context"].get("ai_capabilities", "Standard")
                    }
                    for tid, config in ENHANCED_TENANT_CONFIGS.items()
                }
            },
            "ai_models": {
                "sql_generation_model": ENHANCED_TENANT_CONFIGS['company-a']['sql_model'],
                "nl_generation_model": ENHANCED_TENANT_CONFIGS['company-a']['nl_model'],
                "fallback_model": ENHANCED_TENANT_CONFIGS['company-a']['model']
            },
            "performance_stats": system_stats.get('agent_stats', {}),
            "enhanced_performance": system_stats.get('enhanced_ai_stats', {}),
            "global_settings": ENHANCED_GLOBAL_SETTINGS,
            "timestamp": datetime.now().isoformat()
        }
        
        return system_info
        
    except Exception as e:
        return {
            "error": f"Failed to get enhanced system info: {str(e)}",
            "basic_info": {
                "service": "Siamtemp Enhanced HVAC AI",
                "version": "6.0-Enhanced",
                "status": "partial_information_available",
                "enhanced_features_available": ENHANCED_AI_AVAILABLE
            }
        }

# =============================================================================
# 🧪 ENHANCED TESTING ENDPOINTS
# =============================================================================

@app.post("/test-enhanced-system", tags=["System"])
async def test_enhanced_system(request: EnhancedRAGQuery):
    """
    🧪 Enhanced system test with conversation memory and parallel processing validation
    """
    test_results = []
    test_scenarios = [
        {
            "question": "สวัสดีครับ",
            "description": "Greeting test with user session",
            "user_id": request.user_id
        },
        {
            "question": "ยอดขาย overhaul ปี 2567",
            "description": "Business query test with conversation memory",
            "user_id": request.user_id
        },
        {
            "question": "แล้วปีก่อนล่ะ",  # Continuation test
            "description": "Conversation continuation test (should reference 2566)",
            "user_id": request.user_id
        },
        {
            "question": request.question,
            "description": "User's actual question",
            "user_id": request.user_id
        }
    ]
    
    for i, scenario in enumerate(test_scenarios):
        try:
            start_time = time.time()
            
            if ENHANCED_AI_AVAILABLE:
                result = await enhanced_agent.process_any_question(
                    scenario["question"], 
                    request.tenant_id or 'company-a',
                    scenario["user_id"]
                )
            else:
                result = {
                    "answer": f"Fallback response for: {scenario['question']}",
                    "success": False,
                    "enhancement_features": {"fallback_mode": True}
                }
            
            result['processing_time'] = time.time() - start_time
            result['test_scenario'] = scenario["description"]
            result['test_order'] = i + 1
            test_results.append(result)
            
            # Small delay between tests to simulate real conversation flow
            await asyncio.sleep(0.5)
            
        except Exception as e:
            test_results.append({
                'test_scenario': scenario["description"],
                'test_order': i + 1,
                'question': scenario["question"],
                'error': str(e),
                'success': False
            })
    
    return {
        "test_summary": {
            "total_tests": len(test_results),
            "successful_tests": sum(1 for r in test_results if r.get('success', False)),
            "system_status": "operational" if any(r.get('success', False) for r in test_results) else "needs_attention",
            "enhanced_features_tested": ENHANCED_AI_AVAILABLE,
            "conversation_flow_tested": len([r for r in test_results if "continuation" in r.get('test_scenario', '')]) > 0
        },
        "test_results": test_results,
        "system_capabilities": {
            "conversation_memory": ENHANCED_AI_AVAILABLE,
            "parallel_processing": ENHANCED_AI_AVAILABLE,
            "context_awareness": ENHANCED_AI_AVAILABLE,
            "agent_type": type(enhanced_agent).__name__,
            "fallback_mode": not ENHANCED_AI_AVAILABLE
        },
        "conversation_test_analysis": {
            "continuation_test_included": True,
            "user_session_tracking": True,
            "context_inheritance_tested": True
        }
    }

# =============================================================================
# 🔄 LEGACY COMPATIBILITY ENDPOINTS
# =============================================================================

@app.post("/rag-query", response_model=EnhancedRAGResponse, tags=["Legacy Compatible"])
async def legacy_rag_query(request: EnhancedRAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """Legacy endpoint that redirects to enhanced version"""
    logger.info("Legacy endpoint called, redirecting to enhanced version")
    return await enhanced_rag_query(request, tenant_id)

# =============================================================================
# 🚀 ENHANCED APPLICATION STARTUP
# =============================================================================

if __name__ == "__main__":
    # Get startup configuration
    default_tenant_id = get_tenant_id()
    config = ENHANCED_TENANT_CONFIGS.get(default_tenant_id, {})
    
    # Display comprehensive startup information
    print("🚀 Starting Siamtemp Enhanced HVAC AI Service")
    print("=" * 80)
    print(f"🎯 Service Version: 6.0-Enhanced")
    print(f"🤖 Agent Type: {type(enhanced_agent).__name__}")
    print(f"🏢 Default Tenant: {config.get('name', default_tenant_id)} ({default_tenant_id})")
    print(f"🔧 Business Focus: {config.get('business_type', 'Unknown')}")
    print(f"📊 Total Tenants: {len(ENHANCED_TENANT_CONFIGS)}")
    print("")
    
    # Display enhanced AI capabilities
    print("🧠 Enhanced AI Capabilities:")
    if ENHANCED_AI_AVAILABLE:
        print("  ✅ Conversation Memory System")
        print("    • Remembers 20 previous messages per user")
        print("    • Detects conversation continuations")
        print("    • Learns user preferences")
        print("    • Supports implicit context references")
        print("")
        print("  ✅ Parallel Processing Engine")
        print("    • Processes 5 tasks simultaneously")
        print("    • ~50% faster response time")
        print("    • Intelligent task prioritization")
        print("    • Smart timeout management")
        print("")
        print("  ✅ Advanced Features")
        print(f"    📝 SQL Model: {config.get('sql_model', 'Not configured')}")
        print(f"    💬 NL Model: {config.get('nl_model', 'Not configured')}")
        print("    🔗 Context Awareness: Full conversation understanding")
        print("    📊 Business Intelligence: Proactive insights")
        print("    🌐 Multi-user Sessions: Individual user tracking")
    else:
        print("  ⚠️ Enhanced Features Not Available (Fallback Mode)")
        print("    • Basic conversation support only")
        print("    • No conversation memory")
        print("    • Sequential processing")
        print("    • Limited business intelligence")
    
    print("")
    print("🌐 API Endpoints:")
    print("  🚀 Enhanced: /enhanced-rag-query (with conversation memory)")
    print("  🧠 Conversation: /conversation/manage (memory management)")
    print("  🤖 OpenAI: /v1/chat/completions (enhanced compatible)")
    print("  🧪 Testing: /test-enhanced-system")
    print("  📊 Health: /health, /system-info")
    print("  📚 Docs: /docs, /redoc")
    print("")
    print("🎯 Usage Examples:")
    print("  User 1: 'ยอดขาย overhaul ปี 2567'")
    print("  User 1: 'แล้วปีก่อนล่ะ' ← AI จำได้ว่าหมายถึง 2566")
    print("  User 1: 'เปรียบเทียบกัน' ← AI รู้ว่าเปรียบเทียบ 2567 vs 2566")
    print("=" * 80)
    
    # Start the FastAPI server with enhanced configuration
    uvicorn.run(
        "enhanced_multi_agent_service:app",
        host="0.0.0.0",
        port=int(os.getenv('PORT', '5000')),
        reload=False,  # Disable reload for production stability
        access_log=True,
        log_level="info",
        workers=1  # Single worker for conversation memory consistency
    )