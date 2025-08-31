import os
import asyncio
import time
import json
from datetime import datetime
from typing import Dict, Any, AsyncGenerator, Optional, List
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import uvicorn
import logging

# =============================================================================
# 🔧 SIMPLIFIED AI AGENT IMPORT
# =============================================================================
try:
    from agents.dual_model_dynamic_ai import EnhancedUnifiedPostgresOllamaAgent
    DUAL_MODEL_AVAILABLE = True
    DYNAMIC_AI_AVAILABLE = True
    print("✅ Dual-Model Dynamic AI System loaded successfully")
    print("📝 SQL Generation: mannix/defog-llama3-sqlcoder-8b:latest")
    print("💬 Response Generation: llama3.2:3b")
except ImportError as e:
    print(f"❌ Could not import AI agent: {e}")
    print("💡 Creating minimal fallback agent...")
    
    # Create a minimal fallback agent for testing
    class EnhancedUnifiedPostgresOllamaAgent:
        def __init__(self):
            self.stats = {'total_queries': 0, 'successful_queries': 0}
            print("⚠️ Using minimal fallback agent")
        
        async def process_any_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
            """Minimal fallback processing"""
            self.stats['total_queries'] += 1
            
            # Simple response for basic functionality
            if any(word in question.lower() for word in ['สวัสดี', 'hello', 'hi']):
                response = f"สวัสดีครับ! ผมคือ AI Assistant ของ {tenant_id}"
                self.stats['successful_queries'] += 1
                return {
                    'answer': response,
                    'success': True,
                    'sql_query': None,
                    'results_count': 0,
                    'ai_system_used': 'minimal_fallback'
                }
            else:
                return {
                    'answer': f"ขออภัยครับ ระบบ AI หลักยังไม่พร้อมใช้งาน กรุณาติดต่อผู้ดูแลระบบ\n\nคำถาม: {question}",
                    'success': False,
                    'sql_query': None,
                    'results_count': 0,
                    'ai_system_used': 'minimal_fallback'
                }
    
    DUAL_MODEL_AVAILABLE = False
    DYNAMIC_AI_AVAILABLE = False

# Configure logging with clear formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# 🔧 SIMPLIFIED TENANT CONFIGURATION (NO EXTERNAL FILES)
# =============================================================================

# All tenant configurations are now embedded directly in the code
# This eliminates the dependency on tenant_config.yaml and makes the system self-contained
TENANT_CONFIGS = {
    'company-a': {
        'name': 'Siamtemp Bangkok HQ (HVAC Service)',
        'description': 'สำนักงานใหญ่ กรุงเทพมฯ - บริการซ่อมบำรุงและอะไหล่ HVAC',
        'sql_model': 'mannix/defog-llama3-sqlcoder-8b:latest',  # Specialized SQL generation
        'nl_model': 'llama3.1:8b',                              # Natural language responses
        'model': 'llama3.2:3b',  # Legacy fallback model
        'language': 'th',
        'business_type': 'hvac_service_spare_parts',
        'emoji': '🔧',
        
        # Database configuration from environment variables
        'database': {
            'host': os.getenv('POSTGRES_HOST_COMPANY_A', 'postgres-company-a'),
            'port': int(os.getenv('POSTGRES_PORT_COMPANY_A', '5432')),
            'database': os.getenv('POSTGRES_DB_COMPANY_A', 'siamtemp_company_a'),
            'username': os.getenv('POSTGRES_USER_COMPANY_A', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD_COMPANY_A', 'password123')
        },
        
        # HVAC business context for better understanding
        'business_context': {
            'industry': 'HVAC Service & Maintenance',
            'specialization': 'Chiller maintenance, Air conditioning repair, Spare parts supply',
            'key_services': ['PM (Preventive Maintenance)', 'Overhaul', 'Emergency Repair', 'Parts Replacement'],
            'main_brands': ['Hitachi', 'Daikin', 'EuroKlimat', 'Toyota', 'Mitsubishi', 'York', 'Carrier'],
            'location': 'Bangkok, Thailand'
        }
    },
    
    'company-b': {
        'name': 'Siamtemp Chiang Mai Regional (HVAC)',
        'description': 'สาขาภาคเหนือ เชียงใหม่ - บริการ HVAC ภูมิภาค',
        'sql_model': 'mannix/defog-llama3-sqlcoder-8b:latest',
        'nl_model': 'llama3.2:3b',
        'model': 'llama3.1:8b',
        'language': 'th',
        'business_type': 'hvac_regional_service',
        'emoji': '🏔️',
        
        'database': {
            'host': os.getenv('POSTGRES_HOST_COMPANY_B', 'postgres-company-b'),
            'port': int(os.getenv('POSTGRES_PORT_COMPANY_B', '5433')),
            'database': os.getenv('POSTGRES_DB_COMPANY_B', 'siamtemp_company_b'),
            'username': os.getenv('POSTGRES_USER_COMPANY_B', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD_COMPANY_B', 'password123')
        },
        
        'business_context': {
            'industry': 'Regional HVAC Services',
            'specialization': 'Northern Thailand HVAC systems, Hotel air conditioning',
            'key_services': ['Regional maintenance', 'Tourism facility HVAC', 'Local business support'],
            'location': 'Chiang Mai, Thailand'
        }
    },
    
    'company-c': {
        'name': 'Siamtemp International (HVAC)',
        'description': 'International Operations - Global HVAC Solutions',
        'sql_model': 'mannix/defog-llama3-sqlcoder-8b:latest',
        'nl_model': 'llama3.2:3b',
        'model': 'llama3.1:8b',
        'language': 'en',
        'business_type': 'hvac_international',
        'emoji': '🌏',
        
        'database': {
            'host': os.getenv('POSTGRES_HOST_COMPANY_C', 'postgres-company-c'),
            'port': int(os.getenv('POSTGRES_PORT_COMPANY_C', '5434')),
            'database': os.getenv('POSTGRES_DB_COMPANY_C', 'siamtemp_company_c'),
            'username': os.getenv('POSTGRES_USER_COMPANY_C', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD_COMPANY_C', 'password123')
        },
        
        'business_context': {
            'industry': 'International HVAC Solutions',
            'specialization': 'Global projects, Cross-border maintenance, International standards',
            'key_services': ['International installations', 'Global service contracts', 'Multi-country projects'],
            'location': 'Bangkok, Thailand (Global Operations)'
        }
    }
}

# Global settings that apply to all tenants
GLOBAL_SETTINGS = {
    'ollama_server': {
        'base_url': os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434'),
        'timeout': int(os.getenv('OLLAMA_TIMEOUT', '60')),
        'retry_attempts': 3
    },
    'security': {
        'enable_tenant_isolation': True,
        'session_timeout': 3600  # 1 hour
    },
    'monitoring': {
        'enable_metrics': True,
        'log_level': 'INFO',
        'track_usage': True
    }
}

# =============================================================================
# 🚀 INITIALIZE AI AGENT WITH ERROR HANDLING
# =============================================================================

try:
    # Initialize the main AI agent with proper error handling
    enhanced_agent = EnhancedUnifiedPostgresOllamaAgent()
    
    # Log the successful initialization with system details
    if DUAL_MODEL_AVAILABLE:
        logger.info("✅ Enhanced Agent initialized with Dual-Model support")
        logger.info(f"📝 SQL Model: {TENANT_CONFIGS['company-a']['sql_model']}")
        logger.info(f"💬 NL Model: {TENANT_CONFIGS['company-a']['nl_model']}")
    elif DYNAMIC_AI_AVAILABLE:
        logger.info("✅ Enhanced Agent initialized with Dynamic AI")
    else:
        logger.info("⚠️ Enhanced Agent initialized with minimal fallback")
        
except Exception as e:
    logger.error(f"❌ Failed to initialize Enhanced Agent: {e}")
    raise RuntimeError(f"Cannot start service: {e}")

# =============================================================================
# 🌐 FASTAPI APPLICATION SETUP
# =============================================================================

app = FastAPI(
    title="Siamtemp Simplified HVAC AI Service",
    description="Simplified Multi-Tenant RAG System for HVAC Service & Maintenance",
    version="5.0-Simplified",
    docs_url="/docs",  # Enable automatic API documentation
    redoc_url="/redoc"  # Alternative documentation interface
)

# =============================================================================
# 📋 REQUEST/RESPONSE MODELS
# =============================================================================

class RAGQuery(BaseModel):
    """Model for incoming questions"""
    question: str
    tenant_id: str = "company-a"  # Default to Bangkok HQ
    use_dual_model: bool = True
    use_dynamic_ai: bool = True
    streaming: bool = False

class RAGResponse(BaseModel):
    """Model for responses"""
    answer: str
    success: bool
    sql_query: Optional[str] = None
    results_count: int = 0
    tenant_id: str
    processing_time: float
    ai_system_used: str
    question_analysis: Optional[Dict[str, Any]] = None
    models_used: Optional[Dict[str, str]] = None

class ChatMessage(BaseModel):
    """OpenAI-compatible chat message"""
    role: str  # 'user', 'assistant', or 'system'
    content: str

class ChatCompletionRequest(BaseModel):
    """OpenAI-compatible chat completion request"""
    model: str
    messages: List[ChatMessage]
    stream: bool = False
    temperature: float = 0.7
    max_tokens: int = 2000

# =============================================================================
# 🛠️ UTILITY FUNCTIONS
# =============================================================================

def get_tenant_id() -> str:
    """Get tenant ID from environment or default to company-a"""
    return os.getenv('TENANT_ID', 'company-a')

def ensure_required_fields(result: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure response has all required fields with sensible defaults"""
    required_fields = {
        'answer': result.get('answer', 'ไม่สามารถสร้างคำตอบได้'),
        'success': result.get('success', False),
        'sql_query': result.get('sql_query'),
        'results_count': result.get('results_count', 0),
        'tenant_id': result.get('tenant_id', 'unknown'),
        'processing_time': result.get('processing_time', 0.0),
        'ai_system_used': result.get('ai_system_used', 'unknown'),
        'question_analysis': result.get('question_analysis', {}),
        'models_used': result.get('models_used', {})
    }
    
    return required_fields

def validate_tenant_id(tenant_id: str) -> str:
    """Validate and return a proper tenant ID"""
    if tenant_id not in TENANT_CONFIGS:
        logger.warning(f"⚠️ Invalid tenant_id '{tenant_id}', using default 'company-a'")
        return 'company-a'
    return tenant_id

# =============================================================================
# 🚀 MAIN API ENDPOINTS
# =============================================================================

@app.post("/enhanced-rag-query", response_model=RAGResponse)
async def enhanced_rag_query(request: RAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """
    Main endpoint for processing HVAC-related questions
    
    This endpoint handles all types of HVAC business questions including:
    - Service history and maintenance records
    - Spare parts inventory and pricing
    - Customer information and job tracking
    - Financial analysis and reporting
    """
    try:
        start_time = time.time()
        
        # Validate and clean the tenant ID
        validated_tenant_id = validate_tenant_id(request.tenant_id or tenant_id)
        tenant_config = TENANT_CONFIGS[validated_tenant_id]
        
        logger.info(f"🎯 Processing question for {tenant_config['name']}: {request.question[:100]}...")
        
        # Choose AI system based on availability and request preferences
        if request.use_dual_model and DUAL_MODEL_AVAILABLE:
            logger.info("📝 Using Dual-Model AI System (SQL Coder + NL Generator)")
            result = await enhanced_agent.process_any_question(request.question, validated_tenant_id)
            result['ai_system_used'] = 'dual_model'
            
        elif request.use_dynamic_ai and DYNAMIC_AI_AVAILABLE:
            logger.info("🧠 Using Dynamic AI System")
            result = await enhanced_agent.process_any_question(request.question, validated_tenant_id)
            result['ai_system_used'] = 'dynamic_ai'
            
        else:
            logger.info("🔧 Using Standard Enhanced System")
            # For the simplified version, we'll use the same method but mark it differently
            result = await enhanced_agent.process_any_question(request.question, validated_tenant_id)
            result['ai_system_used'] = 'standard_enhanced'
        
        # Ensure all required fields are present and calculate processing time
        result = ensure_required_fields(result)
        result['tenant_id'] = validated_tenant_id
        result['processing_time'] = time.time() - start_time
        
        # Add tenant context to the response for debugging
        result['tenant_context'] = {
            'name': tenant_config['name'],
            'business_type': tenant_config['business_type'],
            'language': tenant_config['language']
        }
        
        logger.info(f"✅ Query processed successfully in {result['processing_time']:.2f}s")
        return result
        
    except Exception as e:
        logger.error(f"❌ Enhanced RAG query failed: {e}")
        error_response = ensure_required_fields({
            'answer': f"เกิดข้อผิดพลาดในการประมวลผล: {str(e)}\n\nกรุณาลองใหม่อีกครั้ง หรือติดต่อผู้ดูแลระบบ",
            'success': False,
            'tenant_id': validated_tenant_id,
            'ai_system_used': 'error_handler',
            'processing_time': time.time() - start_time
        })
        return error_response

# =============================================================================
# 🔗 OPENAI-COMPATIBLE API ENDPOINTS
# =============================================================================

@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest, tenant_id: str = Depends(get_tenant_id)):
    """
    OpenAI-compatible chat completions endpoint
    
    This allows the HVAC AI system to work with OpenAI-compatible clients
    while maintaining all the specialized HVAC knowledge and anti-hallucination features
    """
    try:
        start_time = time.time()
        
        # Extract the user's question from the messages
        user_messages = [msg for msg in request.messages if msg.role == "user"]
        if not user_messages:
            raise HTTPException(400, "No user message found in request")
        
        question = user_messages[-1].content  # Get the latest user message
        validated_tenant_id = validate_tenant_id(tenant_id)
        
        # Process the question using our HVAC AI system
        rag_result = await enhanced_agent.process_any_question(question, validated_tenant_id)
        fixed_result = ensure_required_fields(rag_result)
        
        # Determine which AI system was used for metadata
        ai_system = 'dual_model' if DUAL_MODEL_AVAILABLE else ('dynamic_ai' if DYNAMIC_AI_AVAILABLE else 'standard')
        
        # Return in OpenAI-compatible format
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
            "siamtemp_metadata": {
                "tenant_id": validated_tenant_id,
                "ai_system_used": ai_system,
                "processing_time": time.time() - start_time,
                "dual_model_available": DUAL_MODEL_AVAILABLE,
                "sql_query": fixed_result.get("sql_query"),
                "models_used": fixed_result.get("models_used", {})
            }
        }
            
    except Exception as e:
        raise HTTPException(500, f"Chat completions failed: {str(e)}")

@app.get("/v1/models")
async def list_models():
    """
    List available models in OpenAI-compatible format
    
    This endpoint helps clients discover what models are available
    and includes metadata about our HVAC-specific capabilities
    """
    models_data = []
    
    # Create a model entry for each tenant configuration
    for tid, config in TENANT_CONFIGS.items():
        model_info = {
            "id": f"siamtemp-hvac-{tid}",
            "object": "model",
            "created": int(datetime.now().timestamp()),
            "owned_by": f"siamtemp-{tid}",
            "streaming_supported": False,  # Simplified version doesn't support streaming yet
            "dual_model_enabled": DUAL_MODEL_AVAILABLE,
            "dynamic_ai_enabled": DYNAMIC_AI_AVAILABLE,
            "siamtemp_metadata": {
                "tenant_id": tid,
                "tenant_name": config['name'],
                "business_type": config['business_type'],
                "language": config['language'],
                "emoji": config['emoji'],
                "specialization": config['business_context']['specialization'],
                "models_configured": {
                    "sql_generation": config.get('sql_model', 'not_configured'),
                    "response_generation": config.get('nl_model', 'not_configured'),
                    "legacy_fallback": config.get('model', 'not_configured')
                }
            }
        }
        models_data.append(model_info)
    
    return {
        "object": "list",
        "data": models_data
    }

# =============================================================================
# 📊 SYSTEM MONITORING AND HEALTH
# =============================================================================

@app.get("/health")
async def health_check():
    """
    System health check endpoint
    
    This provides a quick way to verify that the system is running
    and shows the current configuration status
    """
    return {
        "status": "healthy",
        "service": "Siamtemp Simplified HVAC AI Service",
        "version": "5.0-Simplified",
        "dual_model_available": DUAL_MODEL_AVAILABLE,
        "dynamic_ai_available": DYNAMIC_AI_AVAILABLE,
        "tenant_count": len(TENANT_CONFIGS),
        "default_tenant": get_tenant_id(),
        "timestamp": datetime.now().isoformat(),
        "models_configured": {
            "sql_generation": TENANT_CONFIGS['company-a']['sql_model'],
            "response_generation": TENANT_CONFIGS['company-a']['nl_model']
        },
        "ollama_server": GLOBAL_SETTINGS['ollama_server']['base_url']
    }

@app.get("/system-info")
async def system_info():
    """
    Detailed system information endpoint
    
    This provides comprehensive information about the system's capabilities,
    configuration, and current status for debugging and monitoring
    """
    try:
        # Get statistics from the AI agent if available
        stats = enhanced_agent.stats if hasattr(enhanced_agent, 'stats') else {
            'total_queries': 0,
            'successful_queries': 0
        }
        
        system_info = {
            "service_name": "Siamtemp Simplified HVAC AI Service",
            "version": "5.0-Simplified",
            "status": "operational",
            "capabilities": {
                "dual_model_sql_generation": DUAL_MODEL_AVAILABLE,
                "dual_model_nl_generation": DUAL_MODEL_AVAILABLE,
                "advanced_dynamic_ai": DYNAMIC_AI_AVAILABLE,
                "hvac_domain_knowledge": True,
                "multi_tenant_support": True,
                "thai_english_support": True,
                "anti_hallucination": True,
                "openai_compatibility": True
            },
            "tenant_information": {
                "total_tenants": len(TENANT_CONFIGS),
                "available_tenants": list(TENANT_CONFIGS.keys()),
                "default_tenant": get_tenant_id(),
                "tenant_details": {
                    tid: {
                        "name": config["name"],
                        "business_type": config["business_type"],
                        "language": config["language"],
                        "emoji": config["emoji"]
                    }
                    for tid, config in TENANT_CONFIGS.items()
                }
            },
            "ai_models": {
                "sql_generation_model": TENANT_CONFIGS['company-a']['sql_model'],
                "nl_generation_model": TENANT_CONFIGS['company-a']['nl_model'],
                "fallback_model": TENANT_CONFIGS['company-a']['model']
            },
            "performance_stats": stats,
            "global_settings": GLOBAL_SETTINGS,
            "timestamp": datetime.now().isoformat()
        }
        
        return system_info
        
    except Exception as e:
        return {
            "error": f"Failed to get system info: {str(e)}",
            "basic_info": {
                "service": "Siamtemp HVAC AI",
                "version": "5.0-Simplified",
                "status": "partial_information_available"
            }
        }

# =============================================================================
# 🧪 SIMPLIFIED TESTING ENDPOINTS
# =============================================================================

@app.post("/test-system")
async def test_system(request: RAGQuery):
    """
    Simple system test endpoint
    
    This endpoint provides a quick way to test the system with various
    types of HVAC-related questions to ensure everything is working properly
    """
    test_results = []
    test_questions = [
        "สวัสดีครับ",  # Greeting test
        "มีอะไหล่ Hitachi ไหม",  # Parts inquiry test
        request.question  # User's actual question
    ]
    
    for question in test_questions:
        try:
            start_time = time.time()
            result = await enhanced_agent.process_any_question(question, request.tenant_id or 'company-a')
            result['processing_time'] = time.time() - start_time
            result['test_question'] = question
            test_results.append(result)
        except Exception as e:
            test_results.append({
                'test_question': question,
                'error': str(e),
                'success': False
            })
    
    return {
        "test_summary": {
            "total_tests": len(test_results),
            "successful_tests": sum(1 for r in test_results if r.get('success', False)),
            "system_status": "operational" if any(r.get('success', False) for r in test_results) else "needs_attention"
        },
        "test_results": test_results,
        "system_capabilities": {
            "dual_model": DUAL_MODEL_AVAILABLE,
            "dynamic_ai": DYNAMIC_AI_AVAILABLE,
            "agent_type": type(enhanced_agent).__name__
        }
    }

# =============================================================================
# 🔄 LEGACY COMPATIBILITY
# =============================================================================

@app.post("/rag-query", response_model=RAGResponse)
async def legacy_rag_query(request: RAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """Legacy endpoint that redirects to the enhanced version for backward compatibility"""
    return await enhanced_rag_query(request, tenant_id)

# =============================================================================
# 🚀 MAIN APPLICATION STARTUP
# =============================================================================

if __name__ == "__main__":
    # Get the default tenant configuration for startup logging
    default_tenant_id = get_tenant_id()
    config = TENANT_CONFIGS.get(default_tenant_id, {})
    
    # Display comprehensive startup information
    print("🚀 Starting Siamtemp Simplified HVAC AI Service")
    print("=" * 70)
    print(f"🎯 Service Version: 5.0-Simplified")
    print(f"🤖 Agent Type: {type(enhanced_agent).__name__}")
    print(f"🏢 Default Tenant: {config.get('name', default_tenant_id)} ({default_tenant_id})")
    print(f"🔧 Business Focus: {config.get('business_type', 'Unknown')}")
    print(f"📊 Total Tenants: {len(TENANT_CONFIGS)}")
    print("")
    
    # Display AI capabilities information
    print("🎯 AI System Capabilities:")
    if DUAL_MODEL_AVAILABLE:
        print("  ✅ Dual-Model System Active:")
        print(f"    📝 SQL Generation: {config.get('sql_model', 'Not configured')}")
        print(f"    💬 Response Generation: {config.get('nl_model', 'Not configured')}")
        print("  ✅ Specialized Text-to-SQL Processing")
        print("  ✅ Natural Language Response Generation")
        print("  ✅ Anti-Hallucination Measures")
        print("  ✅ HVAC Domain Knowledge")
    
    if DYNAMIC_AI_AVAILABLE:
        print("  ✅ Dynamic AI System (Backup Available)")
        print("  ✅ Real-time Schema Discovery") 
        print("  ✅ Intelligent Question Analysis")
    
    if not DUAL_MODEL_AVAILABLE and not DYNAMIC_AI_AVAILABLE:
        print("  ⚠️ Running with Minimal Fallback Agent")
        print("  ⚠️ Full AI capabilities not available")
    
    print("  ✅ Multi-tenant Support")
    print("  ✅ OpenAI Compatible API")
    print("  ✅ Thai & English Language Support")
    print("=" * 70)
    
    # Start the FastAPI server with production-ready settings
    uvicorn.run(
        "enhanced_multi_agent_service:app",
        host="0.0.0.0",
        port=int(os.getenv('PORT', '5000')),
        reload=False,  # Disable reload for production stability
        access_log=True,
        log_level="info",
        workers=1  # Single worker for consistency with AI agent state
    )