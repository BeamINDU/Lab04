import os
import asyncio
import time
import json
from datetime import datetime
from typing import Dict, Any, AsyncGenerator
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import uvicorn
import logging

# =============================================================================
# 🔧 DUAL-MODEL AI SYSTEM IMPORT
# =============================================================================
try:
    from refactored_modules.dual_model_dynamic_ai import EnhancedUnifiedPostgresOllamaAgent
    DUAL_MODEL_AVAILABLE = True
    DYNAMIC_AI_AVAILABLE = True  # ← เพิ่มนี้เพื่อ backward compatibility
    print("✅ Dual-Model Dynamic AI System loaded successfully")
    print("📝 SQL Generation: mannix/defog-llama3-sqlcoder-8b:latest")
    print("💬 Response Generation: llama3.2:3b")
except ImportError as e:
    # Fallback to original advanced system
    try:
        from refactored_modules.advanced_dynamic_ai_system import EnhancedUnifiedPostgresOllamaAgent
        DUAL_MODEL_AVAILABLE = False
        DYNAMIC_AI_AVAILABLE = True
        print("⚠️ Dual-Model not available, using Advanced Dynamic AI")
    except ImportError as e2:
        # Final fallback to standard agent
        from refactored_modules.enhanced_postgres_agent_unified import UnifiedEnhancedPostgresOllamaAgent as EnhancedUnifiedPostgresOllamaAgent
        DUAL_MODEL_AVAILABLE = False
        DYNAMIC_AI_AVAILABLE = False
        print(f"⚠️ Using standard agent: {e}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# 🔧 ENHANCED CONFIGURATION WITH DUAL-MODEL SUPPORT
# =============================================================================

TENANT_CONFIGS = {
    'company-a': {
        'name': 'Siamtemp Bangkok HQ (HVAC Service)',
        'sql_model': 'mannix/defog-llama3-sqlcoder-8b:latest',  # ← SQL Generation Model
        'nl_model': 'llama3.2:3b',                              # ← Natural Language Model
        'model': 'llama3.1:8b',  # Legacy fallback
        'language': 'th',
        'business_type': 'hvac_service_spare_parts',
        'emoji': '🔧'
    },
    'company-b': {
        'name': 'Siamtemp Chiang Mai Regional (HVAC)',
        'sql_model': 'mannix/defog-llama3-sqlcoder-8b:latest',
        'nl_model': 'llama3.2:3b',
        'model': 'gemma2:9b', # Legacy fallback
        'language': 'th',
        'business_type': 'hvac_regional_service',
        'emoji': '❄️'
    },
    'company-c': {
        'name': 'Siamtemp International (HVAC Global)',
        'sql_model': 'mannix/defog-llama3-sqlcoder-8b:latest',
        'nl_model': 'llama3.2:3b',
        'model': 'llama3.1:8b', # Legacy fallback
        'language': 'en',
        'business_type': 'hvac_international',
        'emoji': '🌍'
    }
}

# =============================================================================
# INITIALIZE ENHANCED DUAL-MODEL AGENT
# =============================================================================

try:
    enhanced_agent = EnhancedUnifiedPostgresOllamaAgent()
    logger.info(f"🚀 Enhanced Dual-Model Agent initialized successfully")
    logger.info(f"🧠 Dual-Model Available: {DUAL_MODEL_AVAILABLE}")
    logger.info(f"🧠 Dynamic AI Available: {DYNAMIC_AI_AVAILABLE}")
    
    if DUAL_MODEL_AVAILABLE:
        logger.info("✅ Using Dual-Model Dynamic AI System")
        logger.info("   📝 SQL Generation: mannix/defog-llama3-sqlcoder-8b:latest")
        logger.info("   💬 Response Generation: llama3.2:3b")
        logger.info("   🎯 Can answer ANY question with specialized models!")
    elif DYNAMIC_AI_AVAILABLE:
        logger.info("✅ Using Advanced Dynamic AI System") 
    else:
        logger.info("⚠️ Using Standard Enhanced Agent - limited capabilities")
        
except Exception as e:
    logger.error(f"❌ Failed to initialize Enhanced Agent: {e}")
    raise RuntimeError(f"Cannot start service: {e}")

# =============================================================================
# FASTAPI APP
# =============================================================================

app = FastAPI(
    title="Siamtemp Dual-Model HVAC AI Service",
    description="Advanced Multi-Tenant RAG System with Dual-Model AI (SQL Coder + NL Generator)",
    version="4.1-DualModel"
)

# =============================================================================
# MODELS
# =============================================================================

class RAGQuery(BaseModel):
    question: str
    tenant_id: str = "company-a"
    use_dual_model: bool = True      # ← เพิ่มพารามิเตอร์ใหม่
    use_dynamic_ai: bool = True      # ← เก็บไว้เพื่อ backward compatibility
    streaming: bool = False

class RAGResponse(BaseModel):
    answer: str
    success: bool
    sql_query: str = None
    results_count: int = 0
    tenant_id: str
    processing_time: float
    ai_system_used: str  # "dual_model", "dynamic_ai", "standard_enhanced"
    question_analysis: Dict[str, Any] = None
    models_used: Dict[str, str] = None  # ← เพิ่มข้อมูล models ที่ใช้

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    model: str
    messages: list[ChatMessage]
    stream: bool = False
    temperature: float = 0.7
    max_tokens: int = 2000

# =============================================================================
# DEPENDENCY FUNCTIONS
# =============================================================================

def get_tenant_id() -> str:
    """Get tenant ID from environment or default"""
    return os.getenv('TENANT_ID', 'company-a')

def ensure_required_fields(result: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure response has all required fields"""
    required_fields = {
        'answer': result.get('answer', 'No answer generated'),
        'success': result.get('success', False),
        'sql_query': result.get('sql_query'),
        'results_count': result.get('results_count', 0),
        'tenant_id': result.get('tenant_id', 'unknown'),
        'processing_time': result.get('processing_time', 0.0),
        'ai_system_used': result.get('ai_system_used', 'unknown'),
        'question_analysis': result.get('question_analysis', {}),
        'models_used': result.get('models_used', {})  # ← เพิ่มนี้
    }
    
    return required_fields

# =============================================================================
# 🚀 MAIN API ENDPOINTS
# =============================================================================

@app.post("/enhanced-rag-query", response_model=RAGResponse)
async def enhanced_rag_query(request: RAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """🚀 Enhanced RAG endpoint with Dual-Model support"""
    
    try:
        start_time = time.time()
        logger.info(f"🎯 Processing question: {request.question[:100]}...")
        
        # Choose AI system based on request
        if request.use_dual_model and DUAL_MODEL_AVAILABLE:
            logger.info("📝 Using Dual-Model AI System")
            result = await enhanced_agent.process_any_question(request.question, tenant_id)
            result['ai_system_used'] = 'dual_model'
            
        elif request.use_dynamic_ai and DYNAMIC_AI_AVAILABLE:
            logger.info("🧠 Using Dynamic AI System")  
            result = await enhanced_agent.process_any_question(request.question, tenant_id)
            result['ai_system_used'] = 'dynamic_ai'
            
        else:
            logger.info("🔧 Using Standard Enhanced System")
            result = await enhanced_agent.process_enhanced_question(request.question, tenant_id)
            result['ai_system_used'] = 'standard_enhanced'
        
        # Ensure result has all required fields
        result = ensure_required_fields(result)
        result['tenant_id'] = tenant_id
        result['processing_time'] = time.time() - start_time
        
        logger.info(f"✅ Query processed successfully in {result['processing_time']:.2f}s")
        return result
        
    except Exception as e:
        logger.error(f"❌ Enhanced RAG query failed: {e}")
        return ensure_required_fields({
            'answer': f"เกิดข้อผิดพลาด: {str(e)}",
            'success': False,
            'tenant_id': tenant_id,
            'ai_system_used': 'error_handler',
            'processing_time': time.time() - start_time
        })

@app.get("/health")
async def health_check():
    """🏥 Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Siamtemp Dual-Model HVAC AI Service",
        "version": "4.1-DualModel",
        "dual_model_available": DUAL_MODEL_AVAILABLE,
        "dynamic_ai_available": DYNAMIC_AI_AVAILABLE,
        "timestamp": datetime.now().isoformat(),
        "models_configured": {
            "sql_generation": "mannix/defog-llama3-sqlcoder-8b:latest",
            "response_generation": "llama3.2:3b"
        }
    }

# =============================================================================
# 🧪 TESTING ENDPOINTS
# =============================================================================

@app.post("/test-dual-model")
async def test_dual_model(request: RAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """🧪 Test endpoint specifically for Dual-Model AI"""
    
    if not DUAL_MODEL_AVAILABLE:
        return {
            "error": "Dual-Model system not available",
            "fallback_available": DYNAMIC_AI_AVAILABLE,
            "current_system": "standard_enhanced" if not DYNAMIC_AI_AVAILABLE else "dynamic_ai"
        }
    
    try:
        start_time = time.time()
        
        # Force use Dual-Model
        result = await enhanced_agent.process_any_question(request.question, tenant_id)
        
        processing_time = time.time() - start_time
        result['processing_time'] = processing_time
        result['test_type'] = 'dual_model_test'
        result['ai_system_used'] = 'dual_model_test'
        
        return result
        
    except Exception as e:
        return {
            "error": f"Dual-Model test failed: {str(e)}",
            "success": False,
            "tenant_id": tenant_id,
            "ai_system_used": "error_handler"
        }

@app.post("/test-sql-generation") 
async def test_sql_generation(request: RAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """📝 Test เฉพาะ SQL Generation ด้วย SQL Coder"""
    
    if not DUAL_MODEL_AVAILABLE:
        return {"error": "Dual-Model system required for this test"}
    
    try:
        # Test only SQL generation part
        schema = await enhanced_agent.dual_model_ai._discover_complete_schema(tenant_id)
        sql_result = await enhanced_agent.dual_model_ai._generate_sql_with_specialist(
            request.question, schema, tenant_id
        )
        
        return {
            "question": request.question,
            "sql_generated": sql_result.get("sql_query"),
            "generation_success": sql_result.get("success"),
            "model_used": "mannix/defog-llama3-sqlcoder-8b:latest",
            "analysis": sql_result.get("analysis", {}),
            "schema_tables": list(schema.keys())
        }
        
    except Exception as e:
        return {"error": f"SQL generation test failed: {str(e)}"}

@app.post("/test-nl-generation")
async def test_nl_generation(request: Dict[str, Any]):
    """💬 Test เฉพาะ Natural Language Generation"""
    
    if not DUAL_MODEL_AVAILABLE:
        return {"error": "Dual-Model system required for this test"}
    
    try:
        question = request.get("question", "")
        mock_results = request.get("mock_results", [{"test": "data"}])
        sql_query = request.get("sql_query", "SELECT * FROM test")
        tenant_id = request.get("tenant_id", "company-a")
        
        # Test only NL generation part
        nl_response = await enhanced_agent.dual_model_ai._generate_natural_response(
            question, mock_results, sql_query, tenant_id
        )
        
        return {
            "question": question,
            "nl_response": nl_response,
            "model_used": "llama3.2:3b",
            "input_data_count": len(mock_results)
        }
        
    except Exception as e:
        return {"error": f"NL generation test failed: {str(e)}"}

# =============================================================================
# 🔗 OPENAI-COMPATIBLE API
# =============================================================================

@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest, tenant_id: str = Depends(get_tenant_id)):
    """🔗 OpenAI-compatible chat completions endpoint with Dual-Model"""
    
    try:
        # Extract question from messages
        if not request.messages:
            raise HTTPException(400, "No messages provided")
        
        last_message = request.messages[-1]
        if last_message.role != "user":
            raise HTTPException(400, "Last message must be from user")
        
        question = last_message.content
        
        # Process with appropriate AI system
        if DUAL_MODEL_AVAILABLE:
            result = await enhanced_agent.process_any_question(question, tenant_id)
            ai_system = "dual_model"
        elif DYNAMIC_AI_AVAILABLE:
            result = await enhanced_agent.process_any_question(question, tenant_id)
            ai_system = "dynamic_ai"
        else:
            result = await enhanced_agent.process_enhanced_question(question, tenant_id)
            ai_system = "standard_enhanced"
        
        # Ensure required fields
        fixed_result = ensure_required_fields(result)
        
        # Format as OpenAI response
        if request.stream:
            async def generate_stream():
                answer = fixed_result.get('answer', '')
                chunks = [answer[i:i+50] for i in range(0, len(answer), 50)]
                
                for i, chunk in enumerate(chunks):
                    chunk_response = {
                        "id": f"chatcmpl-{int(time.time())}",
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": f"{ai_system}-{tenant_id}",
                        "choices": [{
                            "index": 0,
                            "delta": {"content": chunk},
                            "finish_reason": None if i < len(chunks) - 1 else "stop"
                        }]
                    }
                    yield f"data: {json.dumps(chunk_response)}\n\n"
                
                yield "data: [DONE]\n\n"
            
            return StreamingResponse(generate_stream(), media_type="text/plain")
        
        else:
            return {
                "id": f"chatcmpl-{int(time.time())}",
                "object": "chat.completion", 
                "created": int(time.time()),
                "model": f"{ai_system}-{tenant_id}",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": fixed_result.get('answer', '')
                    },
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": len(question.split()),
                    "completion_tokens": len(fixed_result.get('answer', '').split()),
                    "total_tokens": len(question.split()) + len(fixed_result.get('answer', '').split())
                },
                "siamtemp_metadata": {
                    "tenant_id": tenant_id,
                    "ai_system_used": ai_system,
                    "processing_time": fixed_result.get("processing_time", 0),
                    "dual_model_available": DUAL_MODEL_AVAILABLE,
                    "sql_query": fixed_result.get("sql_query"),
                    "models_used": fixed_result.get("models_used", {})
                }
            }
            
    except Exception as e:
        raise HTTPException(500, f"Chat completions failed: {str(e)}")

@app.get("/v1/models")
async def list_models():
    """📋 List available models with Dual-Model info"""
    
    tenant_id = get_tenant_id()
    models_data = []
    
    # Add models for each tenant
    for tid, config in TENANT_CONFIGS.items():
        base_model_info = {
            "id": f"dual-model-{tid}",
            "object": "model", 
            "created": int(datetime.now().timestamp()),
            "owned_by": f"siamtemp-{tid}",
            "streaming_supported": True,
            "dual_model_enabled": DUAL_MODEL_AVAILABLE,
            "dynamic_ai_enabled": DYNAMIC_AI_AVAILABLE,
            "siamtemp_metadata": {
                "tenant_id": tid,
                "tenant_name": config['name'],
                "business_type": config['business_type'],
                "language": config['language'],
                "emoji": config['emoji'],
                "models_configured": {
                    "sql_generation": config.get('sql_model', 'not_configured'),
                    "response_generation": config.get('nl_model', 'not_configured'),
                    "legacy_fallback": config.get('model', 'not_configured')
                }
            }
        }
        models_data.append(base_model_info)
    
    return {
        "object": "list",
        "data": models_data
    }

# =============================================================================
# 📊 SYSTEM INFORMATION
# =============================================================================

@app.get("/system-info")
async def system_info():
    """📊 Get detailed system information with Dual-Model status"""
    
    try:
        stats = enhanced_agent.stats if hasattr(enhanced_agent, 'stats') else {}
        
        system_info = {
            "service_name": "Siamtemp Dual-Model HVAC AI Service",
            "version": "4.1-DualModel",
            "dual_model_available": DUAL_MODEL_AVAILABLE,
            "dynamic_ai_available": DYNAMIC_AI_AVAILABLE,
            "agent_type": type(enhanced_agent).__name__,
            "tenant_configs": {
                tid: {
                    "name": config["name"],
                    "business_type": config["business_type"],
                    "sql_model": config.get("sql_model", "not_configured"),
                    "nl_model": config.get("nl_model", "not_configured"),
                    "legacy_model": config.get("model", "not_configured")
                }
                for tid, config in TENANT_CONFIGS.items()
            },
            "capabilities": {
                "dual_model_sql_generation": DUAL_MODEL_AVAILABLE,
                "dual_model_nl_generation": DUAL_MODEL_AVAILABLE,
                "advanced_dynamic_ai": DYNAMIC_AI_AVAILABLE,
                "standard_enhanced": True,
                "real_time_schema_discovery": DYNAMIC_AI_AVAILABLE or DUAL_MODEL_AVAILABLE,
                "fuzzy_search": DYNAMIC_AI_AVAILABLE or DUAL_MODEL_AVAILABLE,
                "multi_table_queries": True,
                "financial_analysis": DUAL_MODEL_AVAILABLE,
                "time_series_analysis": DUAL_MODEL_AVAILABLE
            },
            "performance_stats": stats,
            "timestamp": datetime.now().isoformat()
        }
        
        # Add Dual-Model specific information
        if DUAL_MODEL_AVAILABLE and hasattr(enhanced_agent, 'dual_model_ai'):
            dual_system = enhanced_agent.dual_model_ai
            system_info["dual_model_info"] = {
                "sql_model": dual_system.SQL_MODEL,
                "nl_model": dual_system.NL_MODEL,
                "schema_cache_size": len(dual_system.schema_cache),
                "sql_cache_size": len(dual_system.sql_cache)
            }
        
        return system_info
        
    except Exception as e:
        return {
            "error": f"Failed to get system info: {str(e)}",
            "basic_info": {
                "service": "Siamtemp HVAC AI",
                "version": "4.1-DualModel",
                "status": "partial"
            }
        }

# =============================================================================
# LEGACY COMPATIBILITY
# =============================================================================

@app.post("/rag-query", response_model=RAGResponse)
async def legacy_rag_query(request: RAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """Legacy endpoint - redirects to enhanced version"""
    return await enhanced_rag_query(request, tenant_id)

# =============================================================================
# MAIN APPLICATION
# =============================================================================

if __name__ == "__main__":
    tenant_id = get_tenant_id()
    config = TENANT_CONFIGS.get(tenant_id, {})
    
    print("🚀 Starting Siamtemp Dual-Model HVAC AI Service")
    print("=" * 70)
    print(f"🎯 Service Version: 4.1-DualModel")
    print(f"🤖 Agent Type: {type(enhanced_agent).__name__}")
    print(f"🧠 Dual-Model Available: {'✅ YES' if DUAL_MODEL_AVAILABLE else '❌ NO'}")
    print(f"🧠 Dynamic AI Available: {'✅ YES' if DYNAMIC_AI_AVAILABLE else '❌ NO'}")
    print(f"🏢 Default Tenant: {config.get('name', tenant_id)} ({tenant_id})")
    print(f"🔧 Business Type: {config.get('business_type', 'Unknown')}")
    print(f"📊 Total Tenants: {len(TENANT_CONFIGS)}")
    print("")
    print("🎯 AI Models Configuration:")
    
    if DUAL_MODEL_AVAILABLE:
        print("  ✅ Dual-Model System:")
        print(f"    📝 SQL Generation: {config.get('sql_model', 'Not configured')}")
        print(f"    💬 NL Generation: {config.get('nl_model', 'Not configured')}")
        print("  ✅ Specialized Text-to-SQL Processing")
        print("  ✅ Natural Language Response Generation")
        print("  ✅ Advanced Financial Analysis")
        print("  ✅ Complex Multi-table Queries")
    
    if DYNAMIC_AI_AVAILABLE:
        print("  ✅ Dynamic AI System (Backup)")
        print("  ✅ Real-time Schema Discovery") 
        print("  ✅ Intelligent Question Analysis")
        print("  ✅ Fuzzy Search & Fallback")
    else:
        print("  ❌ Dynamic AI System (Not Available)")
    
    print("  ✅ Standard Enhanced AI (Fallback)")
    print("  ✅ OpenAI Compatible API")
    print("  ✅ Streaming Responses") 
    print("  ✅ Multi-tenant Support")
    print("=" * 70)
    
    uvicorn.run(
        "enhanced_multi_agent_service:app",
        host="0.0.0.0", 
        port=5000,
        reload=False,
        access_log=True,
        log_level="info"
    )