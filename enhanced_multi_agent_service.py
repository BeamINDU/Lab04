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

try:
    from refactored_modules.advanced_dynamic_ai_system import EnhancedUnifiedPostgresOllamaAgent
    DYNAMIC_AI_AVAILABLE = True
    print("✅ FIXED Dynamic AI System loaded successfully")
except ImportError as e:
    # Fallback to standard agent
    from refactored_modules.enhanced_postgres_agent_unified import UnifiedEnhancedPostgresOllamaAgent as EnhancedUnifiedPostgresOllamaAgent
    DYNAMIC_AI_AVAILABLE = False
    print(f"⚠️ Dynamic AI not available, using standard agent: {e}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

TENANT_CONFIGS = {
    'company-a': {
        'name': 'Siamtemp Bangkok HQ (HVAC Service)',
        'model': 'llama3.1:8b',
        'language': 'th',
        'business_type': 'hvac_service_spare_parts',
        'emoji': '🔧'
    },
    'company-b': {
        'name': 'Siamtemp Chiang Mai Regional (HVAC)',
        'model': 'gemma2:9b', 
        'language': 'th',
        'business_type': 'hvac_regional_service',
        'emoji': '❄️'
    },
    'company-c': {
        'name': 'Siamtemp International (HVAC Global)',
        'model': 'llama3.1:8b',
        'language': 'en',
        'business_type': 'hvac_international',
        'emoji': '🌍'
    }
}

# =============================================================================
# INITIALIZE ENHANCED DYNAMIC AGENT
# =============================================================================

try:
    enhanced_agent = EnhancedUnifiedPostgresOllamaAgent()
    logger.info(f"🚀 Enhanced Dynamic Agent initialized successfully")
    logger.info(f"🧠 Dynamic AI Available: {DYNAMIC_AI_AVAILABLE}")
    
    if DYNAMIC_AI_AVAILABLE:
        logger.info("✅ Using Advanced Dynamic AI System - can answer ANY question!")
    else:
        logger.info("⚠️ Using Standard Enhanced Agent - limited capabilities")
        
except Exception as e:
    logger.error(f"❌ Failed to initialize Enhanced Agent: {e}")
    raise RuntimeError(f"Cannot start service: {e}")

# =============================================================================
# FASTAPI APP
# =============================================================================

app = FastAPI(
    title="Siamtemp Dynamic HVAC AI Service",
    description="Advanced Multi-Tenant RAG System with Dynamic AI for HVAC Business",
    version="4.0-Dynamic"
)

# =============================================================================
# MODELS
# =============================================================================

class RAGQuery(BaseModel):
    question: str
    tenant_id: str = "company-a"
    use_dynamic_ai: bool = True  # New parameter to control AI type
    streaming: bool = False

class RAGResponse(BaseModel):
    answer: str
    success: bool
    sql_query: str = None
    results_count: int = 0
    tenant_id: str
    processing_time: float
    ai_system_used: str  # "dynamic_ai" or "standard_enhanced"
    question_analysis: Dict[str, Any] = None

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

def ensure_required_fields(result: Dict[str, Any], tenant_id: str) -> Dict[str, Any]:
    """Ensure all required fields exist"""
    
    defaults = {
        'success': result.get('success', True),
        'answer': result.get('answer', ''),
        'sql_query': result.get('sql_query'),
        'tenant_id': tenant_id,
        'processing_time': result.get('processing_time', 0.0),
        'ai_system_used': result.get('ai_system_used', 'unknown'),
        'question_analysis': result.get('question_analysis', {}),
        'results_count': result.get('results_count', 0)
    }
    
    # Merge with original result
    for key, value in defaults.items():
        if key not in result or result[key] is None:
            result[key] = value
    
    return result

# =============================================================================
# MAIN RAG ENDPOINTS
# =============================================================================

@app.post("/enhanced-rag-query", response_model=RAGResponse)
async def enhanced_rag_query(request: RAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """🚀 Enhanced RAG Query with Dynamic AI Support"""
    
    start_time = time.time()
    
    try:
        logger.info(f"🎯 Enhanced RAG Query: {request.question[:100]}...")
        logger.info(f"🏢 Tenant: {tenant_id}")
        logger.info(f"🤖 Use Dynamic AI: {request.use_dynamic_ai}")
        
        # Choose AI system based on request and availability
        if request.use_dynamic_ai and DYNAMIC_AI_AVAILABLE:
            logger.info("🚀 Using Dynamic AI System")
            result = await enhanced_agent.process_any_question(request.question, tenant_id)
            result['ai_system_used'] = 'dynamic_ai'
        else:
            logger.info("🔧 Using Standard Enhanced System")
            result = await enhanced_agent.process_enhanced_question(request.question, tenant_id)
            result['ai_system_used'] = 'standard_enhanced'
            
            # Try Dynamic AI as fallback if standard fails
            if (not result.get('success') and DYNAMIC_AI_AVAILABLE and 
                hasattr(enhanced_agent, 'process_any_question')):
                logger.info("🔄 Fallback to Dynamic AI System")
                result = await enhanced_agent.process_any_question(request.question, tenant_id)
                result['ai_system_used'] = 'dynamic_ai_fallback'
        
        # Calculate processing time
        processing_time = time.time() - start_time
        result['processing_time'] = processing_time
        
        # Ensure all required fields
        result = ensure_required_fields(result, tenant_id)
        
        logger.info(f"✅ Query completed in {processing_time:.2f}s using {result['ai_system_used']}")
        
        return RAGResponse(**result)
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"❌ Enhanced RAG query failed: {e}")
        
        error_response = {
            'answer': f"เกิดข้อผิดพลาดในการประมวลผล: {str(e)}",
            'success': False,
            'tenant_id': tenant_id,
            'processing_time': processing_time,
            'ai_system_used': 'error_handler',
            'error': str(e)
        }
        
        return RAGResponse(**error_response)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    
    tenant_id = get_tenant_id()
    config = TENANT_CONFIGS.get(tenant_id, {})
    
    return {
        "status": "healthy",
        "service": "Siamtemp Dynamic HVAC AI Service",
        "version": "4.0-Dynamic",
        "tenant_id": tenant_id,
        "tenant_name": config.get('name', 'Unknown'),
        "business_type": config.get('business_type', 'Unknown'),
        "dynamic_ai_available": DYNAMIC_AI_AVAILABLE,
        "ai_capabilities": [
            "Standard Enhanced AI",
            "Dynamic AI (Any Question)" if DYNAMIC_AI_AVAILABLE else "Dynamic AI (Not Available)"
        ],
        "timestamp": datetime.now().isoformat()
    }

# =============================================================================
# OPENAI COMPATIBLE ENDPOINTS
# =============================================================================

@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest, tenant_id: str = Depends(get_tenant_id)):
    """OpenAI compatible chat completions endpoint"""
    
    try:
        if not request.messages:
            raise HTTPException(400, "No messages provided")
        
        # Get the last user message
        user_message = None
        for msg in reversed(request.messages):
            if msg.role == "user":
                user_message = msg.content
                break
        
        if not user_message:
            raise HTTPException(400, "No user message found")
        
        # Process with Dynamic AI if available
        if DYNAMIC_AI_AVAILABLE:
            result = await enhanced_agent.process_any_question(user_message, tenant_id)
        else:
            result = await enhanced_agent.process_enhanced_question(user_message, tenant_id)
        
        if request.stream:
            # Streaming response
            async def generate_openai_streaming():
                chunks = [
                    {"role": "assistant", "content": chunk}
                    for chunk in result.get("answer", "").split('. ')
                    if chunk.strip()
                ]
                
                for i, chunk in enumerate(chunks):
                    response_chunk = {
                        "id": f"chatcmpl-{int(time.time())}-{i}",
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": request.model,
                        "choices": [{
                            "index": 0,
                            "delta": chunk,
                            "finish_reason": "stop" if i == len(chunks) - 1 else None
                        }]
                    }
                    yield f"data: {json.dumps(response_chunk)}\n\n"
                    await asyncio.sleep(0.1)  # Simulate streaming delay
                
                yield "data: [DONE]\n\n"
            
            return StreamingResponse(
                generate_openai_streaming(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
            )
        else:
            # Non-streaming
            fixed_result = ensure_required_fields(result, tenant_id)
            config = TENANT_CONFIGS[tenant_id]
            
            return {
                "id": f"chatcmpl-{int(time.time())}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": config['model'],
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": fixed_result.get("answer", "")},
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": len(user_message.split()),
                    "completion_tokens": len(fixed_result.get("answer", "").split()),
                    "total_tokens": len(user_message.split()) + len(fixed_result.get("answer", "").split())
                },
                "siamtemp_metadata": {
                    "tenant_id": tenant_id,
                    "ai_system_used": fixed_result.get("ai_system_used", "unknown"),
                    "processing_time": fixed_result.get("processing_time", 0),
                    "dynamic_ai_available": DYNAMIC_AI_AVAILABLE
                }
            }
            
    except Exception as e:
        raise HTTPException(500, f"Chat completions failed: {str(e)}")

@app.get("/v1/models")
async def list_models():
    """List available models"""
    
    tenant_id = get_tenant_id()
    tenant_config = TENANT_CONFIGS.get(tenant_id, {})
    
    models_data = []
    
    # Add models for each tenant
    for tid, config in TENANT_CONFIGS.items():
        models_data.append({
            "id": f"{config['model']}-{tid}",
            "object": "model",
            "created": int(datetime.now().timestamp()),
            "owned_by": f"siamtemp-{tid}",
            "streaming_supported": True,
            "dynamic_ai_enabled": DYNAMIC_AI_AVAILABLE,
            "siamtemp_metadata": {
                "tenant_id": tid,
                "tenant_name": config['name'],
                "business_type": config['business_type'],
                "language": config['language'],
                "emoji": config['emoji']
            }
        })
    
    return {
        "object": "list",
        "data": models_data
    }

# =============================================================================
# TESTING AND DEBUGGING ENDPOINTS
# =============================================================================

@app.post("/test-dynamic-ai")
async def test_dynamic_ai(request: RAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """Test endpoint specifically for Dynamic AI"""
    
    if not DYNAMIC_AI_AVAILABLE:
        return {
            "error": "Dynamic AI system not available",
            "fallback_used": False
        }
    
    try:
        start_time = time.time()
        
        # Force use Dynamic AI
        result = await enhanced_agent.process_any_question(request.question, tenant_id)
        
        processing_time = time.time() - start_time
        result['processing_time'] = processing_time
        result['ai_system_used'] = 'dynamic_ai_test'
        
        return result
        
    except Exception as e:
        return {
            "error": f"Dynamic AI test failed: {str(e)}",
            "success": False
        }

@app.get("/system-info")
async def system_info():
    """Get detailed system information"""
    
    try:
        stats = enhanced_agent.stats if hasattr(enhanced_agent, 'stats') else {}
        
        system_info = {
            "service_name": "Siamtemp Dynamic HVAC AI Service",
            "version": "4.0-Dynamic",
            "dynamic_ai_available": DYNAMIC_AI_AVAILABLE,
            "agent_type": type(enhanced_agent).__name__,
            "tenant_configs": {
                tid: {
                    "name": config["name"],
                    "business_type": config["business_type"],
                    "model": config["model"]
                }
                for tid, config in TENANT_CONFIGS.items()
            },
            "capabilities": {
                "standard_enhanced": True,
                "dynamic_ai": DYNAMIC_AI_AVAILABLE,
                "real_time_schema_discovery": DYNAMIC_AI_AVAILABLE,
                "fuzzy_search": DYNAMIC_AI_AVAILABLE,
                "multi_table_queries": DYNAMIC_AI_AVAILABLE,
                "time_series_analysis": DYNAMIC_AI_AVAILABLE
            },
            "performance_stats": stats,
            "timestamp": datetime.now().isoformat()
        }
        
        if DYNAMIC_AI_AVAILABLE and hasattr(enhanced_agent, 'dynamic_ai_system'):
            # Get Dynamic AI specific stats
            dynamic_system = enhanced_agent.dynamic_ai_system
            system_info["dynamic_ai_info"] = {
                "schema_cache_size": len(dynamic_system.schema_cache),
                "question_patterns": len(dynamic_system.question_patterns),
                "entity_patterns": len(dynamic_system.entity_patterns)
            }
        
        return system_info
        
    except Exception as e:
        return {
            "error": f"Failed to get system info: {str(e)}",
            "basic_info": {
                "service": "Siamtemp HVAC AI",
                "version": "4.0-Dynamic",
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
    
    print("🚀 Starting Siamtemp Dynamic HVAC AI Service")
    print("=" * 70)
    print(f"🎯 Service Version: 4.0-Dynamic")
    print(f"🤖 Agent Type: {type(enhanced_agent).__name__}")
    print(f"🧠 Dynamic AI Available: {'✅ YES' if DYNAMIC_AI_AVAILABLE else '❌ NO'}")
    print(f"🏢 Default Tenant: {config.get('name', tenant_id)} ({tenant_id})")
    print(f"🔧 Business Type: {config.get('business_type', 'Unknown')}")
    print(f"📊 Total Tenants: {len(TENANT_CONFIGS)}")
    print("")
    print("🎯 Key Features:")
    print("  ✅ Standard Enhanced AI")
    if DYNAMIC_AI_AVAILABLE:
        print("  ✅ Dynamic AI System (Answer ANY question!)")
        print("  ✅ Real-time Schema Discovery")
        print("  ✅ Intelligent Question Analysis") 
        print("  ✅ Fuzzy Search & Fallback")
        print("  ✅ Multi-table Complex Queries")
    else:
        print("  ❌ Dynamic AI System (Not Available)")
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