# enhanced_multi_agent_service.py
# 🎯 SIMPLIFIED Enhanced Multi-Tenant RAG Service - Clean Version

import os
import asyncio
import uvicorn
import time
import json
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import Optional, Dict, Any
from datetime import datetime
import logging
from pydantic import BaseModel
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# =============================================================================
# SIMPLE TENANT CONFIGURATIONS
# =============================================================================

TENANT_CONFIGS = {
    'company-a': {
        'name': 'Siamtemp Bangkok HQ',
        'model': 'llama3.1:8b',
        'language': 'th',
        'business_type': 'enterprise',
        'emoji': '🏦'
    },
    'company-b': {
        'name': 'Siamtemp Chiang Mai Regional',
        'model': 'llama3.1:8b',
        'language': 'th',
        'business_type': 'tourism_hospitality',
        'emoji': '🏨'
    },
    'company-c': {
        'name': 'Siamtemp International',
        'model': 'llama3.1:8b',
        'language': 'en',
        'business_type': 'global_operations',
        'emoji': '🌍'
    }
}

# =============================================================================
# FASTAPI APP SETUP
# =============================================================================

app = FastAPI(
    title="Siamtemp Simple Multi-Tenant RAG Service",
    description="Clean, simplified RAG service with essential features only",
    version="5.0.0-clean"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# ENHANCED AGENT INITIALIZATION
# =============================================================================

try:
    from refactored_modules.enhanced_postgres_agent_unified import UnifiedEnhancedPostgresOllamaAgentWithAIResponse 
    enhanced_agent = UnifiedEnhancedPostgresOllamaAgentWithAIResponse ()
    print("✅ Modular Enhanced Agent loaded")
except Exception as e:
    print(f"⚠️ Modular system failed: {e}")
    try:
        from refactored_modules.enhanced_postgres_agent_unified import UnifiedEnhancedPostgresOllamaAgentWithAIResponse 
        enhanced_agent = UnifiedEnhancedPostgresOllamaAgentWithAIResponse ()
        print("✅ Enhanced agent loaded (fallback)")
    except Exception as fallback_error:
        print(f"❌ All systems failed: {fallback_error}")
        raise fallback_error

# =============================================================================
# SIMPLE PYDANTIC MODELS
# =============================================================================

class RAGQuery(BaseModel):
    query: str
    tenant_id: Optional[str] = None

class RAGResponse(BaseModel):
    answer: str
    success: bool
    tenant_id: str
    data_source_used: str
    sql_query: Optional[str] = None
    processing_time: Optional[float] = None

# =============================================================================
# SIMPLE HELPER FUNCTIONS
# =============================================================================

def get_tenant_id(x_tenant_id: Optional[str] = Header(None, alias="X-Tenant-ID")) -> str:
    """Extract tenant ID"""
    tenant_id = x_tenant_id or "company-a"
    if tenant_id not in TENANT_CONFIGS:
        raise HTTPException(400, f"Invalid tenant: {tenant_id}")
    return tenant_id

def ensure_required_fields(result: Dict[str, Any], tenant_id: str, processing_time: float = 0.0) -> Dict[str, Any]:
    """Ensure all required response fields are present"""
    
    config = TENANT_CONFIGS.get(tenant_id, {})
    
    return {
        "answer": result.get("answer", "ไม่สามารถประมวลผลได้"),
        "success": result.get("success", True),
        "tenant_id": tenant_id,
        "data_source_used": result.get("data_source_used", f"enhanced_{config.get('model', 'default')}"),
        "sql_query": result.get("sql_query"),
        "processing_time": result.get("processing_time", processing_time)
    }

def create_error_response(error_message: str, tenant_id: str) -> Dict[str, Any]:
    """Create safe error response"""
    config = TENANT_CONFIGS.get(tenant_id, {})
    
    return {
        "answer": f"เกิดข้อผิดพลาด: {error_message}",
        "success": False,
        "tenant_id": tenant_id,
        "data_source_used": f"error_{config.get('model', 'default')}",
        "sql_query": None,
        "processing_time": 0.0
    }

# =============================================================================
# CORE ENDPOINTS - เฉพาะที่จำเป็น
# =============================================================================

@app.post("/enhanced-rag-query", response_model=RAGResponse)
async def enhanced_rag_query(
    request: RAGQuery,
    tenant_id: str = Depends(get_tenant_id)
):
    """🎯 Main query endpoint"""
    
    if request.tenant_id and request.tenant_id in TENANT_CONFIGS:
        tenant_id = request.tenant_id
    
    try:
        start_time = time.time()
        
        # Call enhanced agent
        result = await enhanced_agent.process_enhanced_question(request.query, tenant_id)
        processing_time = time.time() - start_time
        
        # Ensure required fields
        fixed_result = ensure_required_fields(result, tenant_id, processing_time)
        
        return RAGResponse(**fixed_result)
        
    except Exception as e:
        logger.error(f"Query processing failed for {tenant_id}: {e}")
        error_response = create_error_response(str(e), tenant_id)
        return RAGResponse(**error_response)

@app.post("/enhanced-rag-query-stream")  # ← Fix OpenWebUI compatibility
@app.post("/enhanced-rag-query-streaming")  # ← Keep original name
async def enhanced_rag_query_streaming(
    request: RAGQuery,
    tenant_id: str = Depends(get_tenant_id)
):
    """🔄 Streaming endpoint"""
    
    if request.tenant_id and request.tenant_id in TENANT_CONFIGS:
        tenant_id = request.tenant_id
    
    async def generate_streaming_response():
        try:
            config = TENANT_CONFIGS[tenant_id]
            
            # Send metadata
            metadata = {
                "type": "metadata",
                "tenant_id": tenant_id,
                "tenant_name": config["name"],
                "model": config["model"],
                "status": "started"
            }
            yield f"data: {json.dumps(metadata)}\n\n"

            # Process with streaming if available
            if hasattr(enhanced_agent, 'process_enhanced_question_streaming'):
                async for chunk in enhanced_agent.process_enhanced_question_streaming(request.query, tenant_id):
                    yield f"data: {json.dumps(chunk)}\n\n"
                    await asyncio.sleep(0.01)
            else:
                # Fallback: simulate streaming
                result = await enhanced_agent.process_enhanced_question(request.query, tenant_id)
                fixed_result = ensure_required_fields(result, tenant_id)
                
                # Send answer in chunks
                answer = fixed_result['answer']
                chunk_size = 50
                
                for i in range(0, len(answer), chunk_size):
                    chunk = answer[i:i+chunk_size]
                    chunk_data = {"type": "answer_chunk", "content": chunk}
                    yield f"data: {json.dumps(chunk_data)}\n\n"
                    await asyncio.sleep(0.05)
                
                # Send completion
                completion_data = {
                    "type": "answer_complete",
                    "sql_query": fixed_result.get('sql_query'),
                    "tenant_id": tenant_id
                }
                yield f"data: {json.dumps(completion_data)}\n\n"
                
        except Exception as e:
            error_data = {"type": "error", "message": f"เกิดข้อผิดพลาด: {str(e)}"}
            yield f"data: {json.dumps(error_data)}\n\n"

    return StreamingResponse(
        generate_streaming_response(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*"
        }
    )

@app.get("/health")
async def health_check():
    """Simple health check"""
    
    # Check agent type
    modular_available = hasattr(enhanced_agent, 'modular_available') and enhanced_agent.modular_available
    
    return {
        "status": "healthy",
        "service": "Siamtemp Simple Multi-Tenant RAG",
        "version": "5.0.0-clean",
        "agent_type": "ModularEnhancedAgent" if modular_available else "EnhancedPostgresOllamaAgent",
        "features": ["simple_intent_logic", "multi_tenant", "streaming"],
        "tenants": list(TENANT_CONFIGS.keys()),
        "ollama_server": os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434'),
        "companies": {
            tid: {
                "name": config["name"],
                "emoji": config["emoji"],
                "language": config["language"],
                "model": config["model"]
            } for tid, config in TENANT_CONFIGS.items()
        }
    }
@app.get("/schema-stats")
async def get_schema_statistics():
    """📊 ดูสถิติของระบบ Intelligent Schema Discovery"""
    try:
        stats = await enhanced_agent.get_intelligent_schema_stats()
        return stats
    except Exception as e:
        raise HTTPException(500, f"Failed to get schema stats: {str(e)}")
    
@app.post("/enhanced-rag-query-streaming-response")
async def enhanced_rag_query_with_streaming_response(
    request: RAGQuery,
    tenant_id: str = Depends(get_tenant_id)
):
    """🌊 New endpoint: Non-streaming SQL + Streaming Response"""
    
    if request.tenant_id and request.tenant_id in TENANT_CONFIGS:
        tenant_id = request.tenant_id
    
    async def generate_selective_streaming():
        try:
            config = TENANT_CONFIGS[tenant_id]
            
            # Send initial metadata
            metadata = {
                "type": "session_start",
                "tenant_id": tenant_id,
                "tenant_name": config["name"],
                "model": config["model"],
                "streaming_mode": "response_only"
            }
            yield f"data: {json.dumps(metadata)}\n\n"
            
            # Process with streaming response
            async for chunk in enhanced_agent._process_sql_unified_with_streaming_response(
                request.query, tenant_id, {"intent": "sql_query", "confidence": 0.8}
            ):
                yield f"data: {json.dumps(chunk)}\n\n"
                
                # Small delay for better UX
                if chunk.get("type") == "response_chunk":
                    await asyncio.sleep(0.03)  # Slightly slower than typing speed
                    
        except Exception as e:
            error_data = {
                "type": "error", 
                "message": f"Streaming failed: {str(e)}",
                "tenant_id": tenant_id
            }
            yield f"data: {json.dumps(error_data)}\n\n"

    return StreamingResponse(
        generate_selective_streaming(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*"
        }
    )

@app.post("/clear-schema-cache")
async def clear_schema_cache(tenant_id: Optional[str] = None):
    """🗑️ ล้าง cache ของ schema discovery"""
    try:
        enhanced_agent.clear_schema_cache(tenant_id)
        return {"message": f"Schema cache cleared for {tenant_id if tenant_id else 'all tenants'}"}
    except Exception as e:
        raise HTTPException(500, f"Failed to clear cache: {str(e)}")
    
@app.get("/tenants")
async def list_tenants():
    """List all tenants"""
    return {
        "tenants": [
            {
                "tenant_id": tid,
                "name": config["name"],
                "emoji": config["emoji"],
                "language": config["language"],
                "business_type": config["business_type"],
                "model": config["model"]
            } for tid, config in TENANT_CONFIGS.items()
        ]
    }

# =============================================================================
# OPENAI COMPATIBILITY - Simple Version
# =============================================================================

@app.post("/v1/chat/completions")
async def openai_chat_completions(
    request: Dict[str, Any],
    tenant_id: str = Depends(get_tenant_id)
):
    """OpenAI-compatible endpoint"""
    
    try:
        messages = request.get("messages", [])
        stream = request.get("stream", False)
        
        if not messages:
            raise HTTPException(400, "No messages provided")
        
        user_message = messages[-1].get("content", "")
        
        if stream:
            # Simple streaming for OpenAI format
            async def generate_openai_streaming():
                try:
                    config = TENANT_CONFIGS[tenant_id]
                    
                    # Initial chunk
                    initial_chunk = {
                        "id": f"chatcmpl-{int(time.time())}",
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": config['model'],
                        "choices": [{
                            "index": 0,
                            "delta": {"role": "assistant", "content": ""},
                            "finish_reason": None
                        }]
                    }
                    yield f"data: {json.dumps(initial_chunk)}\n\n"
                    
                    # Process with enhanced agent
                    result = await enhanced_agent.process_enhanced_question(user_message, tenant_id)
                    fixed_result = ensure_required_fields(result, tenant_id)
                    
                    # Content chunk
                    content_chunk = {
                        "id": f"chatcmpl-{int(time.time())}",
                        "object": "chat.completion.chunk", 
                        "created": int(time.time()),
                        "model": config['model'],
                        "choices": [{
                            "index": 0,
                            "delta": {"content": fixed_result.get('answer', '')},
                            "finish_reason": "stop"
                        }]
                    }
                    yield f"data: {json.dumps(content_chunk)}\n\n"
                    yield "data: [DONE]\n\n"
                    
                except Exception as e:
                    error_chunk = {"error": {"message": str(e)}}
                    yield f"data: {json.dumps(error_chunk)}\n\n"

            return StreamingResponse(
                generate_openai_streaming(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
            )
        else:
            # Non-streaming
            result = await enhanced_agent.process_enhanced_question(user_message, tenant_id)
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
                }
            }
            
    except Exception as e:
        raise HTTPException(500, f"Chat completions failed: {str(e)}")

# =============================================================================
# LEGACY COMPATIBILITY - Simple Aliases
# =============================================================================

@app.post("/rag-query", response_model=RAGResponse)
async def legacy_rag_query(request: RAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """Legacy endpoint alias"""
    return await enhanced_rag_query(request, tenant_id)

# =============================================================================
# MAIN APPLICATION
# =============================================================================

if __name__ == "__main__":
    print("🚀 Starting Siamtemp Simple Multi-Tenant RAG Service")
    print("=" * 60)
    print(f"🎯 Agent type: {type(enhanced_agent).__name__}")
    print(f"🏢 Companies: {len(TENANT_CONFIGS)}")
    for tid, config in TENANT_CONFIGS.items():
        print(f"   {config['emoji']} {tid}: {config['name']}")
    print("🔧 Features: Simple Intent Logic, Multi-tenant, Streaming")
    print("=" * 60)
    
    uvicorn.run(
        "enhanced_multi_agent_service:app",
        host="0.0.0.0",
        port=5000,
        reload=False,
        access_log=True,
        log_level="info"
    )