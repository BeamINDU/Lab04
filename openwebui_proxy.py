# openwebui_proxy.py
# 🚀 ปรับปรุงให้รองรับ Dynamic AI System

import os
import json
import asyncio
import aiohttp
from datetime import datetime
from typing import Dict, Any, Optional
import uvicorn
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# ENHANCED CONFIGURATION WITH DYNAMIC AI SUPPORT
# =============================================================================

class ProxyConfig:
    def __init__(self):
        # Original configuration
        self.rag_service_url = os.getenv('ENHANCED_RAG_SERVICE', 'http://rag-service:5000')
        self.default_tenant = os.getenv('DEFAULT_TENANT', 'company-a')
        self.force_tenant = os.getenv('FORCE_TENANT')
        self.port = int(os.getenv('PORT', '8001'))
        
        # 🆕 N8N Integration Configuration
        self.use_n8n = os.getenv('USE_N8N_WORKFLOW', 'true').lower() == 'true'
        self.n8n_base_url = os.getenv('N8N_BASE_URL', 'http://n8n:5678')
        
        # 🚀 Dynamic AI Configuration
        self.use_dynamic_ai = os.getenv('USE_DYNAMIC_AI', 'true').lower() == 'true'
        self.dynamic_ai_fallback = os.getenv('DYNAMIC_AI_FALLBACK', 'true').lower() == 'true'
        
        # N8N Webhook URLs for each company (Updated for HVAC)
        self.n8n_webhooks = {
            'company-a': f"{self.n8n_base_url}/webhook/hvac-company-a-chat",
            'company-b': f"{self.n8n_base_url}/webhook/hvac-company-b-chat", 
            'company-c': f"{self.n8n_base_url}/webhook/hvac-company-c-chat"
        }
        
        # Tenant configurations (Updated for HVAC Business)
        self.tenant_configs = {
            'company-a': {
                'name': 'Siamtemp Bangkok HQ (HVAC Service)',
                'model': 'llama3.1:8b',
                'language': 'th',
                'business_type': 'hvac_service_spare_parts',
                'emoji': '🔧'
            },
            'company-b': {
                'name': 'Siamtemp Chiang Mai Regional (HVAC)',
                'model': 'llama3.1:8b',
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

config = ProxyConfig()
app = FastAPI(title=f"OpenWebUI HVAC Dynamic AI Proxy v5.0", version="5.0.0")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# =============================================================================
# MODELS
# =============================================================================

class ChatCompletionRequest(BaseModel):
    model: str
    messages: list
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = 2000
    stream: Optional[bool] = False
    use_dynamic_ai: Optional[bool] = True  # 🆕 Control Dynamic AI usage

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_tenant_id() -> str:
    if config.force_tenant:
        return config.force_tenant
    return config.default_tenant

def get_tenant_config(tenant_id: str) -> Dict[str, Any]:
    return config.tenant_configs.get(tenant_id, config.tenant_configs['company-a'])

# =============================================================================
# 🚀 DYNAMIC AI INTEGRATION FUNCTIONS
# =============================================================================

async def call_dynamic_ai_service(tenant_id: str, message: str, use_dynamic_ai: bool = True):
    """🚀 Call Dynamic AI Service"""
    
    # เลือก endpoint ตามการตั้งค่า
    if use_dynamic_ai and config.use_dynamic_ai:
        endpoint = "/enhanced-rag-query"
        payload = {
            "question": message,
            "tenant_id": tenant_id,
            "use_dynamic_ai": True
        }
    else:
        endpoint = "/enhanced-rag-query"
        payload = {
            "question": message,
            "tenant_id": tenant_id,
            "use_dynamic_ai": False
        }
    
    try:
        logger.info(f"🚀 Calling Dynamic AI Service: {endpoint}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{config.rag_service_url}{endpoint}",
                json=payload,
                headers={"X-Tenant-ID": tenant_id},
                timeout=aiohttp.ClientTimeout(total=120)
            ) as response:
                
                if response.status == 200:
                    result = await response.json()
                    
                    # Log AI system used
                    ai_system = result.get('ai_system_used', 'unknown')
                    logger.info(f"✅ Dynamic AI responded using: {ai_system}")
                    
                    yield {
                        "type": "answer",
                        "content": result.get("answer", ""),
                        "sql_query": result.get("sql_query"),
                        "results_count": result.get("results_count", 0),
                        "ai_system_used": ai_system,
                        "processing_time": result.get("processing_time", 0),
                        "source": "dynamic_ai_service"
                    }
                else:
                    logger.error(f"❌ Dynamic AI Service error: {response.status}")
                    yield {
                        "type": "error",
                        "message": f"Dynamic AI Service returned {response.status}"
                    }
                    
    except Exception as e:
        logger.error(f"❌ Dynamic AI Service connection failed: {e}")
        yield {
            "type": "error", 
            "message": f"Failed to connect to Dynamic AI Service: {str(e)}"
        }

# =============================================================================
# 🌐 N8N WORKFLOW INTEGRATION (Updated for Dynamic AI)
# =============================================================================

async def call_n8n_workflow(tenant_id: str, message: str, use_dynamic_ai: bool = True):
    """🌐 Call N8N workflow with Dynamic AI support"""
    
    if tenant_id not in config.n8n_webhooks:
        logger.error(f"No N8N webhook configured for tenant: {tenant_id}")
        async for chunk in call_dynamic_ai_service(tenant_id, message, use_dynamic_ai):
            yield chunk
        return
    
    webhook_url = config.n8n_webhooks[tenant_id]
    payload = {
        "message": message,
        "tenant_id": tenant_id,
        "use_dynamic_ai": use_dynamic_ai,  # 🆕 Pass Dynamic AI preference
        "timestamp": datetime.now().isoformat(),
        "source": "openwebui_proxy_v5",
        "proxy_version": "5.0.0",
        "business_type": config.tenant_configs[tenant_id]['business_type']
    }
    
    try:
        logger.info(f"🌐 Calling N8N HVAC workflow for {tenant_id}: {webhook_url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url, 
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=120)
            ) as response:
                if response.status == 200:
                    logger.info(f"✅ N8N HVAC workflow responded for {tenant_id}")
                    
                    # Handle different response types
                    content_type = response.headers.get('content-type', '')
                    
                    if 'application/json' in content_type:
                        # Single JSON response
                        result = await response.json()
                        yield {
                            "type": "answer",
                            "content": result.get("answer", ""),
                            "sql_query": result.get("sql_query"),
                            "ai_system_used": result.get("ai_system_used", "n8n_workflow"),
                            "source": "n8n_hvac_workflow"
                        }
                    else:
                        # Streaming response
                        async for line in response.content:
                            if line:
                                try:
                                    line_str = line.decode('utf-8').strip()
                                    if line_str and line_str.startswith('data: '):
                                        data_str = line_str[6:]
                                        if data_str != '[DONE]':
                                            chunk_data = json.loads(data_str)
                                            yield chunk_data
                                except json.JSONDecodeError:
                                    continue
                else:
                    logger.error(f"❌ N8N workflow error: {response.status}")
                    # Fallback to direct Dynamic AI service
                    async for chunk in call_dynamic_ai_service(tenant_id, message, use_dynamic_ai):
                        yield chunk
                        
    except Exception as e:
        logger.error(f"❌ N8N workflow failed: {e}")
        # Fallback to direct Dynamic AI service
        async for chunk in call_dynamic_ai_service(tenant_id, message, use_dynamic_ai):
            yield chunk

# =============================================================================
# 🎯 MAIN PROCESSING FUNCTION WITH DYNAMIC AI
# =============================================================================

async def process_chat_request(tenant_id: str, message: str, use_dynamic_ai: bool = True, stream: bool = False):
    """🎯 Main processing with Dynamic AI and N8N workflow integration"""
    
    if config.use_n8n:
        # Route through N8N workflow (with Dynamic AI support)
        logger.info(f"🌐 Using N8N HVAC workflow for {tenant_id} (Dynamic AI: {use_dynamic_ai})")
        async for chunk in call_n8n_workflow(tenant_id, message, use_dynamic_ai):
            yield chunk
    else:
        # Direct Dynamic AI service
        logger.info(f"🚀 Using direct Dynamic AI for {tenant_id}")
        async for chunk in call_dynamic_ai_service(tenant_id, message, use_dynamic_ai):
            yield chunk

# =============================================================================
# 🎯 MAIN STREAMING ENDPOINT (Updated for Dynamic AI)
# =============================================================================

@app.post("/v1/chat/completions")
async def chat_completions_streaming(request: ChatCompletionRequest):
    """🎯 OpenAI-compatible endpoint with Dynamic AI support"""
    
    tenant_id = get_tenant_id()
    tenant_config = get_tenant_config(tenant_id)
    
    try:
        if not request.messages:
            raise HTTPException(400, "No messages provided")
        
        # Extract user message
        user_message = ""
        for msg in request.messages:
            if hasattr(msg, 'dict'):
                msg_data = msg.dict()
            elif isinstance(msg, dict):
                msg_data = msg
            else:
                msg_data = {"role": "user", "content": str(msg)}
            
            if msg_data.get("role") == "user":
                user_message = msg_data.get("content", "")
        
        if not user_message:
            raise HTTPException(400, "No user message found")
        
        # Get Dynamic AI preference
        use_dynamic_ai = getattr(request, 'use_dynamic_ai', True)
        
        logger.info(f"🎯 Processing HVAC request for {tenant_id}: {user_message[:50]}...")
        logger.info(f"🚀 Dynamic AI: {use_dynamic_ai}")
        
        # 🚀 Streaming response
        if request.stream:
            async def generate_openai_streaming():
                try:
                    # Send initial chunk
                    initial_chunk = {
                        "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                        "object": "chat.completion.chunk",
                        "created": int(datetime.now().timestamp()),
                        "model": tenant_config['model'],
                        "choices": [{
                            "index": 0,
                            "delta": {"role": "assistant", "content": ""},
                            "finish_reason": None
                        }],
                        "siamtemp_metadata": {
                            "tenant_id": tenant_id,
                            "business_type": tenant_config['business_type'],
                            "dynamic_ai_enabled": use_dynamic_ai,
                            "proxy_version": "5.0.0"
                        }
                    }
                    yield f"data: {json.dumps(initial_chunk)}\n\n"
                    
                    # Process through Dynamic AI or N8N
                    full_answer = ""
                    ai_system_used = "unknown"
                    
                    async for chunk in process_chat_request(tenant_id, user_message, use_dynamic_ai, stream=True):
                        chunk_type = chunk.get("type", "")
                        
                        if chunk_type == "answer":
                            content = chunk.get("content", "")
                            ai_system_used = chunk.get("ai_system_used", "dynamic_ai")
                            
                            # Split into smaller chunks for better streaming experience
                            sentences = content.split('. ')
                            for i, sentence in enumerate(sentences):
                                if sentence.strip():
                                    chunk_content = sentence + '. ' if i < len(sentences) - 1 else sentence
                                    
                                    content_chunk = {
                                        "id": f"chatcmpl-{int(datetime.now().timestamp())}-{i}",
                                        "object": "chat.completion.chunk",
                                        "created": int(datetime.now().timestamp()),
                                        "model": tenant_config['model'],
                                        "choices": [{
                                            "index": 0,
                                            "delta": {"content": chunk_content},
                                            "finish_reason": None
                                        }]
                                    }
                                    yield f"data: {json.dumps(content_chunk)}\n\n"
                                    await asyncio.sleep(0.05)  # Small delay for streaming effect
                        
                        elif chunk_type == "error":
                            error_chunk = {
                                "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                                "object": "chat.completion.chunk",
                                "created": int(datetime.now().timestamp()),
                                "model": tenant_config['model'],
                                "choices": [{
                                    "index": 0,
                                    "delta": {"content": f"❌ Error: {chunk.get('message', 'Unknown error')}"},
                                    "finish_reason": "stop"
                                }]
                            }
                            yield f"data: {json.dumps(error_chunk)}\n\n"
                    
                    # Final chunk
                    final_chunk = {
                        "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                        "object": "chat.completion.chunk",
                        "created": int(datetime.now().timestamp()),
                        "model": tenant_config['model'],
                        "choices": [{
                            "index": 0,
                            "delta": {},
                            "finish_reason": "stop"
                        }],
                        "siamtemp_metadata": {
                            "ai_system_used": ai_system_used,
                            "proxy_version": "5.0.0",
                            "total_chunks": len(sentences) if 'sentences' in locals() else 1
                        }
                    }
                    yield f"data: {json.dumps(final_chunk)}\n\n"
                    yield "data: [DONE]\n\n"
                    
                except Exception as e:
                    logger.error(f"❌ Streaming error: {e}")
                    error_chunk = {
                        "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                        "object": "chat.completion.chunk",
                        "created": int(datetime.now().timestamp()),
                        "model": tenant_config['model'],
                        "choices": [{
                            "index": 0,
                            "delta": {"content": f"❌ Streaming error: {str(e)}"},
                            "finish_reason": "stop"
                        }]
                    }
                    yield f"data: {json.dumps(error_chunk)}\n\n"
                    yield "data: [DONE]\n\n"
            
            return StreamingResponse(
                generate_openai_streaming(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no"  # Disable Nginx buffering
                }
            )
        
        else:
            # 🚀 Non-streaming response with Dynamic AI
            full_response = ""
            ai_system_used = "unknown"
            sql_query = None
            results_count = 0
            processing_time = 0
            
            async for chunk in process_chat_request(tenant_id, user_message, use_dynamic_ai, stream=False):
                chunk_type = chunk.get("type", "")
                
                if chunk_type == "answer":
                    full_response = chunk.get("content", "")
                    ai_system_used = chunk.get("ai_system_used", "dynamic_ai")
                    sql_query = chunk.get("sql_query")
                    results_count = chunk.get("results_count", 0)
                    processing_time = chunk.get("processing_time", 0)
                elif chunk_type == "error":
                    full_response = f"❌ Error: {chunk.get('message', 'Unknown error')}"
                    ai_system_used = "error_handler"
            
            return {
                "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                "object": "chat.completion",
                "created": int(datetime.now().timestamp()),
                "model": tenant_config['model'],
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": full_response},
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": len(user_message.split()),
                    "completion_tokens": len(full_response.split()),
                    "total_tokens": len(user_message.split()) + len(full_response.split())
                },
                "siamtemp_metadata": {
                    "tenant_id": tenant_id,
                    "tenant_name": tenant_config['name'],
                    "business_type": tenant_config['business_type'],
                    "ai_system_used": ai_system_used,
                    "dynamic_ai_enabled": use_dynamic_ai,
                    "sql_query": sql_query,
                    "results_count": results_count,
                    "processing_time": processing_time,
                    "proxy_version": "5.0.0"
                }
            }
            
    except Exception as e:
        logger.error(f"❌ Chat completions failed: {e}")
        raise HTTPException(500, f"Chat completions failed: {str(e)}")

# =============================================================================
# 📊 HEALTH CHECK AND MONITORING
# =============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint with Dynamic AI status"""
    
    tenant_id = get_tenant_id()
    tenant_config = get_tenant_config(tenant_id)
    
    # Test RAG Service connection
    rag_status = "unknown"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{config.rag_service_url}/health", 
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    rag_data = await response.json()
                    rag_status = "healthy"
                    dynamic_ai_available = rag_data.get("dynamic_ai_available", False)
                else:
                    rag_status = f"error_{response.status}"
                    dynamic_ai_available = False
    except:
        rag_status = "unreachable"
        dynamic_ai_available = False
    
    # Test N8N connection
    n8n_status = "disabled"
    if config.use_n8n:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{config.n8n_base_url}/healthz",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    n8n_status = "healthy" if response.status == 200 else f"error_{response.status}"
        except:
            n8n_status = "unreachable"
    
    return {
        "status": "healthy",
        "service": "OpenWebUI HVAC Dynamic AI Proxy",
        "version": "5.0.0",
        "tenant_id": tenant_id,
        "tenant_name": tenant_config['name'],
        "business_type": tenant_config['business_type'],
        "model": tenant_config['model'],
        "architecture": "openwebui_proxy_dynamic_ai_n8n",
        "dynamic_ai": {
            "enabled": config.use_dynamic_ai,
            "available": dynamic_ai_available,
            "fallback_enabled": config.dynamic_ai_fallback
        },
        "n8n_integration": {
            "enabled": config.use_n8n,
            "status": n8n_status,
            "base_url": config.n8n_base_url,
            "webhooks_configured": len(config.n8n_webhooks)
        },
        "rag_service": {
            "url": config.rag_service_url,
            "status": rag_status
        },
        "streaming_enabled": True,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/v1/models")
async def list_models():
    """List available models with Dynamic AI info"""
    
    tenant_id = get_tenant_id()
    tenant_config = get_tenant_config(tenant_id)
    
    return {
        "object": "list",
        "data": [{
            "id": tenant_config['model'],
            "object": "model",
            "created": int(datetime.now().timestamp()),
            "owned_by": f"siamtemp-{tenant_id}",
            "streaming_supported": True,
            "n8n_workflow_enabled": config.use_n8n,
            "dynamic_ai_enabled": config.use_dynamic_ai,
            "siamtemp_metadata": {
                "tenant_id": tenant_id,
                "tenant_name": tenant_config['name'],
                "business_type": tenant_config['business_type'],
                "language": tenant_config['language'],
                "emoji": tenant_config['emoji'],
                "proxy_version": "5.0.0",
                "workflow_integration": "n8n_dynamic_ai",
                "capabilities": [
                    "Standard Enhanced AI",
                    "Dynamic AI (Any Question)" if config.use_dynamic_ai else "Dynamic AI (Disabled)",
                    "N8N Workflow Integration" if config.use_n8n else "Direct RAG",
                    "Real-time Schema Discovery",
                    "HVAC Business Intelligence"
                ]
            }
        }]
    }

# =============================================================================
# 🔧 DEBUGGING AND TESTING ENDPOINTS
# =============================================================================

@app.post("/test-dynamic-ai-proxy")
async def test_dynamic_ai_via_proxy(request: dict):
    """Test Dynamic AI through proxy"""
    
    tenant_id = get_tenant_id()
    question = request.get("question", "test question")
    
    try:
        results = []
        async for chunk in call_dynamic_ai_service(tenant_id, question, use_dynamic_ai=True):
            results.append(chunk)
        
        return {
            "proxy_test": "success",
            "question": question,
            "tenant_id": tenant_id,
            "chunks_received": len(results),
            "results": results
        }
        
    except Exception as e:
        return {
            "proxy_test": "failed",
            "error": str(e)
        }

@app.get("/proxy-info")
async def proxy_info():
    """Get detailed proxy information"""
    
    tenant_id = get_tenant_id()
    
    return {
        "proxy_service": "OpenWebUI HVAC Dynamic AI Proxy",
        "version": "5.0.0",
        "configuration": {
            "default_tenant": config.default_tenant,
            "force_tenant": config.force_tenant,
            "use_n8n": config.use_n8n,
            "use_dynamic_ai": config.use_dynamic_ai,
            "dynamic_ai_fallback": config.dynamic_ai_fallback,
            "rag_service_url": config.rag_service_url,
            "n8n_base_url": config.n8n_base_url
        },
        "tenant_configs": config.tenant_configs,
        "n8n_webhooks": config.n8n_webhooks,
        "current_tenant": {
            "id": tenant_id,
            "config": get_tenant_config(tenant_id)
        }
    }

# =============================================================================
# MAIN APPLICATION
# =============================================================================

if __name__ == "__main__":
    tenant_id = get_tenant_id()
    tenant_config = get_tenant_config(tenant_id)
    
    print("🔗 OpenWebUI HVAC Dynamic AI Proxy v5.0")
    print("=" * 60)
    print(f"🏢 Tenant: {tenant_config['name']} ({tenant_id})")
    print(f"🔧 Business: {tenant_config['business_type']}")
    print(f"🤖 Model: {tenant_config['model']}")
    print(f"🌐 Language: {tenant_config['language']}")
    print(f"📡 RAG Service: {config.rag_service_url}")
    print(f"🚀 Dynamic AI: {'✅ ENABLED' if config.use_dynamic_ai else '❌ DISABLED'}")
    print(f"🌐 N8N Workflows: {'✅ ENABLED' if config.use_n8n else '❌ DISABLED'}")
    print(f"🔧 Port: {config.port}")
    print(f"🔥 Streaming: ENABLED")
    print("=" * 60)
    
    uvicorn.run("openwebui_proxy:app", host="0.0.0.0", port=config.port, reload=False)