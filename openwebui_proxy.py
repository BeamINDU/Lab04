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
# ENHANCED CONFIGURATION WITH N8N SUPPORT
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
        
        # N8N Webhook URLs for each company
        self.n8n_webhooks = {
            'company-a': f"{self.n8n_base_url}/webhook/company-a-chat",
            'company-b': f"{self.n8n_base_url}/webhook/company-b-chat", 
            'company-c': f"{self.n8n_base_url}/webhook/company-c-chat"
        }
        
        # Tenant configurations
        self.tenant_configs = {
            'company-a': {'name': 'Siamtemp Bangkok HQ', 'model': 'llama3.1:8b', 'language': 'th'},
            'company-b': {'name': 'Siamtemp Chiang Mai Regional', 'model': 'llama3.1:8b', 'language': 'th'},
            'company-c': {'name': 'Siamtemp International', 'model': 'llama3.1:8b', 'language': 'en'}
        }

config = ProxyConfig()
app = FastAPI(title=f"OpenWebUI N8N Proxy v4.0", version="4.0.0")

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

# =============================================================================
# 🌐 N8N WORKFLOW INTEGRATION FUNCTIONS
# =============================================================================

async def call_n8n_workflow(tenant_id: str, message: str):
    """🌐 Call N8N workflow for processing"""
    
    if tenant_id not in config.n8n_webhooks:
        logger.error(f"No N8N webhook configured for tenant: {tenant_id}")
        async for chunk in call_rag_service_direct(tenant_id, message):
            yield chunk
        return
    
    webhook_url = config.n8n_webhooks[tenant_id]
    payload = {
        "message": message,
        "tenant_id": tenant_id,
        "timestamp": datetime.now().isoformat(),
        "source": "openwebui_proxy",
        "use_streaming": True
    }
    
    try:
        logger.info(f"🌐 Calling N8N workflow for {tenant_id}: {webhook_url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url, 
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=120)
            ) as response:
                if response.status == 200:
                    logger.info(f"✅ N8N workflow responded for {tenant_id}")
                    
                    # Handle different response types
                    content_type = response.headers.get('content-type', '')
                    
                    if 'application/json' in content_type:
                        # Single JSON response
                        result = await response.json()
                        yield {
                            "type": "answer",
                            "content": result.get("answer", ""),
                            "sql_query": result.get("sql_query"),
                            "source": "n8n_workflow"
                        }
                    else:
                        # Streaming response
                        async for line in response.content:
                            if line:
                                try:
                                    line_str = line.decode('utf-8').strip()
                                    if line_str:
                                        data = json.loads(line_str)
                                        data["source"] = "n8n_workflow"
                                        yield data
                                except json.JSONDecodeError:
                                    continue
                else:
                    logger.error(f"❌ N8N workflow error for {tenant_id}: HTTP {response.status}")
                    yield {"type": "error", "message": f"N8N workflow error: HTTP {response.status}"}
                    
    except asyncio.TimeoutError:
        logger.error(f"⏰ N8N workflow timeout for {tenant_id}, falling back to direct RAG")
        async for chunk in call_rag_service_direct(tenant_id, message):
            yield chunk
    except Exception as e:
        logger.error(f"🔄 N8N workflow failed for {tenant_id}: {e}, falling back to direct RAG")
        async for chunk in call_rag_service_direct(tenant_id, message):
            yield chunk

async def call_rag_service_direct(tenant_id: str, message: str):
    """🔄 Direct RAG service call (fallback)"""
    
    payload = {
        "query": message,
        "tenant_id": tenant_id,
        "agent_type": "enhanced_sql",
        "include_metadata": True
    }
    
    try:
        logger.info(f"🔄 Direct RAG call for {tenant_id}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{config.rag_service_url}/enhanced-rag-query",
                json=payload,
                headers={"X-Tenant-ID": tenant_id},
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    yield {
                        "type": "answer",
                        "content": result.get("answer", ""),
                        "sql_query": result.get("sql_query"),
                        "source": "direct_rag"
                    }
                else:
                    yield {"type": "error", "message": f"RAG service error: HTTP {response.status}"}
                    
    except Exception as e:
        logger.error(f"❌ Direct RAG failed for {tenant_id}: {e}")
        yield {"type": "error", "message": f"การเชื่อมต่อมีปัญหา: {str(e)}"}

# =============================================================================
# 🌐 MAIN PROCESSING FUNCTION WITH N8N INTEGRATION
# =============================================================================

async def process_chat_request(tenant_id: str, message: str, stream: bool = False):
    """🎯 Main processing with N8N workflow integration"""
    
    if config.use_n8n:
        # Route through N8N workflow
        logger.info(f"🌐 Using N8N workflow for {tenant_id}")
        async for chunk in call_n8n_workflow(tenant_id, message):
            yield chunk
    else:
        # Direct RAG service
        logger.info(f"🔄 Using direct RAG for {tenant_id}")
        async for chunk in call_rag_service_direct(tenant_id, message):
            yield chunk

# =============================================================================
# 🎯 MAIN STREAMING ENDPOINT
# =============================================================================

async def process_chat_request_with_response_streaming(tenant_id: str, message: str):
    """🎯 Enhanced processing with response streaming only"""
    
    payload = {
        "query": message,
        "tenant_id": tenant_id
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{config.rag_service_url}/enhanced-rag-query-streaming-response",
                json=payload,
                headers={"X-Tenant-ID": tenant_id},
                timeout=aiohttp.ClientTimeout(total=120)
            ) as response:
                
                if response.status == 200:
                    async for line in response.content:
                        if line:
                            try:
                                line_str = line.decode('utf-8').strip()
                                if line_str.startswith('data: '):
                                    data_str = line_str[6:]  # Remove 'data: '
                                    if data_str and data_str != '[DONE]':
                                        chunk_data = json.loads(data_str)
                                        yield chunk_data
                            except json.JSONDecodeError:
                                continue
                                
    except Exception as e:
        yield {"type": "error", "message": f"Connection failed: {str(e)}"}

@app.post("/v1/chat/completions")
async def chat_completions_streaming(request: ChatCompletionRequest):
    """🎯 OpenAI-compatible endpoint with N8N workflow support"""
    
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
        
        logger.info(f"🎯 Processing request for {tenant_id}: {user_message[:50]}...")
        
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
                        }]
                    }
                    yield f"data: {json.dumps(initial_chunk)}\n\n"
                    
                    # Process through N8N or direct RAG
                    full_answer = ""
                    
                    async for chunk in process_chat_request_with_response_streaming(tenant_id, user_message):
                        chunk_type = chunk.get("type", "")
                        
                        if chunk_type == "response_chunk":
                            # Convert to OpenAI format
                            content_chunk = {
                                "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                                "object": "chat.completion.chunk",
                                "created": int(datetime.now().timestamp()),
                                "model": tenant_config['model'],
                                "choices": [{
                                    "index": 0,
                                    "delta": {"content": chunk.get("content", "")},
                                    "finish_reason": None
                                }]
                            }
                            yield f"data: {json.dumps(content_chunk)}\n\n"
                            
                        elif chunk_type == "response_complete":
                            # Send final chunk
                            final_chunk = {
                                "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                                "object": "chat.completion.chunk",
                                "created": int(datetime.now().timestamp()),
                                "model": tenant_config['model'],
                                "choices": [{
                                    "index": 0,
                                    "delta": {},
                                    "finish_reason": "stop"
                                }]
                            }
                            yield f"data: {json.dumps(final_chunk)}\n\n"
                            yield "data: [DONE]\n\n"
                            break
                            
                        elif chunk_type == "error":
                            error_chunk = {
                                "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                                "object": "chat.completion.chunk",
                                "created": int(datetime.now().timestamp()),
                                "model": tenant_config['model'],
                                "choices": [{
                                    "index": 0,
                                    "delta": {"content": f"\n❌ {chunk.get('message', 'เกิดข้อผิดพลาด')}"},
                                    "finish_reason": "stop"
                                }]
                            }
                            yield f"data: {json.dumps(error_chunk)}\n\n"
                            yield "data: [DONE]\n\n"
                            return
                    
                    # Send final chunk
                    final_chunk = {
                        "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                        "object": "chat.completion.chunk",
                        "created": int(datetime.now().timestamp()),
                        "model": tenant_config['model'],
                        "choices": [{
                            "index": 0,
                            "delta": {},
                            "finish_reason": "stop"
                        }]
                    }
                    yield f"data: {json.dumps(final_chunk)}\n\n"
                    yield "data: [DONE]\n\n"
                    
                except Exception as e:
                    logger.error(f"🔥 Streaming error: {e}")
                    error_chunk = {
                        "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                        "object": "chat.completion.chunk",
                        "created": int(datetime.now().timestamp()),
                        "model": tenant_config['model'],
                        "choices": [{
                            "index": 0,
                            "delta": {"content": f"\n❌ เกิดข้อผิดพลาดในระบบ: {str(e)}"},
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
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "*"
                }
            )
        
        # 🔄 Non-streaming
        else:
            full_answer = ""
            
            async for chunk in process_chat_request(tenant_id, user_message, stream=False):
                if chunk.get("type") == "answer":
                    full_answer += chunk.get("content", "")
                elif chunk.get("type") == "error":
                    full_answer = chunk.get("message", "เกิดข้อผิดพลาด")
                    break
            
            return {
                "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                "object": "chat.completion",
                "created": int(datetime.now().timestamp()),
                "model": tenant_config['model'],
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": full_answer},
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": len(user_message.split()),
                    "completion_tokens": len(full_answer.split()),
                    "total_tokens": len(user_message.split()) + len(full_answer.split())
                }
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"🔥 Unexpected error: {e}")
        return {
            "id": f"chatcmpl-{int(datetime.now().timestamp())}",
            "object": "chat.completion",
            "created": int(datetime.now().timestamp()),
            "model": tenant_config['model'],
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": f"เกิดข้อผิดพลาดในระบบ: {str(e)}"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            "error": str(e)
        }

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_tenant_id() -> str:
    return config.force_tenant or config.default_tenant

def get_tenant_config(tenant_id: str) -> Dict:
    return config.tenant_configs.get(tenant_id, config.tenant_configs['company-a'])

# =============================================================================
# HEALTH AND STATUS ENDPOINTS
# =============================================================================

@app.get("/health")
async def health():
    tenant_id = get_tenant_id()
    tenant_config = get_tenant_config(tenant_id)
    
    # Test N8N connectivity
    n8n_status = "unknown"
    if config.use_n8n:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{config.n8n_base_url}/healthz", timeout=aiohttp.ClientTimeout(total=5)) as response:
                    n8n_status = "healthy" if response.status == 200 else f"error_{response.status}"
        except:
            n8n_status = "unreachable"
    else:
        n8n_status = "disabled"
    
    return {
        "status": "healthy",
        "service": "OpenWebUI N8N Proxy",
        "version": "4.0.0",
        "tenant_id": tenant_id,
        "tenant_name": tenant_config['name'],
        "model": tenant_config['model'],
        "architecture": "openwebui_proxy_n8n_rag",
        "n8n_integration": {
            "enabled": config.use_n8n,
            "status": n8n_status,
            "base_url": config.n8n_base_url,
            "webhooks_configured": len(config.n8n_webhooks)
        },
        "rag_service": config.rag_service_url,
        "streaming_enabled": True,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/v1/models")
async def list_models():
    tenant_id = get_tenant_id()
    tenant_config = get_tenant_config(tenant_id)
    
    return {
        "object": "list",
        "data": [{
            "id": "llama3.1:8b",
            "object": "model",
            "created": int(datetime.now().timestamp()),
            "owned_by": f"siamtemp-{tenant_id}",
            "streaming_supported": True,
            "n8n_workflow_enabled": config.use_n8n,
            "siamtemp_metadata": {
                "tenant_id": tenant_id,
                "tenant_name": tenant_config['name'],
                "language": tenant_config['language'],
                "proxy_version": "4.0.0",
                "workflow_integration": "n8n_enhanced"
            }
        }]
    }

# =============================================================================
# MAIN APPLICATION
# =============================================================================

if __name__ == "__main__":
    tenant_id = get_tenant_id()
    tenant_config = get_tenant_config(tenant_id)
    
    print("🔗 OpenWebUI Streaming Proxy v3.0")
    print("=" * 50)
    print(f"🏢 Tenant: {tenant_config['name']} ({tenant_id})")
    print(f"🤖 Model: {tenant_config['model']}")
    print(f"🌐 Language: {tenant_config['language']}")
    print(f"📡 RAG Service: {config.rag_service_url}")
    print(f"🔧 Port: {config.port}")
    print(f"🔥 Streaming: ENABLED")
    print("=" * 50)
    
    uvicorn.run("openwebui_proxy:app", host="0.0.0.0", port=config.port, reload=False)