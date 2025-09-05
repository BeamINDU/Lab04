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
# CONFIGURATION WITH N8N SUPPORT
# =============================================================================

class ProxyConfig:
    def __init__(self):
        # Main service URL
        self.main_service_url = os.getenv('MAIN_SERVICE_URL', 'http://hvac-ai-service:5000')
        self.default_tenant = os.getenv('DEFAULT_TENANT', 'company-a')
        self.force_tenant = os.getenv('FORCE_TENANT')
        self.port = int(os.getenv('PROXY_PORT', '8001'))
        
        # N8N Integration Configuration
        self.use_n8n = os.getenv('USE_N8N_WORKFLOW', 'false').lower() == 'true'
        self.n8n_base_url = os.getenv('N8N_BASE_URL', 'http://13.250.235.228:5678')
        
        # N8N Webhook URLs for each tenant
        self.n8n_webhooks = {
            'company-a': f"{self.n8n_base_url}/webhook/company-a-chat",
            'company-b': f"{self.n8n_base_url}/webhook/company-b-chat",
            'company-c': f"{self.n8n_base_url}/webhook/company-c-chat"
        }
        
        # Fallback configuration
        self.n8n_fallback_to_direct = os.getenv('N8N_FALLBACK_TO_DIRECT', 'true').lower() == 'true'
        self.n8n_timeout = int(os.getenv('N8N_TIMEOUT', '30'))
        
        # Tenant configurations
        self.tenant_configs = {
            'company-a': {
                'name': 'SiamTech Bangkok HQ', 
                'model': 'llama3.1:8b', 
                'language': 'th',
                'use_n8n': True
            },
            'company-b': {
                'name': 'SiamTech Chiang Mai', 
                'model': 'llama3.1:8b', 
                'language': 'th',
                'use_n8n': True
            },
            'company-c': {
                'name': 'SiamTech International', 
                'model': 'llama3.1:8b', 
                'language': 'en',
                'use_n8n': False
            }
        }

config = ProxyConfig()
app = FastAPI(title="OpenWebUI Proxy with N8N", version="3.0.0")

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True, 
    allow_methods=["*"], 
    allow_headers=["*"]
)

# =============================================================================
# REQUEST MODELS
# =============================================================================

class ChatCompletionRequest(BaseModel):
    """OpenAI-compatible request format"""
    model: str
    messages: list
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = 2000
    stream: Optional[bool] = False

# =============================================================================
# N8N WORKFLOW INTEGRATION
# =============================================================================

async def call_n8n_workflow(tenant_id: str, message: str, user_id: str = "default"):
    """Call N8N workflow for message processing"""
    if tenant_id not in config.n8n_webhooks:
        logger.warning(f"No N8N webhook for tenant {tenant_id}")
        return
    
    webhook_url = config.n8n_webhooks[tenant_id]
    
    payload = {
        "message": message,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "timestamp": datetime.now().isoformat(),
        "source": "openwebui_proxy",
        "mode": "streaming"
    }
    
    try:
        logger.info(f"Calling N8N workflow for {tenant_id}: {webhook_url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=config.n8n_timeout)
            ) as response:
                if response.status == 200:
                    logger.info(f"N8N workflow responded for {tenant_id}")
                    
                    content_type = response.headers.get('content-type', '')
                    
                    if 'application/json' in content_type:
                        result = await response.json()
                        yield {
                            "type": "complete",
                            "content": result.get("answer", ""),
                            "sql_query": result.get("sql_query"),
                            "source": "n8n_workflow"
                        }
                    else:
                        async for line in response.content:
                            if line:
                                try:
                                    line_str = line.decode('utf-8').strip()
                                    if line_str.startswith('data: '):
                                        data_str = line_str[6:]
                                        if data_str and data_str != '[DONE]':
                                            data = json.loads(data_str)
                                            data["source"] = "n8n_workflow"
                                            yield data
                                except Exception as e:
                                    logger.debug(f"N8N stream parse error: {e}")
                                    continue
                else:
                    logger.error(f"N8N workflow error: HTTP {response.status}")
                    return
                    
    except asyncio.TimeoutError:
        logger.error(f"N8N workflow timeout for {tenant_id}")
        return
    except Exception as e:
        logger.error(f"N8N workflow failed: {e}")
        return

# =============================================================================
# DIRECT SERVICE CALL
# =============================================================================

async def call_main_service_direct(endpoint: str, payload: dict, stream: bool = False):
    """Direct call to main service"""
    url = f"{config.main_service_url}{endpoint}"
    
    try:
        async with aiohttp.ClientSession() as session:
            if stream:
                async with session.post(
                    url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=120)
                ) as response:
                    if response.status == 200:
                        async for line in response.content:
                            if line:
                                yield line
                    else:
                        logger.error(f"Service error: {response.status}")
                        error_msg = json.dumps({"error": f"Service error: {response.status}"})
                        yield error_msg.encode()
            else:
                async with session.post(
                    url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        yield result
                    else:
                        yield {"error": f"Service error: {response.status}"}
                        
    except Exception as e:
        logger.error(f"Direct service call error: {e}")
        if stream:
            error_msg = json.dumps({"error": str(e)})
            yield error_msg.encode()
        else:
            yield {"error": str(e)}

# =============================================================================
# INTELLIGENT ROUTING
# =============================================================================

async def process_message_with_routing(
    message: str, 
    tenant_id: str, 
    user_id: str = "default",
    stream: bool = False
):
    """Route message through N8N or direct to service"""
    tenant_config = config.tenant_configs.get(tenant_id, {})
    use_n8n = config.use_n8n and tenant_config.get('use_n8n', True)
    
    if use_n8n:
        logger.info(f"Attempting N8N workflow for {tenant_id}")
        
        n8n_success = False
        async for chunk in call_n8n_workflow(tenant_id, message, user_id):
            if chunk:
                n8n_success = True
                yield chunk
        
        if n8n_success:
            return
        
        if not config.n8n_fallback_to_direct:
            yield {"error": "N8N workflow failed and fallback is disabled"}
            return
        
        logger.info(f"Falling back to direct service call for {tenant_id}")
    
    service_payload = {
        "question": message,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "stream": stream
    }
    
    endpoint = "/v1/chat/stream" if stream else "/v1/chat"
    
    async for chunk in call_main_service_direct(endpoint, service_payload, stream):
        if stream:
            try:
                line_str = chunk.decode('utf-8').strip()
                if line_str:
                    data = json.loads(line_str)
                    data["source"] = "direct_service"
                    yield data
            except:
                continue
        else:
            chunk["source"] = "direct_service"
            yield chunk

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_tenant_id(x_tenant_id: Optional[str] = None) -> str:
    """Get tenant ID from header or config"""
    if config.force_tenant:
        return config.force_tenant
    if x_tenant_id:
        return x_tenant_id
    return config.default_tenant

def extract_user_message(messages: list) -> str:
    """Extract user message from OpenAI format"""
    for msg in reversed(messages):
        if isinstance(msg, dict):
            if msg.get("role") == "user":
                return msg.get("content", "")
    return ""

# =============================================================================
# MAIN OPENAI-COMPATIBLE ENDPOINT
# =============================================================================

@app.post("/v1/chat/completions")
async def chat_completions(
    request: ChatCompletionRequest,
    x_tenant_id: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None)
):
    """OpenAI-compatible endpoint with N8N support"""
    tenant_id = get_tenant_id(x_tenant_id)
    user_id = x_user_id or "openwebui_user"
    tenant_config = config.tenant_configs.get(tenant_id, config.tenant_configs['company-a'])
    
    try:
        user_message = extract_user_message(request.messages)
        if not user_message:
            raise HTTPException(400, "No user message found")
        
        routing_method = "N8N + Fallback" if config.use_n8n else "Direct Only"
        logger.info(f"Request for {tenant_id} via {routing_method}: {user_message[:50]}...")
        
        if request.stream:
            async def generate_openai_stream():
                try:
                    # Initial chunk
                    initial_chunk = {
                        "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                        "object": "chat.completion.chunk",
                        "created": int(datetime.now().timestamp()),
                        "model": request.model,
                        "choices": [{
                            "index": 0,
                            "delta": {"role": "assistant", "content": ""},
                            "finish_reason": None
                        }]
                    }
                    yield f"data: {json.dumps(initial_chunk)}\n\n"
                    
                    accumulated_content = ""
                    source_used = "unknown"
                    
                    async for chunk in process_message_with_routing(
                        user_message, tenant_id, user_id, stream=True
                    ):
                        if isinstance(chunk, dict):
                            source_used = chunk.get('source', source_used)
                            
                            if chunk.get('done') or chunk.get('type') == 'complete':
                                if chunk.get('content'):
                                    accumulated_content += chunk['content']
                                    content_chunk = {
                                        "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                                        "object": "chat.completion.chunk",
                                        "created": int(datetime.now().timestamp()),
                                        "model": request.model,
                                        "choices": [{
                                            "index": 0,
                                            "delta": {"content": chunk['content']},
                                            "finish_reason": None
                                        }]
                                    }
                                    yield f"data: {json.dumps(content_chunk)}\n\n"
                                
                                final_chunk = {
                                    "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                                    "object": "chat.completion.chunk",
                                    "created": int(datetime.now().timestamp()),
                                    "model": request.model,
                                    "choices": [{
                                        "index": 0,
                                        "delta": {},
                                        "finish_reason": "stop"
                                    }],
                                    "system_fingerprint": source_used
                                }
                                yield f"data: {json.dumps(final_chunk)}\n\n"
                                yield "data: [DONE]\n\n"
                                break
                            else:
                                content = chunk.get('chunk', chunk.get('content', ''))
                                if content:
                                    accumulated_content += content
                                    stream_chunk = {
                                        "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                                        "object": "chat.completion.chunk",
                                        "created": int(datetime.now().timestamp()),
                                        "model": request.model,
                                        "choices": [{
                                            "index": 0,
                                            "delta": {"content": content},
                                            "finish_reason": None
                                        }]
                                    }
                                    yield f"data: {json.dumps(stream_chunk)}\n\n"
                    
                    if not accumulated_content:
                        error_chunk = {
                            "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                            "object": "chat.completion.chunk",
                            "created": int(datetime.now().timestamp()),
                            "model": request.model,
                            "choices": [{
                                "index": 0,
                                "delta": {"content": "ขออภัย ไม่สามารถประมวลผลได้"},
                                "finish_reason": "stop"
                            }]
                        }
                        yield f"data: {json.dumps(error_chunk)}\n\n"
                        yield "data: [DONE]\n\n"
                    
                    logger.info(f"Streaming completed via {source_used}")
                    
                except Exception as e:
                    logger.error(f"Streaming error: {e}")
                    error_response = {
                        "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                        "object": "chat.completion.chunk",
                        "created": int(datetime.now().timestamp()),
                        "model": request.model,
                        "choices": [{
                            "index": 0,
                            "delta": {"content": f"Error: {str(e)}"},
                            "finish_reason": "stop"
                        }]
                    }
                    yield f"data: {json.dumps(error_response)}\n\n"
                    yield "data: [DONE]\n\n"
            
            return StreamingResponse(
                generate_openai_stream(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no"
                }
            )
        
        else:
            result = None
            source_used = "unknown"
            
            async for chunk in process_message_with_routing(
                user_message, tenant_id, user_id, stream=False
            ):
                if isinstance(chunk, dict):
                    result = chunk
                    source_used = chunk.get('source', source_used)
                    break
            
            if result and not result.get('error'):
                answer = result.get("answer", "ไม่สามารถประมวลผลได้")
                
                logger.info(f"Non-streaming completed via {source_used}")
                
                return {
                    "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                    "object": "chat.completion",
                    "created": int(datetime.now().timestamp()),
                    "model": request.model,
                    "choices": [{
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": answer
                        },
                        "finish_reason": "stop"
                    }],
                    "usage": {
                        "prompt_tokens": len(user_message.split()),
                        "completion_tokens": len(answer.split()),
                        "total_tokens": len(user_message.split()) + len(answer.split())
                    },
                    "system_fingerprint": source_used
                }
            else:
                error_msg = result.get('error', 'Unknown error') if result else 'No response'
                raise HTTPException(500, error_msg)
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        
        return {
            "id": f"chatcmpl-{int(datetime.now().timestamp())}",
            "object": "chat.completion",
            "created": int(datetime.now().timestamp()),
            "model": request.model,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": f"ขออภัย เกิดข้อผิดพลาด: {str(e)}"
                },
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        }

# =============================================================================
# HEALTH & MONITORING ENDPOINTS
# =============================================================================

@app.get("/health")
async def health():
    """Health check with N8N status"""
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{config.main_service_url}/health",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                service_healthy = response.status == 200
    except:
        service_healthy = False
    
    n8n_status = "disabled"
    if config.use_n8n:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    config.n8n_base_url,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    n8n_status = "healthy" if response.status == 200 else f"error_{response.status}"
        except:
            n8n_status = "unreachable"
    
    overall_status = "healthy"
    if not service_healthy:
        overall_status = "degraded" if config.n8n_fallback_to_direct else "unhealthy"
    
    return {
        "status": overall_status,
        "service": "OpenWebUI Proxy with N8N",
        "version": "3.0.0",
        "components": {
            "main_service": "connected" if service_healthy else "disconnected",
            "n8n_workflow": n8n_status,
            "n8n_enabled": config.use_n8n,
            "n8n_fallback": config.n8n_fallback_to_direct
        },
        "routing": "N8N + Fallback" if config.use_n8n else "Direct Only",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/v1/models")
async def list_models():
    """List available models"""
    tenant_id = get_tenant_id()
    tenant_config = config.tenant_configs.get(tenant_id, config.tenant_configs['company-a'])
    
    return {
        "object": "list",
        "data": [{
            "id": tenant_config['model'],
            "object": "model",
            "created": int(datetime.now().timestamp()),
            "owned_by": f"siamtech-{tenant_id}",
            "metadata": {
                "n8n_enabled": config.use_n8n and tenant_config.get('use_n8n', True),
                "language": tenant_config.get('language', 'th')
            }
        }]
    }

@app.get("/n8n/status")
async def n8n_status():
    """Detailed N8N status check"""
    webhooks_status = {}
    
    for tenant, webhook_url in config.n8n_webhooks.items():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url,
                    json={"test": True, "timestamp": datetime.now().isoformat()},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    webhooks_status[tenant] = {
                        "url": webhook_url,
                        "status": "active" if response.status in [200, 201] else f"error_{response.status}"
                    }
        except Exception as e:
            webhooks_status[tenant] = {
                "url": webhook_url,
                "status": "unreachable",
                "error": str(e)
            }
    
    return {
        "n8n_base_url": config.n8n_base_url,
        "n8n_enabled": config.use_n8n,
        "fallback_enabled": config.n8n_fallback_to_direct,
        "timeout": config.n8n_timeout,
        "webhooks": webhooks_status,
        "timestamp": datetime.now().isoformat()
    }

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("OpenWebUI Proxy with N8N Integration")
    print("=" * 60)
    print(f"Main Service: {config.main_service_url}")
    print(f"N8N URL: {config.n8n_base_url}")
    print(f"N8N Enabled: {config.use_n8n}")
    print(f"Fallback: {config.n8n_fallback_to_direct}")
    print(f"Proxy Port: {config.port}")
    print(f"Default Tenant: {config.default_tenant}")
    print("=" * 60)
    print("Endpoints:")
    print("  POST /v1/chat/completions - OpenAI-compatible chat")
    print("  GET  /health - Health check")
    print("  GET  /n8n/status - N8N status details")
    print("  GET  /v1/models - List models")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=config.port)