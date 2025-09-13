# openwebui_proxy.py - Updated with n8n Support
"""
OpenWebUI Proxy with n8n Workflow Integration
เพิ่มการรองรับ n8n workflow routing
"""

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
        
        # ========== เพิ่ม n8n Configuration ==========
        self.use_n8n_workflow = os.getenv('USE_N8N_WORKFLOW', 'false').lower() == 'true'
        self.n8n_base_url = os.getenv('N8N_BASE_URL', 'http://13.250.235.228:5678')  # ใช้ชื่อ container
        self.n8n_webhook_path = 'webhook/siamtemp-chat'  # path ของ webhook ใน n8n
        self.n8n_timeout = int(os.getenv('N8N_TIMEOUT', '60'))
        self.n8n_fallback_to_direct = os.getenv('N8N_FALLBACK_TO_DIRECT', 'true').lower() == 'true'
        self.block_system_prompts = os.getenv('BLOCK_SYSTEM_PROMPTS', 'true').lower() == 'true'
        # Tenant configurations
        self.tenant_configs = {
            'company-a': {'name': 'siamtemp Bangkok HQ', 'model': 'AI', 'language': 'th'},
        }

config = ProxyConfig()
app = FastAPI(title="OpenWebUI Proxy", version="2.0.0")

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
# N8N WORKFLOW INTEGRATION (เพิ่มใหม่)
# =============================================================================

async def call_n8n_workflow(message: str, tenant_id: str, user_id: str = "default") -> Optional[Dict]:
    """Call n8n workflow for processing"""
    
    if not config.use_n8n_workflow:
        return None
    
    webhook_url = f"{config.n8n_base_url}/{config.n8n_webhook_path}"
    
    # Prepare payload with body wrapper for n8n
    payload = {
        "body": {
            "message": message,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "timestamp": datetime.now().isoformat()
        }
    }
    
    try:
        logger.info(f"🔄 Calling n8n workflow at: {webhook_url}")
        logger.debug(f"Sending payload: {json.dumps(payload, indent=2)}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                json=payload,  # ✅ ใช้ json= แค่ครั้งเดียว
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=config.n8n_timeout)
            ) as response:
                
                if response.status == 200:
                    result = await response.json()
                    logger.info("✅ n8n workflow executed successfully")
                    return result
                else:
                    logger.error(f"❌ n8n workflow failed: HTTP {response.status}")
                    if config.n8n_fallback_to_direct:
                        logger.info("⚠️ Falling back to direct service call")
                    return None
                    
    except asyncio.TimeoutError:
        logger.error(f"⏱️ n8n workflow timeout after {config.n8n_timeout}s")
        return None
    except Exception as e:
        logger.error(f"❌ n8n workflow error: {e}")
        return None


# =============================================================================
# EXISTING FUNCTIONS (ไม่เปลี่ยน)
# =============================================================================

async def call_main_service(endpoint: str, payload: dict, stream: bool = False):
    """Direct call to main AI service"""
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
        logger.error(f"Service call error: {e}")
        if stream:
            error_msg = json.dumps({"error": str(e)})
            yield error_msg.encode()
        else:
            yield {"error": str(e)}

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
# MAIN ENDPOINT WITH N8N SUPPORT (แก้ไข)
# =============================================================================

def is_system_prompt(message: str) -> bool:
    """Detect OpenWebUI system prompts"""
    if not message:
        return False
    
    system_indicators = [
        "### Task:",
        "### Guidelines:",
        "### Output:",
        "Suggest 3-5 relevant follow-up",
        "follow_ups",
        "<chat_history>",
        "USER:",
        "ASSISTANT:",
        "JSON format:"
    ]
    
    return any(indicator in message for indicator in system_indicators)

@app.post("/v1/chat/completions")
async def chat_completions(
    request: ChatCompletionRequest,
    x_tenant_id: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None)
):
    """OpenAI-compatible endpoint with n8n workflow support"""
    
    tenant_id = get_tenant_id(x_tenant_id)
    user_id = x_user_id or "openwebui_user"
    tenant_config = config.tenant_configs.get(tenant_id, config.tenant_configs['company-a'])
    
    try:
        # ✅ Extract message เพียงครั้งเดียว
        user_message = extract_user_message(request.messages)
        if not user_message:
            raise HTTPException(400, "No user message found")
        
        # ✅ Safety check ทันทีหลังได้ message
        if config.block_system_prompts and is_system_prompt(user_message):
            logger.warning(f"🚫 Blocked system prompt from {user_id}: {user_message[:100]}...")
            
            # Return empty response for system prompts
            return {
                "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                "object": "chat.completion",
                "created": int(datetime.now().timestamp()),
                "model": request.model,
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": '{"follow_ups": []}'  # Return empty JSON for follow-up requests
                    },
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            }
        
        logger.info(f"📨 Valid request for {tenant_id}: {user_message[:50]}...")
        
        # ========== Try n8n workflow first (ถ้าเปิดใช้งาน) ==========
        result = None
        if config.use_n8n_workflow:
            logger.info("🔄 Attempting n8n workflow processing...")
            n8n_result = await call_n8n_workflow(user_message, tenant_id, user_id)
            
            if n8n_result:
                result = n8n_result
                logger.info("✅ Using n8n workflow response")
            elif config.n8n_fallback_to_direct:
                logger.info("⚠️ n8n failed, falling back to direct service")
            else:
                raise HTTPException(503, "n8n workflow unavailable and fallback disabled")
        
        # ========== Direct service call (ถ้าไม่ใช้ n8n หรือ fallback) ==========
        if not result:
            service_payload = {
                "question": user_message,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "stream": request.stream
            }
            
            endpoint = "/v1/chat/stream" if request.stream else "/v1/chat"
            
            async for chunk in call_main_service(endpoint, service_payload, request.stream):
                result = chunk
                break
        
        # ========== Format response ==========
        if request.stream:
            # Streaming response
            async def generate():
                try:
                    # Send initial chunk
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
                    
                    # Stream the answer
                    answer = result.get('answer', '') if isinstance(result, dict) else str(result)
                    chunk_size = 50
                    
                    for i in range(0, len(answer), chunk_size):
                        chunk = answer[i:i + chunk_size]
                        stream_chunk = {
                            "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                            "object": "chat.completion.chunk",
                            "created": int(datetime.now().timestamp()),
                            "model": request.model,
                            "choices": [{
                                "index": 0,
                                "delta": {"content": chunk},
                                "finish_reason": None
                            }]
                        }
                        yield f"data: {json.dumps(stream_chunk)}\n\n"
                        await asyncio.sleep(0.05)
                    
                    # Send completion
                    final_chunk = {
                        "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                        "object": "chat.completion.chunk",
                        "created": int(datetime.now().timestamp()),
                        "model": request.model,
                        "choices": [{
                            "index": 0,
                            "delta": {},
                            "finish_reason": "stop"
                        }]
                    }
                    yield f"data: {json.dumps(final_chunk)}\n\n"
                    yield "data: [DONE]\n\n"
                    
                except Exception as e:
                    logger.error(f"Streaming error: {e}")
                    yield f"data: {json.dumps({'error': str(e)})}\n\n"
            
            return StreamingResponse(
                generate(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no"
                }
            )
        
        else:
            # Non-streaming response
            answer = result.get('answer', 'ไม่สามารถประมวลผลได้') if isinstance(result, dict) else str(result)
            
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
                }
            }
            
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
# HEALTH CHECK WITH N8N STATUS (แก้ไข)
# =============================================================================

@app.get("/health")
async def health():
    """Health check endpoint with n8n status"""
    
    # Check main AI service
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{config.main_service_url}/health",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                service_healthy = response.status == 200
    except:
        service_healthy = False
    
    # Check n8n status (ถ้าเปิดใช้งาน)
    n8n_status = "disabled"
    if config.use_n8n_workflow:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{config.n8n_base_url}/healthz",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    n8n_status = "healthy" if response.status == 200 else "unhealthy"
        except:
            n8n_status = "unreachable"
    
    return {
        "status": "healthy" if service_healthy else "degraded",
        "service": "OpenWebUI Proxy",
        "version": "2.0.0",
        "main_service": "connected" if service_healthy else "disconnected",
        "n8n_workflow": {
            "enabled": config.use_n8n_workflow,
            "status": n8n_status,
            "url": config.n8n_base_url if config.use_n8n_workflow else None,
            "fallback": config.n8n_fallback_to_direct
        },
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
            "owned_by": f"siamtemp-{tenant_id}"
        }]
    }

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("OpenWebUI Proxy Service")
    print("=" * 60)
    print(f"Main Service: {config.main_service_url}")
    print(f"n8n Workflow: {'ENABLED' if config.use_n8n_workflow else 'DISABLED'}")
    if config.use_n8n_workflow:
        print(f"n8n URL: {config.n8n_base_url}")
        print(f"n8n Webhook: /{config.n8n_webhook_path}")
        print(f"Fallback: {config.n8n_fallback_to_direct}")
    print(f"Port: {config.port}")
    print(f"Default Tenant: {config.default_tenant}")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=config.port)