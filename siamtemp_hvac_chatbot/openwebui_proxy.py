# openwebui_proxy.py - Enhanced Version with Advanced Streaming & Monitoring
"""
OpenWebUI Proxy with n8n Workflow Integration, Advanced Streaming & Monitoring
เพิ่มการรองรับ streaming ที่ดีขึ้นและ monitoring แบบเจาะลึก
"""

import os
import json
import asyncio
import aiohttp
import time
from datetime import datetime
from typing import Dict, Any, Optional, AsyncGenerator
from collections import defaultdict, deque
import uvicorn
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import logging

# Import prompt manager utility
from prompt_manager import StreamingPromptManager, MonitoringManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# ENHANCED CONFIGURATION WITH MONITORING
# =============================================================================

class ProxyConfig:
    def __init__(self):
        # Main service URL
        self.main_service_url = os.getenv('MAIN_SERVICE_URL', 'http://hvac-ai-service:5000')
        self.default_tenant = os.getenv('DEFAULT_TENANT', 'company-a')
        self.force_tenant = os.getenv('FORCE_TENANT')
        self.port = int(os.getenv('PROXY_PORT', '8001'))
        
        # ========== N8N Configuration ==========
        self.use_n8n_workflow = os.getenv('USE_N8N_WORKFLOW', 'true').lower() == 'true'
        self.n8n_base_url = os.getenv('N8N_BASE_URL', 'http://13.250.235.228:5678')
        self.n8n_webhook_path = 'webhook/siamtemp-chat'
        self.n8n_timeout = int(os.getenv('N8N_TIMEOUT', '600'))  # ✅ ปรับเป็น 10 วินาที
        self.n8n_fallback_to_direct = os.getenv('N8N_FALLBACK_TO_DIRECT', 'true').lower() == 'true'
        self.block_system_prompts = os.getenv('BLOCK_SYSTEM_PROMPTS', 'true').lower() == 'true'
        
        # ========== Enhanced Features ==========
        self.enable_advanced_monitoring = os.getenv('ENABLE_ADVANCED_MONITORING', 'true').lower() == 'true'
        self.enable_streaming_optimization = os.getenv('ENABLE_STREAMING_OPTIMIZATION', 'true').lower() == 'true'
        self.chunk_size = int(os.getenv('STREAMING_CHUNK_SIZE', '25'))
        self.chunk_delay = float(os.getenv('STREAMING_CHUNK_DELAY', '0.03'))
        
        # Tenant configurations
        self.tenant_configs = {
            'company-a': {'name': 'siamtemp Bangkok HQ', 'model': 'siamtemp-ai', 'language': 'th'},
            'company-b': {'name': 'siamtemp Branch B', 'model': 'siamtemp-ai', 'language': 'th'},
        }

config = ProxyConfig()

# Initialize enhanced managers
streaming_manager = StreamingPromptManager(config)
monitoring_manager = MonitoringManager() if config.enable_advanced_monitoring else None

app = FastAPI(title="OpenWebUI Proxy Enhanced", version="3.0.0")

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True, 
    allow_methods=["*"], 
    allow_headers=["*"]
)

# =============================================================================
# REQUEST MODELS (เก็บเดิม)
# =============================================================================

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    model: str
    messages: list[ChatMessage]
    stream: bool = False
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None

# =============================================================================
# ENHANCED N8N WORKFLOW INTEGRATION
# =============================================================================

async def call_n8n_workflow(message: str, tenant_id: str, user_id: str) -> Optional[Dict]:
    """Enhanced N8N workflow call with monitoring"""
    if not config.use_n8n_workflow:
        return None
    
    start_time = time.time()
    
    try:
        webhook_url = f"{config.n8n_base_url}/{config.n8n_webhook_path}"
        payload = {
            "message": message,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "timestamp": datetime.now().isoformat()
        }
        
        # ✅ Monitor N8N call
        if monitoring_manager:
            monitoring_manager.record_n8n_attempt(tenant_id)
        
        logger.info(f"🔄 Calling n8n workflow at: {webhook_url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=config.n8n_timeout)
            ) as response:
                
                if response.status == 200:
                    result = await response.json()
                    processing_time = time.time() - start_time
                    
                    # ✅ Monitor success
                    if monitoring_manager:
                        monitoring_manager.record_n8n_success(tenant_id, processing_time)
                    
                    logger.info(f"✅ N8N workflow completed in {processing_time:.2f}s")
                    return result
                else:
                    logger.error(f"N8N HTTP error: {response.status}")
                    if monitoring_manager:
                        monitoring_manager.record_n8n_error(tenant_id, f"HTTP {response.status}")
                    return None
                    
    except asyncio.TimeoutError:
        processing_time = time.time() - start_time
        logger.error(f"⏱️ n8n workflow timeout after {config.n8n_timeout}s")
        if monitoring_manager:
            monitoring_manager.record_n8n_timeout(tenant_id, processing_time)
        return None
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"❌ N8N workflow error: {e}")
        if monitoring_manager:
            monitoring_manager.record_n8n_error(tenant_id, str(e))
        return None

# =============================================================================
# ENHANCED STREAMING RESPONSE
# =============================================================================

async def generate_enhanced_streaming_response(
    answer: str, 
    model: str, 
    user_message: str,
    tenant_id: str
) -> AsyncGenerator[str, None]:
    """✅ Enhanced streaming with proper OpenAI format"""
    
    request_id = f"chatcmpl-{int(time.time() * 1000)}"
    current_time = int(time.time())
    
    # Monitor streaming start
    if monitoring_manager:
        monitoring_manager.record_streaming_start(tenant_id, len(answer))
    
    try:
        # ✅ Send initial chunk with role
        initial_chunk = {
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": current_time,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {"role": "assistant", "content": ""},
                "finish_reason": None
            }]
        }
        yield f"data: {json.dumps(initial_chunk, ensure_ascii=False)}\n\n"
        
        # ✅ Stream content in optimized chunks
        total_chunks = 0
        for i in range(0, len(answer), config.chunk_size):
            chunk_text = answer[i:i + config.chunk_size]
            
            content_chunk = {
                "id": request_id,
                "object": "chat.completion.chunk", 
                "created": current_time,
                "model": model,
                "choices": [{
                    "index": 0,
                    "delta": {"content": chunk_text},
                    "finish_reason": None
                }]
            }
            
            # ✅ Use ensure_ascii=False for proper Thai support
            yield f"data: {json.dumps(content_chunk, ensure_ascii=False)}\n\n"
            total_chunks += 1
            
            # Optimized delay
            await asyncio.sleep(config.chunk_delay)
        
        # ✅ Send completion chunk
        final_chunk = {
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": current_time,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "stop"
            }]
        }
        yield f"data: {json.dumps(final_chunk, ensure_ascii=False)}\n\n"
        yield "data: [DONE]\n\n"
        
        # Monitor streaming success
        if monitoring_manager:
            monitoring_manager.record_streaming_success(tenant_id, total_chunks)
        
        logger.info(f"✅ Streaming completed: {total_chunks} chunks, {len(answer)} chars")
        
    except Exception as e:
        logger.error(f"❌ Streaming error: {e}")
        
        # Send error in proper streaming format
        error_chunk = {
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": current_time,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {"content": f"\n\n⚠️ เกิดข้อผิดพลาดในการส่งข้อมูล: {str(e)}"},
                "finish_reason": "stop"
            }]
        }
        yield f"data: {json.dumps(error_chunk, ensure_ascii=False)}\n\n"
        yield "data: [DONE]\n\n"
        
        # Monitor streaming error
        if monitoring_manager:
            monitoring_manager.record_streaming_error(tenant_id, str(e))

# =============================================================================
# ENHANCED SERVICE CALL FUNCTIONS
# =============================================================================

async def call_main_service(endpoint: str, payload: dict, stream: bool = False):
    """Enhanced direct call to main AI service"""
    url = f"{config.main_service_url}{endpoint}"
    start_time = time.time()
    
    try:
        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=120 if stream else 60)
            
            async with session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=timeout
            ) as response:
                
                processing_time = time.time() - start_time
                
                if response.status == 200:
                    result = await response.json()
                    
                    # Monitor successful service call
                    if monitoring_manager:
                        monitoring_manager.record_service_call_success(
                            payload.get('tenant_id', 'unknown'),
                            processing_time,
                            endpoint
                        )
                    
                    return result
                else:
                    error_text = await response.text()
                    logger.error(f"Service error {response.status}: {error_text}")
                    
                    # Monitor service error
                    if monitoring_manager:
                        monitoring_manager.record_service_call_error(
                            payload.get('tenant_id', 'unknown'),
                            response.status,
                            endpoint
                        )
                    
                    return {"error": f"Service error: {response.status}"}
                        
    except asyncio.TimeoutError:
        processing_time = time.time() - start_time
        logger.error(f"Service timeout after {processing_time:.1f}s")
        
        if monitoring_manager:
            monitoring_manager.record_service_timeout(
                payload.get('tenant_id', 'unknown'),
                processing_time,
                endpoint
            )
        
        return {"error": f"Service timeout ({processing_time:.1f}s)"}
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"Service call error: {e}")
        
        if monitoring_manager:
            monitoring_manager.record_service_call_error(
                payload.get('tenant_id', 'unknown'),
                str(e),
                endpoint
            )
        
        return {"error": str(e)}

# =============================================================================
# HELPER FUNCTIONS (เก็บเดิม)
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

def is_system_prompt(message: str) -> bool:
    """Detect OpenWebUI system prompts"""
    if not message:
        return False
    
    system_indicators = [
        "### Task:", "### Guidelines:", "### Output:",
        "Suggest 3-5 relevant follow-up", "follow_ups",
        "<chat_history>", "USER:", "ASSISTANT:", "JSON format:"
    ]
    
    return any(indicator in message for indicator in system_indicators)

# =============================================================================
# ENHANCED MAIN ENDPOINT
# =============================================================================

@app.post("/v1/chat/completions")
async def chat_completions(
    request: ChatCompletionRequest,
    x_tenant_id: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None)
):
    """Enhanced OpenAI-compatible endpoint with monitoring"""
    
    tenant_id = get_tenant_id(x_tenant_id)
    user_id = x_user_id or "openwebui_user"
    tenant_config = config.tenant_configs.get(tenant_id, config.tenant_configs['company-a'])
    
    start_time = time.time()
    
    # Monitor request
    if monitoring_manager:
        monitoring_manager.record_request_start(tenant_id, user_id, request.stream)
    
    try:
        # Extract message
        user_message = extract_user_message(request.messages)
        if not user_message:
            if monitoring_manager:
                monitoring_manager.record_request_error(tenant_id, "No user message")
            raise HTTPException(400, "No user message found")
        
        # Block system prompts
        if config.block_system_prompts and is_system_prompt(user_message):
            logger.warning(f"🚫 Blocked system prompt from {user_id}: {user_message[:50]}...")
            
            if monitoring_manager:
                monitoring_manager.record_blocked_request(tenant_id, "system_prompt")
            
            return {
                "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                "object": "chat.completion",
                "created": int(datetime.now().timestamp()),
                "model": request.model,
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": ""
                    },
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            }
        
        logger.info(f"📨 Valid request for {tenant_id}: {user_message[:50]}...")
        
        # ========== Try N8N workflow first ==========
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
                if monitoring_manager:
                    monitoring_manager.record_request_error(tenant_id, "n8n_unavailable")
                raise HTTPException(503, "n8n workflow unavailable and fallback disabled")
        
        # ========== Direct service call ==========
        if not result:
            service_payload = {
                "question": user_message,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "stream": False  # Always get full response first
            }
            
            result = await call_main_service("/v1/chat", service_payload, False)
        
        # Extract answer
        if isinstance(result, dict) and "error" in result:
            error_msg = result["error"]
            if monitoring_manager:
                monitoring_manager.record_request_error(tenant_id, error_msg)
            
            error_response = {
                "id": f"chatcmpl-{int(datetime.now().timestamp())}",
                "object": "chat.completion",
                "created": int(datetime.now().timestamp()),
                "model": request.model,
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": f"ขออภัย เกิดข้อผิดพลาด: {error_msg}"
                    },
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            }
            
            if request.stream:
                async def error_stream():
                    yield f"data: {json.dumps(error_response)}\n\n"
                    yield "data: [DONE]\n\n"
                return StreamingResponse(error_stream(), media_type="text/event-stream")
            else:
                return error_response
        
        # Get answer
        answer = result.get('answer', 'ไม่สามารถประมวลผลได้') if isinstance(result, dict) else str(result)
        
        # ========== Format response ==========
        if request.stream and config.enable_streaming_optimization:
            # ✅ Enhanced streaming response
            return StreamingResponse(
                generate_enhanced_streaming_response(answer, request.model, user_message, tenant_id),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                    "Access-Control-Allow-Origin": "*"
                }
            )
        else:
            # Non-streaming response
            processing_time = time.time() - start_time
            
            # Monitor successful response
            if monitoring_manager:
                monitoring_manager.record_request_success(
                    tenant_id, 
                    processing_time,
                    len(answer),
                    request.stream
                )
            
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
        processing_time = time.time() - start_time
        logger.error(f"Unexpected error: {e}", exc_info=True)
        
        if monitoring_manager:
            monitoring_manager.record_request_error(tenant_id, str(e))
        
        error_response = {
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
        
        if request.stream:
            async def error_stream():
                yield f"data: {json.dumps(error_response, ensure_ascii=False)}\n\n"
                yield "data: [DONE]\n\n"
            return StreamingResponse(error_stream(), media_type="text/event-stream")
        else:
            return error_response

# =============================================================================
# ENHANCED HEALTH CHECK & MONITORING ENDPOINTS
# =============================================================================

@app.get("/health")
async def health():
    """Enhanced health check with detailed monitoring"""
    
    # Check main AI service
    main_service_healthy = False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{config.main_service_url}/health",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                main_service_healthy = response.status == 200
    except:
        main_service_healthy = False
    
    # Check N8N status
    n8n_status = "disabled"
    if config.use_n8n_workflow:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{config.n8n_base_url}/healthz",
                    timeout=aiohttp.ClientTimeout(total=3)
                ) as response:
                    n8n_status = "healthy" if response.status == 200 else "unhealthy"
        except:
            n8n_status = "unreachable"
    
    # Get monitoring stats
    stats = {}
    if monitoring_manager:
        stats = monitoring_manager.get_health_stats()
    
    return {
        "status": "healthy" if main_service_healthy else "degraded",
        "service": "OpenWebUI Proxy Enhanced",
        "version": "3.0.0",
        "main_service": "connected" if main_service_healthy else "disconnected",
        "n8n_workflow": {
            "enabled": config.use_n8n_workflow,
            "status": n8n_status,
            "timeout": f"{config.n8n_timeout}s",
            "fallback": config.n8n_fallback_to_direct
        },
        "features": {
            "advanced_monitoring": config.enable_advanced_monitoring,
            "streaming_optimization": config.enable_streaming_optimization,
            "chunk_size": config.chunk_size,
            "chunk_delay": f"{config.chunk_delay}s"
        },
        "stats": stats,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/metrics")
async def get_metrics():
    """Get detailed metrics"""
    if not monitoring_manager:
        raise HTTPException(404, "Advanced monitoring not enabled")
    
    return monitoring_manager.get_detailed_metrics()

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
    print("OpenWebUI Proxy Enhanced v3.0.0")
    print("=" * 60)
    print(f"Main Service: {config.main_service_url}")
    print(f"N8N Workflow: {'ENABLED' if config.use_n8n_workflow else 'DISABLED'}")
    if config.use_n8n_workflow:
        print(f"N8N URL: {config.n8n_base_url}")
        print(f"N8N Timeout: {config.n8n_timeout}s")
        print(f"Fallback: {config.n8n_fallback_to_direct}")
    print(f"Advanced Monitoring: {'ENABLED' if config.enable_advanced_monitoring else 'DISABLED'}")
    print(f"Streaming Optimization: {'ENABLED' if config.enable_streaming_optimization else 'DISABLED'}")
    print(f"Port: {config.port}")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=config.port)