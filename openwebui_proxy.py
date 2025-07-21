import time
import asyncio
import aiohttp
from typing import Dict, Any, List, Optional
from datetime import datetime

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from tenant_manager import get_tenant_config, list_available_tenants, validate_tenant_id

# ============================================================================
# Models & Config
# ============================================================================

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    messages: List[ChatMessage]
    model: Optional[str] = "siamtech-auto"
    max_tokens: Optional[int] = 1000
    temperature: Optional[float] = 0.7

class Config:
    """Simple configuration management"""
    TENANT_ID = None  # Set by environment
    N8N_BASE = "http://n8n:5678"
    TIMEOUT = 60
    
    @classmethod
    def init_from_env(cls):
        import os
        cls.TENANT_ID = os.getenv('FORCE_TENANT', os.getenv('DEFAULT_TENANT', 'company-a'))
        cls.N8N_BASE = os.getenv('N8N_BASE_URL', cls.N8N_BASE)
        if not validate_tenant_id(cls.TENANT_ID):
            cls.TENANT_ID = 'company-a'

# ============================================================================
# Response Extraction - Ultra Simple
# ============================================================================

class ResponseExtractor:
    """Ultra-compact response extraction with fallback chain"""
    
    @staticmethod
    def extract(data: Dict[str, Any]) -> str:
        """Smart extraction with fallback chain"""
        if not isinstance(data, dict):
            return "Invalid response format"
        
        # Try direct paths first
        for path in ['answer', 'content', 'message', 'response']:
            if path in data and data[path]:
                result = str(data[path]).strip()
                if len(result) > 3:  # Minimum viable answer
                    return result
        
        # Try nested paths
        for container in ['response', 'original_response', 'data', 'result']:
            if container in data and isinstance(data[container], dict):
                nested = ResponseExtractor.extract(data[container])
                if nested != "Invalid response format":
                    return nested
        
        # Try first list item
        for key, value in data.items():
            if isinstance(value, list) and value:
                if isinstance(value[0], dict):
                    nested = ResponseExtractor.extract(value[0])
                    if nested != "Invalid response format":
                        return nested
                elif isinstance(value[0], str) and len(value[0]) > 3:
                    return value[0]
        
        return "No valid response found in data"

# ============================================================================
# N8N Client - Minimal & Fast
# ============================================================================

class N8NClient:
    """Ultra-efficient N8N communication"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.config = get_tenant_config(tenant_id)
        self.webhook_url = self.config.webhooks.get('n8n_endpoint') or f"{Config.N8N_BASE}/webhook/{tenant_id}-chat"
    
    async def send_message(self, message: str, history: List[Dict] = None) -> Dict[str, Any]:
        """Send message to N8N and get response"""
        payload = {
            "message": message,
            "tenant_id": self.tenant_id,
            "tenant_name": self.config.name,
            "conversation_history": history or [],
            "timestamp": int(time.time()),
            "agent_type": "auto"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.webhook_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=Config.TIMEOUT)
            ) as response:
                if response.status != 200:
                    raise HTTPException(response.status, f"N8N error: {await response.text()}")
                return await response.json()

# ============================================================================
# Main FastAPI App - Ultra Compact
# ============================================================================

app = FastAPI(title="SiamTech OpenWebUI Proxy", version="3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Initialize config
Config.init_from_env()

@app.on_event("startup")
async def startup():
    config = get_tenant_config(Config.TENANT_ID)
    print(f"🚀 Proxy v3.0 - Tenant: {Config.TENANT_ID} ({config.name})")

@app.get("/health")
def health():
    """Health check"""
    config = get_tenant_config(Config.TENANT_ID)
    return {
        "status": "healthy",
        "tenant_id": Config.TENANT_ID,
        "tenant_name": config.name,
        "version": "3.0"
    }

@app.get("/v1/models")
def models():
    """OpenAI compatible models list"""
    config = get_tenant_config(Config.TENANT_ID)
    return {
        "object": "list",
        "data": [{
            "id": f"siamtech-{Config.TENANT_ID}",
            "object": "model",
            "created": int(time.time()),
            "owned_by": "siamtech",
            "description": f"Auto Agent for {config.name}"
        }]
    }

@app.post("/v1/chat/completions")
async def chat_completions(request: ChatRequest):
    """Main OpenAI compatible endpoint - Ultra streamlined"""
    start_time = time.time()
    
    try:
        # Extract user message and history
        if not request.messages:
            raise HTTPException(400, "No messages provided")
        
        user_message = request.messages[-1].content
        history = [{"role": msg.role, "content": msg.content} for msg in request.messages[:-1]]
        
        # Send to N8N
        client = N8NClient(Config.TENANT_ID)
        n8n_response = await client.send_message(user_message, history)
        
        # Extract answer
        answer = ResponseExtractor.extract(n8n_response)
        
        # Build OpenAI response
        config = get_tenant_config(Config.TENANT_ID)
        return {
            "id": f"chatcmpl-{Config.TENANT_ID}-{int(time.time())}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": request.model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": answer},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": len(user_message.split()),
                "completion_tokens": len(answer.split()),
                "total_tokens": len(user_message.split()) + len(answer.split())
            },
            "metadata": {
                "tenant_id": Config.TENANT_ID,
                "tenant_name": config.name,
                "response_time_ms": int((time.time() - start_time) * 1000),
                "agent": "auto",
                "proxy_version": "3.0"
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        # Simple error response
        config = get_tenant_config(Config.TENANT_ID)
        error_msg = "Sorry, I encountered an error while processing your request."
        if config.settings.get('response_language') == 'th':
            error_msg = "ขออภัย เกิดข้อผิดพลาดในการประมวลผลคำขอของคุณ"
        
        return {
            "id": f"chatcmpl-error-{int(time.time())}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": request.model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": error_msg},
                "finish_reason": "stop"
            }],
            "usage": {"total_tokens": 0},
            "error": {"message": str(e), "type": "proxy_error"}
        }

# ============================================================================
# Optional Endpoints - Minimal
# ============================================================================

@app.get("/tenants")
def get_tenants():
    """List available tenants"""
    return {"tenants": list(list_available_tenants().keys()), "current": Config.TENANT_ID}

@app.post("/test")
async def test_connection():
    """Quick test endpoint"""
    try:
        client = N8NClient(Config.TENANT_ID)
        result = await client.send_message("test connection")
        answer = ResponseExtractor.extract(result)
        return {"status": "ok", "test_response": answer[:100] + "..."}
    except Exception as e:
        return {"status": "error", "error": str(e)}

# ============================================================================
# Run Server
# ============================================================================

if __name__ == '__main__':
    print("🚀 Ultra-Compact OpenWebUI Proxy v3.0")
    print(f"🏢 Tenant: {Config.TENANT_ID}")
    print(f"🔗 N8N: {Config.N8N_BASE}")
    print("🎯 Optimized for speed and simplicity")
    
    uvicorn.run(
        "openwebui_proxy:app",
        host="0.0.0.0",
        port=8001,
        access_log=False,  # Reduce noise
        loop="uvloop"      # Faster event loop
    )