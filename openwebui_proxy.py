import json
import os
import asyncio
import aiohttp
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
import time


# Pydantic models
class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    messages: List[ChatMessage]
    model: Optional[str] = "siamtech-n8n-agent"
    max_tokens: Optional[int] = 1000
    temperature: Optional[float] = 0.7

class ChatCompletionResponse(BaseModel):
    id: str
    object: str
    created: int
    model: str
    choices: List[dict]
    usage: dict

# FastAPI app
app = FastAPI(
    title="OpenWebUI to n8n Proxy",
    description="Proxy service that forwards OpenWebUI requests to n8n webhooks",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
N8N_WEBHOOK_URL = os.getenv('N8N_WEBHOOK_URL', 'http://localhost:5678/webhook/chat')

@app.on_event("startup")
async def startup_event():
    print("🚀 OpenWebUI to n8n Proxy starting...")
    print(f"🔗 n8n Webhook URL: {N8N_WEBHOOK_URL}")
    print("🎯 Ready to forward requests to n8n")

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "OpenWebUI to n8n Proxy",
        "n8n_webhook": N8N_WEBHOOK_URL,
        "version": "1.0.0"
    }

@app.get("/v1/models")
async def list_models():
    """OpenAI compatible models endpoint"""
    return {
        "object": "list",
        "data": [
            {
                "id": "siamtech-n8n-agent",
                "object": "model",
                "created": int(time.time()),
                "owned_by": "siamtech",
                "permission": [],
                "root": "siamtech-n8n-agent",
                "parent": None,
                "description": "SiamTech AI Assistant via n8n Workflows"
            },
            {
                "id": "siamtech-n8n-postgres",
                "object": "model",
                "created": int(time.time()),
                "owned_by": "siamtech",
                "permission": [],
                "root": "siamtech-n8n-postgres",
                "parent": None,
                "description": "PostgreSQL Agent via n8n"
            },
            {
                "id": "siamtech-n8n-knowledge",
                "object": "model",
                "created": int(time.time()),
                "owned_by": "siamtech",
                "permission": [],
                "root": "siamtech-n8n-knowledge",
                "parent": None,
                "description": "Knowledge Base Agent via n8n"
            }
        ]
    }

@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest):
    """OpenAI compatible endpoint that forwards to n8n"""
    try:
        if not request.messages:
            raise HTTPException(status_code=400, detail="No messages provided")
        
        # Extract the user message
        user_message = request.messages[-1].content
        
        # Build conversation history
        conversation_history = []
        for msg in request.messages[:-1]:  # All messages except the last one
            conversation_history.append({
                "role": msg.role,
                "content": msg.content
            })
        
        # Prepare payload for n8n
        n8n_payload = {
            "message": user_message,
            "conversation_history": conversation_history,
            "model": request.model,
            "max_tokens": request.max_tokens,
            "temperature": request.temperature,
            "timestamp": int(time.time())
        }
        
        # Forward to n8n webhook
        async with aiohttp.ClientSession() as session:
            async with session.post(
                N8N_WEBHOOK_URL,
                json=n8n_payload,
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                
                if response.status != 200:
                    error_text = await response.text()
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"n8n webhook error: {error_text}"
                    )
                
                n8n_response = await response.json()
        
        # Extract answer from n8n response
        if isinstance(n8n_response, dict):
            answer = n8n_response.get('answer', n8n_response.get('response', str(n8n_response)))
            agent_info = n8n_response.get('agent', 'n8n-workflow')
            success = n8n_response.get('success', True)
        else:
            answer = str(n8n_response)
            agent_info = 'n8n-workflow'
            success = True
        
        # Format as OpenAI response
        openai_response = {
            "id": f"chatcmpl-n8n-{int(time.time())}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": request.model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": answer
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": len(user_message.split()),
                "completion_tokens": len(answer.split()),
                "total_tokens": len(user_message.split()) + len(answer.split())
            },
            "metadata": {
                "agent_used": agent_info,
                "workflow": "n8n",
                "success": success,
                "original_response": n8n_response
            }
        }
        
        return openai_response
        
    except aiohttp.ClientError as e:
        print(f"Connection error to n8n: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=f"Cannot connect to n8n webhook: {str(e)}"
        )
    except Exception as e:
        print(f"Error in chat completion: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/models/{model_id}")
async def get_model(model_id: str):
    """Get specific model info"""
    models = {
        "siamtech-n8n-agent": {
            "id": "siamtech-n8n-agent",
            "object": "model",
            "created": int(time.time()),
            "owned_by": "siamtech",
            "description": "SiamTech AI Assistant via n8n Workflows"
        },
        "siamtech-n8n-postgres": {
            "id": "siamtech-n8n-postgres",
            "object": "model",
            "created": int(time.time()),
            "owned_by": "siamtech",
            "description": "PostgreSQL Agent via n8n"
        },
        "siamtech-n8n-knowledge": {
            "id": "siamtech-n8n-knowledge",
            "object": "model",
            "created": int(time.time()),
            "owned_by": "siamtech",
            "description": "Knowledge Base Agent via n8n"
        }
    }
    
    if model_id not in models:
        raise HTTPException(status_code=404, detail="Model not found")
    
    return models[model_id]

@app.post("/test-n8n")
async def test_n8n_connection():
    """Test connection to n8n webhook"""
    try:
        test_payload = {
            "message": "test connection",
            "test": True,
            "timestamp": int(time.time())
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                N8N_WEBHOOK_URL,
                json=test_payload,
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                
                status = response.status
                response_text = await response.text()
                
                return {
                    "status": "success" if status == 200 else "error",
                    "http_status": status,
                    "response": response_text,
                    "webhook_url": N8N_WEBHOOK_URL
                }
                
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "webhook_url": N8N_WEBHOOK_URL
        }

if __name__ == '__main__':
    print(f"🚀 OpenWebUI to n8n Proxy Service")
    print(f"🔗 n8n Webhook: {N8N_WEBHOOK_URL}")
    print(f"🎯 Forwarding OpenWebUI requests to n8n workflows")
    print(f"💡 Available models:")
    print(f"   - siamtech-n8n-agent (main agent)")
    print(f"   - siamtech-n8n-postgres (database agent)")
    print(f"   - siamtech-n8n-knowledge (knowledge base agent)")
    
    uvicorn.run(
        "openwebui_proxy:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )