import json
import os
import asyncio
import aiohttp
from fastapi import FastAPI, HTTPException, Header, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import uvicorn
import time
import logging
from datetime import datetime

# Import tenant management
from tenant_manager import (
    tenant_manager,
    get_tenant_config,
    list_available_tenants,
    validate_tenant_id
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models
class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    messages: List[ChatMessage]
    model: Optional[str] = "siamtech-n8n-agent"
    max_tokens: Optional[int] = 1000
    temperature: Optional[float] = 0.7
    tenant_id: Optional[str] = None  # Can override header

class ChatCompletionResponse(BaseModel):
    id: str
    object: str
    created: int
    model: str
    choices: List[dict]
    usage: dict
    metadata: Optional[dict] = None

class TenantSelectionRequest(BaseModel):
    tenant_id: str

# FastAPI app
app = FastAPI(
    title="Multi-Tenant OpenWebUI to n8n Proxy",
    description="Proxy service that forwards OpenWebUI requests to n8n webhooks with multi-tenant support",
    version="2.0.0"
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
N8N_BASE_URL = os.getenv('N8N_BASE_URL', 'http://localhost:5678')
DEFAULT_TENANT = os.getenv('DEFAULT_TENANT', 'company-a')
REQUIRE_TENANT_HEADER = os.getenv('REQUIRE_TENANT_HEADER', 'false').lower() == 'true'

# Dependency to get tenant ID with multiple sources
def get_tenant_id(
    x_tenant_id: Optional[str] = Header(None, alias="X-Tenant-ID"),
    tenant_query: Optional[str] = Query(None, alias="tenant"),
    request_tenant: Optional[str] = None
) -> str:
    """Get tenant ID from various sources with priority"""
    
    # Priority: 1. Request body, 2. Header, 3. Query param, 4. Default
    tenant_id = request_tenant or x_tenant_id or tenant_query or DEFAULT_TENANT
    
    # Validate tenant ID
    if not validate_tenant_id(tenant_id):
        logger.warning(f"Invalid tenant ID: {tenant_id}, using default: {DEFAULT_TENANT}")
        if REQUIRE_TENANT_HEADER:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid tenant ID: {tenant_id}. Available tenants: {list(list_available_tenants().keys())}"
            )
        tenant_id = DEFAULT_TENANT
    
    return tenant_id

def extract_tenant_from_model(model_name: str) -> Optional[str]:
    """Extract tenant ID from model name (e.g., 'siamtech-n8n-agent-company-b' -> 'company-b')"""
    if not model_name:
        return None
    
    parts = model_name.split('-')
    if len(parts) >= 2:
        # Look for patterns like "company-x" at the end
        for i in range(len(parts) - 1):
            if parts[i] == 'company' and i + 1 < len(parts):
                tenant_candidate = f"company-{parts[i + 1]}"
                if validate_tenant_id(tenant_candidate):
                    return tenant_candidate
    
    return None

@app.on_event("startup")
async def startup_event():
    print("🚀 Multi-Tenant OpenWebUI to n8n Proxy starting...")
    print(f"🔗 n8n Base URL: {N8N_BASE_URL}")
    print(f"🏢 Available Tenants:")
    
    tenants = list_available_tenants()
    for tenant_id, name in tenants.items():
        config = get_tenant_config(tenant_id)
        webhook_url = config.webhooks.get('n8n_endpoint', f"{N8N_BASE_URL}/webhook/{tenant_id}-chat")
        print(f"   - {tenant_id}: {name} -> {webhook_url}")
    
    print(f"🔧 Default Tenant: {DEFAULT_TENANT}")
    print(f"🔒 Require Tenant Header: {REQUIRE_TENANT_HEADER}")
    print("🎯 Ready to forward requests to n8n")

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Multi-Tenant OpenWebUI to n8n Proxy",
        "n8n_base_url": N8N_BASE_URL,
        "default_tenant": DEFAULT_TENANT,
        "available_tenants": list(list_available_tenants().keys()),
        "version": "2.0.0"
    }

@app.get("/tenants")
async def get_available_tenants():
    """Get list of available tenants"""
    tenants = []
    for tenant_id, name in list_available_tenants().items():
        try:
            config = get_tenant_config(tenant_id)
            tenants.append({
                "tenant_id": tenant_id,
                "name": name,
                "webhook_endpoint": config.webhooks.get('n8n_endpoint'),
                "contact_info": config.contact_info
            })
        except Exception as e:
            logger.error(f"Error getting config for tenant {tenant_id}: {e}")
            tenants.append({
                "tenant_id": tenant_id,
                "name": name,
                "error": str(e)
            })
    
    return {
        "tenants": tenants,
        "default_tenant": DEFAULT_TENANT
    }

@app.post("/select-tenant")
async def select_tenant(request: TenantSelectionRequest):
    """Endpoint for frontend to select tenant (returns tenant info)"""
    if not validate_tenant_id(request.tenant_id):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid tenant ID: {request.tenant_id}"
        )
    
    config = get_tenant_config(request.tenant_id)
    return {
        "tenant_id": request.tenant_id,
        "name": config.name,
        "webhook_endpoint": config.webhooks.get('n8n_endpoint'),
        "settings": config.settings,
        "contact_info": config.contact_info
    }

@app.get("/v1/models")
async def list_models():
    """OpenAI compatible models endpoint with tenant-specific models"""
    models = []
    
    # Base model types
    base_models = [
        {
            "id": "siamtech-n8n-agent",
            "description": "SiamTech AI Assistant via n8n Workflows"
        },
        {
            "id": "siamtech-n8n-postgres",
            "description": "PostgreSQL Agent via n8n"
        },
        {
            "id": "siamtech-n8n-knowledge",
            "description": "Knowledge Base Agent via n8n"
        },
        {
            "id": "siamtech-n8n-hybrid",
            "description": "Hybrid Agent via n8n"
        }
    ]
    
    # Add tenant-specific models
    tenants = list_available_tenants()
    for tenant_id, tenant_name in tenants.items():
        for base_model in base_models:
            models.append({
                "id": f"{base_model['id']}-{tenant_id}",
                "object": "model",
                "created": int(time.time()),
                "owned_by": "siamtech",
                "permission": [],
                "root": base_model['id'],
                "parent": None,
                "description": f"{base_model['description']} - {tenant_name}",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name
            })
    
    # Add base models (will use default tenant or header)
    for base_model in base_models:
        models.append({
            "id": base_model['id'],
            "object": "model",
            "created": int(time.time()),
            "owned_by": "siamtech",
            "permission": [],
            "root": base_model['id'],
            "parent": None,
            "description": base_model['description']
        })
    
    return {
        "object": "list",
        "data": models
    }

@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest):
    """OpenAI compatible endpoint that forwards to tenant-specific n8n workflows"""
    try:
        if not request.messages:
            raise HTTPException(status_code=400, detail="No messages provided")
        
        # Determine tenant ID from multiple sources
        tenant_id = request.tenant_id
        
        # Extract from model name if not specified
        if not tenant_id:
            tenant_id = extract_tenant_from_model(request.model)
        
        # Use default if still not found
        if not tenant_id:
            tenant_id = DEFAULT_TENANT
        
        # Validate tenant
        if not validate_tenant_id(tenant_id):
            if REQUIRE_TENANT_HEADER:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid tenant ID: {tenant_id}. Available: {list(list_available_tenants().keys())}"
                )
            tenant_id = DEFAULT_TENANT
        
        # Get tenant configuration
        tenant_config = get_tenant_config(tenant_id)
        
        # Extract the user message
        user_message = request.messages[-1].content
        
        # Build conversation history
        conversation_history = []
        for msg in request.messages[:-1]:  # All messages except the last one
            conversation_history.append({
                "role": msg.role,
                "content": msg.content
            })
        
        # Determine webhook URL
        webhook_url = tenant_config.webhooks.get('n8n_endpoint')
        if not webhook_url:
            webhook_url = f"{N8N_BASE_URL}/webhook/{tenant_id}-chat"
        
        # Determine agent type from model
        agent_type = "auto"
        if "postgres" in request.model.lower():
            agent_type = "postgres"
        elif "knowledge" in request.model.lower():
            agent_type = "knowledge_base"
        elif "hybrid" in request.model.lower():
            agent_type = "hybrid"
        
        # Prepare payload for n8n
        n8n_payload = {
            "message": user_message,
            "conversation_history": conversation_history,
            "tenant_id": tenant_id,
            "tenant_name": tenant_config.name,
            "agent_type": agent_type,
            "model": request.model,
            "max_tokens": min(request.max_tokens, tenant_config.settings.get('max_tokens', 1000)),
            "temperature": request.temperature,
            "timestamp": int(time.time()),
            "settings": tenant_config.settings
        }
        
        logger.info(f"Forwarding request to {webhook_url} for tenant {tenant_id}")
        
        # Forward to n8n webhook
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                json=n8n_payload,
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"n8n webhook error for tenant {tenant_id}: HTTP {response.status} - {error_text}")
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"n8n webhook error: {error_text}"
                    )
                
                try:
                    n8n_response = await response.json()
                except json.JSONDecodeError as e:
                    error_text = await response.text()
                    logger.error(f"Invalid JSON response from n8n for tenant {tenant_id}: {error_text}")
                    raise HTTPException(
                        status_code=502,
                        detail=f"Invalid JSON response from n8n: {str(e)}"
                    )
        
        # Extract answer from n8n response
        if isinstance(n8n_response, dict):
            answer = n8n_response.get('answer', n8n_response.get('response', str(n8n_response)))
            agent_info = n8n_response.get('agent', 'n8n-workflow')
            success = n8n_response.get('success', True)
            routing_decision = n8n_response.get('routing_decision')
        else:
            answer = str(n8n_response)
            agent_info = 'n8n-workflow'
            success = True
            routing_decision = None
        
        # Add tenant context to answer if successful
        if success and tenant_config.name:
            answer = f"{answer}\n\n---\n*ข้อมูลจาก: {tenant_config.name}*"
        
        # Format as OpenAI response
        openai_response = {
            "id": f"chatcmpl-{tenant_id}-{int(time.time())}",
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
                "tenant_id": tenant_id,
                "tenant_name": tenant_config.name,
                "agent_used": agent_info,
                "agent_type_requested": agent_type,
                "workflow": "n8n",
                "success": success,
                "routing_decision": routing_decision,
                "webhook_url": webhook_url,
                "timestamp": datetime.now().isoformat(),
                "original_response": n8n_response if isinstance(n8n_response, dict) else None
            }
        }
        
        logger.info(f"Successfully processed request for tenant {tenant_id}, agent: {agent_info}")
        return openai_response
        
    except aiohttp.ClientError as e:
        logger.error(f"Connection error to n8n for tenant {tenant_id}: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=f"Cannot connect to n8n webhook for tenant {tenant_id}: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error in chat completion for tenant {tenant_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/models/{model_id}")
async def get_model(model_id: str):
    """Get specific model info"""
    # Extract tenant from model ID if present
    tenant_id = extract_tenant_from_model(model_id)
    
    # Base model types
    base_models = {
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
        },
        "siamtech-n8n-hybrid": {
            "id": "siamtech-n8n-hybrid",
            "object": "model",
            "created": int(time.time()),
            "owned_by": "siamtech",
            "description": "Hybrid Agent via n8n"
        }
    }
    
    # If tenant-specific model
    if tenant_id:
        base_model_id = model_id.replace(f"-{tenant_id}", "")
        if base_model_id in base_models:
            tenant_config = get_tenant_config(tenant_id)
            model_info = base_models[base_model_id].copy()
            model_info["id"] = model_id
            model_info["description"] += f" - {tenant_config.name}"
            model_info["tenant_id"] = tenant_id
            model_info["tenant_name"] = tenant_config.name
            return model_info
    
    # Base model
    if model_id in base_models:
        return base_models[model_id]
    
    raise HTTPException(status_code=404, detail="Model not found")

@app.post("/test-n8n")
async def test_n8n_connection(tenant_id: str = Query(DEFAULT_TENANT)):
    """Test connection to n8n webhook for specific tenant"""
    try:
        if not validate_tenant_id(tenant_id):
            raise HTTPException(status_code=400, detail=f"Invalid tenant ID: {tenant_id}")
        
        tenant_config = get_tenant_config(tenant_id)
        webhook_url = tenant_config.webhooks.get('n8n_endpoint')
        if not webhook_url:
            webhook_url = f"{N8N_BASE_URL}/webhook/{tenant_id}-chat"
        
        test_payload = {
            "message": "test connection",
            "tenant_id": tenant_id,
            "test": True,
            "timestamp": int(time.time())
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                json=test_payload,
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                
                status = response.status
                try:
                    response_data = await response.json()
                except:
                    response_data = await response.text()
                
                return {
                    "tenant_id": tenant_id,
                    "tenant_name": tenant_config.name,
                    "status": "success" if status == 200 else "error",
                    "http_status": status,
                    "response": response_data,
                    "webhook_url": webhook_url
                }
                
    except Exception as e:
        return {
            "tenant_id": tenant_id,
            "status": "error",
            "error": str(e),
            "webhook_url": webhook_url if 'webhook_url' in locals() else "unknown"
        }

@app.post("/test-all-tenants")
async def test_all_tenants():
    """Test n8n connections for all tenants"""
    results = {
        "timestamp": datetime.now().isoformat(),
        "tenants": {}
    }
    
    for tenant_id in list_available_tenants().keys():
        try:
            tenant_config = get_tenant_config(tenant_id)
            webhook_url = tenant_config.webhooks.get('n8n_endpoint')
            if not webhook_url:
                webhook_url = f"{N8N_BASE_URL}/webhook/{tenant_id}-chat"
            
            test_payload = {
                "message": "test connection",
                "tenant_id": tenant_id,
                "test": True,
                "timestamp": int(time.time())
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url,
                    json=test_payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    
                    status = response.status
                    try:
                        response_data = await response.json()
                    except:
                        response_data = await response.text()
                    
                    results["tenants"][tenant_id] = {
                        "name": tenant_config.name,
                        "status": "success" if status == 200 else "error",
                        "http_status": status,
                        "webhook_url": webhook_url,
                        "response_size": len(str(response_data))
                    }
                    
        except Exception as e:
            results["tenants"][tenant_id] = {
                "status": "error",
                "error": str(e)
            }
    
    return results

@app.get("/tenant/{tenant_id}/config")
async def get_tenant_config_endpoint(tenant_id: str):
    """Get tenant configuration (limited info for security)"""
    if not validate_tenant_id(tenant_id):
        raise HTTPException(status_code=404, detail=f"Tenant {tenant_id} not found")
    
    config = get_tenant_config(tenant_id)
    
    return {
        "tenant_id": tenant_id,
        "name": config.name,
        "settings": {
            "max_tokens": config.settings.get('max_tokens'),
            "temperature": config.settings.get('temperature'),
            "response_language": config.settings.get('response_language'),
            "allow_hybrid_search": config.settings.get('allow_hybrid_search'),
            "enable_postgres_agent": config.settings.get('enable_postgres_agent'),
            "enable_knowledge_base_agent": config.settings.get('enable_knowledge_base_agent')
        },
        "contact_info": config.contact_info,
        "webhook_endpoint": config.webhooks.get('n8n_endpoint')
    }

@app.post("/admin/reload-config")
async def reload_tenant_config():
    """Reload tenant configuration from file"""
    try:
        tenant_manager._load_config()
        return {
            "success": True,
            "message": "Configuration reloaded successfully",
            "tenants": list(list_available_tenants().keys()),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error reloading config: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def get_metrics():
    """Get basic metrics (placeholder for future implementation)"""
    return {
        "total_tenants": len(list_available_tenants()),
        "default_tenant": DEFAULT_TENANT,
        "n8n_base_url": N8N_BASE_URL,
        "require_tenant_header": REQUIRE_TENANT_HEADER,
        "uptime": time.time(),  # This would be actual uptime in real implementation
        "version": "2.0.0"
    }

if __name__ == '__main__':
    print(f"🚀 Multi-Tenant OpenWebUI to n8n Proxy Service")
    print(f"🔗 n8n Base URL: {N8N_BASE_URL}")
    print(f"🏢 Available Tenants:")
    
    tenants = list_available_tenants()
    for tenant_id, name in tenants.items():
        try:
            config = get_tenant_config(tenant_id)
            webhook = config.webhooks.get('n8n_endpoint', f"{N8N_BASE_URL}/webhook/{tenant_id}-chat")
            print(f"   - {tenant_id}: {name}")
            print(f"     Webhook: {webhook}")
        except Exception as e:
            print(f"   - {tenant_id}: {name} (Error: {e})")
    
    print(f"🔧 Default Tenant: {DEFAULT_TENANT}")
    print(f"🔒 Require Tenant Header: {REQUIRE_TENANT_HEADER}")
    print(f"🎯 Forwarding OpenWebUI requests to tenant-specific n8n workflows")
    print(f"💡 Available models:")
    print(f"   - siamtech-n8n-agent (main agent)")
    print(f"   - siamtech-n8n-postgres (database agent)")
    print(f"   - siamtech-n8n-knowledge (knowledge base agent)")
    print(f"   - siamtech-n8n-hybrid (hybrid agent)")
    print(f"   - Add '-company-a', '-company-b', '-company-c' for tenant-specific models")
    
    uvicorn.run(
        "openwebui_proxy:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )