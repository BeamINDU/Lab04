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
    """OpenAI compatible models endpoint - Auto Agent only"""
    
    # Determine current tenant from environment or default
    tenant_id = os.getenv('FORCE_TENANT', os.getenv('DEFAULT_TENANT', 'company-a'))
    
    # Get tenant configuration
    try:
        tenant_config = get_tenant_config(tenant_id)
        tenant_name = tenant_config.name
    except:
        tenant_name = f"SiamTech {tenant_id.replace('-', ' ').title()}"
    
    # Single Auto Agent model only
    model = {
        "id": f"siamtech-auto-agent-{tenant_id}",
        "object": "model",
        "created": int(time.time()),
        "owned_by": "siamtech",
        "permission": [],
        "root": "siamtech-auto-agent",
        "parent": None,
        "description": f"Auto Agent for {tenant_name} - Smart routing via n8n",
        "tenant_id": tenant_id,
        "tenant_name": tenant_name,
        "workflow": "n8n"
    }
    
    return {
        "object": "list",
        "data": [model]  # Only one Auto Agent model
    }
@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest):
    """OpenAI compatible endpoint that forwards to tenant-specific n8n workflows - FIXED VERSION"""
    try:
        if not request.messages:
            raise HTTPException(status_code=400, detail="No messages provided")
        
        # Determine tenant ID from environment variables
        tenant_id = os.getenv('FORCE_TENANT', os.getenv('DEFAULT_TENANT', 'company-a'))
        
        # Validate tenant
        if not validate_tenant_id(tenant_id):
            tenant_id = 'company-a'  # fallback
        
        # Get tenant configuration
        tenant_config = get_tenant_config(tenant_id)
        
        # Extract the user message
        user_message = request.messages[-1].content
        
        # Build conversation history
        conversation_history = []
        for msg in request.messages[:-1]:
            conversation_history.append({
                "role": msg.role,
                "content": msg.content
            })
        
        # Get n8n webhook URL
        webhook_url = tenant_config.webhooks.get('n8n_endpoint')
        if not webhook_url:
            webhook_url = f"{N8N_BASE_URL}/webhook/{tenant_id}-chat"
        
        # Prepare payload for n8n
        n8n_payload = {
            "message": user_message,
            "conversation_history": conversation_history,
            "tenant_id": tenant_id,
            "tenant_name": tenant_config.name,
            "agent_type": "auto",  # Always use auto agent
            "model": request.model,
            "max_tokens": min(request.max_tokens or 1000, tenant_config.settings.get('max_tokens', 1000)),
            "temperature": request.temperature or 0.7,
            "timestamp": int(time.time()),
            "settings": tenant_config.settings
        }
        
        logger.info(f"🚀 Forwarding to {webhook_url} for tenant {tenant_id}")
        logger.info(f"📝 Message: {user_message[:100]}...")
        
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
                    logger.error(f"❌ n8n webhook error: HTTP {response.status} - {error_text}")
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"n8n webhook error: {error_text}"
                    )
                
                try:
                    n8n_response = await response.json()
                    logger.info(f"✅ Got n8n response with keys: {list(n8n_response.keys()) if isinstance(n8n_response, dict) else 'Not dict'}")
                except json.JSONDecodeError as e:
                    error_text = await response.text()
                    logger.error(f"❌ Invalid JSON from n8n: {error_text}")
                    raise HTTPException(
                        status_code=502,
                        detail=f"Invalid JSON response from n8n: {str(e)}"
                    )
        
        # === EXTRACT ANSWER FROM N8N RESPONSE ===
        final_answer = None
        agent_used = "auto"
        success_status = False
        routing_info = "unknown"
        
        if isinstance(n8n_response, dict):
            # Step 1: Try to get 'answer' field directly
            raw_answer = n8n_response.get('answer')
            
            if raw_answer and raw_answer not in [None, 'None', 'null', '']:
                logger.info(f"✅ Found 'answer' field: {str(raw_answer)[:100]}...")
                final_answer = str(raw_answer)
                success_status = True
            else:
                # Step 2: Try alternative fields
                for alt_field in ['response', 'content', 'text', 'message']:
                    alt_value = n8n_response.get(alt_field)
                    if alt_value and alt_value not in [None, 'None', 'null', '']:
                        logger.info(f"📝 Using '{alt_field}' field: {str(alt_value)[:50]}...")
                        final_answer = str(alt_value)
                        success_status = True
                        break
                
                # Step 3: If still no answer, check nested objects
                if not final_answer:
                    for key, value in n8n_response.items():
                        if isinstance(value, dict) and 'answer' in value:
                            final_answer = str(value['answer'])
                            logger.info(f"🔍 Found nested answer in '{key}': {final_answer[:50]}...")
                            success_status = True
                            break
            
            # Extract metadata
            agent_used = n8n_response.get('agent', 'auto')
            success_status = n8n_response.get('success', success_status)
            routing_info = n8n_response.get('routing_decision', 'auto')
            
        else:
            logger.error(f"❌ n8n response is not a dict: {type(n8n_response)}")
            final_answer = f"Received non-dict response from n8n: {type(n8n_response)}"
        
        # === CLEAN UP THE ANSWER ===
        if final_answer:
            # Convert escaped newlines
            final_answer = final_answer.replace('\\n', '\n')
            
            # Handle Thai text encoding if needed
            if isinstance(final_answer, bytes):
                final_answer = final_answer.decode('utf-8', errors='ignore')
            
            # Clean up whitespace
            final_answer = final_answer.strip()
            
            # Remove footer metadata if desired (optional)
            if '\n---\n*ข้อมูลจาก:' in final_answer:
                main_content = final_answer.split('\n---\n*ข้อมูลจาก:')[0].strip()
                footer_content = final_answer.split('\n---\n*ข้อมูลจาก:')[1].strip()
                # Keep both parts but clean them up
                final_answer = f"{main_content}\n\n---\n*{footer_content}*"
        
        # === FALLBACK IF NO ANSWER ===
        if not final_answer or final_answer.strip() == "":
            logger.error("❌ No valid answer extracted from n8n response")
            final_answer = f"ขออภัย ไม่สามารถดึงคำตอบจากระบบได้\n\nDebug: n8n keys = {list(n8n_response.keys()) if isinstance(n8n_response, dict) else 'No dict'}"
            success_status = False
        
        # === LOG FINAL RESULT ===
        logger.info(f"🎯 Final answer ({len(final_answer)} chars): {final_answer[:100]}...")
        logger.info(f"✅ Success: {success_status}")
        
        # === CREATE OPENAI RESPONSE ===
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
                        "content": final_answer
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": len(user_message.split()),
                "completion_tokens": len(final_answer.split()),
                "total_tokens": len(user_message.split()) + len(final_answer.split())
            },
            "metadata": {
                "tenant_id": tenant_id,
                "tenant_name": tenant_config.name,
                "agent_used": agent_used,
                "workflow": "n8n",
                "success": success_status,
                "routing_decision": routing_info,
                "webhook_url": webhook_url,
                "timestamp": datetime.now().isoformat(),
                "debug_info": {
                    "n8n_response_keys": list(n8n_response.keys()) if isinstance(n8n_response, dict) else "Not dict",
                    "answer_length": len(final_answer) if final_answer else 0,
                    "extraction_method": "direct_answer_field" if n8n_response.get('answer') else "fallback_method"
                }
            }
        }
        
        logger.info(f"📤 Sending response to OpenWebUI with content length: {len(final_answer)}")
        return openai_response
        
    except aiohttp.ClientError as e:
        logger.error(f"🌐 Connection error to n8n: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=f"Cannot connect to n8n webhook: {str(e)}"
        )
    except Exception as e:
        logger.error(f"💥 Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

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