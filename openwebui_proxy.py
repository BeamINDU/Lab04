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
    model: Optional[str] = "siamtech-auto-agent"
    max_tokens: Optional[int] = 1000
    temperature: Optional[float] = 0.7
    tenant_id: Optional[str] = None

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
    description="Proxy service that forwards OpenWebUI requests to n8n workflows with multi-tenant support",
    version="2.2.0"
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

def extract_answer_from_n8n_response(n8n_response, tenant_id):
    """
    IMPROVED: Extract answer from n8n response with multiple fallback methods
    """
    final_answer = None
    agent_used = "auto"
    success_status = True
    routing_info = "unknown"
    extraction_method = "unknown"
    
    logger.info(f"🔍 [Tenant {tenant_id}] Extracting answer from n8n response type: {type(n8n_response)}")
    
    if not isinstance(n8n_response, dict):
        logger.error(f"❌ [Tenant {tenant_id}] n8n response is not dict: {type(n8n_response)}")
        return {
            "answer": f"Invalid response format from n8n: {type(n8n_response)}",
            "agent": "error",
            "success": False,
            "routing": "error",
            "extraction_method": "error"
        }
    
    logger.info(f"📋 [Tenant {tenant_id}] n8n response keys: {list(n8n_response.keys())}")
    
    # Debug: Log all potential answer fields
    answer_fields = ['answer', 'content', 'message', 'result', 'data', 'response', 'original_response']
    for field in answer_fields:
        if field in n8n_response:
            value = n8n_response[field]
            logger.debug(f"🔍 [Tenant {tenant_id}] Field '{field}': {type(value)} - {str(value)[:50]}...")
    
    # Debug: Log all potential agent fields  
    agent_fields = ['agent', 'agent_used', 'agent_type', 'routing_decision', 'data_source_used']
    for field in agent_fields:
        if field in n8n_response:
            value = n8n_response[field]
            logger.debug(f"🤖 [Tenant {tenant_id}] Field '{field}': {value}")
    
    # Method 1: Try to extract from 'answer' field directly
    if 'answer' in n8n_response:
        candidate = n8n_response['answer']
        if candidate and str(candidate).strip() not in ['None', 'null', 'undefined', '']:
            final_answer = str(candidate).strip()
            extraction_method = "direct_answer"
            logger.info(f"✅ [Tenant {tenant_id}] Method 1: Found answer in root.answer")
    
    # Method 2: Try to extract from nested response structures
    if not final_answer and 'response' in n8n_response:
        response_data = n8n_response['response']
        if isinstance(response_data, dict) and 'answer' in response_data:
            candidate = response_data['answer']
            if candidate and str(candidate).strip() not in ['None', 'null', 'undefined', '']:
                final_answer = str(candidate).strip()
                extraction_method = "nested_response"
                logger.info(f"✅ [Tenant {tenant_id}] Method 2: Found answer in response.answer")
    
    # Method 3: Try to extract from original_response (for n8n workflow formatted responses)
    if not final_answer and 'original_response' in n8n_response:
        original_resp = n8n_response['original_response']
        if isinstance(original_resp, dict) and 'answer' in original_resp:
            candidate = original_resp['answer']
            if candidate and str(candidate).strip() not in ['None', 'null', 'undefined', '']:
                final_answer = str(candidate).strip()
                extraction_method = "original_response"
                logger.info(f"✅ [Tenant {tenant_id}] Method 3: Found answer in original_response.answer")
    
    # Method 4: Try to extract from content field
    if not final_answer and 'content' in n8n_response:
        candidate = n8n_response['content']
        if candidate and str(candidate).strip() not in ['None', 'null', 'undefined', '']:
            final_answer = str(candidate).strip()
            extraction_method = "content_field"
            logger.info(f"✅ [Tenant {tenant_id}] Method 4: Found answer in content")
    
    # Method 5: Try to extract from data field (some n8n responses)
    if not final_answer and 'data' in n8n_response:
        data = n8n_response['data']
        if isinstance(data, dict) and 'answer' in data:
            candidate = data['answer']
            if candidate and str(candidate).strip() not in ['None', 'null', 'undefined', '']:
                final_answer = str(candidate).strip()
                extraction_method = "data_answer"
                logger.info(f"✅ [Tenant {tenant_id}] Method 5: Found answer in data.answer")
    
    # Method 6: Try to extract from result field
    if not final_answer and 'result' in n8n_response:
        result = n8n_response['result']
        if isinstance(result, dict) and 'answer' in result:
            candidate = result['answer']
            if candidate and str(candidate).strip() not in ['None', 'null', 'undefined', '']:
                final_answer = str(candidate).strip()
                extraction_method = "result_answer"
                logger.info(f"✅ [Tenant {tenant_id}] Method 6: Found answer in result.answer")
        elif isinstance(result, str) and result.strip():
            final_answer = result.strip()
            extraction_method = "result_direct"
            logger.info(f"✅ [Tenant {tenant_id}] Method 6b: Found answer in result directly")
    
    # Method 8: Try to extract from any nested structures that might contain answer
    if not final_answer:
        # Look for answer in any nested object
        for key, value in n8n_response.items():
            if isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    if nested_key == 'answer' and nested_value:
                        candidate = str(nested_value).strip()
                        if candidate not in ['None', 'null', 'undefined', '']:
                            final_answer = candidate
                            extraction_method = f"nested_{key}_{nested_key}"
                            logger.info(f"✅ [Tenant {tenant_id}] Method 8: Found answer in {key}.{nested_key}")
                            break
                if final_answer:
                    break
    
    # Method 9: Try to extract from lists/arrays
    if not final_answer:
        for key, value in n8n_response.items():
            if isinstance(value, list) and len(value) > 0:
                first_item = value[0]
                if isinstance(first_item, dict) and 'answer' in first_item:
                    candidate = str(first_item['answer']).strip()
                    if candidate not in ['None', 'null', 'undefined', '']:
                        final_answer = candidate
                        extraction_method = f"list_{key}_answer"
                        logger.info(f"✅ [Tenant {tenant_id}] Method 9: Found answer in {key}[0].answer")
                        break
                elif isinstance(first_item, str) and first_item.strip():
                    final_answer = first_item.strip()
                    extraction_method = f"list_{key}_direct"
                    logger.info(f"✅ [Tenant {tenant_id}] Method 9b: Found answer in {key}[0] directly")
                    break
    
    # Extract metadata with multiple field names and nested structures
    success_status = n8n_response.get('success', True)
    
    # Try multiple field names for agent
    agent_used = (n8n_response.get('agent_used') or 
                  n8n_response.get('agent') or 
                  n8n_response.get('agent_type') or 
                  'auto')
    
    # Try nested structures for agent info
    if agent_used == 'auto':
        for key, value in n8n_response.items():
            if isinstance(value, dict):
                nested_agent = (value.get('agent_used') or 
                               value.get('agent') or 
                               value.get('agent_type'))
                if nested_agent:
                    agent_used = nested_agent
                    break
    
    # Try multiple field names for routing
    routing_info = (n8n_response.get('routing_decision') or 
                   n8n_response.get('data_source_used') or 
                   n8n_response.get('source') or 
                   n8n_response.get('agent_used') or 
                   'unknown')
    
    # Try nested structures for routing info
    if routing_info == 'unknown':
        for key, value in n8n_response.items():
            if isinstance(value, dict):
                nested_routing = (value.get('routing_decision') or 
                                 value.get('data_source_used') or 
                                 value.get('source'))
                if nested_routing:
                    routing_info = nested_routing
                    break
    
    # Try nested structures for success status
    if success_status is True:  # Only override if default True
        for key, value in n8n_response.items():
            if isinstance(value, dict) and 'success' in value:
                success_status = value['success']
                break
    
    # Clean up the final answer
    if final_answer:
        final_answer = final_answer.replace('\\n', '\n').strip()
        # Remove JSON artifacts if present
        if final_answer.startswith('"') and final_answer.endswith('"'):
            final_answer = final_answer[1:-1]
        
        logger.info(f"🎯 [Tenant {tenant_id}] FINAL ANSWER extracted via {extraction_method}: {final_answer[:100]}...")
        logger.info(f"🤖 [Tenant {tenant_id}] Agent used: {agent_used}")
        logger.info(f"🎯 [Tenant {tenant_id}] Routing info: {routing_info}")
        logger.info(f"✅ [Tenant {tenant_id}] Success status: {success_status}")
    else:
        logger.error(f"❌ [Tenant {tenant_id}] CRITICAL: No answer found in any field!")
        logger.error(f"🔍 [Tenant {tenant_id}] Available fields: {list(n8n_response.keys())}")
        
        # Log sample of each field for debugging
        for key, value in n8n_response.items():
            if isinstance(value, (str, int, float, bool)):
                logger.error(f"🔍 [Tenant {tenant_id}] {key}: {value}")
            elif isinstance(value, dict):
                logger.error(f"🔍 [Tenant {tenant_id}] {key}: dict with keys {list(value.keys())}")
                if 'answer' in value:
                    logger.error(f"🔍 [Tenant {tenant_id}] {key}.answer: {value['answer']}")
            elif isinstance(value, list):
                logger.error(f"🔍 [Tenant {tenant_id}] {key}: list with {len(value)} items")
                if len(value) > 0:
                    logger.error(f"🔍 [Tenant {tenant_id}] {key}[0]: {value[0]}")
        
        # Create fallback answer
        tenant_config = get_tenant_config(tenant_id)
        if tenant_config.settings.get('response_language') == 'en':
            final_answer = f"I received your message but couldn't generate a proper response. Please try again."
        else:
            final_answer = f"ได้รับข้อความของคุณแล้ว แต่ไม่สามารถสร้างคำตอบได้ กรุณาลองใหม่อีกครั้ง"
        
        success_status = False
        extraction_method = "fallback"
    
    return {
        "answer": final_answer,
        "agent": agent_used,
        "success": success_status,
        "routing": routing_info,
        "extraction_method": extraction_method
    }

# Dependency to get tenant ID
def get_tenant_id(
    x_tenant_id: Optional[str] = Header(None, alias="X-Tenant-ID"),
    tenant_query: Optional[str] = Query(None, alias="tenant"),
    request_tenant: Optional[str] = None
) -> str:
    """Get tenant ID from various sources with priority"""
    tenant_id = request_tenant or x_tenant_id or tenant_query or DEFAULT_TENANT
    
    if not validate_tenant_id(tenant_id):
        logger.warning(f"Invalid tenant ID: {tenant_id}, using default: {DEFAULT_TENANT}")
        if REQUIRE_TENANT_HEADER:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid tenant ID: {tenant_id}. Available tenants: {list(list_available_tenants().keys())}"
            )
        tenant_id = DEFAULT_TENANT
    
    return tenant_id

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
    print("🎯 Ready to forward requests to n8n with improved response handling")

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Multi-Tenant OpenWebUI to n8n Proxy",
        "n8n_base_url": N8N_BASE_URL,
        "default_tenant": DEFAULT_TENANT,
        "available_tenants": list(list_available_tenants().keys()),
        "version": "2.2.0"
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

@app.get("/v1/models")
async def list_models():
    """OpenAI compatible models endpoint"""
    tenant_id = os.getenv('FORCE_TENANT', os.getenv('DEFAULT_TENANT', 'company-a'))
    
    try:
        tenant_config = get_tenant_config(tenant_id)
        tenant_name = tenant_config.name
    except:
        tenant_name = f"SiamTech {tenant_id.replace('-', ' ').title()}"
    
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
        "data": [model]
    }

@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest):
    """OpenAI compatible endpoint - FIXED VERSION with improved response handling"""
    try:
        if not request.messages:
            raise HTTPException(status_code=400, detail="No messages provided")
        
        # Determine tenant ID from environment
        tenant_id = os.getenv('FORCE_TENANT', os.getenv('DEFAULT_TENANT', 'company-a'))
        
        if not validate_tenant_id(tenant_id):
            tenant_id = 'company-a'
        
        tenant_config = get_tenant_config(tenant_id)
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
            "agent_type": "auto",
            "model": request.model,
            "max_tokens": min(request.max_tokens or 1000, tenant_config.settings.get('max_tokens', 1000)),
            "temperature": request.temperature or 0.7,
            "timestamp": int(time.time()),
            "settings": tenant_config.settings
        }
        
        logger.info(f"🚀 [Tenant {tenant_id}] Forwarding to {webhook_url}")
        logger.info(f"📝 [Tenant {tenant_id}] Message: {user_message[:100]}...")
        
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
                    logger.error(f"❌ [Tenant {tenant_id}] n8n webhook error: HTTP {response.status} - {error_text}")
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"n8n webhook error: {error_text}"
                    )
                
                try:
                    n8n_response = await response.json()
                    logger.info(f"✅ [Tenant {tenant_id}] Got n8n response")
                except json.JSONDecodeError as e:
                    error_text = await response.text()
                    logger.error(f"❌ [Tenant {tenant_id}] Invalid JSON from n8n: {error_text}")
                    raise HTTPException(
                        status_code=502,
                        detail=f"Invalid JSON response from n8n: {str(e)}"
                    )
        
        # Extract answer using improved method
        extraction_result = extract_answer_from_n8n_response(n8n_response, tenant_id)
        
        final_answer = extraction_result["answer"]
        agent_used = extraction_result["agent"]
        success_status = extraction_result["success"]
        routing_info = extraction_result["routing"]
        extraction_method = extraction_result["extraction_method"]
        
        # Log final result
        logger.info(f"🎯 [Tenant {tenant_id}] FINAL RESULT:")
        logger.info(f"   Answer Length: {len(final_answer) if final_answer else 0}")
        logger.info(f"   Success: {success_status}")
        logger.info(f"   Agent: {agent_used}")
        logger.info(f"   Routing: {routing_info}")
        logger.info(f"   Extraction Method: {extraction_method}")
        
        # Create OpenAI response
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
                "completion_tokens": len(final_answer.split()) if final_answer else 0,
                "total_tokens": len(user_message.split()) + (len(final_answer.split()) if final_answer else 0)
            },
            "metadata": {
                "tenant_id": tenant_id,
                "tenant_name": tenant_config.name,
                "agent_used": agent_used,
                "workflow": "n8n",
                "success": success_status,
                "routing_decision": routing_info,
                "extraction_method": extraction_method,
                "webhook_url": webhook_url,
                "timestamp": datetime.now().isoformat(),
                "proxy_version": "2.2.0",
                "debug_info": {
                    "n8n_response_keys": list(n8n_response.keys()) if isinstance(n8n_response, dict) else "Not dict",
                    "answer_length": len(final_answer) if final_answer else 0,
                    "has_answer": bool(final_answer and final_answer.strip())
                }
            }
        }
        
        logger.info(f"📤 [Tenant {tenant_id}] Sending response to OpenWebUI with content length: {len(final_answer)}")
        return openai_response
        
    except aiohttp.ClientError as e:
        logger.error(f"🌐 [Tenant {tenant_id}] Connection error to n8n: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=f"Cannot connect to n8n webhook: {str(e)}"
        )
    except Exception as e:
        logger.error(f"💥 [Tenant {tenant_id}] Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

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
                    # Test the extraction method
                    extraction_result = extract_answer_from_n8n_response(response_data, tenant_id)
                    
                    return {
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_config.name,
                        "status": "success" if status == 200 else "error",
                        "http_status": status,
                        "response": response_data,
                        "webhook_url": webhook_url,
                        "extraction_test": extraction_result
                    }
                except Exception as e:
                    response_text = await response.text()
                    return {
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_config.name,
                        "status": "error",
                        "http_status": status,
                        "response": response_text,
                        "webhook_url": webhook_url,
                        "extraction_error": str(e)
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
    """Test n8n connections for all tenants with extraction testing"""
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
                        extraction_result = extract_answer_from_n8n_response(response_data, tenant_id)
                        
                        results["tenants"][tenant_id] = {
                            "name": tenant_config.name,
                            "status": "success" if status == 200 else "error",
                            "http_status": status,
                            "webhook_url": webhook_url,
                            "response_size": len(str(response_data)),
                            "extraction_test": {
                                "success": extraction_result["success"],
                                "method": extraction_result["extraction_method"],
                                "has_answer": bool(extraction_result["answer"])
                            }
                        }
                    except Exception as e:
                        results["tenants"][tenant_id] = {
                            "name": tenant_config.name,
                            "status": "error",
                            "http_status": status,
                            "webhook_url": webhook_url,
                            "error": str(e)
                        }
                    
        except Exception as e:
            results["tenants"][tenant_id] = {
                "status": "error",
                "error": str(e)
            }
    
    return results

@app.get("/tenant/{tenant_id}/config")
async def get_tenant_config_endpoint(tenant_id: str):
    """Get tenant configuration"""
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

@app.get("/metrics")
async def get_metrics():
    """Get basic metrics"""
    return {
        "total_tenants": len(list_available_tenants()),
        "default_tenant": DEFAULT_TENANT,
        "n8n_base_url": N8N_BASE_URL,
        "require_tenant_header": REQUIRE_TENANT_HEADER,
        "uptime": time.time(),
        "version": "2.2.0",
        "extraction_methods": [
            "direct_answer",
            "nested_response", 
            "original_response",
            "content_field",
            "data_answer",
            "result_answer",
            "message_content",
            "fallback"
        ]
    }

if __name__ == '__main__':
    print(f"🚀 Multi-Tenant OpenWebUI to n8n Proxy Service v2.2.0")
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
    print(f"🎯 Improved response extraction with 8 fallback methods")
    print(f"🔍 Better logging and debugging for multi-tenant responses")
    
    uvicorn.run(
        "openwebui_proxy:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )