import json
import os
import asyncio
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
import logging
from datetime import datetime

# Import our simplified components
from aggregator_agent import AggregatorAgent
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
class RAGQuery(BaseModel):
    query: str
    tenant_id: Optional[str] = None

class RAGResponse(BaseModel):
    answer: str
    source: str
    agent: str
    success: bool
    tenant_id: str
    tenant_name: str
    data_source_used: Optional[str] = None
    timestamp: datetime

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    messages: List[ChatMessage]
    model: Optional[str] = "siamtech-auto-agent"
    max_tokens: Optional[int] = 1000
    tenant_id: Optional[str] = None

class TenantInfo(BaseModel):
    tenant_id: str
    name: str
    agent_type: str
    settings: dict

# FastAPI app
app = FastAPI(
    title="SiamTech Auto Agent Service",
    description="Simplified AI Agent with Smart Routing - Multi-Tenant Support",
    version="4.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Auto Agent
aggregator = AggregatorAgent()

# Dependency to get tenant ID
def get_tenant_id(
    x_tenant_id: Optional[str] = Header(None, alias="X-Tenant-ID"),
    tenant_id_param: Optional[str] = None
) -> str:
    """Get tenant ID from header or parameter"""
    tenant_id = x_tenant_id or tenant_id_param or tenant_manager.default_tenant
    
    # Validate tenant ID
    if not validate_tenant_id(tenant_id):
        logger.warning(f"Invalid tenant ID: {tenant_id}, using default: {tenant_manager.default_tenant}")
        tenant_id = tenant_manager.default_tenant
    
    logger.info(f"Using tenant ID: {tenant_id}")  # Add logging
    return tenant_id

@app.on_event("startup")
async def startup_event():
    print("🚀 SiamTech Auto Agent Service starting...")
    print("🤖 Single Auto Agent with Smart Routing")
    print("🎯 Automatically chooses between Database and Knowledge Base")
    print("🏢 Available Tenants:")
    
    tenants = list_available_tenants()
    for tenant_id, name in tenants.items():
        print(f"   - {tenant_id}: {name}")
    
    print(f"🔧 Default Tenant: {tenant_manager.default_tenant}")

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Auto Agent",
        "agent_type": "smart_routing",
        "version": "4.0.0",
        "tenants": list(list_available_tenants().keys()),
        "default_tenant": tenant_manager.default_tenant,
        "description": "Single Auto Agent with intelligent routing to database or knowledge base"
    }

@app.get("/tenants", response_model=List[TenantInfo])
async def list_tenants():
    """List all available tenants and their configurations"""
    tenants = []
    
    for tenant_id, name in list_available_tenants().items():
        try:
            config = get_tenant_config(tenant_id)
            
            tenants.append(TenantInfo(
                tenant_id=tenant_id,
                name=name,
                agent_type="auto_agent",
                settings=config.settings
            ))
        except Exception as e:
            logger.error(f"Error getting config for tenant {tenant_id}: {e}")
    
    return tenants

@app.get("/tenants/{tenant_id}/status")
async def get_tenant_status(tenant_id: str):
    """Get status of auto agent for specific tenant"""
    try:
        if not validate_tenant_id(tenant_id):
            raise HTTPException(status_code=404, detail=f"Tenant {tenant_id} not found")
        
        status = await aggregator.get_tenant_agent_status(tenant_id)
        return status
        
    except Exception as e:
        logger.error(f"Error getting tenant status for {tenant_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/models")
async def list_models():
    """OpenAI compatible models endpoint - simplified to Auto Agent only"""
    models = []
    
    # Single model type for all tenants
    base_model = {
        "id": "siamtech-auto-agent",
        "description": "Smart Auto Agent (Database + Knowledge Base with intelligent routing)"
    }
    
    # Add tenant-specific models
    tenants = list_available_tenants()
    for tenant_id, tenant_name in tenants.items():
        models.append({
            "id": f"siamtech-auto-agent-{tenant_id}",
            "object": "model",
            "created": 1234567890,
            "owned_by": "siamtech",
            "permission": [],
            "root": "siamtech-auto-agent",
            "parent": None,
            "description": f"Auto Agent for {tenant_name} - Smart routing between database and knowledge base",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "agent_type": "auto"
        })
    
    # Add base model (will use default tenant or header)
    models.append({
        "id": "siamtech-auto-agent",
        "object": "model",
        "created": 1234567890,
        "owned_by": "siamtech",
        "permission": [],
        "root": "siamtech-auto-agent",
        "parent": None,
        "description": "Smart Auto Agent with intelligent routing",
        "agent_type": "auto"
    })
    
    return {
        "object": "list",
        "data": models
    }

@app.post("/rag-query", response_model=RAGResponse)
async def rag_query(request: RAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """RAG query endpoint ที่รับจาก n8n"""
    try:
        # ถ้าไม่ได้ระบุ agent_type หรือระบุเป็น 'auto'
        if not hasattr(request, 'agent_type') or request.agent_type in [None, 'auto']:
            # ใช้ aggregator_agent ตัดสินใจ
            result = await aggregator.process_question(request.query, tenant_id)
        else:
            # ใช้ agent ที่ระบุ (backward compatibility)
            result = await process_specific_agent(request, tenant_id)
        
        return RAGResponse(
            answer=result["answer"],
            source=result.get("source", "Auto Agent"),
            agent="auto",
            success=result.get("success", True),
            tenant_id=tenant_id,
            tenant_name=result.get("tenant_name", ""),
            data_source_used=result.get("data_source_used"),
            timestamp=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Error in auto agent query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/chat/completions")
async def openai_compatible(
    request: ChatCompletionRequest, 
    tenant_id: str = Depends(get_tenant_id)
):
    """OpenAI compatible endpoint for Open WebUI with auto agent"""
    try:
        if not request.messages:
            raise HTTPException(status_code=400, detail="No messages provided")
        
        # Override tenant_id if specified in request
        if request.tenant_id:
            tenant_id = request.tenant_id
            if not validate_tenant_id(tenant_id):
                raise HTTPException(status_code=400, detail=f"Invalid tenant ID: {tenant_id}")
        
        # Extract tenant from model name if specified
        if "-" in request.model:
            model_parts = request.model.split('-')
            if len(model_parts) >= 4 and model_parts[-2] == 'company':
                model_tenant_id = f"company-{model_parts[-1]}"
                if validate_tenant_id(model_tenant_id):
                    tenant_id = model_tenant_id
        
        user_message = request.messages[-1].content
        
        logger.info(f"OpenAI compatible request for tenant: {tenant_id}, model: {request.model}")
        
        # Get tenant info
        tenant_config = get_tenant_config(tenant_id)
        
        # Process with Auto Agent (always uses smart routing)
        result = await aggregator.process_question(user_message, tenant_id)
        
        # Format as OpenAI response
        response = {
            "id": f"chatcmpl-auto-{tenant_id}-{int(datetime.now().timestamp())}",
            "object": "chat.completion",
            "created": int(datetime.now().timestamp()),
            "model": request.model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": result["answer"]
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": len(user_message.split()),
                "completion_tokens": len(result["answer"].split()),
                "total_tokens": len(user_message.split()) + len(result["answer"].split())
            },
            "metadata": {
                "tenant_id": tenant_id,
                "tenant_name": tenant_config.name,
                "agent_type": "auto",
                "data_source_used": result.get("data_source_used", "auto"),
                "source": result.get("source", "Auto Agent"),
                "success": result.get("success", True),
                "timestamp": datetime.now().isoformat(),
                "routing": "smart_auto"
            }
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error in OpenAI endpoint for tenant {tenant_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/agents/status")
async def agents_status(tenant_id: str = Depends(get_tenant_id)):
    """Check status of auto agent for specific tenant"""
    try:
        status = await aggregator.get_tenant_agent_status(tenant_id)
        return status
    except Exception as e:
        logger.error(f"Error getting agent status for tenant {tenant_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tenants/{tenant_id}/test")
async def test_tenant(tenant_id: str):
    """Test auto agent for specific tenant"""
    try:
        if not validate_tenant_id(tenant_id):
            raise HTTPException(status_code=404, detail=f"Tenant {tenant_id} not found")
        
        tenant_config = get_tenant_config(tenant_id)
        
        # Test questions for auto agent
        test_questions = [
            {"question": "How many employees are there?", "expected_source": "database"},
            {"question": "What services does the company provide?", "expected_source": "documents"},
            {"question": "Tell me about the company and employee statistics", "expected_source": "both"}
        ]
        
        results = {
            "tenant_id": tenant_id,
            "tenant_name": tenant_config.name,
            "agent_type": "auto",
            "tests": []
        }
        
        for test in test_questions:
            try:
                result = await aggregator.process_question(test["question"], tenant_id)
                
                results["tests"].append({
                    "question": test["question"],
                    "expected_source": test["expected_source"],
                    "actual_source": result.get("data_source_used"),
                    "success": result.get("success", False),
                    "answer_length": len(result.get("answer", "")),
                    "has_answer": bool(result.get("answer", "").strip())
                })
                
            except Exception as e:
                results["tests"].append({
                    "question": test["question"],
                    "expected_source": test["expected_source"],
                    "success": False,
                    "error": str(e)
                })
        
        return results
        
    except Exception as e:
        logger.error(f"Error testing tenant {tenant_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/info")
async def get_system_info():
    """Get system information"""
    return {
        "service_name": "SiamTech Auto Agent",
        "version": "4.0.0",
        "agent_type": "single_auto_agent",
        "description": "Simplified AI system with one intelligent agent that automatically routes to database or knowledge base",
        "features": [
            "Smart routing between database and knowledge base",
            "Multi-tenant support",
            "OpenAI compatible API",
            "Simplified architecture"
        ],
        "data_sources": ["PostgreSQL Database", "AWS Bedrock Knowledge Base"],
        "routing_strategy": "intelligent_keyword_and_ai",
        "tenants": list(list_available_tenants().keys()),
        "default_tenant": tenant_manager.default_tenant
    }

@app.post("/admin/reload-config")
async def reload_tenant_config():
    """Reload tenant configuration (admin endpoint)"""
    try:
        # Reload tenant manager
        tenant_manager._load_config()
        
        return {
            "success": True,
            "message": "Tenant configuration reloaded",
            "tenants": list(list_available_tenants().keys()),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error reloading config: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/admin/tenant-stats")
async def get_tenant_stats():
    """Get statistics for all tenants (admin endpoint)"""
    try:
        stats = {}
        
        for tenant_id in list_available_tenants().keys():
            try:
                status = await aggregator.get_tenant_agent_status(tenant_id)
                config = get_tenant_config(tenant_id)
                
                stats[tenant_id] = {
                    "name": config.name,
                    "agent_type": "auto",
                    "database_status": status.get("database", {}).get("status"),
                    "knowledge_base_status": status.get("knowledge_base", {}).get("status"),
                    "auto_agent_status": status.get("auto_agent", {}).get("status"),
                    "settings": {
                        "max_tokens": config.settings.get("max_tokens"),
                        "temperature": config.settings.get("temperature"),
                        "response_language": config.settings.get("response_language"),
                        "database_enabled": config.settings.get("enable_postgres_agent", True),
                        "knowledge_base_enabled": config.settings.get("enable_knowledge_base_agent", True),
                        "hybrid_search_enabled": config.settings.get("allow_hybrid_search", True)
                    }
                }
            except Exception as e:
                stats[tenant_id] = {"error": str(e)}
        
        return {
            "total_tenants": len(list_available_tenants()),
            "default_tenant": tenant_manager.default_tenant,
            "agent_architecture": "single_auto_agent",
            "tenant_stats": stats,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting tenant stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    print(f"🚀 SiamTech Auto Agent Service")
    print(f"🔗 API Documentation: http://localhost:5000/docs")
    print(f"🎯 Single Auto Agent with Smart Routing")
    print(f"🏢 Available Tenants:")
    
    tenants = list_available_tenants()
    for tenant_id, name in tenants.items():
        print(f"   - {tenant_id}: {name}")
    
    print(f"💡 Available model:")
    print(f"   - siamtech-auto-agent (smart routing)")
    print(f"   - siamtech-auto-agent-company-a (Bangkok HQ)")
    print(f"   - siamtech-auto-agent-company-b (Chiang Mai)")
    print(f"   - siamtech-auto-agent-company-c (International)")
    print(f"")
    print(f"🧠 How it works:")
    print(f"   - Automatically detects if question needs database or documents")
    print(f"   - Routes to PostgreSQL for: employee data, statistics, numbers")
    print(f"   - Routes to Knowledge Base for: company info, policies, services")
    print(f"   - Uses both sources for comprehensive questions")
    print(f"   - Single API endpoint, smart backend routing")
    
    uvicorn.run(
        "multi_agent_service:app",
        host="0.0.0.0",
        port=5000,
        reload=True
    )