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

# Import our multi-tenant components
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
    agent_type: Optional[str] = "auto"  # auto, postgres, knowledge_base, hybrid
    tenant_id: Optional[str] = None  # Can override header

class RAGResponse(BaseModel):
    answer: str
    source: str
    agent: str
    success: bool
    tenant_id: str
    tenant_name: str
    routing_decision: Optional[str] = None
    timestamp: datetime

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    messages: List[ChatMessage]
    model: Optional[str] = "siamtech-multi-agent"
    max_tokens: Optional[int] = 1000
    tenant_id: Optional[str] = None  # Can override header

class TenantInfo(BaseModel):
    tenant_id: str
    name: str
    enabled_agents: List[str]
    settings: dict

# FastAPI app
app = FastAPI(
    title="SiamTech Multi-Tenant RAG Service",
    description="Multi-Agent RAG Service with PostgreSQL and Knowledge Base - Multi-Tenant Support",
    version="3.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Aggregator Agent
aggregator = AggregatorAgent()

# Dependency to get tenant ID
def get_tenant_id(
    x_tenant_id: Optional[str] = Header(None, alias="X-Tenant-ID"),
    tenant_id_param: Optional[str] = None
) -> str:
    """Get tenant ID from header or parameter"""
    tenant_id = tenant_id_param or x_tenant_id or tenant_manager.default_tenant
    
    # Validate tenant ID
    if not validate_tenant_id(tenant_id):
        logger.warning(f"Invalid tenant ID: {tenant_id}, using default: {tenant_manager.default_tenant}")
        tenant_id = tenant_manager.default_tenant
    
    return tenant_id

@app.on_event("startup")
async def startup_event():
    print("🚀 SiamTech Multi-Tenant RAG Service starting...")
    print("🤖 Agents available: PostgreSQL, Knowledge Base")
    print("🎯 Aggregator Agent initialized")
    print("🏢 Available Tenants:")
    
    tenants = list_available_tenants()
    for tenant_id, name in tenants.items():
        print(f"   - {tenant_id}: {name}")
    
    print(f"🔧 Default Tenant: {tenant_manager.default_tenant}")

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Multi-Tenant RAG",
        "agents": ["postgres", "knowledge_base", "aggregator"],
        "version": "3.0.0",
        "tenants": list(list_available_tenants().keys()),
        "default_tenant": tenant_manager.default_tenant
    }

@app.get("/tenants", response_model=List[TenantInfo])
async def list_tenants():
    """List all available tenants and their configurations"""
    tenants = []
    
    for tenant_id, name in list_available_tenants().items():
        try:
            config = get_tenant_config(tenant_id)
            enabled_agents = []
            
            if config.settings.get('enable_postgres_agent', True):
                enabled_agents.append('postgres')
            if config.settings.get('enable_knowledge_base_agent', True):
                enabled_agents.append('knowledge_base')
            if config.settings.get('allow_hybrid_search', True):
                enabled_agents.append('hybrid')
            
            tenants.append(TenantInfo(
                tenant_id=tenant_id,
                name=name,
                enabled_agents=enabled_agents,
                settings=config.settings
            ))
        except Exception as e:
            logger.error(f"Error getting config for tenant {tenant_id}: {e}")
    
    return tenants

@app.get("/tenants/{tenant_id}/status")
async def get_tenant_status(tenant_id: str):
    """Get status of agents for specific tenant"""
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
    """OpenAI compatible models endpoint with tenant support"""
    models = []
    
    # Base models
    base_models = [
        {
            "id": "siamtech-multi-agent",
            "description": "Multi-Agent RAG with Auto-routing (Multi-tenant)"
        },
        {
            "id": "siamtech-postgres-agent",
            "description": "PostgreSQL Agent only (Multi-tenant)"
        },
        {
            "id": "siamtech-knowledge-agent",
            "description": "Knowledge Base Agent only (Multi-tenant)"
        },
        {
            "id": "siamtech-hybrid-agent",
            "description": "Hybrid Search (PostgreSQL + Knowledge Base) (Multi-tenant)"
        }
    ]
    
    # Add tenant-specific models
    tenants = list_available_tenants()
    for tenant_id, tenant_name in tenants.items():
        for base_model in base_models:
            models.append({
                "id": f"{base_model['id']}-{tenant_id}",
                "object": "model",
                "created": 1234567890,
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
            "created": 1234567890,
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

@app.post("/rag-query", response_model=RAGResponse)
async def rag_query(request: RAGQuery, tenant_id: str = Depends(get_tenant_id)):
    """RAG query endpoint with multi-tenant support"""
    try:
        # Override tenant_id if specified in request
        if request.tenant_id:
            tenant_id = request.tenant_id
            if not validate_tenant_id(tenant_id):
                raise HTTPException(status_code=400, detail=f"Invalid tenant ID: {tenant_id}")
        
        logger.info(f"Processing RAG query for tenant: {tenant_id}, agent_type: {request.agent_type}")
        
        # Get tenant info
        tenant_config = get_tenant_config(tenant_id)
        
        # Route to appropriate agent
        if request.agent_type == "postgres":
            result = await aggregator.query_postgres_agent(request.query, tenant_id)
        elif request.agent_type == "knowledge_base":
            result = await aggregator.query_knowledge_base_agent(request.query, tenant_id)
        elif request.agent_type == "hybrid":
            result = await aggregator.hybrid_search(request.query, tenant_id)
        else:
            # Auto routing
            result = await aggregator.process_question(request.query, tenant_id)
        
        return RAGResponse(
            answer=result["answer"],
            source=result.get("source", "Unknown"),
            agent=result.get("agent", "unknown"),
            success=result.get("success", True),
            tenant_id=tenant_id,
            tenant_name=tenant_config.name,
            routing_decision=result.get("routing_decision"),
            timestamp=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Error in RAG query for tenant {tenant_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/chat/completions")
async def openai_compatible(
    request: ChatCompletionRequest, 
    tenant_id: str = Depends(get_tenant_id)
):
    """OpenAI compatible endpoint for Open WebUI with multi-tenant support"""
    try:
        if not request.messages:
            raise HTTPException(status_code=400, detail="No messages provided")
        
        # Override tenant_id if specified in request
        if request.tenant_id:
            tenant_id = request.tenant_id
            if not validate_tenant_id(tenant_id):
                raise HTTPException(status_code=400, detail=f"Invalid tenant ID: {tenant_id}")
        
        # Extract tenant from model name if specified (e.g., "siamtech-multi-agent-company-b")
        model_parts = request.model.split('-')
        if len(model_parts) >= 4 and model_parts[-2] == 'company':
            model_tenant_id = f"company-{model_parts[-1]}"
            if validate_tenant_id(model_tenant_id):
                tenant_id = model_tenant_id
        
        user_message = request.messages[-1].content
        
        logger.info(f"OpenAI compatible request for tenant: {tenant_id}, model: {request.model}")
        
        # Get tenant info
        tenant_config = get_tenant_config(tenant_id)
        
        # Determine agent type from model selection
        agent_type = "auto"
        if "postgres" in request.model.lower():
            agent_type = "postgres"
        elif "knowledge" in request.model.lower():
            agent_type = "knowledge_base"
        elif "hybrid" in request.model.lower():
            agent_type = "hybrid"
        elif "multi-agent" in request.model.lower():
            agent_type = "auto"
        
        # Process with appropriate agent
        if agent_type == "postgres":
            result = await aggregator.query_postgres_agent(user_message, tenant_id)
        elif agent_type == "knowledge_base":
            result = await aggregator.query_knowledge_base_agent(user_message, tenant_id)
        elif agent_type == "hybrid":
            result = await aggregator.hybrid_search(user_message, tenant_id)
        else:
            result = await aggregator.process_question(user_message, tenant_id)
        
        # Format as OpenAI response
        response = {
            "id": f"chatcmpl-{tenant_id}-{int(datetime.now().timestamp())}",
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
                "agent_used": result.get("agent", "unknown"),
                "source": result.get("source", "unknown"),
                "routing_decision": result.get("routing_decision"),
                "success": result.get("success", True),
                "timestamp": datetime.now().isoformat()
            }
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error in OpenAI endpoint for tenant {tenant_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/agents/status")
async def agents_status(tenant_id: str = Depends(get_tenant_id)):
    """Check status of all agents for specific tenant"""
    try:
        status = await aggregator.get_tenant_agent_status(tenant_id)
        return status
    except Exception as e:
        logger.error(f"Error getting agent status for tenant {tenant_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/agents/metrics")
async def agents_metrics(tenant_id: str = Depends(get_tenant_id)):
    """Get performance metrics for specific tenant"""
    # This would be implemented with actual metrics collection
    # For now, return placeholder data
    return {
        "tenant_id": tenant_id,
        "total_queries": 0,
        "postgres_queries": 0,
        "knowledge_base_queries": 0,
        "hybrid_queries": 0,
        "avg_response_time": 0,
        "success_rate": 0,
        "last_24h": {
            "queries": 0,
            "errors": 0,
            "avg_response_time": 0
        }
    }

@app.post("/tenants/{tenant_id}/test")
async def test_tenant(tenant_id: str):
    """Test all agents for specific tenant"""
    try:
        if not validate_tenant_id(tenant_id):
            raise HTTPException(status_code=404, detail=f"Tenant {tenant_id} not found")
        
        tenant_config = get_tenant_config(tenant_id)
        
        # Test questions
        test_questions = [
            {"question": "มีพนักงานกี่คน?", "expected_agent": "postgres"},
            {"question": "บริษัททำธุรกิจอะไร?", "expected_agent": "knowledge_base"},
            {"question": "ข้อมูลทั่วไปของบริษัท", "expected_agent": "hybrid"}
        ]
        
        results = {
            "tenant_id": tenant_id,
            "tenant_name": tenant_config.name,
            "tests": []
        }
        
        for test in test_questions:
            try:
                if test["expected_agent"] == "postgres":
                    result = await aggregator.query_postgres_agent(test["question"], tenant_id)
                elif test["expected_agent"] == "knowledge_base":
                    result = await aggregator.query_knowledge_base_agent(test["question"], tenant_id)
                elif test["expected_agent"] == "hybrid":
                    result = await aggregator.hybrid_search(test["question"], tenant_id)
                else:
                    result = await aggregator.process_question(test["question"], tenant_id)
                
                results["tests"].append({
                    "question": test["question"],
                    "expected_agent": test["expected_agent"],
                    "actual_agent": result.get("agent"),
                    "success": result.get("success", False),
                    "answer_length": len(result.get("answer", "")),
                    "has_answer": bool(result.get("answer", "").strip())
                })
                
            except Exception as e:
                results["tests"].append({
                    "question": test["question"],
                    "expected_agent": test["expected_agent"],
                    "success": False,
                    "error": str(e)
                })
        
        return results
        
    except Exception as e:
        logger.error(f"Error testing tenant {tenant_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

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
                    "postgres_status": status.get("postgres_agent", {}).get("status"),
                    "knowledge_base_status": status.get("knowledge_base_agent", {}).get("status"),
                    "enabled_agents": [
                        agent for agent, enabled in [
                            ("postgres", config.settings.get("enable_postgres_agent", True)),
                            ("knowledge_base", config.settings.get("enable_knowledge_base_agent", True)),
                            ("hybrid", config.settings.get("allow_hybrid_search", True))
                        ] if enabled
                    ],
                    "settings": {
                        "max_tokens": config.settings.get("max_tokens"),
                        "temperature": config.settings.get("temperature"),
                        "response_language": config.settings.get("response_language")
                    }
                }
            except Exception as e:
                stats[tenant_id] = {"error": str(e)}
        
        return {
            "total_tenants": len(list_available_tenants()),
            "default_tenant": tenant_manager.default_tenant,
            "tenant_stats": stats,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting tenant stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    print(f"🚀 SiamTech Multi-Tenant RAG Service")
    print(f"🔗 API Documentation: http://localhost:5000/docs")
    print(f"🎯 Agents: PostgreSQL + Knowledge Base + Aggregator")
    print(f"🏢 Available Tenants:")
    
    tenants = list_available_tenants()
    for tenant_id, name in tenants.items():
        print(f"   - {tenant_id}: {name}")
    
    print(f"💡 Models available:")
    print(f"   - siamtech-multi-agent (auto-routing)")
    print(f"   - siamtech-postgres-agent (database only)")
    print(f"   - siamtech-knowledge-agent (documents only)")
    print(f"   - siamtech-hybrid-agent (both agents)")
    print(f"   - Add '-company-a', '-company-b', '-company-c' for tenant-specific models")
    
    uvicorn.run(
        "multi_agent_service:app",
        host="0.0.0.0",
        port=5000,
        reload=True
    )