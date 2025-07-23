import time
from datetime import datetime
from typing import Dict, Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from aggregator_agent import AggregatorAgent
from tenant_manager import get_tenant_config, list_available_tenants, validate_tenant_id

# Models (minimal)
class RAGQuery(BaseModel):
    query: str
    tenant_id: Optional[str] = None

class RAGResponse(BaseModel):
    answer: str
    success: bool
    tenant_id: str
    tenant_name: str
    data_source_used: Optional[str] = None
    response_time_ms: int

# App setup
app = FastAPI(title="SiamTech AI", version="6.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Global agent (singleton)asd
_agent = AggregatorAgent()

# Unified dependency
def get_tenant(x_tenant_id: Optional[str] = Header(None, alias="X-Tenant-ID")) -> str:
    """Get and validate tenant ID"""
    tenant_id = x_tenant_id or "company-a"
    if not validate_tenant_id(tenant_id):
        raise HTTPException(400, f"Invalid tenant: {tenant_id}")
    return tenant_id

# Core endpoints only
@app.get("/health")
def health():
    return {"status": "ok", "tenants": len(list_available_tenants())}

@app.post("/rag-query", response_model=RAGResponse)
async def query(request: RAGQuery, tenant_id: str = Depends(get_tenant)):
    """Main RAG endpoint"""
    start = time.time()
    
    # Override tenant if provided in request
    if request.tenant_id and validate_tenant_id(request.tenant_id):
        tenant_id = request.tenant_id
    
    try:
        # Process query
        result = await _agent.process_question(request.query, tenant_id)
        tenant_config = get_tenant_config(tenant_id)
        
        return RAGResponse(
            answer=result["answer"],
            success=result.get("success", True),
            tenant_id=tenant_id,
            tenant_name=tenant_config.name,
            data_source_used=result.get("data_source_used"),
            response_time_ms=int((time.time() - start) * 1000)
        )
    except Exception as e:
        return RAGResponse(
            answer=f"Error: {str(e)}",
            success=False,
            tenant_id=tenant_id,
            tenant_name="Error",
            response_time_ms=int((time.time() - start) * 1000)
        )

# OpenAI compatibility (simplified)
@app.post("/v1/chat/completions")
async def chat(request: Dict[str, Any], tenant_id: str = Depends(get_tenant)):
    """OpenAI compatible endpoint"""
    try:
        messages = request.get("messages", [])
        if not messages:
            raise HTTPException(400, "No messages")
        
        user_message = messages[-1].get("content", "")
        result = await _agent.process_question(user_message, tenant_id)
        
        return {
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": result["answer"]
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "total_tokens": len(user_message.split()) + len(result["answer"].split())
            }
        }
    except Exception as e:
        raise HTTPException(500, str(e))

# Minimal models list
@app.get("/v1/models")
def models():
    return {
        "data": [
            {"id": f"siamtech-{tid}", "object": "model"}
            for tid in list_available_tenants().keys()
        ]
    }
@app.get("/tenants")
def list_tenants():
    return {
        "tenants": [
            {"tenant_id": tid, "name": get_tenant_config(tid).name}
            for tid in list_available_tenants().keys()
        ]
    }
# Tenant status (combined)
@app.get("/tenants/{tenant_id}/status")
async def status(tenant_id: str):
    if not validate_tenant_id(tenant_id):
        raise HTTPException(404, "Tenant not found")
    try:
        return await _agent.get_tenant_agent_status(tenant_id)
    except Exception as e:
        raise HTTPException(500, str(e))

# Run
if __name__ == '__main__':
    print("🚀 SiamTech AI v6.0 - Ultra Minimal")
    uvicorn.run(
        "multi_agent_service:app",
        host="0.0.0.0",
        port=5000,
        access_log=False
    )