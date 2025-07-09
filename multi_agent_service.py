import json
import os
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import uvicorn

# Import our agents
from aggregator_agent import AggregatorAgent

# Pydantic models
class RAGQuery(BaseModel):
    query: str
    agent_type: Optional[str] = "auto"  # auto, postgres, knowledge_base, hybrid

class RAGResponse(BaseModel):
    answer: str
    source: str
    agent: str
    success: bool
    routing_decision: Optional[str] = None

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    messages: List[ChatMessage]
    model: Optional[str] = "siamtech-multi-agent"
    max_tokens: Optional[int] = 1000

# FastAPI app
app = FastAPI(
    title="SiamTech Multi-Agent RAG Service",
    description="Multi-Agent RAG Service with PostgreSQL and Knowledge Base",
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

# Initialize Aggregator Agent
aggregator = AggregatorAgent()

@app.on_event("startup")
async def startup_event():
    print("🚀 Multi-Agent RAG Service starting...")
    print("🤖 Agents available: PostgreSQL, Knowledge Base")
    print("🎯 Aggregator Agent initialized")

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Multi-Agent RAG",
        "agents": ["postgres", "knowledge_base", "aggregator"],
        "version": "2.0.0"
    }

@app.get("/v1/models")
async def list_models():
    """OpenAI compatible models endpoint"""
    return {
        "object": "list",
        "data": [
            {
                "id": "siamtech-multi-agent",
                "object": "model",
                "created": 1234567890,
                "owned_by": "siamtech",
                "permission": [],
                "root": "siamtech-multi-agent",
                "parent": None,
                "description": "Multi-Agent RAG with PostgreSQL and Knowledge Base"
            },
            {
                "id": "claude-postgres-agent",
                "object": "model", 
                "created": 1234567890,
                "owned_by": "siamtech",
                "permission": [],
                "root": "claude-postgres-agent",
                "parent": None,
                "description": "PostgreSQL Agent only"
            },
            {
                "id": "claude-knowledge-agent",
                "object": "model",
                "created": 1234567890,
                "owned_by": "siamtech",
                "permission": [],
                "root": "claude-knowledge-agent", 
                "parent": None,
                "description": "Knowledge Base Agent only"
            }
        ]
    }

@app.post("/rag-query", response_model=RAGResponse)
async def rag_query(request: RAGQuery):
    """RAG query endpoint with agent selection"""
    try:
        if request.agent_type == "postgres":
            # Force PostgreSQL agent
            result = await aggregator.query_postgres_agent(request.query)
        elif request.agent_type == "knowledge_base":
            # Force Knowledge Base agent
            result = await aggregator.query_knowledge_base_agent(request.query)
        elif request.agent_type == "hybrid":
            # Force hybrid search
            result = await aggregator.hybrid_search(request.query)
        else:
            # Auto routing
            result = await aggregator.process_question(request.query)
        
        return RAGResponse(
            answer=result["answer"],
            source=result.get("source", "Unknown"),
            agent=result.get("agent", "unknown"),
            success=result.get("success", True),
            routing_decision=result.get("routing_decision")
        )
        
    except Exception as e:
        print(f"Error in RAG query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/chat/completions")
async def openai_compatible(request: ChatCompletionRequest):
    """OpenAI compatible endpoint for Open WebUI"""
    try:
        if not request.messages:
            raise HTTPException(status_code=400, detail="No messages provided")
            
        user_message = request.messages[-1].content
        
        # Determine agent type from model selection
        agent_type = "auto"
        if "postgres" in request.model.lower():
            agent_type = "postgres"
        elif "knowledge" in request.model.lower():
            agent_type = "knowledge_base"
        elif "multi-agent" in request.model.lower():
            agent_type = "auto"
        
        # Process with appropriate agent
        if agent_type == "postgres":
            result = await aggregator.query_postgres_agent(user_message)
        elif agent_type == "knowledge_base":
            result = await aggregator.query_knowledge_base_agent(user_message)
        else:
            result = await aggregator.process_question(user_message)
        
        # Format as OpenAI response
        response = {
            "id": "chatcmpl-multiagent",
            "object": "chat.completion",
            "created": 1234567890,
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
                "prompt_tokens": len(user_message),
                "completion_tokens": len(result["answer"]),
                "total_tokens": len(user_message) + len(result["answer"])
            },
            "metadata": {
                "agent_used": result.get("agent", "unknown"),
                "source": result.get("source", "unknown"),
                "routing_decision": result.get("routing_decision"),
                "success": result.get("success", True)
            }
        }
        
        return response
        
    except Exception as e:
        print(f"Error in OpenAI endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/agents/status")
async def agents_status():
    """Check status of all agents"""
    status = {
        "aggregator": "active",
        "postgres_agent": "unknown",
        "knowledge_base_agent": "unknown"
    }
    
    # Test PostgreSQL agent
    try:
        test_result = aggregator.postgres_agent.query("SELECT 1 as test")
        status["postgres_agent"] = "active" if test_result["success"] else "error"
    except Exception as e:
        status["postgres_agent"] = f"error: {str(e)}"
    
    # Test Knowledge Base agent
    try:
        test_result = await aggregator.query_knowledge_base_agent("test")
        status["knowledge_base_agent"] = "active" if test_result["success"] else "error"
    except Exception as e:
        status["knowledge_base_agent"] = f"error: {str(e)}"
    
    return status

@app.get("/agents/metrics")
async def agents_metrics():
    """Get performance metrics (placeholder)"""
    return {
        "total_queries": 0,
        "postgres_queries": 0,
        "knowledge_base_queries": 0,
        "hybrid_queries": 0,
        "avg_response_time": 0,
        "success_rate": 0
    }

if __name__ == '__main__':
    print(f"🚀 SiamTech Multi-Agent RAG Service")
    print(f"🔗 API Documentation: http://localhost:5000/docs")
    print(f"🎯 Agents: PostgreSQL + Knowledge Base + Aggregator")
    print(f"💡 Models available:")
    print(f"   - siamtech-multi-agent (auto-routing)")
    print(f"   - claude-postgres-agent (database only)")
    print(f"   - claude-knowledge-agent (documents only)")
    
    uvicorn.run(
        "multi_agent_service:app",
        host="0.0.0.0",
        port=5000,
        reload=True
    )