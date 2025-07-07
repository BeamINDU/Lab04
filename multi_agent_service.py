import json
import os
import asyncio
import time
from fastapi import FastAPI, HTTPException, Query
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
    speed_mode: Optional[str] = "fast"  # ultra_fast, fast, parallel, normal

class RAGResponse(BaseModel):
    answer: str
    source: str
    agent: str
    success: bool
    routing_decision: Optional[str] = None
    response_time: Optional[float] = None
    method: Optional[str] = None

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    messages: List[ChatMessage]
    model: Optional[str] = "siamtech-multi-agent"
    max_tokens: Optional[int] = 1000
    speed_mode: Optional[str] = "fast"  # 🚀 NEW: Speed mode parameter

# FastAPI app
app = FastAPI(
    title="SiamTech Multi-Agent RAG Service",
    description="Multi-Agent RAG Service with PostgreSQL and Knowledge Base + Speed Optimizations",
    version="2.1.0"  # 🚀 Updated version
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
    print("⚡ Speed optimizations: Enabled")

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Multi-Agent RAG",
        "agents": ["postgres", "knowledge_base", "aggregator"],
        "version": "2.1.0",
        "features": ["speed_optimization", "caching", "quick_responses"]
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
                "id": "siamtech-ultra-fast",
                "object": "model", 
                "created": 1234567890,
                "owned_by": "siamtech",
                "permission": [],
                "root": "siamtech-ultra-fast",
                "parent": None,
                "description": "Ultra-fast Multi-Agent with Quick Responses"
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

# 🚀 NEW: Fast endpoint with speed modes
@app.post("/rag-query", response_model=RAGResponse)
async def rag_query(request: RAGQuery):
    """RAG query endpoint with agent selection and speed modes"""
    start_time = time.time()
    
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
            # Auto routing with speed mode
            result = await aggregator.process_question(
                request.query, 
                mode=request.speed_mode or "fast"
            )
        
        # Add response time if not already set
        if "response_time" not in result:
            result["response_time"] = time.time() - start_time
        
        return RAGResponse(
            answer=result["answer"],
            source=result.get("source", "Unknown"),
            agent=result.get("agent", "unknown"),
            success=result.get("success", True),
            routing_decision=result.get("routing_decision"),
            response_time=result.get("response_time"),
            method=result.get("method")
        )
        
    except Exception as e:
        print(f"Error in RAG query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 🚀 NEW: Ultra-fast endpoint
@app.post("/v1/chat/fast")
async def ultra_fast_chat(request: ChatCompletionRequest):
    """Ultra-fast OpenAI compatible endpoint"""
    try:
        if not request.messages:
            raise HTTPException(status_code=400, detail="No messages provided")
            
        user_message = request.messages[-1].content
        
        # Use ultra_fast mode
        result = await aggregator.process_question(user_message, mode="ultra_fast")
        
        # Format as OpenAI response
        response = {
            "id": "chatcmpl-ultrafast",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": "siamtech-ultra-fast",
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
                "method": result.get("method", "unknown"),
                "response_time": result.get("response_time", 0),
                "success": result.get("success", True)
            }
        }
        
        return response
        
    except Exception as e:
        print(f"Error in Ultra-fast endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/chat/completions")
async def openai_compatible(request: ChatCompletionRequest):
    """OpenAI compatible endpoint for Open WebUI with speed optimization"""
    try:
        if not request.messages:
            raise HTTPException(status_code=400, detail="No messages provided")
            
        user_message = request.messages[-1].content
        
        # Determine agent type and speed mode from model selection
        agent_type = "auto"
        speed_mode = request.speed_mode or "fast"
        
        if "ultra-fast" in request.model.lower():
            speed_mode = "ultra_fast"
        elif "postgres" in request.model.lower():
            agent_type = "postgres"
        elif "knowledge" in request.model.lower():
            agent_type = "knowledge_base"
        elif "multi-agent" in request.model.lower():
            agent_type = "auto"
        
        # Process with appropriate agent and speed mode
        if agent_type == "postgres":
            result = await aggregator.query_postgres_agent(user_message)
        elif agent_type == "knowledge_base":
            result = await aggregator.query_knowledge_base_agent(user_message)
        else:
            result = await aggregator.process_question(user_message, mode=speed_mode)
        
        # Format as OpenAI response
        response = {
            "id": "chatcmpl-multiagent",
            "object": "chat.completion",
            "created": int(time.time()),
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
                "method": result.get("method", "unknown"),
                "response_time": result.get("response_time", 0),
                "success": result.get("success", True)
            }
        }
        
        return response
        
    except Exception as e:
        print(f"Error in OpenAI endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 🚀 NEW: Speed test endpoint
@app.get("/speed-test")
async def speed_test_endpoint(
    question: str = Query(default="สวัสดี", description="Test question"),
    mode: str = Query(default="all", description="Speed mode: ultra_fast, fast, parallel, normal, or all")
):
    """Test response speed with different modes"""
    
    if mode == "all":
        modes = ["ultra_fast", "fast", "parallel", "normal"]
    else:
        modes = [mode]
    
    results = {}
    
    for test_mode in modes:
        start_time = time.time()
        try:
            if test_mode == "normal":
                result = await aggregator.original_process_question(question)
            else:
                result = await aggregator.process_question(question, mode=test_mode)
            
            duration = time.time() - start_time
            
            results[test_mode] = {
                "success": True,
                "response_time": duration,
                "answer": result.get("answer", "")[:100] + "..." if len(result.get("answer", "")) > 100 else result.get("answer", ""),
                "agent": result.get("agent", "unknown"),
                "method": result.get("method", "unknown"),
                "source": result.get("source", "unknown")
            }
        except Exception as e:
            results[test_mode] = {
                "success": False,
                "error": str(e),
                "response_time": time.time() - start_time
            }
    
    return {
        "question": question,
        "results": results,
        "fastest_mode": min(results.keys(), key=lambda k: results[k].get("response_time", float('inf'))),
        "speed_comparison": {
            mode: f"{results[mode].get('response_time', 0):.3f}s" 
            for mode in results.keys()
        }
    }

# 🚀 NEW: Cache management endpoints
@app.get("/cache/stats")
async def cache_stats():
    """Get cache statistics"""
    return {
        "cache_size": len(aggregator.response_cache),
        "cache_keys": list(aggregator.response_cache.keys())[:10],  # Show first 10
        "quick_responses_count": len(aggregator.quick_responses)
    }

@app.post("/cache/clear")
async def clear_cache():
    """Clear response cache"""
    cache_size = len(aggregator.response_cache)
    aggregator.response_cache.clear()
    return {
        "message": f"Cache cleared. Removed {cache_size} items.",
        "cache_size": len(aggregator.response_cache)
    }

@app.get("/agents/status")
async def agents_status():
    """Check status of all agents with speed metrics"""
    status = {
        "aggregator": "active",
        "postgres_agent": "unknown",
        "knowledge_base_agent": "unknown",
        "cache_enabled": True,
        "quick_responses_enabled": True
    }
    
    # Test PostgreSQL agent
    try:
        start_time = time.time()
        test_result = aggregator.postgres_agent.query("SELECT 1 as test")
        postgres_time = time.time() - start_time
        status["postgres_agent"] = {
            "status": "active" if test_result["success"] else "error",
            "response_time": postgres_time
        }
    except Exception as e:
        status["postgres_agent"] = {
            "status": f"error: {str(e)}",
            "response_time": None
        }
    
    # Test Knowledge Base agent
    try:
        start_time = time.time()
        test_result = await aggregator.query_knowledge_base_agent("test")
        kb_time = time.time() - start_time
        status["knowledge_base_agent"] = {
            "status": "active" if test_result["success"] else "error",
            "response_time": kb_time
        }
    except Exception as e:
        status["knowledge_base_agent"] = {
            "status": f"error: {str(e)}",
            "response_time": None
        }
    
    return status

@app.get("/agents/metrics")
async def agents_metrics():
    """Get performance metrics with cache info"""
    return {
        "total_queries": 0,  # TODO: Implement counter
        "postgres_queries": 0,
        "knowledge_base_queries": 0,
        "hybrid_queries": 0,
        "cache_hits": 0,     # TODO: Implement counter
        "quick_responses": 0, # TODO: Implement counter
        "avg_response_time": 0,
        "success_rate": 0,
        "cache_size": len(aggregator.response_cache),
        "quick_responses_available": len(aggregator.quick_responses)
    }

if __name__ == '__main__':
    print(f"🚀 SiamTech Multi-Agent RAG Service v2.1.0")
    print(f"🔗 API Documentation: http://localhost:5000/docs")
    print(f"🎯 Agents: PostgreSQL + Knowledge Base + Aggregator")
    print(f"⚡ Speed Features:")
    print(f"   - Quick Responses (< 1ms)")
    print(f"   - Response Caching (1-10ms)")
    print(f"   - Parallel Processing")
    print(f"   - Ultra-fast mode")
    print(f"💡 Models available:")
    print(f"   - siamtech-multi-agent (auto-routing)")
    print(f"   - siamtech-ultra-fast (maximum speed)")
    print(f"   - claude-postgres-agent (database only)")
    print(f"   - claude-knowledge-agent (documents only)")
    print(f"🧪 Test endpoints:")
    print(f"   - GET /speed-test?question=สวัสดี&mode=all")
    print(f"   - GET /cache/stats")
    print(f"   - POST /v1/chat/fast (ultra-fast endpoint)")
    
    uvicorn.run(
        "multi_agent_service:app",
        host="0.0.0.0",
        port=5000,
        reload=True
    )