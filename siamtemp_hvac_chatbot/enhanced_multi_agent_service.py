"""
enhanced_multi_agent_service.py - Ultimate Version
===================================================
FastAPI service layer for the Ultimate Dual Model Dynamic AI System
"""

import os
import asyncio
import time
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Depends, Header, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn
import logging
from contextlib import asynccontextmanager
from prometheus_client import Counter, Histogram, Gauge, generate_latest
# Import the ultimate AI system
# from agents.dual_model_dynamic_ai import (
#     DualModelDynamicAISystem,
#     UnifiedEnhancedPostgresOllamaAgent
# )

from agents import (
    ImprovedDualModelDynamicAISystem as UnifiedEnhancedPostgresOllamaAgent
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# METRICS FOR MONITORING
# =============================================================================

# Prometheus metrics
request_count = Counter('chatbot_requests_total', 'Total number of requests', ['endpoint', 'status'])
response_time = Histogram('chatbot_response_time_seconds', 'Response time in seconds', ['endpoint'])
active_users = Gauge('chatbot_active_users', 'Number of active users')
cache_hit_rate = Gauge('chatbot_cache_hit_rate', 'Cache hit rate percentage')

# =============================================================================
# CONFIGURATION
# =============================================================================

class ServiceConfig:
    """Service configuration"""
    
    def __init__(self):
        self.service_name = "Siamtemp Ultimate HVAC AI Service"
        self.version = "8.0-Ultimate"
        self.tenant_configs = {
            'company-a': {
                'name': 'Siamtemp Bangkok HQ',
                'description': 'Main headquarters with full AI features',
                'features': {
                    'conversation_memory': True,
                    'parallel_processing': True,
                    'data_cleaning': True,
                    'sql_validation': True
                }
            },
            'company-b': {
                'name': 'Siamtemp Branch B',
                'description': 'Branch office with standard features',
                'features': {
                    'conversation_memory': True,
                    'parallel_processing': False,
                    'data_cleaning': True,
                    'sql_validation': True
                }
            }
        }
        
        # Rate limiting
        self.rate_limit_enabled = os.getenv('RATE_LIMIT_ENABLED', 'false').lower() == 'true'
        self.max_requests_per_minute = int(os.getenv('MAX_REQUESTS_PER_MINUTE', '60'))
        
        # Model settings
        self.sql_model = os.getenv('SQL_MODEL', 'mannix/defog-llama3-sqlcoder-8b:latest')
        self.nl_model = os.getenv('NL_MODEL', 'llama3.1:8b')
        
        # Feature flags
        self.enable_streaming = os.getenv('ENABLE_STREAMING', 'false').lower() == 'true'
        self.enable_metrics = os.getenv('ENABLE_METRICS', 'true').lower() == 'true'
        self.enable_cors = os.getenv('ENABLE_CORS', 'true').lower() == 'true'

config = ServiceConfig()

# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class ChatRequest(BaseModel):
    """Main chat request model"""
    question: str = Field(..., description="User's question in natural language")
    tenant_id: str = Field(default="company-a", description="Tenant identifier")
    user_id: str = Field(default="default", description="User identifier for conversation tracking")
    use_conversation_memory: bool = Field(default=True, description="Enable conversation memory")
    use_parallel_processing: bool = Field(default=True, description="Enable parallel processing")
    use_data_cleaning: bool = Field(default=True, description="Enable data cleaning")
    stream: bool = Field(default=False, description="Enable streaming response")
    context: Optional[Dict[str, Any]] = Field(default=None, description="Additional context from conversation history")

class ChatResponse(BaseModel):
    """Enhanced chat response model with conversation support"""
    answer: str
    success: bool
    sql_query: Optional[str] = None
    results_count: int = 0
    tenant_id: str
    user_id: str
    processing_time: float
    ai_system_used: str
    intent: Optional[str] = None
    entities: Optional[Dict] = None
    data_quality: Optional[Dict] = None
    features_used: Optional[Dict] = None
    
    # Multi-turn additions
    conversation_id: Optional[str] = None
    session_id: Optional[str] = None
    is_followup: bool = False
    resolved_question: Optional[str] = None
    suggested_followups: Optional[List[str]] = None
    conversation_turn: int = 1
    references_resolved: Optional[Dict] = None


class SystemStatus(BaseModel):
    """System status model"""
    model_config = {'protected_namespaces': ()}
    status: str
    version: str
    uptime_seconds: float
    total_queries: int
    success_rate: float
    avg_response_time: float
    active_features: Dict[str, bool]
    model_status: Dict[str, str]

class ConversationHistory(BaseModel):
    """Conversation history model"""
    user_id: str
    conversations: List[Dict]
    total_count: int
    session_stats: Dict

# =============================================================================
# DEPENDENCY INJECTION
# =============================================================================

def get_tenant_id(tenant_id: Optional[str] = None) -> str:
    """Get and validate tenant ID"""
    if not tenant_id:
        tenant_id = os.getenv('DEFAULT_TENANT_ID', 'company-a')
    
    if tenant_id not in config.tenant_configs:
        logger.warning(f"Unknown tenant {tenant_id}, using default")
        tenant_id = 'company-a'
    
    return tenant_id

def get_user_id(x_user_id: Optional[str] = Header(None)) -> str:
    """Extract user ID from header or use default"""
    return x_user_id or "default"

# =============================================================================
# AI SYSTEM INITIALIZATION
# =============================================================================

# Initialize the ultimate AI system
try:
    logger.info("🚀 Initializing Ultimate AI System...")
    ai_agent = UnifiedEnhancedPostgresOllamaAgent()
    
    # Configure features based on environment
    ai_agent.enable_conversation_memory = os.getenv('ENABLE_MEMORY', 'true').lower() == 'true'
    ai_agent.enable_parallel_processing = os.getenv('ENABLE_PARALLEL', 'true').lower() == 'true'
    ai_agent.enable_data_cleaning = os.getenv('ENABLE_CLEANING', 'true').lower() == 'true'
    ai_agent.enable_sql_validation = os.getenv('ENABLE_VALIDATION', 'true').lower() == 'true'
    
    logger.info("✅ Ultimate AI System initialized successfully")
    logger.info(f"📊 Features: Memory={ai_agent.enable_conversation_memory}, "
                f"Parallel={ai_agent.enable_parallel_processing}, "
                f"Cleaning={ai_agent.enable_data_cleaning}, "
                f"Validation={ai_agent.enable_sql_validation}")
    
    AI_SYSTEM_AVAILABLE = True
    
except Exception as e:
    logger.error(f"❌ Failed to initialize AI System: {e}")
    AI_SYSTEM_AVAILABLE = False
    raise RuntimeError(f"Cannot start service: {e}")

# Track service start time
SERVICE_START_TIME = datetime.now()

# =============================================================================
# FASTAPI APPLICATION
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ========== STARTUP ==========
    logger.info(f"""
    {'='*60}
    🚀 {config.service_name}
    Version: {config.version}
    {'='*60}
    Features Enabled:
    - Conversation Memory: {ai_agent.enable_conversation_memory}
    - Parallel Processing: {ai_agent.enable_parallel_processing}
    - Data Cleaning: {ai_agent.enable_data_cleaning}
    - SQL Validation: {ai_agent.enable_sql_validation}
    
    Models:
    - SQL: {config.sql_model}
    - NL: {config.nl_model}
    
    API Documentation: http://localhost:8000/docs
    {'='*60}
    """)
    
    yield  # ⬅️ ส่วนนี้สำคัญ! Application runs here
    
    # ========== SHUTDOWN ==========
    logger.info("Shutting down service...")
    
    # Close database connections
    if hasattr(ai_agent, 'db_handler'):
        ai_agent.db_handler.close_connections()
    
    logger.info("Service shutdown complete")

app = FastAPI(
    title=config.service_name,
    description="Ultimate AI-powered HVAC business intelligence system with advanced features",
    version=config.version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {"name": "Chat", "description": "Main chat endpoints"},
        {"name": "System", "description": "System monitoring and health"},
        {"name": "History", "description": "Conversation history management"},
        {"name": "Admin", "description": "Administrative functions"}
    ]
)

# Add CORS middleware if enabled
if config.enable_cors:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# =============================================================================
# MAIN ENDPOINTS
# =============================================================================

@app.post("/v1/chat", response_model=ChatResponse, tags=["Chat"])
async def chat_endpoint(
    request: ChatRequest,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_user_id)
):
    """
    Main chat endpoint with all features
    
    Features:
    - Conversation memory tracking
    - Parallel processing for faster response
    - Real-time data cleaning
    - SQL validation and retry
    """
    start_time = time.time()
    
    try:
        # Update metrics
        request_count.labels(endpoint='chat', status='processing').inc()
        active_users.inc()
        
        # Override user_id if provided in header
        if user_id != "default":
            request.user_id = user_id
        
        # Configure features for this request
        if not request.use_conversation_memory:
            ai_agent.enable_conversation_memory = False
        if not request.use_parallel_processing:
            ai_agent.enable_parallel_processing = False
        if not request.use_data_cleaning:
            ai_agent.enable_data_cleaning = False
        
        # Process the question
        result = await ai_agent.process_any_question(
            question=request.question,
            tenant_id=request.tenant_id,
            user_id=request.user_id
        )
        
        # Reset features to default
        ai_agent.enable_conversation_memory = True
        ai_agent.enable_parallel_processing = True
        ai_agent.enable_data_cleaning = True
        
        # Prepare response
        response = ChatResponse(
            answer=result.get('answer', 'ไม่สามารถสร้างคำตอบได้'),
            success=result.get('success', False),
            sql_query=result.get('sql_query'),
            results_count=result.get('results_count', 0),
            tenant_id=request.tenant_id,
            user_id=request.user_id,
            processing_time=time.time() - start_time,
            ai_system_used='ultimate_dual_model',
            intent=result.get('intent'),
            entities=result.get('entities'),
            data_quality=result.get('data_quality'),
            features_used=result.get('features_used')
        )
        
        # Update metrics
        response_time.labels(endpoint='chat').observe(response.processing_time)
        request_count.labels(endpoint='chat', status='success').inc()
        
        # Background task to update cache hit rate
        if config.enable_metrics:
            background_tasks.add_task(update_cache_metrics)
        
        return response
        
    except Exception as e:
        logger.error(f"Chat endpoint error: {e}")
        request_count.labels(endpoint='chat', status='error').inc()
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        active_users.dec()

@app.post("/v1/chat/stream", tags=["Chat"])
async def chat_stream_endpoint(request: ChatRequest):
    """
    Streaming chat endpoint for real-time responses
    """
    if not config.enable_streaming:
        raise HTTPException(status_code=400, detail="Streaming is not enabled")
    
    async def generate():
        try:
            # Process question
            result = await ai_agent.process_any_question(
                question=request.question,
                tenant_id=request.tenant_id,
                user_id=request.user_id
            )
            
            # Stream response in chunks
            answer = result.get('answer', '')
            chunk_size = 50
            
            for i in range(0, len(answer), chunk_size):
                chunk = answer[i:i + chunk_size]
                yield json.dumps({'chunk': chunk, 'done': False}) + '\n'
                await asyncio.sleep(0.05)  # Small delay for streaming effect
            
            # Send completion signal
            yield json.dumps({
                'done': True,
                'sql_query': result.get('sql_query'),
                'results_count': result.get('results_count', 0)
            }) + '\n'
            
        except Exception as e:
            yield json.dumps({'error': str(e), 'done': True}) + '\n'
    
    return StreamingResponse(generate(), media_type="application/x-ndjson")

# =============================================================================
# CONVERSATION HISTORY ENDPOINTS
# =============================================================================

@app.get("/v1/history/{user_id}", response_model=ConversationHistory, tags=["History"])
async def get_conversation_history(user_id: str, limit: int = 20):
    """
    Get conversation history for a user
    """
    try:
        conversations = list(ai_agent.conversation_memory.conversations[user_id])[-limit:]
        
        return ConversationHistory(
            user_id=user_id,
            conversations=conversations,
            total_count=len(ai_agent.conversation_memory.conversations[user_id]),
            session_stats={
                'total_queries': len(conversations),
                'successful_queries': sum(1 for c in conversations if c.get('success', False)),
                'avg_processing_time': sum(c.get('processing_time', 0) for c in conversations) / max(len(conversations), 1)
            }
        )
    except Exception as e:
        logger.error(f"Failed to get history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/v1/history/{user_id}", tags=["History"])
async def clear_conversation_history(user_id: str):
    """
    Clear conversation history for a user
    """
    try:
        if user_id in ai_agent.conversation_memory.conversations:
            ai_agent.conversation_memory.conversations[user_id].clear()
            return {"message": f"History cleared for user {user_id}"}
        else:
            return {"message": f"No history found for user {user_id}"}
    except Exception as e:
        logger.error(f"Failed to clear history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# SYSTEM MONITORING ENDPOINTS
# =============================================================================

@app.get("/health", tags=["System"])
async def health_check():
    """
    Basic health check endpoint
    """
    uptime = (datetime.now() - SERVICE_START_TIME).total_seconds()
    
    return {
        "status": "healthy" if AI_SYSTEM_AVAILABLE else "degraded",
        "service": config.service_name,
        "version": config.version,
        "uptime_seconds": uptime,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/v1/system/status", response_model=SystemStatus, tags=["System"])
async def get_system_status():
    """
    Detailed system status with metrics
    """
    try:
        stats = ai_agent.get_system_stats()
        uptime = (datetime.now() - SERVICE_START_TIME).total_seconds()
        
        return SystemStatus(
            status="operational" if AI_SYSTEM_AVAILABLE else "degraded",
            version=config.version,
            uptime_seconds=uptime,
            total_queries=stats['performance']['total_queries'],
            success_rate=stats['performance']['success_rate'],
            avg_response_time=stats['performance']['avg_response_time'],
            active_features=stats['features'],
            model_status={
                'sql_model': stats['models']['sql_generation'],
                'nl_model': stats['models']['response_generation']
            }
        )
    except Exception as e:
        logger.error(f"Failed to get system status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics", tags=["System"])
async def get_prometheus_metrics():
    """
    Prometheus metrics endpoint
    """
    if not config.enable_metrics:
        raise HTTPException(status_code=404, detail="Metrics not enabled")
    
    return generate_latest()

# =============================================================================
# ADMIN ENDPOINTS
# =============================================================================

@app.post("/v1/admin/clear-cache", tags=["Admin"])
async def clear_cache():
    """
    Clear all caches
    """
    try:
        ai_agent.sql_cache.clear()
        ai_agent.conversation_memory.successful_patterns.clear()
        
        return {
            "message": "All caches cleared successfully",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to clear cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/admin/toggle-feature", tags=["Admin"])
async def toggle_feature(feature: str, enabled: bool):
    """
    Toggle system features on/off
    """
    valid_features = [
        'conversation_memory',
        'parallel_processing',
        'data_cleaning',
        'sql_validation'
    ]
    
    if feature not in valid_features:
        raise HTTPException(status_code=400, detail=f"Invalid feature. Valid options: {valid_features}")
    
    try:
        setattr(ai_agent, f"enable_{feature}", enabled)
        
        return {
            "message": f"Feature '{feature}' {'enabled' if enabled else 'disabled'}",
            "current_state": {
                'conversation_memory': ai_agent.enable_conversation_memory,
                'parallel_processing': ai_agent.enable_parallel_processing,
                'data_cleaning': ai_agent.enable_data_cleaning,
                'sql_validation': ai_agent.enable_sql_validation
            }
        }
    except Exception as e:
        logger.error(f"Failed to toggle feature: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/admin/sql-examples", tags=["Admin"])
async def get_sql_examples():
    """
    Get current SQL examples used for learning
    """
    try:
        examples = ai_agent.sql_examples
        return {
            "total_examples": len(examples),
            "examples": examples
        }
    except Exception as e:
        logger.error(f"Failed to get SQL examples: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def update_cache_metrics():
    """Update cache hit rate metric"""
    try:
        stats = ai_agent.get_system_stats()
        cache_hit_rate.set(stats['performance'].get('cache_hit_rate', 0))
    except Exception as e:
        logger.error(f"Failed to update cache metrics: {e}")

# =============================================================================
# STARTUP AND SHUTDOWN EVENTS
# =============================================================================




# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)