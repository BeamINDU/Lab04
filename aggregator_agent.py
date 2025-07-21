import asyncio
import logging
from typing import Dict, Any, Optional
from enum import Enum
from dataclasses import dataclass

from postgres_agent import PostgreSQLAgent
from ollama_agent import OllamaAgent
from tenant_manager import get_tenant_config

logger = logging.getLogger(__name__)

class AgentType(Enum):
    DATABASE = "database"
    OLLAMA = "ollama"
    HYBRID = "hybrid"

@dataclass
class AgentResponse:
    content: str
    source: str
    success: bool = True
    error: Optional[str] = None

class SmartRouter:
    """Ultra-compact smart routing logic"""
    
    DB_KEYWORDS = {
        'employees', 'พนักงาน', 'salary', 'เงินเดือน', 'projects', 'โปรเจค',
        'count', 'จำนวน', 'average', 'เฉลี่ย', 'budget', 'งบประมาณ', 
        'department', 'แผนก', 'statistics', 'สถิติ', 'how many', 'กี่คน'
    }
    
    @classmethod
    def route(cls, question: str) -> AgentType:
        """Determine routing based on keyword scoring"""
        q_lower = question.lower()
        score = sum(1 for kw in cls.DB_KEYWORDS if kw in q_lower)
        
        if score >= 2: return AgentType.DATABASE
        elif score == 1: return AgentType.HYBRID
        else: return AgentType.OLLAMA

class AgentPool:
    """Efficient agent pool with lazy loading"""
    
    def __init__(self):
        self._postgres: Dict[str, PostgreSQLAgent] = {}
        self._ollama: Dict[str, OllamaAgent] = {}
    
    def get_postgres(self, tenant_id: str) -> PostgreSQLAgent:
        if tenant_id not in self._postgres:
            self._postgres[tenant_id] = PostgreSQLAgent(tenant_id)
        return self._postgres[tenant_id]
    
    def get_ollama(self, tenant_id: str) -> OllamaAgent:
        if tenant_id not in self._ollama:
            self._ollama[tenant_id] = OllamaAgent(tenant_id)
        return self._ollama[tenant_id]

class AutoAgent:
    """Streamlined Multi-tenant AI Agent with smart routing"""
    
    def __init__(self):
        self.pool = AgentPool()
        self.router = SmartRouter()
    
    async def process(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Main processing pipeline - simple and fast"""
        try:
            config = get_tenant_config(tenant_id)
            route_type = self.router.route(question)
            
            # Execute based on route
            if route_type == AgentType.DATABASE:
                response = await self._db_query(question, tenant_id, config)
            elif route_type == AgentType.OLLAMA:
                response = await self._ollama_query(question, tenant_id, config)
            else:  # HYBRID
                response = await self._hybrid_query(question, tenant_id, config)
            
            return self._format_success(response, tenant_id, config.name, route_type.value)
            
        except Exception as e:
            logger.error(f"Agent error for {tenant_id}: {e}")
            return self._format_error(str(e), tenant_id)
    
    async def _db_query(self, question: str, tenant_id: str, config) -> AgentResponse:
        """Database query with fallback"""
        if not config.settings.get('enable_postgres_agent', True):
            return AgentResponse("Database queries disabled", "Config", False)
        
        try:
            agent = self.pool.get_postgres(tenant_id)
            result = agent.query(question, tenant_id)
            
            if result["success"]:
                return AgentResponse(result["answer"], "Database")
            else:
                return AgentResponse(f"DB Error: {result.get('answer')}", "Database", False)
                
        except Exception as e:
            return AgentResponse(f"Database access failed: {e}", "Database", False)
    
    async def _ollama_query(self, question: str, tenant_id: str, config) -> AgentResponse:
        """Ollama query with fallback"""
        if not config.settings.get('enable_ollama_agent', True):
            return AgentResponse("Ollama queries disabled", "Config", False)
        
        try:
            agent = self.pool.get_ollama(tenant_id)
            result = await agent.async_query(question, tenant_id)
            
            if result["success"]:
                return AgentResponse(result["answer"], "Ollama AI")
            else:
                return AgentResponse(f"AI Error: {result.get('error')}", "Ollama", False)
                
        except Exception as e:
            return AgentResponse(f"AI access failed: {e}", "Ollama", False)
    
    async def _hybrid_query(self, question: str, tenant_id: str, config) -> AgentResponse:
        """Parallel hybrid query with intelligent combining"""
        if not config.settings.get('allow_hybrid_search', True):
            return await self._ollama_query(question, tenant_id, config)
        
        # Run both concurrently
        db_task = self._db_query(question, tenant_id, config)
        ollama_task = self._ollama_query(question, tenant_id, config)
        
        db_resp, ollama_resp = await asyncio.gather(db_task, ollama_task, return_exceptions=True)
        
        # Handle exceptions
        if isinstance(db_resp, Exception):
            db_resp = AgentResponse(f"DB Error: {db_resp}", "Database", False)
        if isinstance(ollama_resp, Exception):
            ollama_resp = AgentResponse(f"AI Error: {ollama_resp}", "Ollama", False)
        
        # Smart combining
        return self._combine_responses(db_resp, ollama_resp, config.name)
    
    def _combine_responses(self, db: AgentResponse, ollama: AgentResponse, tenant_name: str) -> AgentResponse:
        """Intelligent response combination"""
        db_ok = db.success and len(db.content.strip()) > 10 and "error" not in db.content.lower()
        ollama_ok = ollama.success and len(ollama.content.strip()) > 10 and "error" not in ollama.content.lower()
        
        if db_ok and ollama_ok:
            combined = f"📊 **Database**: {db.content}\n\n🤖 **AI**: {ollama.content}"
            return AgentResponse(combined, "Database + AI")
        elif db_ok:
            return AgentResponse(f"📊 {db.content}", "Database")
        elif ollama_ok:
            return AgentResponse(f"🤖 {ollama.content}", "AI Assistant")
        else:
            return AgentResponse(
                f"Sorry, couldn't find relevant information for {tenant_name}. Please try rephrasing.",
                "No Data", False
            )
    
    def _format_success(self, response: AgentResponse, tenant_id: str, tenant_name: str, route: str) -> Dict[str, Any]:
        return {
            "success": response.success,
            "answer": response.content,
            "source": f"Auto Agent - {tenant_name}",
            "agent": "auto",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "data_source_used": route,
            "routing_decision": route
        }
    
    def _format_error(self, error: str, tenant_id: str) -> Dict[str, Any]:
        return {
            "success": False,
            "answer": f"Processing error: {error}",
            "source": "Auto Agent - Error",
            "agent": "auto",
            "tenant_id": tenant_id,
            "error": error
        }
    
    async def health_check(self, tenant_id: str) -> Dict[str, Any]:
        """Quick tenant health check"""
        try:
            config = get_tenant_config(tenant_id)
            status = {"tenant_id": tenant_id, "tenant_name": config.name}
            
            # Test DB if enabled
            if config.settings.get('enable_postgres_agent', True):
                try:
                    agent = self.pool.get_postgres(tenant_id)
                    test = agent.test_connection(tenant_id)
                    status["database"] = "ok" if test["success"] else "error"
                except:
                    status["database"] = "error"
            else:
                status["database"] = "disabled"
            
            # Test Ollama if enabled
            if config.settings.get('enable_ollama_agent', True):
                try:
                    agent = self.pool.get_ollama(tenant_id)
                    test = agent.test_connection(tenant_id)
                    status["ollama"] = "ok" if test["success"] else "error"
                except:
                    status["ollama"] = "error"
            else:
                status["ollama"] = "disabled"
            
            return status
            
        except Exception as e:
            return {"tenant_id": tenant_id, "error": str(e)}

# Backward compatibility wrapper
class AggregatorAgent:
    def __init__(self):
        self.auto_agent = AutoAgent()
    
    async def process_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        return await self.auto_agent.process(question, tenant_id)
    
    async def get_tenant_agent_status(self, tenant_id: str) -> Dict[str, Any]:
        return await self.auto_agent.health_check(tenant_id)

# Quick functions
async def process_tenant_question(question: str, tenant_id: str) -> Dict[str, Any]:
    """One-liner for processing tenant questions"""
    agent = AutoAgent()
    return await agent.process(question, tenant_id)

def create_aggregator_agent() -> AggregatorAgent:
    """Factory function"""
    return AggregatorAgent()

# Test runner
async def test_agents():
    """Compact test suite"""
    agent = AutoAgent()
    tests = [
        ("company-a", "How many employees?"),
        ("company-a", "Hello, who are you?"),
        ("company-b", "What's the average salary?"),
    ]
    
    for tenant, question in tests:
        print(f"\n🏢 {tenant}: {question}")
        try:
            result = await agent.process(question, tenant)
            print(f"✅ {result['data_source_used']}: {result['answer'][:100]}...")
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_agents())