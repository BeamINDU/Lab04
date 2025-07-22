import asyncio
import logging
from typing import Dict, Any, Optional
from enum import Enum
from dataclasses import dataclass

from postgres_agent import PostgreSQLAgent  # Updated version without AWS
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
    """Smart routing logic - enhanced for better database detection"""
    
    DB_KEYWORDS = {
        # Core database keywords (Thai)
        'พนักงาน', 'employees', 'employee', 'คน', 'จำนวน', 'count', 'กี่คน',
        'เงินเดือน', 'salary', 'เฉลี่ย', 'average', 'เท่าไหร่', 'how much',
        'โปรเจค', 'project', 'projects', 'งาน', 'work',
        'แผนก', 'department', 'departments', 'ฝ่าย',
        'ลูกค้า', 'client', 'clients', 'customer',
        'งบประมาณ', 'budget', 'cost', 'expense', 'ค่าใช้จ่าย',
        'สถิติ', 'statistics', 'stat', 'report', 'รายงาน',
        'ข้อมูล', 'data', 'information', 'info',
        
        # Question words that usually need database
        'กี่', 'how many', 'เท่าไหร่', 'how much', 'มาก', 'น้อย',
        'สูงสุด', 'ต่ำสุด', 'max', 'min', 'maximum', 'minimum',
        'รวม', 'total', 'sum', 'ทั้งหมด', 'all',
        
        # Business terms
        'บริษัท', 'company', 'organization', 'องค์กร',
        'ทีม', 'team', 'กลุ่ม', 'group'
    }
    
    @classmethod
    def route(cls, question: str) -> AgentType:
        """Enhanced routing based on keyword scoring"""
        q_lower = question.lower()
        
        # Count database-related keywords
        score = sum(1 for kw in cls.DB_KEYWORDS if kw in q_lower)
        
        # Special patterns that definitely need database
        db_patterns = [
            'กี่คน', 'how many', 'จำนวน', 'count',
            'เฉลี่ย', 'average', 'รวม', 'total',
            'เงินเดือน', 'salary', 'งบประมาณ', 'budget'
        ]
        
        # Check for definite database patterns
        has_db_pattern = any(pattern in q_lower for pattern in db_patterns)
        
        if has_db_pattern or score >= 2:
            return AgentType.DATABASE
        elif score == 1:
            return AgentType.HYBRID
        else:
            return AgentType.OLLAMA

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
    """Pure Ollama + PostgreSQL Multi-tenant AI Agent (No AWS)"""
    
    def __init__(self):
        self.pool = AgentPool()
        self.router = SmartRouter()
    
    async def process(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Main processing pipeline - Ollama + PostgreSQL only"""
        try:
            config = get_tenant_config(tenant_id)
            route_type = self.router.route(question)
            
            logger.info(f"Routing '{question}' to {route_type.value} for tenant {tenant_id}")
            
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
        """Database query using PostgreSQL + Ollama for SQL generation"""
        if not config.settings.get('enable_postgres_agent', True):
            return AgentResponse("Database queries disabled", "Config", False)
        
        try:
            agent = self.pool.get_postgres(tenant_id)
            result = await agent.async_query(question, tenant_id)
            
            if result["success"]:
                return AgentResponse(result["answer"], "Database + Ollama SQL")
            else:
                # Fallback to Ollama if database fails
                logger.warning(f"Database query failed, falling back to Ollama for {tenant_id}")
                return await self._ollama_query(question, tenant_id, config)
                
        except Exception as e:
            logger.error(f"Database query error for {tenant_id}: {e}")
            # Fallback to Ollama
            return await self._ollama_query(question, tenant_id, config)
    
    async def _ollama_query(self, question: str, tenant_id: str, config) -> AgentResponse:
        """Ollama query with enhanced prompt"""
        if not config.settings.get('enable_ollama_agent', True):
            return AgentResponse("Ollama queries disabled", "Config", False)
        
        try:
            agent = self.pool.get_ollama(tenant_id)
            
            # Enhanced prompt with company context
            company_context = f"""คุณเป็น AI Assistant ของ {config.name}
            
บริบทบริษัท:
- ชื่อ: {config.name}
- ที่ตั้ง: {config.contact_info.get('address', 'ไม่ระบุ')}
- โทร: {config.contact_info.get('phone', 'ไม่ระบุ')}

หากถูกถามเกี่ยวกับข้อมูลบริษัทที่เฉพาะเจาะจง (เช่น จำนวนพนักงาน เงินเดือน โปรเจค) 
ให้แนะนำให้ใช้คำถามที่ชัดเจนมากขึ้น เพื่อให้ระบบสามารถค้นหาข้อมูลจากฐานข้อมูลได้

คำถาม: {question}"""

            result = await agent.async_query(company_context, tenant_id)
            
            if result["success"]:
                return AgentResponse(result["answer"], "Ollama AI")
            else:
                return AgentResponse(f"AI Error: {result.get('error')}", "Ollama", False)
                
        except Exception as e:
            return AgentResponse(f"AI access failed: {e}", "Ollama", False)
    
    async def _hybrid_query(self, question: str, tenant_id: str, config) -> AgentResponse:
        """Parallel hybrid query - try database first, then Ollama"""
        if not config.settings.get('allow_hybrid_search', True):
            return await self._ollama_query(question, tenant_id, config)
        
        try:
            # Try database first for hybrid queries
            db_response = await self._db_query(question, tenant_id, config)
            
            if db_response.success and len(db_response.content.strip()) > 20:
                # Database query successful, enhance with Ollama context
                try:
                    agent = self.pool.get_ollama(tenant_id)
                    
                    enhancement_prompt = f"""ข้อมูลจากฐานข้อมูล {config.name}:
{db_response.content}

กรุณาปรับปรุงคำตอบนี้ให้เข้าใจง่ายและเป็นธรรมชาติมากขึ้น โดยคงข้อมูลที่สำคัญไว้ครบถ้วน"""

                    enhancement_result = await agent.async_query(enhancement_prompt, tenant_id)
                    
                    if enhancement_result["success"]:
                        enhanced_content = enhancement_result["answer"]
                        return AgentResponse(enhanced_content, "Database + AI Enhancement")
                    else:
                        return db_response  # Return original database response
                        
                except Exception as e:
                    logger.warning(f"Enhancement failed for {tenant_id}: {e}")
                    return db_response  # Return original database response
            else:
                # Database failed or insufficient data, use Ollama
                return await self._ollama_query(question, tenant_id, config)
                
        except Exception as e:
            logger.error(f"Hybrid query error for {tenant_id}: {e}")
            return await self._ollama_query(question, tenant_id, config)
    
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
    """Test suite for Ollama + PostgreSQL"""
    agent = AutoAgent()
    tests = [
        ("company-a", "มีพนักงานกี่คน?"),  # Should use database
        ("company-a", "สวัสดี คุณเป็นใคร?"),  # Should use Ollama
        ("company-a", "เงินเดือนเฉลี่ยเท่าไหร่?"),  # Should use database
        ("company-b", "สาขาเชียงใหม่มีพนักงานกี่คน?"),  # Should use database
        ("company-c", "Hello, who are you?"),  # Should use Ollama
    ]
    
    for tenant, question in tests:
        print(f"\n🏢 {tenant}: {question}")
        try:
            result = await agent.process(question, tenant)
            print(f"🎯 Route: {result['data_source_used']}")
            print(f"✅ Answer: {result['answer'][:100]}...")
            print(f"📍 Source: {result['source']}")
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_agents())