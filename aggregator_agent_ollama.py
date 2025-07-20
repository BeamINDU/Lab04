import json
import asyncio
from typing import Dict, Any, List, Optional
import logging
from postgres_agent import PostgreSQLAgent
from ollama_agent import OllamaAgent  # เพิ่ม OllamaAgent
from tenant_manager import get_tenant_config, get_tenant_database_config

logger = logging.getLogger(__name__)

class AutoAgent:
    """Enhanced Auto Agent - routes to PostgreSQL or Ollama based on question type"""
    
    def __init__(self):
        # Initialize tenant-aware agents (will be created on-demand)
        self.postgres_agents: Dict[str, PostgreSQLAgent] = {}
        self.ollama_agents: Dict[str, OllamaAgent] = {}
        
        # Simple routing keywords
        self.database_keywords = [
            'employees', 'พนักงาน', 'salary', 'เงินเดือน', 'projects', 'โปรเจค',
            'how many', 'กี่คน', 'count', 'จำนวน', 'average', 'เฉลี่ย',
            'budget', 'งบประมาณ', 'department', 'แผนก', 'statistics', 'สถิติ',
            'client', 'ลูกค้า', 'meeting', 'ประชุม', 'training', 'อบรม',
            'equipment', 'อุปกรณ์', 'timesheet', 'เวลาทำงาน', 'expense', 'ค่าใช้จ่าย'
        ]

    def get_postgres_agent(self, tenant_id: str) -> PostgreSQLAgent:
        """Get or create PostgreSQL agent for specific tenant"""
        if tenant_id not in self.postgres_agents:
            logger.info(f"Creating PostgreSQL agent for tenant: {tenant_id}")
            self.postgres_agents[tenant_id] = PostgreSQLAgent(tenant_id)
        return self.postgres_agents[tenant_id]

    def get_ollama_agent(self, tenant_id: str) -> OllamaAgent:
        """Get or create Ollama agent for specific tenant"""
        if tenant_id not in self.ollama_agents:
            logger.info(f"Creating Ollama agent for tenant: {tenant_id}")
            self.ollama_agents[tenant_id] = OllamaAgent(tenant_id)
        return self.ollama_agents[tenant_id]

    async def smart_route_and_answer(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Auto Agent - intelligently routes and provides unified answer"""
        
        try:
            tenant_config = get_tenant_config(tenant_id)
            logger.info(f"Auto Agent processing question for tenant: {tenant_id}")
            
            # Step 1: Determine data source using keywords
            data_source = self.determine_data_source(question, tenant_id)
            
            # Step 2: Get data from appropriate source(s)
            if data_source == "database":
                answer = await self.get_database_answer(question, tenant_id)
            elif data_source == "ollama":
                answer = await self.get_ollama_answer(question, tenant_id)
            else:  # both
                answer = await self.get_combined_answer(question, tenant_id)
            
            return {
                "success": True,
                "answer": answer["content"],
                "source": f"Auto Agent - {tenant_config.name}",
                "agent": "auto",
                "tenant_id": tenant_id,
                "tenant_name": tenant_config.name,
                "data_source_used": data_source,
                "routing_decision": "auto"
            }
            
        except Exception as e:
            logger.error(f"Error in Auto Agent for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "answer": f"Sorry, I encountered an error while processing your question: {str(e)}",
                "source": "Auto Agent - Error",
                "agent": "auto",
                "tenant_id": tenant_id,
                "error": str(e)
            }

    def determine_data_source(self, question: str, tenant_id: str) -> str:
        """Determine whether to use database or Ollama based on keywords"""
        
        question_lower = question.lower()
        
        # Simple keyword matching
        database_score = sum(1 for keyword in self.database_keywords if keyword in question_lower)
        
        # If clear database question, use database
        if database_score >= 2:
            logger.info(f"Routing to database for tenant {tenant_id} (keyword match: {database_score})")
            return "database"
        elif database_score >= 1:
            logger.info(f"Routing to both for tenant {tenant_id} (partial keyword match: {database_score})")
            return "both"
        else:
            logger.info(f"Routing to Ollama for tenant {tenant_id} (no database keywords)")
            return "ollama"

    async def get_database_answer(self, question: str, tenant_id: str) -> Dict[str, str]:
        """Get answer from database"""
        try:
            tenant_config = get_tenant_config(tenant_id)
            
            # Check if postgres agent is enabled
            if not tenant_config.settings.get('enable_postgres_agent', True):
                return {
                    "content": f"Database queries are not enabled for {tenant_config.name}",
                    "source": "Configuration"
                }
            
            postgres_agent = self.get_postgres_agent(tenant_id)
            result = postgres_agent.query(question, tenant_id)
            
            if result["success"]:
                return {
                    "content": result["answer"],
                    "source": "Company Database"
                }
            else:
                return {
                    "content": f"I couldn't retrieve data from the database: {result.get('answer', 'Unknown error')}",
                    "source": "Database Error"
                }
                
        except Exception as e:
            logger.error(f"Database query error for tenant {tenant_id}: {e}")
            return {
                "content": f"Database access error: {str(e)}",
                "source": "Database Error"
            }

    async def get_ollama_answer(self, question: str, tenant_id: str) -> Dict[str, str]:
        """Get answer from Ollama"""
        try:
            tenant_config = get_tenant_config(tenant_id)
            
            # Check if Ollama agent is enabled
            if not tenant_config.settings.get('enable_ollama_agent', True):
                return {
                    "content": f"Ollama queries are not enabled for {tenant_config.name}",
                    "source": "Configuration"
                }
            
            ollama_agent = self.get_ollama_agent(tenant_id)
            result = await ollama_agent.async_query(question, tenant_id)
            
            if result["success"]:
                return {
                    "content": result["answer"],
                    "source": "Ollama AI Model"
                }
            else:
                return {
                    "content": f"I couldn't get response from Ollama: {result.get('error', 'Unknown error')}",
                    "source": "Ollama Error"
                }
                
        except Exception as e:
            logger.error(f"Ollama query error for tenant {tenant_id}: {e}")
            return {
                "content": f"Ollama access error: {str(e)}",
                "source": "Ollama Error"
            }

    async def get_combined_answer(self, question: str, tenant_id: str) -> Dict[str, str]:
        """Get answer from both database and Ollama"""
        try:
            tenant_config = get_tenant_config(tenant_id)
            
            # Check if hybrid search is allowed
            if not tenant_config.settings.get('allow_hybrid_search', True):
                # Fall back to Ollama only
                return await self.get_ollama_answer(question, tenant_id)
            
            logger.info(f"Getting combined answer for tenant: {tenant_id}")
            
            # Query both sources
            db_task = self.get_database_answer(question, tenant_id)
            ollama_task = self.get_ollama_answer(question, tenant_id)
            
            db_result, ollama_result = await asyncio.gather(db_task, ollama_task, return_exceptions=True)
            
            # Handle exceptions
            if isinstance(db_result, Exception):
                db_result = {"content": f"Database error: {str(db_result)}", "source": "Database Error"}
            if isinstance(ollama_result, Exception):
                ollama_result = {"content": f"Ollama error: {str(ollama_result)}", "source": "Ollama Error"}
            
            # Combine results intelligently
            combined_answer = self.combine_answers(db_result, ollama_result, question, tenant_config)
            
            return {
                "content": combined_answer,
                "source": "Database + Ollama AI"
            }
            
        except Exception as e:
            logger.error(f"Combined query error for tenant {tenant_id}: {e}")
            return {
                "content": f"Error accessing multiple data sources: {str(e)}",
                "source": "Combined Error"
            }

    def combine_answers(self, db_result: Dict, ollama_result: Dict, question: str, tenant_config) -> str:
        """Intelligently combine answers from different sources"""
        
        db_content = db_result.get("content", "")
        ollama_content = ollama_result.get("content", "")
        
        # Check if both sources have useful content
        db_has_content = db_content and "error" not in db_content.lower() and len(db_content.strip()) > 10
        ollama_has_content = ollama_content and "error" not in ollama_content.lower() and len(ollama_content.strip()) > 10
        
        if db_has_content and ollama_has_content:
            return f"""Based on {tenant_config.name}'s data:

📊 **From Database:**
{db_content}

🤖 **From AI Assistant:**
{ollama_content}

---
*Information from: {tenant_config.name} - Database and AI Model*"""

        elif db_has_content:
            return f"""📊 **From {tenant_config.name} Database:**
{db_content}

📝 *Note: Additional AI assistance was not available*"""

        elif ollama_has_content:
            return f"""🤖 **From {tenant_config.name} AI Assistant:**
{ollama_content}

📝 *Note: No specific database records found for this query*"""

        else:
            return f"I apologize, but I couldn't find relevant information in {tenant_config.name}'s database or AI assistant to answer your question. Please try rephrasing your question or contact support for assistance."


# Wrapper for backward compatibility
class AggregatorAgent:
    """Wrapper for backward compatibility"""
    
    def __init__(self):
        self.auto_agent = AutoAgent()
    
    async def process_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Process question using Auto Agent"""
        return await self.auto_agent.smart_route_and_answer(question, tenant_id)
    
    async def get_tenant_agent_status(self, tenant_id: str) -> Dict[str, Any]:
        """Get agent status for tenant"""
        try:
            tenant_config = get_tenant_config(tenant_id)
            
            status = {
                "tenant_id": tenant_id,
                "tenant_name": tenant_config.name,
                "auto_agent": {"status": "active", "description": "Smart routing agent"},
                "settings": tenant_config.settings
            }
            
            # Test database if enabled
            if tenant_config.settings.get('enable_postgres_agent', True):
                try:
                    postgres_agent = self.auto_agent.get_postgres_agent(tenant_id)
                    test_result = postgres_agent.test_connection(tenant_id)
                    status["database"] = {
                        "status": "connected" if test_result["success"] else "error",
                        "error": test_result.get("error") if not test_result["success"] else None
                    }
                except Exception as e:
                    status["database"] = {"status": "error", "error": str(e)}
            else:
                status["database"] = {"status": "disabled"}
            
            # Test Ollama if enabled
            if tenant_config.settings.get('enable_ollama_agent', True):
                try:
                    ollama_agent = self.auto_agent.get_ollama_agent(tenant_id)
                    test_result = ollama_agent.test_connection(tenant_id)
                    status["ollama"] = {
                        "status": "connected" if test_result["success"] else "error",
                        "error": test_result.get("error") if not test_result["success"] else None,
                        "server_url": test_result.get("server_url", "unknown"),
                        "available_models": test_result.get("available_models", [])
                    }
                except Exception as e:
                    status["ollama"] = {"status": "error", "error": str(e)}
            else:
                status["ollama"] = {"status": "disabled"}
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting agent status for tenant {tenant_id}: {e}")
            return {
                "tenant_id": tenant_id,
                "error": str(e),
                "auto_agent": {"status": "error"}
            }


# Global instance
def create_aggregator_agent() -> AggregatorAgent:
    """Create aggregator agent"""
    return AggregatorAgent()

async def process_tenant_question(question: str, tenant_id: str) -> Dict[str, Any]:
    """Quick function to process question for specific tenant"""
    auto_agent = AutoAgent()
    return await auto_agent.smart_route_and_answer(question, tenant_id)


# Test usage
async def test_auto_agent():
    """Test Auto Agent functionality with Ollama"""
    auto_agent = AutoAgent()
    
    test_scenarios = [
        {"tenant": "company-a", "question": "How many employees are there?"},
        {"tenant": "company-a", "question": "สวัสดี คุณเป็นใคร?"},
        {"tenant": "company-b", "question": "What is the average salary?"},
        {"tenant": "company-c", "question": "Tell me about AI and company statistics"},
    ]
    
    for scenario in test_scenarios:
        tenant_id = scenario["tenant"]
        question = scenario["question"]
        
        print(f"\n{'='*70}")
        print(f"🏢 Tenant: {tenant_id}")
        print(f"❓ Question: {question}")
        print(f"{'='*70}")
        
        try:
            result = await auto_agent.smart_route_and_answer(question, tenant_id)
            print(f"🎯 Data Source: {result.get('data_source_used', 'unknown')}")
            print(f"✅ Answer: {result['answer'][:200]}..." if len(result['answer']) > 200 else f"✅ Answer: {result['answer']}")
            print(f"📍 Source: {result['source']}")
            
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_auto_agent())