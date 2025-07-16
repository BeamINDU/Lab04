import json
import asyncio
import boto3
from typing import Dict, Any, List, Optional
import logging
from postgres_agent import PostgreSQLAgent
from tenant_manager import get_tenant_config, get_tenant_knowledge_base_config

logger = logging.getLogger(__name__)

class AutoAgent:
    """Simplified Auto Agent - intelligently routes to best data source"""
    
    def __init__(self):
        # Initialize tenant-aware agents (will be created on-demand)
        self.postgres_agents: Dict[str, PostgreSQLAgent] = {}
        
        # Claude for routing decisions and responses
        self.bedrock_runtime = boto3.client(
            'bedrock-runtime',
            region_name='ap-southeast-1'
        )
        
        # Initialize Knowledge Base agent
        self.bedrock_agent = boto3.client(
            'bedrock-agent-runtime',
            region_name='ap-southeast-1'
        )
        
        # Simple routing keywords
        self.database_keywords = [
            'employees', 'พนักงาน', 'salary', 'เงินเดือน', 'projects', 'โปรเจค',
            'how many', 'กี่คน', 'count', 'จำนวน', 'average', 'เฉลี่ย',
            'budget', 'งบประมาณ', 'department', 'แผนก', 'statistics', 'สถิติ'
        ]

    def get_postgres_agent(self, tenant_id: str) -> PostgreSQLAgent:
        """Get or create PostgreSQL agent for specific tenant"""
        if tenant_id not in self.postgres_agents:
            logger.info(f"Creating PostgreSQL agent for tenant: {tenant_id}")
            self.postgres_agents[tenant_id] = PostgreSQLAgent(tenant_id)
        return self.postgres_agents[tenant_id]

    async def smart_route_and_answer(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Auto Agent - intelligently routes and provides unified answer"""
        
        try:
            tenant_config = get_tenant_config(tenant_id)
            logger.info(f"Auto Agent processing question for tenant: {tenant_id}")
            
            # Step 1: Determine data source using keywords and AI
            data_source = await self.determine_data_source(question, tenant_id)
            
            # Step 2: Get data from appropriate source(s)
            if data_source == "database":
                answer = await self.get_database_answer(question, tenant_id)
            elif data_source == "documents":
                answer = await self.get_knowledge_answer(question, tenant_id)
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

    async def determine_data_source(self, question: str, tenant_id: str) -> str:
        """Determine whether to use database, documents, or both"""
        
        question_lower = question.lower()
        
        # Simple keyword matching first
        database_score = sum(1 for keyword in self.database_keywords if keyword in question_lower)
        
        # If clear database question, use database
        if database_score >= 2:
            logger.info(f"Routing to database for tenant {tenant_id} (keyword match)")
            return "database"
        
        # If no clear database keywords, check with Claude
        tenant_config = get_tenant_config(tenant_id)
        
        prompt = f"""You are a smart routing assistant for {tenant_config.name}.

Determine the best data source for this question:
Question: "{question}"

Available data sources:
1. "database" - Employee data, projects, salaries, statistics, numbers
2. "documents" - Company information, policies, services, general info
3. "both" - Questions requiring both sources

Rules:
- Use "database" for: employee counts, salaries, project data, statistics, numbers
- Use "documents" for: company services, policies, general information, contact info
- Use "both" for: comprehensive questions about company and its data

Respond with exactly one word: database, documents, or both"""

        try:
            claude_request = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 10,
                "messages": [{"role": "user", "content": prompt}]
            }
            
            response = self.bedrock_runtime.invoke_model(
                modelId='apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
                body=json.dumps(claude_request),
                contentType='application/json'
            )
            
            response_body = json.loads(response['body'].read())
            decision = response_body['content'][0]['text'].strip().lower()
            
            if decision in ["database", "documents", "both"]:
                logger.info(f"Claude routing decision for tenant {tenant_id}: {decision}")
                return decision
            else:
                logger.warning(f"Unclear Claude decision for tenant {tenant_id}, defaulting to documents")
                return "documents"
                
        except Exception as e:
            logger.error(f"Error in Claude routing for tenant {tenant_id}: {e}")
            return "documents"  # Safe default

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

    async def get_knowledge_answer(self, question: str, tenant_id: str) -> Dict[str, str]:
        """Get answer from knowledge base"""
        try:
            tenant_config = get_tenant_config(tenant_id)
            kb_config = get_tenant_knowledge_base_config(tenant_id)
            
            # Check if knowledge base agent is enabled
            if not tenant_config.settings.get('enable_knowledge_base_agent', True):
                return {
                    "content": f"Knowledge base queries are not enabled for {tenant_config.name}",
                    "source": "Configuration"
                }
            
            # Add tenant context to question
            enhanced_question = f"Company: {tenant_config.name}\nQuestion: {question}"
            
            # Retrieve from Knowledge Base
            retrieve_response = self.bedrock_agent.retrieve(
                knowledgeBaseId=kb_config['id'],
                retrievalQuery={'text': enhanced_question},
                retrievalConfiguration={
                    'vectorSearchConfiguration': {
                        'numberOfResults': kb_config.get('max_results', 5)
                    }
                }
            )
            
            # Process documents
            retrieved_docs = []
            for result in retrieve_response.get('retrievalResults', []):
                content = result.get('content', {}).get('text', '')
                if content:
                    retrieved_docs.append(content)
            
            if not retrieved_docs:
                return {
                    "content": f"I couldn't find relevant information in {tenant_config.name}'s knowledge base for your question.",
                    "source": "Knowledge Base"
                }
            
            # Generate response with Claude
            context = "\n\n".join(retrieved_docs[:3])
            prompt = f"""Company: {tenant_config.name}

Context from company documents:
{context}

Question: {question}

Please answer the question based on the context provided. If the information is not sufficient, say so clearly. Do not make up information."""

            claude_request = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": tenant_config.settings.get('max_tokens', 1000),
                "messages": [{"role": "user", "content": prompt}]
            }
            
            response = self.bedrock_runtime.invoke_model(
                modelId='apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
                body=json.dumps(claude_request),
                contentType='application/json'
            )
            
            response_body = json.loads(response['body'].read())
            answer = response_body['content'][0]['text']
            
            return {
                "content": answer,
                "source": "Company Knowledge Base"
            }
            
        except Exception as e:
            logger.error(f"Knowledge base query error for tenant {tenant_id}: {e}")
            return {
                "content": f"Knowledge base access error: {str(e)}",
                "source": "Knowledge Base Error"
            }

    async def get_combined_answer(self, question: str, tenant_id: str) -> Dict[str, str]:
        """Get answer from both database and knowledge base"""
        try:
            tenant_config = get_tenant_config(tenant_id)
            
            # Check if hybrid search is allowed
            if not tenant_config.settings.get('allow_hybrid_search', True):
                # Fall back to documents only
                return await self.get_knowledge_answer(question, tenant_id)
            
            logger.info(f"Getting combined answer for tenant: {tenant_id}")
            
            # Query both sources
            db_task = self.get_database_answer(question, tenant_id)
            kb_task = self.get_knowledge_answer(question, tenant_id)
            
            db_result, kb_result = await asyncio.gather(db_task, kb_task, return_exceptions=True)
            
            # Handle exceptions
            if isinstance(db_result, Exception):
                db_result = {"content": f"Database error: {str(db_result)}", "source": "Database Error"}
            if isinstance(kb_result, Exception):
                kb_result = {"content": f"Knowledge base error: {str(kb_result)}", "source": "Knowledge Base Error"}
            
            # Combine results intelligently
            combined_answer = self.combine_answers(db_result, kb_result, question, tenant_config)
            
            return {
                "content": combined_answer,
                "source": "Database + Knowledge Base"
            }
            
        except Exception as e:
            logger.error(f"Combined query error for tenant {tenant_id}: {e}")
            return {
                "content": f"Error accessing multiple data sources: {str(e)}",
                "source": "Combined Error"
            }

    def combine_answers(self, db_result: Dict, kb_result: Dict, question: str, tenant_config) -> str:
        """Intelligently combine answers from different sources"""
        
        db_content = db_result.get("content", "")
        kb_content = kb_result.get("content", "")
        
        # Check if both sources have useful content
        db_has_content = db_content and "error" not in db_content.lower() and len(db_content.strip()) > 10
        kb_has_content = kb_content and "error" not in kb_content.lower() and len(kb_content.strip()) > 10
        
        if db_has_content and kb_has_content:
            return f"""Based on {tenant_config.name}'s data:

📊 **From Database:**
{db_content}

📚 **From Company Information:**
{kb_content}

---
*Information from: {tenant_config.name} - Database and Knowledge Base*"""

        elif db_has_content:
            return f"""📊 **From {tenant_config.name} Database:**
{db_content}

📝 *Note: Additional company information was not found in documents*"""

        elif kb_has_content:
            return f"""📚 **From {tenant_config.name} Information:**
{kb_content}

📝 *Note: No specific database records found for this query*"""

        else:
            return f"I apologize, but I couldn't find relevant information in {tenant_config.name}'s database or knowledge base to answer your question. Please try rephrasing your question or contact support for assistance."


# Convenience functions for backward compatibility
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
            
            # Test knowledge base if enabled
            if tenant_config.settings.get('enable_knowledge_base_agent', True):
                try:
                    # Simple test - just check if we can access the configuration
                    kb_config = get_tenant_knowledge_base_config(tenant_id)
                    if kb_config.get('id'):
                        status["knowledge_base"] = {"status": "configured"}
                    else:
                        status["knowledge_base"] = {"status": "not_configured"}
                except Exception as e:
                    status["knowledge_base"] = {"status": "error", "error": str(e)}
            else:
                status["knowledge_base"] = {"status": "disabled"}
            
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
    """Test Auto Agent functionality"""
    auto_agent = AutoAgent()
    
    test_scenarios = [
        {"tenant": "company-a", "question": "How many employees are there?"},
        {"tenant": "company-a", "question": "What services does the company provide?"},
        {"tenant": "company-b", "question": "What is the average salary?"},
        {"tenant": "company-c", "question": "Tell me about the company and employee count"},
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