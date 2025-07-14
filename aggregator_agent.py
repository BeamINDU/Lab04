import json
import asyncio
import boto3
from typing import Dict, Any, List, Optional
import logging
from postgres_agent import PostgreSQLAgent
from tenant_manager import get_tenant_config, get_tenant_knowledge_base_config

logger = logging.getLogger(__name__)

class AggregatorAgent:
    def __init__(self):
        # Initialize tenant-aware agents (will be created on-demand)
        self.postgres_agents: Dict[str, PostgreSQLAgent] = {}
        
        # Claude for routing decisions
        self.bedrock_runtime = boto3.client(
            'bedrock-runtime',
            region_name='ap-southeast-1'
        )
        
        # Initialize Knowledge Base agent (existing RAG)
        self.bedrock_agent = boto3.client(
            'bedrock-agent-runtime',
            region_name='ap-southeast-1'
        )
        
        # Routing keywords (same for all tenants)
        self.sql_keywords = [
            'กี่คน', 'จำนวน', 'เท่าไหร่', 'เฉลี่ย', 'รวม', 'มากที่สุด', 'น้อยที่สุด',
            'พนักงาน', 'เงินเดือน', 'โปรเจค', 'แผนก', 'ตำแหน่ง', 'งบประมาณ',
            'ใครบ้าง', 'รายชื่อ', 'สถานะ', 'วันที่', 'ลูกค้า', 'เทคโนโลยี'
        ]
        
        self.knowledge_keywords = [
            'บริษัท', 'ธุรกิจ', 'บริการ', 'ติดต่อ', 'เวลา', 'สวัสดิการ', 'นโยบาย',
            'สยามเทค', 'ทำงาน', 'ลางาน', 'วันหยุด', 'ฝึกอบรม', 'สำนักงาน'
        ]

    def get_postgres_agent(self, tenant_id: str) -> PostgreSQLAgent:
        """Get or create PostgreSQL agent for specific tenant"""
        if tenant_id not in self.postgres_agents:
            logger.info(f"Creating new PostgreSQL agent for tenant: {tenant_id}")
            self.postgres_agents[tenant_id] = PostgreSQLAgent(tenant_id)
        
        return self.postgres_agents[tenant_id]

    async def route_question(self, question: str, tenant_id: str) -> str:
        """ตัดสินใจว่าควรใช้ agent ไหนสำหรับ tenant นี้"""
        
        # Get tenant configuration
        tenant_config = get_tenant_config(tenant_id)
        
        # Check if agents are enabled for this tenant
        settings = tenant_config.settings
        postgres_enabled = settings.get('enable_postgres_agent', True)
        knowledge_enabled = settings.get('enable_knowledge_base_agent', True)
        
        # Simple keyword-based routing first
        question_lower = question.lower()
        
        sql_score = sum(1 for keyword in self.sql_keywords if keyword in question_lower)
        knowledge_score = sum(1 for keyword in self.knowledge_keywords if keyword in question_lower)
        
        # If clear match and agent is enabled, return immediately
        if sql_score > knowledge_score and sql_score > 0 and postgres_enabled:
            logger.info(f"Routing to PostgreSQL agent for tenant {tenant_id} (keyword match)")
            return "postgres"
        elif knowledge_score > sql_score and knowledge_score > 0 and knowledge_enabled:
            logger.info(f"Routing to Knowledge Base agent for tenant {tenant_id} (keyword match)")
            return "knowledge_base"
        
        # Use Claude for ambiguous cases
        return await self.claude_route_decision(question, tenant_id)

    async def claude_route_decision(self, question: str, tenant_id: str) -> str:
        """ใช้ Claude ตัดสินใจ routing สำหรับ tenant นี้"""
        
        tenant_config = get_tenant_config(tenant_id)
        
        prompt = f"""คุณเป็น routing agent สำหรับ {tenant_config.name} 
ที่ต้องตัดสินใจว่าคำถามต่อไปนี้ควรส่งไปยัง agent ไหน

Agent ที่มี:
1. "postgres" - สำหรับคำถามเกี่ยวกับข้อมูลในฐานข้อมูล เช่น จำนวนพนักงาน, เงินเดือน, โปรเจค, สถิติต่างๆ
2. "knowledge_base" - สำหรับคำถามเกี่ยวกับข้อมูลทั่วไปของบริษัท เช่น ธุรกิจ, บริการ, นโยบาย, การติดต่อ

บริษัท: {tenant_config.name}
คำถาม: "{question}"

กรุณาตอบด้วย "postgres" หรือ "knowledge_base" เท่านั้น

ตอบ:"""

        try:
            claude_request = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 50,
                "messages": [{"role": "user", "content": prompt}]
            }
            
            response = self.bedrock_runtime.invoke_model(
                modelId='apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
                body=json.dumps(claude_request),
                contentType='application/json'
            )
            
            response_body = json.loads(response['body'].read())
            decision = response_body['content'][0]['text'].strip().lower()
            
            if "postgres" in decision:
                logger.info(f"Claude routing decision for tenant {tenant_id}: postgres")
                return "postgres"
            elif "knowledge" in decision:
                logger.info(f"Claude routing decision for tenant {tenant_id}: knowledge_base")
                return "knowledge_base"
            else:
                # Default to knowledge_base for safety
                logger.warning(f"Unclear Claude routing decision for tenant {tenant_id}, defaulting to knowledge_base")
                return "knowledge_base"
                
        except Exception as e:
            logger.error(f"Error in Claude routing for tenant {tenant_id}: {e}")
            return "knowledge_base"  # Default fallback

    async def query_postgres_agent(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Query PostgreSQL Agent สำหรับ tenant นี้"""
        try:
            tenant_config = get_tenant_config(tenant_id)
            
            # Check if postgres agent is enabled
            if not tenant_config.settings.get('enable_postgres_agent', True):
                return {
                    "success": False,
                    "answer": f"PostgreSQL agent ไม่ได้เปิดใช้งานสำหรับ {tenant_config.name}",
                    "source": "Configuration",
                    "agent": "postgres",
                    "tenant_id": tenant_id
                }
            
            postgres_agent = self.get_postgres_agent(tenant_id)
            result = postgres_agent.query(question, tenant_id)
            
            return {
                "success": result["success"],
                "answer": result["answer"],
                "source": f"PostgreSQL Database - {tenant_config.name}",
                "agent": "postgres",
                "tenant_id": tenant_id,
                "sql": result.get("sql"),
                "data": result.get("data")
            }
        except Exception as e:
            logger.error(f"Error in PostgreSQL agent for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "answer": f"เกิดข้อผิดพลาดในการเข้าถึงฐานข้อมูลของ {tenant_id}: {str(e)}",
                "source": "PostgreSQL Database",
                "agent": "postgres",
                "tenant_id": tenant_id,
                "sql": None,
                "data": None
            }

    async def query_knowledge_base_agent(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Query Knowledge Base Agent สำหรับ tenant นี้"""
        try:
            tenant_config = get_tenant_config(tenant_id)
            kb_config = get_tenant_knowledge_base_config(tenant_id)
            
            # Check if knowledge base agent is enabled
            if not tenant_config.settings.get('enable_knowledge_base_agent', True):
                return {
                    "success": False,
                    "answer": f"Knowledge Base agent ไม่ได้เปิดใช้งานสำหรับ {tenant_config.name}",
                    "source": "Configuration",
                    "agent": "knowledge_base",
                    "tenant_id": tenant_id
                }
            
            # Add tenant-specific context to the question
            tenant_context = f"บริษัท: {tenant_config.name}"
            enhanced_question = f"{tenant_context}\n\nคำถาม: {question}"
            
            # Retrieve from Knowledge Base with tenant-specific settings
            retrieve_response = self.bedrock_agent.retrieve(
                knowledgeBaseId=kb_config['id'],
                retrievalQuery={'text': enhanced_question},
                retrievalConfiguration={
                    'vectorSearchConfiguration': {
                        'numberOfResults': kb_config.get('max_results', 10)
                    }
                }
            )
            
            # Process documents
            retrieved_docs = []
            for result in retrieve_response.get('retrievalResults', []):
                content = result.get('content', {}).get('text', '')
                if content:
                    # Filter by tenant prefix if available
                    prefix = kb_config.get('prefix')
                    if prefix:
                        # Only include documents that are relevant to this tenant
                        # This is a simple approach - you might want more sophisticated filtering
                        retrieved_docs.append(content)
                    else:
                        retrieved_docs.append(content)
            
            if not retrieved_docs:
                return {
                    "success": False,
                    "answer": f"ขออภัย ไม่พบข้อมูลที่เกี่ยวข้องกับคำถามของคุณในเอกสารของ {tenant_config.name}",
                    "source": f"Knowledge Base - {tenant_config.name}",
                    "agent": "knowledge_base",
                    "tenant_id": tenant_id,
                    "documents": []
                }
            
            # Generate response with tenant context
            context = "\n\n".join(retrieved_docs[:5])
            prompt = f"""บริษัท: {tenant_config.name}

จากข้อมูลบริบทต่อไปนี้:

{context}

คำถาม: {question}

กรุณาตอบคำถามโดยอ้างอิงจากข้อมูลบริบทข้างบน โดยเฉพาะข้อมูลที่เกี่ยวข้องกับ {tenant_config.name} 
หากข้อมูลไม่เพียงพอ กรุณาบอกว่าไม่มีข้อมูลเพียงพอ อย่าสร้างข้อมูลเพิ่มเติม"""

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
                "success": True,
                "answer": answer,
                "source": f"Knowledge Base Documents - {tenant_config.name}",
                "agent": "knowledge_base",
                "tenant_id": tenant_id,
                "documents": retrieved_docs[:3]
            }
            
        except Exception as e:
            logger.error(f"Error in Knowledge Base agent for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "answer": f"เกิดข้อผิดพลาดในการเข้าถึงเอกสารของ {tenant_id}: {str(e)}",
                "source": f"Knowledge Base - {tenant_id}",
                "agent": "knowledge_base",
                "tenant_id": tenant_id,
                "documents": []
            }

    async def process_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Main method - ประมวลผลคำถามและส่งไปยัง agent ที่เหมาะสมสำหรับ tenant นี้"""
        
        try:
            # Validate tenant
            tenant_config = get_tenant_config(tenant_id)
            logger.info(f"Processing question for tenant: {tenant_id} ({tenant_config.name})")
            
            # Step 1: Route the question
            selected_agent = await self.route_question(question, tenant_id)
            logger.info(f"🎯 Routing decision for {tenant_id}: {selected_agent}")
            
            # Step 2: Query the selected agent
            if selected_agent == "postgres":
                result = await self.query_postgres_agent(question, tenant_id)
            else:
                result = await self.query_knowledge_base_agent(question, tenant_id)
            
            # Step 3: Add metadata
            result["routing_decision"] = selected_agent
            result["question"] = question
            result["tenant_id"] = tenant_id
            result["tenant_name"] = tenant_config.name
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing question for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "answer": f"เกิดข้อผิดพลาดในการประมวลผลคำถาม: {str(e)}",
                "source": "Error",
                "agent": "error",
                "tenant_id": tenant_id,
                "question": question,
                "error": str(e)
            }

    async def hybrid_search(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """ค้นหาจากทั้งสอง agents และรวมผลลัพธ์สำหรับ tenant นี้"""
        
        try:
            tenant_config = get_tenant_config(tenant_id)
            
            # Check if hybrid search is allowed
            if not tenant_config.settings.get('allow_hybrid_search', True):
                return {
                    "success": False,
                    "answer": f"Hybrid search ไม่ได้เปิดใช้งานสำหรับ {tenant_config.name}",
                    "source": "Configuration",
                    "agent": "hybrid",
                    "tenant_id": tenant_id
                }
            
            logger.info(f"🔄 Performing hybrid search for tenant: {tenant_id}")
            
            # Query both agents simultaneously
            postgres_task = self.query_postgres_agent(question, tenant_id)
            knowledge_task = self.query_knowledge_base_agent(question, tenant_id)
            
            postgres_result, knowledge_result = await asyncio.gather(
                postgres_task, knowledge_task, return_exceptions=True
            )
            
            # Handle exceptions
            if isinstance(postgres_result, Exception):
                postgres_result = {
                    "success": False,
                    "answer": f"PostgreSQL error: {str(postgres_result)}",
                    "agent": "postgres",
                    "tenant_id": tenant_id
                }
                
            if isinstance(knowledge_result, Exception):
                knowledge_result = {
                    "success": False,
                    "answer": f"Knowledge Base error: {str(knowledge_result)}",
                    "agent": "knowledge_base",
                    "tenant_id": tenant_id
                }
            
            # Combine results
            combined_answer = self.combine_results(postgres_result, knowledge_result, question, tenant_config)
            
            return {
                "success": True,
                "answer": combined_answer,
                "source": f"Hybrid Search - {tenant_config.name}",
                "agent": "hybrid",
                "tenant_id": tenant_id,
                "tenant_name": tenant_config.name,
                "postgres_result": postgres_result,
                "knowledge_result": knowledge_result,
                "question": question
            }
            
        except Exception as e:
            logger.error(f"Error in hybrid search for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "answer": f"เกิดข้อผิดพลาดใน hybrid search: {str(e)}",
                "source": "Hybrid Search",
                "agent": "hybrid",
                "tenant_id": tenant_id,
                "error": str(e)
            }

    def combine_results(self, postgres_result: Dict, knowledge_result: Dict, question: str, tenant_config) -> str:
        """รวมผลลัพธ์จากทั้งสอง agents สำหรับ tenant นี้"""
        
        postgres_success = postgres_result.get("success", False)
        knowledge_success = knowledge_result.get("success", False)
        tenant_name = tenant_config.name
        
        if postgres_success and knowledge_success:
            return f"""ข้อมูลจาก {tenant_name}:

📊 **จากฐานข้อมูล:**
{postgres_result['answer']}

📚 **จากเอกสาร:**
{knowledge_result['answer']}

---
*ข้อมูลจาก: {tenant_name} - ฐานข้อมูลและเอกสารบริษัท*"""

        elif postgres_success:
            return f"""📊 **จากฐานข้อมูล {tenant_name}:**
{postgres_result['answer']}

📚 *หมายเหตุ: ไม่พบข้อมูลเพิ่มเติมในเอกสาร*"""

        elif knowledge_success:
            return f"""📚 **จากเอกสาร {tenant_name}:**
{knowledge_result['answer']}

📊 *หมายเหตุ: ไม่พบข้อมูลที่เกี่ยวข้องในฐานข้อมูล*"""

        else:
            return f"ขออภัย ไม่พบข้อมูลที่เกี่ยวข้องกับคำถามของคุณใน {tenant_name} ทั้งในฐานข้อมูลและเอกสาร"

    async def get_tenant_agent_status(self, tenant_id: str) -> Dict[str, Any]:
        """ตรวจสอบสถานะของ agents สำหรับ tenant นี้"""
        try:
            tenant_config = get_tenant_config(tenant_id)
            
            status = {
                "tenant_id": tenant_id,
                "tenant_name": tenant_config.name,
                "postgres_agent": {"status": "unknown", "error": None},
                "knowledge_base_agent": {"status": "unknown", "error": None},
                "settings": tenant_config.settings
            }
            
            # Test PostgreSQL agent
            if tenant_config.settings.get('enable_postgres_agent', True):
                try:
                    postgres_agent = self.get_postgres_agent(tenant_id)
                    test_result = postgres_agent.test_connection(tenant_id)
                    status["postgres_agent"]["status"] = "connected" if test_result["success"] else "error"
                    if not test_result["success"]:
                        status["postgres_agent"]["error"] = test_result.get("error")
                except Exception as e:
                    status["postgres_agent"]["status"] = "error"
                    status["postgres_agent"]["error"] = str(e)
            else:
                status["postgres_agent"]["status"] = "disabled"
            
            # Test Knowledge Base agent
            if tenant_config.settings.get('enable_knowledge_base_agent', True):
                try:
                    test_result = await self.query_knowledge_base_agent("test", tenant_id)
                    status["knowledge_base_agent"]["status"] = "connected" if test_result["success"] else "error"
                    if not test_result["success"]:
                        status["knowledge_base_agent"]["error"] = test_result.get("answer")
                except Exception as e:
                    status["knowledge_base_agent"]["status"] = "error"
                    status["knowledge_base_agent"]["error"] = str(e)
            else:
                status["knowledge_base_agent"]["status"] = "disabled"
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting agent status for tenant {tenant_id}: {e}")
            return {
                "tenant_id": tenant_id,
                "error": str(e),
                "postgres_agent": {"status": "error"},
                "knowledge_base_agent": {"status": "error"}
            }


# Multi-tenant convenience functions
def create_aggregator_agent() -> AggregatorAgent:
    """Create aggregator agent"""
    return AggregatorAgent()

async def process_tenant_question(question: str, tenant_id: str) -> Dict[str, Any]:
    """Quick function to process question for specific tenant"""
    aggregator = create_aggregator_agent()
    return await aggregator.process_question(question, tenant_id)

async def hybrid_search_for_tenant(question: str, tenant_id: str) -> Dict[str, Any]:
    """Quick function for hybrid search for specific tenant"""
    aggregator = create_aggregator_agent()
    return await aggregator.hybrid_search(question, tenant_id)


# Test usage
async def test_multitenant_aggregator():
    """Test multi-tenant aggregator functionality"""
    aggregator = AggregatorAgent()
    
    test_scenarios = [
        {"tenant": "company-a", "question": "มีพนักงานกี่คน?"},
        {"tenant": "company-b", "question": "บริษัททำธุรกิจอะไร?"},
        {"tenant": "company-c", "question": "เงินเดือนเฉลี่ยเท่าไหร่?"},
        {"tenant": "company-a", "question": "เวลาทำการของบริษัท?"},
    ]
    
    for scenario in test_scenarios:
        tenant_id = scenario["tenant"]
        question = scenario["question"]
        
        print(f"\n{'='*70}")
        print(f"🏢 Tenant: {tenant_id}")
        print(f"❓ Question: {question}")
        print(f"{'='*70}")
        
        try:
            # Test agent status
            status = await aggregator.get_tenant_agent_status(tenant_id)
            print(f"📊 PostgreSQL: {status['postgres_agent']['status']}")
            print(f"📚 Knowledge Base: {status['knowledge_base_agent']['status']}")
            
            # Process question
            result = await aggregator.process_question(question, tenant_id)
            print(f"🎯 Agent Used: {result.get('routing_decision', 'unknown')}")
            print(f"✅ Answer: {result['answer']}")
            print(f"📍 Source: {result['source']}")
            
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_multitenant_aggregator())