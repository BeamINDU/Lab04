import json
import asyncio
import boto3
from typing import Dict, Any, List
from postgres_agent import PostgreSQLAgent

class AggregatorAgent:
    def __init__(self):
        # Initialize agents
        self.postgres_agent = PostgreSQLAgent()
        
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
        self.knowledge_base_id = 'KJGWQPHFSM'
        
        # Routing keywords
        self.sql_keywords = [
            'กี่คน', 'จำนวน', 'เท่าไหร่', 'เฉลี่ย', 'รวม', 'มากที่สุด', 'น้อยที่สุด',
            'พนักงาน', 'เงินเดือน', 'โปรเจค', 'แผนก', 'ตำแหน่ง', 'งบประมาณ',
            'ใครบ้าง', 'รายชื่อ', 'สถานะ', 'วันที่', 'ลูกค้า', 'เทคโนโลยี'
        ]
        
        self.knowledge_keywords = [
            'บริษัท', 'ธุรกิจ', 'บริการ', 'ติดต่อ', 'เวลา', 'สวัสดิการ', 'นโยบาย',
            'สยามเทค', 'ทำงาน', 'ลางาน', 'วันหยุด', 'ฝึกอบรม', 'สำนักงาน'
        ]

    async def route_question(self, question: str) -> str:
        """ตัดสินใจว่าควรใช้ agent ไหน"""
        
        # Simple keyword-based routing first
        question_lower = question.lower()
        
        sql_score = sum(1 for keyword in self.sql_keywords if keyword in question_lower)
        knowledge_score = sum(1 for keyword in self.knowledge_keywords if keyword in question_lower)
        
        # If clear match, return immediately
        if sql_score > knowledge_score and sql_score > 0:
            return "postgres"
        elif knowledge_score > sql_score and knowledge_score > 0:
            return "knowledge_base"
        
        # Use Claude for ambiguous cases
        return await self.claude_route_decision(question)

    async def claude_route_decision(self, question: str) -> str:
        """ใช้ Claude ตัดสินใจ routing"""
        
        prompt = f"""คุณเป็น routing agent ที่ต้องตัดสินใจว่าคำถามต่อไปนี้ควรส่งไปยัง agent ไหน

Agent ที่มี:
1. "postgres" - สำหรับคำถามเกี่ยวกับข้อมูลในฐานข้อมูล เช่น จำนวนพนักงาน, เงินเดือน, โปรเจค, สถิติต่างๆ
2. "knowledge_base" - สำหรับคำถามเกี่ยวกับข้อมูลทั่วไปของบริษัท เช่น ธุรกิจ, บริการ, นโยบาย, การติดต่อ

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
                return "postgres"
            elif "knowledge" in decision:
                return "knowledge_base"
            else:
                # Default to knowledge_base for safety
                return "knowledge_base"
                
        except Exception as e:
            print(f"Error in Claude routing: {e}")
            return "knowledge_base"  # Default fallback

    async def query_postgres_agent(self, question: str) -> Dict[str, Any]:
        """Query PostgreSQL Agent"""
        try:
            result = self.postgres_agent.query(question)
            return {
                "success": result["success"],
                "answer": result["answer"],
                "source": "PostgreSQL Database",
                "agent": "postgres",
                "sql": result.get("sql"),
                "data": result.get("data")
            }
        except Exception as e:
            return {
                "success": False,
                "answer": f"เกิดข้อผิดพลาดในการเข้าถึงฐานข้อมูล: {str(e)}",
                "source": "PostgreSQL Database",
                "agent": "postgres",
                "sql": None,
                "data": None
            }

    async def query_knowledge_base_agent(self, question: str) -> Dict[str, Any]:
        """Query Knowledge Base Agent"""
        try:
            # Retrieve from Knowledge Base
            retrieve_response = self.bedrock_agent.retrieve(
                knowledgeBaseId=self.knowledge_base_id,
                retrievalQuery={'text': question},
                retrievalConfiguration={
                    'vectorSearchConfiguration': {
                        'numberOfResults': 10
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
                    "success": False,
                    "answer": "ขออภัย ไม่พบข้อมูลที่เกี่ยวข้องกับคำถามของคุณในเอกสาร",
                    "source": "Knowledge Base",
                    "agent": "knowledge_base",
                    "documents": []
                }
            
            # Generate response
            context = "\n\n".join(retrieved_docs[:5])
            prompt = f"""จากข้อมูลบริบทต่อไปนี้:

{context}

คำถาม: {question}

กรุณาตอบคำถามโดยอ้างอิงจากข้อมูลบริบทข้างบน หากข้อมูลไม่เพียงพอ กรุณาบอกว่าไม่มีข้อมูลเพียงพอ อย่าสร้างข้อมูลเพิ่มเติม"""

            claude_request = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
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
                "source": "Knowledge Base Documents",
                "agent": "knowledge_base",
                "documents": retrieved_docs[:3]
            }
            
        except Exception as e:
            return {
                "success": False,
                "answer": f"เกิดข้อผิดพลาดในการเข้าถึงเอกสาร: {str(e)}",
                "source": "Knowledge Base",
                "agent": "knowledge_base",
                "documents": []
            }

    async def process_question(self, question: str) -> Dict[str, Any]:
        """Main method - ประมวลผลคำถามและส่งไปยัง agent ที่เหมาะสม"""
        
        # Step 1: Route the question
        selected_agent = await self.route_question(question)
        print(f"🎯 Routing decision: {selected_agent}")
        
        # Step 2: Query the selected agent
        if selected_agent == "postgres":
            result = await self.query_postgres_agent(question)
        else:
            result = await self.query_knowledge_base_agent(question)
        
        # Step 3: Add metadata
        result["routing_decision"] = selected_agent
        result["question"] = question
        
        return result

    async def hybrid_search(self, question: str) -> Dict[str, Any]:
        """ค้นหาจากทั้งสอง agents และรวมผลลัพธ์"""
        
        print("🔄 Performing hybrid search...")
        
        # Query both agents simultaneously
        postgres_task = self.query_postgres_agent(question)
        knowledge_task = self.query_knowledge_base_agent(question)
        
        postgres_result, knowledge_result = await asyncio.gather(
            postgres_task, knowledge_task, return_exceptions=True
        )
        
        # Handle exceptions
        if isinstance(postgres_result, Exception):
            postgres_result = {
                "success": False,
                "answer": f"PostgreSQL error: {str(postgres_result)}",
                "agent": "postgres"
            }
            
        if isinstance(knowledge_result, Exception):
            knowledge_result = {
                "success": False,
                "answer": f"Knowledge Base error: {str(knowledge_result)}",
                "agent": "knowledge_base"
            }
        
        # Combine results
        combined_answer = self.combine_results(postgres_result, knowledge_result, question)
        
        return {
            "success": True,
            "answer": combined_answer,
            "source": "Hybrid Search (PostgreSQL + Knowledge Base)",
            "agent": "hybrid",
            "postgres_result": postgres_result,
            "knowledge_result": knowledge_result,
            "question": question
        }

    def combine_results(self, postgres_result: Dict, knowledge_result: Dict, question: str) -> str:
        """รวมผลลัพธ์จากทั้งสอง agents"""
        
        postgres_success = postgres_result.get("success", False)
        knowledge_success = knowledge_result.get("success", False)
        
        if postgres_success and knowledge_success:
            return f"""จากการค้นหาข้อมูล:

📊 **จากฐานข้อมูล:**
{postgres_result['answer']}

📚 **จากเอกสาร:**
{knowledge_result['answer']}

---
*ข้อมูลจาก: ฐานข้อมูลพนักงาน และเอกสารบริษัท*"""

        elif postgres_success:
            return f"""📊 **จากฐานข้อมูล:**
{postgres_result['answer']}

📚 *หมายเหตุ: ไม่พบข้อมูลเพิ่มเติมในเอกสาร*"""

        elif knowledge_success:
            return f"""📚 **จากเอกสาร:**
{knowledge_result['answer']}

📊 *หมายเหตุ: ไม่พบข้อมูลที่เกี่ยวข้องในฐานข้อมูล*"""

        else:
            return "ขออภัย ไม่พบข้อมูลที่เกี่ยวข้องกับคำถามของคุณทั้งในฐานข้อมูลและเอกสาร"

# Test usage
async def test_aggregator():
    agent = AggregatorAgent()
    
    test_questions = [
        "มีพนักงานกี่คน?",  # Should go to PostgreSQL
        "บริษัทสยามเทคทำธุรกิจอะไร?",  # Should go to Knowledge Base
        "เงินเดือนเฉลี่ยเท่าไหร่?",  # Should go to PostgreSQL
        "เวลาทำการของบริษัท?",  # Should go to Knowledge Base
        "พนักงานแผนก IT ใครบ้าง?",  # Should go to PostgreSQL
    ]
    
    for question in test_questions:
        print(f"\n" + "="*60)
        print(f"❓ คำถาม: {question}")
        
        result = await agent.process_question(question)
        
        print(f"🎯 Agent: {result['routing_decision']}")
        print(f"📝 คำตอบ: {result['answer']}")
        print(f"📍 Source: {result['source']}")

if __name__ == "__main__":
    asyncio.run(test_aggregator())