import json
import asyncio
import boto3
import hashlib
import time
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
        
        # 🚀 NEW: Speed optimization features
        self.response_cache = {}
        self.quick_responses = {
            "สวัสดี": "สวัสดีครับ! ยินดีให้บริการ มีอะไรให้ช่วยไหมครับ?",
            "ขอบคุณ": "ด้วยความยินดีครับ! มีอะไรอื่นให้ช่วยอีกไหม?",
            "hello": "Hello! How can I help you today?",
            "hi": "Hi there! What can I do for you?",
            "เบอร์โทร": "เบอร์โทรศัพท์บริษัท สยามเทค: 02 123 4567",
            "อีเมล": "อีเมลบริษัท: info@siamtech.co.th",
            "email": "Company email: info@siamtech.co.th",
            "ที่อยู่": "บริษัทสยามเทค จำกัด ตั้งอยู่ที่กรุงเทพมหานคร ประเทศไทย",
            "เวลาทำการ": "เวลาทำการ: จันทร์-ศุกร์ 09:00-18:00 น., เสาร์ 09:00-12:00 น.",
            "line": "Line ID: siamtech"
        }
        
        # Routing keywords (existing)
        self.sql_keywords = [
            'กี่คน', 'จำนวน', 'เท่าไหร่', 'เฉลี่ย', 'รวม', 'มากที่สุด', 'น้อยที่สุด',
            'พนักงาน', 'เงินเดือน', 'โปรเจค', 'แผนก', 'ตำแหน่ง', 'งบประมาณ',
            'ใครบ้าง', 'รายชื่อ', 'สถานะ', 'วันที่', 'ลูกค้า', 'เทคโนโลยี'
        ]
        
        self.knowledge_keywords = [
            'บริษัท', 'ธุรกิจ', 'บริการ', 'ติดต่อ', 'เวลา', 'สวัสดิการ', 'นโยบาย',
            'สยามเทค', 'ทำงาน', 'ลางาน', 'วันหยุด', 'ฝึกอบรม', 'สำนักงาน'
        ]

    # 🚀 NEW: Quick response system
    def get_quick_response(self, question: str) -> str:
        """ตอบคำถามทั่วไปแบบทันที"""
        question_clean = question.lower().strip()
        
        # ตรวจสอบคำตอบสำเร็จรูป
        for keyword, response in self.quick_responses.items():
            if keyword in question_clean:
                return response
        
        # ตรวจสอบคำถามเกี่ยวกับข้อมูลติดต่อ
        contact_keywords = ['ติดต่อ', 'โทร', 'เบอร์', 'email', 'อีเมล', 'contact']
        if any(k in question_clean for k in contact_keywords):
            return """📞 ข้อมูลติดต่อบริษัทสยามเทค:
• โทรศัพท์: 02 123 4567
• อีเมล: info@siamtech.co.th  
• Line ID: siamtech
• เว็บไซต์: www.siamtech.co.th"""
        
        return None

    # 🚀 NEW: Cache system
    def get_cache_key(self, question: str) -> str:
        """สร้าง cache key"""
        return hashlib.md5(question.encode('utf-8')).hexdigest()

    def get_cached_response(self, question: str) -> Dict[str, Any]:
        """ดึงคำตอบจาก cache"""
        cache_key = self.get_cache_key(question)
        return self.response_cache.get(cache_key)

    def cache_response(self, question: str, response: Dict[str, Any]):
        """เก็บคำตอบใน cache"""
        cache_key = self.get_cache_key(question)
        response['cached_at'] = time.time()
        self.response_cache[cache_key] = response
        
        # จำกัดขนาด cache (เก็บแค่ 100 รายการล่าสุด)
        if len(self.response_cache) > 100:
            oldest_key = min(self.response_cache.keys(), 
                           key=lambda k: self.response_cache[k].get('cached_at', 0))
            del self.response_cache[oldest_key]

    # 🚀 NEW: Fast routing with fallback
    async def fast_route_question(self, question: str) -> str:
        """Routing แบบเร็ว"""
        question_lower = question.lower()
        
        # Quick scoring
        sql_score = sum(1 for keyword in self.sql_keywords if keyword in question_lower)
        knowledge_score = sum(1 for keyword in self.knowledge_keywords if keyword in question_lower)
        
        # Clear decision
        if sql_score > knowledge_score and sql_score >= 2:
            return "postgres"
        elif knowledge_score > sql_score and knowledge_score >= 2:
            return "knowledge_base"
        elif sql_score > 0:
            return "postgres"
        elif knowledge_score > 0:
            return "knowledge_base"
        
        # Default to knowledge_base for general questions
        return "knowledge_base"

    # 🚀 NEW: Ultra-fast processing
    async def ultra_fast_process(self, question: str) -> Dict[str, Any]:
        """ประมวลผลแบบเร็วที่สุด"""
        start_time = time.time()
        
        # 1. ลอง quick response ก่อน (< 1ms)
        quick = self.get_quick_response(question)
        if quick:
            return {
                "success": True,
                "answer": quick,
                "source": "Quick Response",
                "agent": "instant",
                "response_time": time.time() - start_time,
                "method": "instant"
            }
        
        # 2. ลอง cache (1-5ms)
        cached = self.get_cached_response(question)
        if cached and (time.time() - cached.get('cached_at', 0)) < 3600:  # 1 ชั่วโมง
            cached_response = cached.copy()
            cached_response["response_time"] = time.time() - start_time
            cached_response["method"] = "cache"
            return cached_response
        
        # 3. Process normally แต่เร็วขึ้น
        return await self.optimized_process_question(question, start_time)

    async def optimized_process_question(self, question: str, start_time: float = None) -> Dict[str, Any]:
        """ประมวลผลแบบปกติแต่เพิ่มประสิทธิภาพ"""
        if start_time is None:
            start_time = time.time()
        
        # Fast routing
        selected_agent = await self.fast_route_question(question)
        
        # Process with selected agent
        if selected_agent == "postgres":
            result = await self.query_postgres_agent(question)
        else:
            result = await self.query_knowledge_base_agent(question)
        
        # Add metadata
        result["routing_decision"] = selected_agent
        result["question"] = question
        result["response_time"] = time.time() - start_time
        result["method"] = "processed"
        
        # Cache successful responses
        if result.get("success"):
            self.cache_response(question, result)
        
        return result

    # 🚀 NEW: Parallel processing option
    async def parallel_fast_process(self, question: str) -> Dict[str, Any]:
        """ประมวลผลแบบ parallel สำหรับคำถามที่ไม่แน่ใจ"""
        start_time = time.time()
        
        # 1. Quick response check
        quick = self.get_quick_response(question)
        if quick:
            return {
                "success": True,
                "answer": quick,
                "source": "Quick Response",
                "agent": "instant",
                "response_time": time.time() - start_time,
                "method": "instant"
            }
        
        # 2. Check cache
        cached = self.get_cached_response(question)
        if cached and (time.time() - cached.get('cached_at', 0)) < 3600:
            cached_response = cached.copy()
            cached_response["response_time"] = time.time() - start_time
            cached_response["method"] = "cache"
            return cached_response
        
        # 3. Run both agents in parallel and take the faster one
        postgres_task = self.query_postgres_agent(question)
        knowledge_task = self.query_knowledge_base_agent(question)
        
        try:
            # Wait for first successful result
            done, pending = await asyncio.wait(
                [postgres_task, knowledge_task], 
                return_when=asyncio.FIRST_COMPLETED,
                timeout=5.0  # 5 second timeout
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
            
            # Get the first successful result
            for task in done:
                try:
                    result = await task
                    if result.get("success"):
                        result["response_time"] = time.time() - start_time
                        result["method"] = "parallel"
                        
                        # Cache the result
                        if result.get("success"):
                            self.cache_response(question, result)
                        
                        return result
                except Exception as e:
                    continue
            
            # If no success, return error
            return {
                "success": False,
                "answer": "ขออภัย ไม่สามารถประมวลผลคำถามได้ในขณะนี้",
                "response_time": time.time() - start_time,
                "method": "parallel_failed"
            }
            
        except asyncio.TimeoutError:
            return {
                "success": False,
                "answer": "ขออภัย การประมวลผลใช้เวลานานเกินไป กรุณาลองใหม่",
                "response_time": time.time() - start_time,
                "method": "timeout"
            }

    # 🚀 UPDATED: Main process method with speed options
    async def process_question(self, question: str, mode: str = "fast") -> Dict[str, Any]:
        """Main method with speed modes"""
        if mode == "ultra_fast":
            return await self.ultra_fast_process(question)
        elif mode == "parallel":
            return await self.parallel_fast_process(question)
        elif mode == "fast":
            return await self.optimized_process_question(question)
        else:
            # Original method
            return await self.original_process_question(question)

    async def original_process_question(self, question: str) -> Dict[str, Any]:
        """Original method (เก็บไว้เป็น fallback)"""
        selected_agent = await self.route_question(question)
        print(f"🎯 Routing decision: {selected_agent}")
        
        if selected_agent == "postgres":
            result = await self.query_postgres_agent(question)
        else:
            result = await self.query_knowledge_base_agent(question)
        
        result["routing_decision"] = selected_agent
        result["question"] = question
        
        return result

    # Existing methods remain the same...
    async def route_question(self, question: str) -> str:
        """ตัดสินใจว่าควรใช้ agent ไหน (original method)"""
        question_lower = question.lower()
        
        sql_score = sum(1 for keyword in self.sql_keywords if keyword in question_lower)
        knowledge_score = sum(1 for keyword in self.knowledge_keywords if keyword in question_lower)
        
        if sql_score > knowledge_score and sql_score > 0:
            return "postgres"
        elif knowledge_score > sql_score and knowledge_score > 0:
            return "knowledge_base"
        
        return await self.claude_route_decision(question)

    async def claude_route_decision(self, question: str) -> str:
        """ใช้ Claude ตัดสินใจ routing (original method)"""
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
                return "knowledge_base"
                
        except Exception as e:
            print(f"Error in Claude routing: {e}")
            return "knowledge_base"

    async def query_postgres_agent(self, question: str) -> Dict[str, Any]:
        """Query PostgreSQL Agent (original method)"""
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
        """Query Knowledge Base Agent (original method)"""
        try:
            retrieve_response = self.bedrock_agent.retrieve(
                knowledgeBaseId=self.knowledge_base_id,
                retrievalQuery={'text': question},
                retrievalConfiguration={
                    'vectorSearchConfiguration': {
                        'numberOfResults': 10
                    }
                }
            )
            
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

    async def hybrid_search(self, question: str) -> Dict[str, Any]:
        """ค้นหาจากทั้งสอง agents และรวมผลลัพธ์ (original method)"""
        print("🔄 Performing hybrid search...")
        
        postgres_task = self.query_postgres_agent(question)
        knowledge_task = self.query_knowledge_base_agent(question)
        
        postgres_result, knowledge_result = await asyncio.gather(
            postgres_task, knowledge_task, return_exceptions=True
        )
        
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
        """รวมผลลัพธ์จากทั้งสอง agents (original method)"""
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

# 🚀 NEW: Convenience function for testing
async def test_speed_comparison():
    """ทดสอบเปรียบเทียบความเร็ว"""
    agent = AggregatorAgent()
    
    test_questions = [
        "สวัสดี",                    # Should be instant
        "เบอร์โทรเท่าไหร่",         # Should be instant  
        "มีพนักงานกี่คน",           # Should go to postgres
        "บริษัททำธุรกิจอะไร",       # Should go to knowledge
        "สวัสดีครับ"                # Should be instant (repeat)
    ]
    
    for question in test_questions:
        print(f"\n{'='*60}")
        print(f"❓ คำถาม: {question}")
        
        # Test different modes
        modes = ["ultra_fast", "fast", "parallel"]
        
        for mode in modes:
            start = time.time()
            result = await agent.process_question(question, mode=mode)
            duration = time.time() - start
            
            print(f"\n🚀 Mode: {mode}")
            print(f"⏱️  Time: {duration:.3f}s")
            print(f"🤖 Agent: {result.get('agent', 'unknown')}")
            print(f"📝 Answer: {result.get('answer', 'No answer')[:100]}...")
            print(f"🔧 Method: {result.get('method', 'unknown')}")

if __name__ == "__main__":
    asyncio.run(test_speed_comparison())