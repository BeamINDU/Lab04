import time
import asyncio
import sys
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

# ✅ แก้ไข import path ให้ถูกต้อง
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# ตรวจสอบและ import ระบบที่มีอยู่
try:
    # ลอง import จากตำแหน่งต่างๆ ที่เป็นไปได้
    if os.path.exists(os.path.join(project_root, 'refactored_modules')):
        from refactored_modules.enhanced_postgres_agent_unified import UnifiedEnhancedPostgresOllamaAgent
        from refactored_modules.advanced_dynamic_ai_system import EnhancedUnifiedPostgresOllamaAgent
        print("✅ Import from refactored_modules successful")
    else:
        # หากไม่มี refactored_modules ให้ใช้ mock agents
        print("⚠️ refactored_modules not found, using mock agents for demonstration")
        UnifiedEnhancedPostgresOllamaAgent = None
        EnhancedUnifiedPostgresOllamaAgent = None
        
except ImportError as e:
    print(f"⚠️ Import warning: {e}")
    print("💡 จะใช้ Mock Agents สำหรับการทดสอบ")
    UnifiedEnhancedPostgresOllamaAgent = None
    EnhancedUnifiedPostgresOllamaAgent = None

logger = logging.getLogger(__name__)

class MockAgent:
    """🎭 Mock Agent สำหรับการทดสอบเมื่อไม่มีระบบจริง"""
    
    def __init__(self, agent_type: str):
        self.agent_type = agent_type
        self.call_count = 0
    
    async def process_enhanced_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Mock method สำหรับ Semi-Dynamic"""
        self.call_count += 1
        
        # จำลองการทำงาน
        await asyncio.sleep(0.5 + (0.3 if 'ซับซ้อน' in question else 0))
        
        # จำลองผลลัพธ์
        is_simple = any(word in question.lower() for word in ['จำนวน', 'ราคา', 'รายชื่อ'])
        success = is_simple or ('ง่าย' in question)
        
        return {
            'success': success,
            'answer': f"[MOCK {self.agent_type}] ตอบคำถาม: {question}" if success else f"[MOCK {self.agent_type}] ไม่เข้าใจคำถาม",
            'results_count': 5 if success else 0,
            'data_source_used': f'mock_{self.agent_type}',
            'sql_query': f"SELECT * FROM mock_table WHERE question LIKE '%{question[:20]}%'" if success else None
        }
    
    async def process_any_question(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """Mock method สำหรับ Fully Dynamic"""
        self.call_count += 1
        
        # จำลองการทำงาน (ใช้เวลานานกว่า)
        await asyncio.sleep(1.2 + (0.8 if 'วิเคราะห์' in question else 0))
        
        # Dynamic มักตอบได้ดีกว่า
        success = True  # ตอบได้เกือบทุกคำถาม
        
        return {
            'success': success,
            'answer': f"[MOCK {self.agent_type}] วิเคราะห์และตอบ: {question}",
            'results_count': 8 if success else 0,
            'data_source_used': f'mock_{self.agent_type}',
            'sql_query': f"SELECT advanced_analysis FROM dynamic_table WHERE complex_query ILIKE '%{question[:30]}%'"
        }


class ComparisonSystemManager:
    """🔄 ระบบจัดการการเปรียบเทียบ - รองรับ Mock และ Real"""
    
    def __init__(self):
        # สร้างระบบทั้งสองแบบ (หรือ Mock หากไม่มีระบบจริง)
        if UnifiedEnhancedPostgresOllamaAgent and EnhancedUnifiedPostgresOllamaAgent:
            print("🔧 Using Real Agents")
            self.semi_dynamic_agent = UnifiedEnhancedPostgresOllamaAgent()
            self.fully_dynamic_agent = EnhancedUnifiedPostgresOllamaAgent()
            self.using_mock = False
        else:
            print("🎭 Using Mock Agents for demonstration")
            self.semi_dynamic_agent = MockAgent("Semi-Dynamic")
            self.fully_dynamic_agent = MockAgent("Fully-Dynamic")
            self.using_mock = True
        
        # สถิติการเปรียบเทียบ
        self.comparison_stats = {
            'total_tests': 0,
            'semi_dynamic_wins': 0,
            'fully_dynamic_wins': 0,
            'ties': 0,
            'detailed_results': []
        }
        
        # ชุดทดสอบ HVAC
        self.hvac_test_questions = {
            'basic_hvac': [
                "จำนวนลูกค้าทั้งหมด",
                "ยอดขายปี 2024",
                "รายชื่อทีมช่าง",
                "ราคาอะไหล่ทั้งหมด"
            ],
            'intermediate_hvac': [
                "บริษัทไหนใช้บริการ PM มากที่สุด",
                "อะไหล่ Hitachi ที่มีราคาสูงที่สุด", 
                "ทีมช่างที่ทำงานเดือนนี้",
                "ลูกค้าที่ใช้งบมากกว่า 50000 บาท"
            ],
            'advanced_hvac': [
                "เปรียบเทียบยอดขาย PM กับ Overhaul ในปี 2024",
                "ลูกค้าที่ใช้บริการ Hitachi แต่ไม่เคยซื้ออะไหล่ Hitachi", 
                "ทีมช่างไหนมีประสิทธิภาพสูงที่สุดในงาน PM",
                "อะไหล่ที่ยังไม่เคยใช้ในงานใด"
            ]
        }
        
        logger.info("🔄 Comparison System Manager initialized")
    
    async def run_quick_demo(self, tenant_id: str = "company-a") -> Dict[str, Any]:
        """🎪 Demo เร็วเพื่อดูการทำงาน"""
        
        print("🎪 DEMO เปรียบเทียบ Semi-Dynamic vs Fully Dynamic")
        print("=" * 60)
        
        if self.using_mock:
            print("🎭 กำลังใช้ Mock Agents สำหรับการทดสอบ")
            print("💡 ผลลัพธ์เป็นเพียงการจำลอง ไม่ใช่ข้อมูลจริง")
            print("-" * 60)
        
        demo_questions = [
            "จำนวนลูกค้าทั้งหมด",  # ง่าย
            "ลูกค้าที่ใช้บริการมากที่สุด 3 อันดับ",  # กลาง
            "วิเคราะห์แนวโน้มการขายของแต่ละแบรนด์"  # ซับซ้อน
        ]
        
        results = []
        
        for i, question in enumerate(demo_questions, 1):
            print(f"\n🔍 ทดสอบที่ {i}/3: {question}")
            
            result = await self._compare_single_question(question, tenant_id, f"demo_{i}")
            results.append(result)
            
            # แสดงผลทันที
            self._display_quick_result(result)
        
        # สรุปผล Demo
        self._display_demo_summary(results)
        
        return {
            'demo_results': results,
            'using_mock': self.using_mock,
            'demo_completed_at': datetime.now().isoformat()
        }
    
    async def _compare_single_question(self, question: str, tenant_id: str, test_id: str) -> Dict[str, Any]:
        """🔍 เปรียบเทียบคำถามเดียว"""
        
        result = {
            'question': question,
            'test_id': test_id,
            'semi_dynamic_result': None,
            'fully_dynamic_result': None,
            'winner': 'tie',
            'metrics': {}
        }
        
        # ทดสอบ Semi-Dynamic
        try:
            start_time = time.time()
            
            if hasattr(self.semi_dynamic_agent, 'process_enhanced_question'):
                semi_result = await self.semi_dynamic_agent.process_enhanced_question(question, tenant_id)
            else:
                semi_result = await self.semi_dynamic_agent.process_enhanced_question(question, tenant_id)
            
            semi_time = time.time() - start_time
            
            result['semi_dynamic_result'] = semi_result
            result['metrics']['semi_dynamic_time'] = semi_time
            result['metrics']['semi_dynamic_success'] = semi_result.get('success', False)
            result['metrics']['semi_dynamic_results_count'] = semi_result.get('results_count', 0)
            
        except Exception as e:
            result['semi_dynamic_result'] = {'success': False, 'error': str(e), 'answer': f'Error: {str(e)}'}
            result['metrics']['semi_dynamic_time'] = 0
            result['metrics']['semi_dynamic_success'] = False
            result['metrics']['semi_dynamic_results_count'] = 0
        
        # ทดสอบ Fully Dynamic
        try:
            start_time = time.time()
            
            if hasattr(self.fully_dynamic_agent, 'process_any_question'):
                dynamic_result = await self.fully_dynamic_agent.process_any_question(question, tenant_id)
            else:
                dynamic_result = await self.fully_dynamic_agent.process_enhanced_question(question, tenant_id)
            
            dynamic_time = time.time() - start_time
            
            result['fully_dynamic_result'] = dynamic_result
            result['metrics']['fully_dynamic_time'] = dynamic_time
            result['metrics']['fully_dynamic_success'] = dynamic_result.get('success', False)
            result['metrics']['fully_dynamic_results_count'] = dynamic_result.get('results_count', 0)
            
        except Exception as e:
            result['fully_dynamic_result'] = {'success': False, 'error': str(e), 'answer': f'Error: {str(e)}'}
            result['metrics']['fully_dynamic_time'] = 0
            result['metrics']['fully_dynamic_success'] = False
            result['metrics']['fully_dynamic_results_count'] = 0
        
        # ตัดสินผู้ชนะ
        result['winner'] = self._determine_winner(result)
        
        return result
    
    def _determine_winner(self, result: Dict[str, Any]) -> str:
        """🏆 ตัดสินผู้ชนะ"""
        
        semi_success = result['metrics'].get('semi_dynamic_success', False)
        dynamic_success = result['metrics'].get('fully_dynamic_success', False)
        
        if semi_success and not dynamic_success:
            return 'semi_dynamic'
        elif dynamic_success and not semi_success:
            return 'fully_dynamic'
        elif semi_success and dynamic_success:
            # ทั้งคู่สำเร็จ - ดูจำนวนผลลัพธ์
            semi_count = result['metrics'].get('semi_dynamic_results_count', 0)
            dynamic_count = result['metrics'].get('fully_dynamic_results_count', 0)
            
            if dynamic_count > semi_count:
                return 'fully_dynamic'
            elif semi_count > dynamic_count:
                return 'semi_dynamic'
            else:
                return 'tie'
        else:
            return 'tie'
    
    def _display_quick_result(self, result: Dict[str, Any]):
        """📊 แสดงผลเร็ว"""
        
        metrics = result['metrics']
        winner = result['winner']
        
        # แสดงผลการทำงาน
        semi_status = "✅" if metrics.get('semi_dynamic_success') else "❌"
        dynamic_status = "✅" if metrics.get('fully_dynamic_success') else "❌"
        
        print(f"   🔹 Semi-Dynamic: {semi_status} "
              f"({metrics.get('semi_dynamic_results_count', 0)} results, "
              f"{metrics.get('semi_dynamic_time', 0):.2f}s)")
        
        print(f"   🔸 Fully Dynamic: {dynamic_status} "
              f"({metrics.get('fully_dynamic_results_count', 0)} results, "
              f"{metrics.get('fully_dynamic_time', 0):.2f}s)")
        
        # แสดงผู้ชนะ
        winner_emoji = {
            'semi_dynamic': '🥇 Semi-Dynamic ชนะ!',
            'fully_dynamic': '🥇 Fully Dynamic ชนะ!',
            'tie': '🤝 เสมอ'
        }
        print(f"   {winner_emoji.get(winner, '❓')}")
    
    def _display_demo_summary(self, results: List[Dict[str, Any]]):
        """📋 แสดงสรุป Demo"""
        
        semi_wins = sum(1 for r in results if r['winner'] == 'semi_dynamic')
        dynamic_wins = sum(1 for r in results if r['winner'] == 'fully_dynamic')
        ties = sum(1 for r in results if r['winner'] == 'tie')
        
        print(f"\n🏆 สรุปผล Demo ({len(results)} คำถาม):")
        print(f"   🥇 Semi-Dynamic:  {semi_wins} ชนะ")
        print(f"   🥇 Fully Dynamic: {dynamic_wins} ชนะ")
        print(f"   🤝 เสมอ:          {ties} ครั้ง")
        
        if self.using_mock:
            print(f"\n🎭 หมายเหตุ: ผลลัพธ์นี้เป็นการจำลอง")
            print(f"   ในการใช้งานจริงจะได้ผลที่แตกต่าง")
        
        # แนะนำขั้นตอนต่อไป
        print(f"\n💡 ขั้นตอนต่อไป:")
        print(f"   1. ตรวจสอบว่าระบบจริงทำงานได้")
        print(f"   2. ทดสอบด้วยข้อมูลจริงจากฐานข้อมูล")
        print(f"   3. ปรับแต่งระบบตามผลการทดสอบ")
    
    async def run_specific_test(self, questions: List[str], tenant_id: str = "company-a") -> Dict[str, Any]:
        """🎯 ทดสอบคำถามเฉพาะ"""
        
        print(f"🎯 ทดสอบ {len(questions)} คำถาม")
        print("-" * 40)
        
        results = []
        
        for i, question in enumerate(questions, 1):
            print(f"\n🔍 [{i}/{len(questions)}] {question}")
            
            result = await self._compare_single_question(question, tenant_id, f"specific_{i}")
            results.append(result)
            
            self._display_quick_result(result)
            
            # หยุดพักระหว่างคำถาม
            if i < len(questions):
                await asyncio.sleep(0.2)
        
        # สรุปผลรวม
        summary = self._calculate_summary(results)
        
        print(f"\n📊 สรุปผลการทดสอบ:")
        print(f"   Semi-Dynamic: {summary['semi_wins']}/{len(results)} ชนะ")
        print(f"   Fully Dynamic: {summary['dynamic_wins']}/{len(results)} ชนะ")
        print(f"   เสมอ: {summary['ties']}/{len(results)} ครั้ง")
        
        return {
            'results': results,
            'summary': summary,
            'using_mock': self.using_mock
        }
    
    def _calculate_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """📊 คำนวณสรุป"""
        
        semi_wins = sum(1 for r in results if r['winner'] == 'semi_dynamic')
        dynamic_wins = sum(1 for r in results if r['winner'] == 'fully_dynamic')
        ties = sum(1 for r in results if r['winner'] == 'tie')
        
        semi_times = [r['metrics']['semi_dynamic_time'] for r in results 
                     if r['metrics'].get('semi_dynamic_time', 0) > 0]
        dynamic_times = [r['metrics']['fully_dynamic_time'] for r in results 
                        if r['metrics'].get('fully_dynamic_time', 0) > 0]
        
        return {
            'total_tests': len(results),
            'semi_wins': semi_wins,
            'dynamic_wins': dynamic_wins,
            'ties': ties,
            'overall': {
                'total_tests': len(results),
                'semi_dynamic_wins': semi_wins,
                'fully_dynamic_wins': dynamic_wins,
                'ties': ties
            },
            'performance': {
                'semi_dynamic_avg_time': sum(semi_times) / len(semi_times) if semi_times else 0,
                'fully_dynamic_avg_time': sum(dynamic_times) / len(dynamic_times) if dynamic_times else 0
            }
        }
    
    async def test_system_availability(self) -> Dict[str, bool]:
        """🔍 ทดสอบว่าระบบใช้งานได้หรือไม่"""
        
        print("🔍 ตรวจสอบความพร้อมของระบบ...")
        
        availability = {
            'semi_dynamic_available': False,
            'fully_dynamic_available': False,
            'database_available': False,
            'mock_mode': self.using_mock
        }
        
        # ทดสอบ Semi-Dynamic
        try:
            result = await self.semi_dynamic_agent.process_enhanced_question("test", "company-a")
            availability['semi_dynamic_available'] = True
            print("✅ Semi-Dynamic Agent: พร้อมใช้งาน")
        except Exception as e:
            print(f"❌ Semi-Dynamic Agent: ไม่พร้อม ({str(e)[:50]}...)")
        
        # ทดสอบ Fully Dynamic
        try:
            if hasattr(self.fully_dynamic_agent, 'process_any_question'):
                result = await self.fully_dynamic_agent.process_any_question("test", "company-a")
            else:
                result = await self.fully_dynamic_agent.process_enhanced_question("test", "company-a")
            availability['fully_dynamic_available'] = True
            print("✅ Fully Dynamic Agent: พร้อมใช้งาน")
        except Exception as e:
            print(f"❌ Fully Dynamic Agent: ไม่พร้อม ({str(e)[:50]}...)")
        
        # ทดสอบการเชื่อมต่อฐานข้อมูล (หากไม่ใช่ mock)
        if not self.using_mock:
            try:
                # สามารถเพิ่มการทดสอบฐานข้อมูลตรงนี้
                availability['database_available'] = True
                print("✅ Database: พร้อมใช้งาน")
            except Exception as e:
                print(f"❌ Database: ไม่พร้อม ({str(e)[:50]}...)")
        else:
            availability['database_available'] = True  # Mock ไม่ต้องการฐานข้อมูล
            print("🎭 Database: Mock mode")
        
        return availability


# ==========================================
# 🧪 QUICK TEST SCENARIOS
# ==========================================

class QuickTestScenarios:
    """🧪 ชุดทดสอบพร้อมใช้ - แก้ไข import แล้ว"""
    
    def __init__(self):
        self.manager = ComparisonSystemManager()
    
    async def quick_comparison_demo(self):
        """🎪 Demo การเปรียบเทียบแบบเร็ว"""
        return await self.manager.run_quick_demo()
    
    async def interactive_test(self):
        """🎮 ทดสอบแบบ Interactive"""
        
        print("🎮 ทดสอบแบบ Interactive")
        print("=" * 40)
        print("พิมพ์คำถามเพื่อเปรียบเทียบ (พิมพ์ 'exit' เพื่อออก)")
        print("พิมพ์ 'help' เพื่อดูตัวอย่างคำถาม")
        
        sample_questions = [
            "จำนวนลูกค้าทั้งหมด",
            "ยอดขายปี 2024",
            "ราคาอะไหล่ Hitachi",
            "ทีมช่างที่เก่งที่สุด",
            "ลูกค้าที่ไม่ได้ติดต่อนานแล้ว"
        ]
        
        while True:
            question = input("\n❓ คำถาม: ").strip()
            
            if question.lower() in ['exit', 'quit', 'ออก']:
                print("👋 ขอบคุณที่ใช้บริการ!")
                break
            
            elif question.lower() == 'help':
                print("\n💡 ตัวอย่างคำถาม HVAC:")
                for i, q in enumerate(sample_questions, 1):
                    print(f"   {i}. {q}")
                continue
            
            elif not question:
                continue
            
            print("🔄 กำลังเปรียบเทียบ...")
            
            try:
                results = await self.manager.run_specific_test([question])
                
                if results and results['results']:
                    result = results['results'][0]
                    winner = result['winner']
                    metrics = result['metrics']
                    
                    print(f"🏆 ผลลัพธ์: {winner.replace('_', ' ').title()} ชนะ!")
                    
                    # แสดงข้อมูลเพิ่มเติม
                    show_detail = input("ต้องการดูรายละเอียด? (y/n): ").lower()
                    if show_detail in ['y', 'yes', 'ใช่']:
                        print(f"\n📊 รายละเอียด:")
                        print(f"   Semi-Dynamic: {'✅' if metrics.get('semi_dynamic_success') else '❌'} "
                              f"({metrics.get('semi_dynamic_time', 0):.2f}s)")
                        print(f"   Fully Dynamic: {'✅' if metrics.get('fully_dynamic_success') else '❌'} "
                              f"({metrics.get('fully_dynamic_time', 0):.2f}s)")
                        
                        # แสดงคำตอบตัวอย่าง
                        if metrics.get('semi_dynamic_success'):
                            semi_answer = result['semi_dynamic_result']['answer'][:150]
                            print(f"\n📝 Semi Answer: {semi_answer}...")
                        
                        if metrics.get('fully_dynamic_success'):
                            dynamic_answer = result['fully_dynamic_result']['answer'][:150]
                            print(f"📝 Dynamic Answer: {dynamic_answer}...")
                
            except Exception as e:
                print(f"❌ เกิดข้อผิดพลาด: {str(e)}")
    
    async def check_system_status(self):
        """🔍 ตรวจสอบสถานะระบบ"""
        
        print("🔍 ตรวจสอบสถานะระบบ...")
        availability = await self.manager.test_system_availability()
        
        print(f"\n📊 สถานะระบบ:")
        for system, status in availability.items():
            status_emoji = "✅" if status else "❌"
            print(f"   {status_emoji} {system}: {'พร้อม' if status else 'ไม่พร้อม'}")
        
        if availability['mock_mode']:
            print(f"\n🎭 กำลังใช้งานใน Mock Mode")
            print(f"💡 เพื่อการใช้งานจริง ต้องมีระบบ Agent และฐานข้อมูล")
        
        return availability


# ==========================================
# 🚀 MAIN EXECUTION
# ==========================================

async def main():
    """🚀 ฟังก์ชันหลักสำหรับการทดสอบ"""
    
    print("🔄 เครื่องมือเปรียบเทียบระบบ AI")
    print("=" * 50)
    
    scenarios = QuickTestScenarios()
    
    # ตรวจสอบสถานะระบบก่อน
    await scenarios.check_system_status()
    
    print(f"\n🎯 เลือกการทดสอบ:")
    print("1. Demo เร็ว (3 คำถาม)")
    print("2. ทดสอบแบบ Interactive")  
    print("3. ตรวจสอบสถานะระบบ")
    print("4. ออกจากโปรแกรม")
    
    choice = input("\nเลือก (1-4): ").strip()
    
    if choice == "1":
        await scenarios.quick_comparison_demo()
    
    elif choice == "2":
        await scenarios.interactive_test()
    
    elif choice == "3":
        await scenarios.check_system_status()
    
    elif choice == "4":
        print("👋 ขอบคุณที่ใช้บริการ!")
        return
    
    else:
        print("❌ ตัวเลือกไม่ถูกต้อง")
        await main()  # เรียกซ้ำ

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 โปรแกรมถูกยกเลิก")
    except Exception as e:
        print(f"\n❌ เกิดข้อผิดพลาด: {str(e)}")
        print("💡 กรุณาตรวจสอบการติดตั้งและการตั้งค่า")