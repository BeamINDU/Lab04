# quick_test_scenarios.py
# 🧪 ชุดทดสอบพร้อมใช้สำหรับเปรียบเทียบระบบ

import asyncio
from comparison_system_manager import ComparisonSystemManager

class QuickTestScenarios:
    """🧪 ชุดทดสอบต่างๆ สำหรับเปรียบเทียบระบบ"""
    
    def __init__(self):
        self.manager = ComparisonSystemManager()
        
        # ชุดทดสอบต่างๆ
        self.test_scenarios = {
            'daily_operations': {
                'name': '📋 การใช้งานประจำวัน',
                'questions': [
                    "จำนวนลูกค้าทั้งหมด",
                    "ยอดขายเดือนนี้",
                    "รายชื่อทีมช่าง", 
                    "แผนงานวันนี้",
                    "อะไหล่ที่หมดสต็อก"
                ]
            },
            
            'business_intelligence': {
                'name': '📊 การวิเคราะห์ทางธุรกิจ',
                'questions': [
                    "ลูกค้าที่ใช้บริการมากที่สุด 5 อันดับ",
                    "เปรียบเทียบยอดขาย PM กับ Overhaul",
                    "ทีมช่างไหนมีประสิทธิภาพสูงสุด",
                    "แนวโน้มการขายในแต่ละเดือน",
                    "อัตราการกลับมาใช้บริการของลูกค้า"
                ]
            },
            
            'inventory_management': {
                'name': '📦 การจัดการสินค้าคงคลัง',
                'questions': [
                    "อะไหล่ที่ราคาสูงที่สุด 10 อันดับ",
                    "อะไหล่ Hitachi ทั้งหมด",
                    "สต็อกที่ต่ำกว่า 5 ชิ้น",
                    "อะไหล่ที่ไม่เคยใช้มากกว่า 6 เดือน",
                    "มูลค่าสินค้าคงคลังทั้งหมด"
                ]
            },
            
            'customer_relationship': {
                'name': '👥 การจัดการลูกค้าสัมพันธ์',
                'questions': [
                    "ลูกค้าใหม่ในเดือนนี้",
                    "ลูกค้าที่ไม่ได้ใช้บริการมากกว่า 3 เดือน",
                    "ลูกค้าที่มีค่าใช้จ่ายสูงที่สุด",
                    "ประวัติการใช้บริการของ บริษัท ABC",
                    "ลูกค้าที่มีศักยภาพสูง"
                ]
            },
            
            'complex_queries': {
                'name': '🧩 คำถามซับซ้อน',
                'questions': [
                    "ลูกค้าที่ใช้บริการ Hitachi แต่ไม่เคยซื้ออะไหล่ Hitachi",
                    "ความสัมพันธ์ระหว่างราคาบริการกับความพึงพอใจของลูกค้า",
                    "คาดการณ์ความต้องการอะไหล่ใน 3 เดือนข้างหน้า",
                    "ROI ของการลงทุนในแต่ละประเภทบริการ",
                    "เหตุผลที่ลูกค้าบางรายหยุดใช้บริการ"
                ]
            },
            
            'edge_cases': {
                'name': '🔍 กรณีพิเศษ',
                'questions': [
                    "ลูกค้าที่ชื่อเหมือนกับชื่อแบรนด์",
                    "งานที่ใช้เวลานานที่สุด",
                    "อะไหล่ที่ชื่อมีตัวเลข",
                    "ทีมช่างที่ทำงานในวันหยุด",
                    "ความผิดพลาดที่เกิดขึ้นบ่อยที่สุด"
                ]
            },
            
            'performance_stress': {
                'name': '⚡ ทดสอบความเร็ว',
                'questions': [
                    "SELECT * FROM sales2024 LIMIT 1",
                    "COUNT(*) จากทุกตาราง",
                    "ข้อมูลตัวอย่างจากทุกตาราง",
                    "สถิติพื้นฐานทั้งหมด",
                    "ตรวจสอบการเชื่อมต่อฐานข้อมูล"
                ]
            }
        }
    
    async def run_scenario(self, scenario_name: str, tenant_id: str = "company-a"):
        """🎯 รันทดสอบแบบ scenario เฉพาะ"""
        
        if scenario_name not in self.test_scenarios:
            print(f"❌ ไม่พบ scenario: {scenario_name}")
            return
        
        scenario = self.test_scenarios[scenario_name]
        print(f"🚀 เริ่มทดสอบ: {scenario['name']}")
        print("=" * 60)
        
        results = await self.manager.run_specific_test(scenario['questions'], tenant_id)
        
        print(f"\n✅ เสร็จสิ้นการทดสอบ: {scenario['name']}")
        return results
    
    async def run_all_scenarios(self, tenant_id: str = "company-a"):
        """🏁 รันทดสอบทุก scenario"""
        
        print("🚀 เริ่มทดสอบทุก Scenario")
        print("=" * 80)
        
        all_results = {}
        
        for scenario_name, scenario in self.test_scenarios.items():
            print(f"\n📍 กำลังทดสอบ: {scenario['name']}")
            print("-" * 40)
            
            try:
                results = await self.run_scenario(scenario_name, tenant_id)
                all_results[scenario_name] = results
                
                # สรุปผลแต่ละ scenario
                summary = results['summary']['overall']
                print(f"   📊 สรุป: Semi({summary['semi_dynamic_wins']}) vs Dynamic({summary['fully_dynamic_wins']}) vs Tie({summary['ties']})")
                
            except Exception as e:
                print(f"   ❌ ผิดพลาด: {str(e)}")
                all_results[scenario_name] = {'error': str(e)}
            
            await asyncio.sleep(1)  # หยุดพักระหว่าง scenario
        
        # สรุปผลรวมทั้งหมด
        self._display_overall_summary(all_results)
        
        return all_results
    
    def _display_overall_summary(self, all_results: dict):
        """📊 แสดงสรุปผลรวมทั้งหมด"""
        
        print("\n" + "=" * 80)
        print("🏆 สรุปผลรวมทุก Scenario")
        print("=" * 80)
        
        total_semi_wins = 0
        total_dynamic_wins = 0
        total_ties = 0
        total_tests = 0
        
        scenario_results = []
        
        for scenario_name, result in all_results.items():
            if 'error' in result:
                continue
                
            scenario_info = self.test_scenarios[scenario_name]
            summary = result['summary']['overall']
            
            scenario_results.append({
                'name': scenario_info['name'],
                'semi_wins': summary['semi_dynamic_wins'],
                'dynamic_wins': summary['fully_dynamic_wins'], 
                'ties': summary['ties'],
                'total': summary['total_tests']
            })
            
            total_semi_wins += summary['semi_dynamic_wins']
            total_dynamic_wins += summary['fully_dynamic_wins']
            total_ties += summary['ties']
            total_tests += summary['total_tests']
        
        # แสดงผลแต่ละ scenario
        print(f"\n📋 ผลตาม Scenario:")
        for result in scenario_results:
            print(f"   {result['name']}")
            print(f"     Semi: {result['semi_wins']}/{result['total']} "
                  f"Dynamic: {result['dynamic_wins']}/{result['total']} "
                  f"Tie: {result['ties']}/{result['total']}")
        
        # สรุปรวม
        print(f"\n🏆 สรุปรวมทั้งหมด ({total_tests} คำถาม):")
        print(f"   🥇 Semi-Dynamic:   {total_semi_wins} ชนะ ({total_semi_wins/total_tests:.1%})")
        print(f"   🥇 Fully Dynamic:  {total_dynamic_wins} ชนะ ({total_dynamic_wins/total_tests:.1%})")
        print(f"   🤝 เสมอ:           {total_ties} ครั้ง ({total_ties/total_tests:.1%})")
        
        # แนะนำการใช้งาน
        print(f"\n💡 คำแนะนำการใช้งาน:")
        
        if total_dynamic_wins > total_semi_wins * 1.5:
            print("   ✅ แนะนำใช้ Fully Dynamic เป็นหลัก")
            print("   📝 เหตุผล: ตอบคำถามได้หลากหลายและซับซ้อนกว่า")
        elif total_semi_wins > total_dynamic_wins * 1.2:
            print("   ✅ แนะนำใช้ Semi-Dynamic เป็นหลัก") 
            print("   📝 เหตุผล: เร็วและเสถียรกว่าสำหรับงานประจำ")
        else:
            print("   ✅ แนะนำใช้แบบ Hybrid")
            print("   📝 เหตุผล: Semi-Dynamic สำหรับงานประจำ, Fully Dynamic สำหรับวิเคราะห์")
        
        print("=" * 80)
    
    async def quick_comparison_demo(self):
        """🎪 Demo การเปรียบเทียบแบบเร็ว"""
        
        print("🎪 DEMO เปรียบเทียบ Semi-Dynamic vs Fully Dynamic")
        print("=" * 60)
        
        demo_questions = [
            "จำนวนลูกค้าทั้งหมด",  # คำถามง่าย
            "ลูกค้าที่ใช้บริการมากที่สุด 3 อันดับ",  # คำถามกลาง
            "ลูกค้าที่ใช้บริการ Hitachi แต่ไม่เคยซื้ออะไหล่ Hitachi"  # คำถามยาก
        ]
        
        print("🎯 ทดสอบด้วย 3 คำถามตัวอย่าง:")
        for i, q in enumerate(demo_questions, 1):
            print(f"   {i}. {q}")
        
        print("\n🚀 เริ่มทดสอบ...")
        
        results = await self.manager.run_specific_test(demo_questions)
        
        # แสดงข้อสรุป
        summary = results['summary']
        overall = summary['overall']
        
        print(f"\n🏁 ผลลัพธ์ Demo:")
        print(f"   Semi-Dynamic: {overall['semi_dynamic_wins']}/{overall['total_tests']} ชนะ")
        print(f"   Fully Dynamic: {overall['fully_dynamic_wins']}/{overall['total_tests']} ชนะ")
        
        winner = "Semi-Dynamic" if overall['semi_dynamic_wins'] > overall['fully_dynamic_wins'] else \
                "Fully Dynamic" if overall['fully_dynamic_wins'] > overall['semi_dynamic_wins'] else "เสมอ"
        
        print(f"   🏆 ผู้ชนะ: {winner}")
        
        return results
    
    async def interactive_test(self):
        """🎮 ทดสอบแบบ Interactive"""
        
        print("🎮 ทดสوบแบบ Interactive")
        print("=" * 40)
        print("พิมพ์คำถามเพื่อเปรียบเทียบ (พิมพ์ 'exit' เพื่อออก)")
        
        while True:
            question = input("\n❓ คำถาม: ").strip()
            
            if question.lower() in ['exit', 'quit', 'ออก']:
                print("👋 ขอบคุณที่ใช้บริการ!")
                break
            
            if not question:
                continue
            
            print("🔄 กำลังเปรียบเทียบ...")
            
            try:
                results = await self.manager.run_specific_test([question])
                
                # แสดงผลสรุป
                result = results['results'][0]
                winner = result['winner']
                
                print(f"🏆 ผลลัพธ์: {winner.replace('_', ' ').title()} ชนะ!")
                
                # แสดงข้อมูลเพิ่มเติมหากต้องการ
                show_detail = input("ต้องการดูรายละเอียด? (y/n): ").lower()
                if show_detail in ['y', 'yes', 'ใช่']:
                    semi = result['metrics']
                    print(f"\n📊 รายละเอียด:")
                    print(f"   Semi-Dynamic: {'✅' if semi.get('semi_dynamic_success') else '❌'} "
                          f"({semi.get('semi_dynamic_time', 0):.2f}s)")
                    print(f"   Fully Dynamic: {'✅' if semi.get('fully_dynamic_success') else '❌'} "
                          f"({semi.get('fully_dynamic_time', 0):.2f}s)")
                
            except Exception as e:
                print(f"❌ เกิดข้อผิดพลาด: {str(e)}")
    
    def list_available_scenarios(self):
        """📋 แสดงรายการ scenario ที่มี"""
        
        print("📋 Scenario ที่มีให้ทดสอบ:")
        print("-" * 40)
        
        for i, (key, scenario) in enumerate(self.test_scenarios.items(), 1):
            print(f"{i}. {scenario['name']} ({len(scenario['questions'])} คำถาม)")
            print(f"   Key: '{key}'")
        
        print("\n💡 วิธีใช้:")
        print("   await scenarios.run_scenario('daily_operations')")
        print("   await scenarios.run_all_scenarios()")


# ==========================================
# 🎯 EXECUTION EXAMPLES
# ==========================================

async def demo_quick_tests():
    """🎪 ตัวอย่างการใช้งาน"""
    
    scenarios = QuickTestScenarios()
    
    print("🎯 เลือกการทดสอบ:")
    print("1. Demo เร็ว (3 คำถาม)")
    print("2. ทดสอบ Scenario เฉพาะ")
    print("3. ทดสอบทุก Scenario")
    print("4. ทดสอบแบบ Interactive")
    print("5. ดูรายการ Scenario")
    
    choice = input("\nเลือก (1-5): ").strip()
    
    if choice == "1":
        await scenarios.quick_comparison_demo()
    
    elif choice == "2":
        scenarios.list_available_scenarios()
        scenario_key = input("\nใส่ key ของ scenario: ").strip()
        await scenarios.run_scenario(scenario_key)
    
    elif choice == "3":
        confirm = input("ทดสอบทุก scenario จะใช้เวลานาน ต้องการดำเนินต่อ? (y/n): ")
        if confirm.lower() in ['y', 'yes', 'ใช่']:
            await scenarios.run_all_scenarios()
    
    elif choice == "4":
        await scenarios.interactive_test()
    
    elif choice == "5":
        scenarios.list_available_scenarios()
    
    else:
        print("❌ ตัวเลือกไม่ถูกต้อง")


# ==========================================
# 🚀 MAIN EXECUTION
# ==========================================

if __name__ == "__main__":
    asyncio.run(demo_quick_tests())