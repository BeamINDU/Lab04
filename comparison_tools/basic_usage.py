# basic_usage.py
import sys
import os
import asyncio

# เพิ่ม path เพื่อ import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from comparison_tools import ComparisonSystemManager, QuickTestScenarios

async def basic_demo():
    """🎯 ตัวอย่างการใช้งานเบื้องต้น"""
    
    print("🚀 เริ่มการเปรียบเทียบระบบ")
    
    # สร้าง instance
    manager = ComparisonSystemManager()
    scenarios = QuickTestScenarios()
    
    # ทดสอบเร็ว
    print("1. 🎪 Demo เร็ว...")
    demo_results = await scenarios.quick_comparison_demo()
    
    # ทดสอบคำถามเฉพาะ
    print("\n2. 🎯 ทดสอบคำถามที่คุณสนใจ...")
    custom_questions = [
        "จำนวนลูกค้าทั้งหมด",
        "ราคาอะไหล่ Hitachi ทั้งหมด",
        "ลูกค้าที่ใช้บริการมากที่สุด"
    ]
    
    custom_results = await manager.run_specific_test(custom_questions)
    
    # แสดงสรุป
    print("\n📊 สรุปผลการทดสอบ:")
    overall = custom_results['summary']['overall']
    print(f"   Semi-Dynamic: {overall['semi_dynamic_wins']} ชนะ")
    print(f"   Fully Dynamic: {overall['fully_dynamic_wins']} ชนะ")

if __name__ == "__main__":
    asyncio.run(basic_demo())