import requests
import json
import time
import asyncio
import aiohttp
from typing import Dict, Any, List

class OllamaConnectionTester:
    def __init__(self, base_url: str = "http://192.168.11.97:12434"):
        self.base_url = base_url
        self.model = "llama3.1:8b"
    
    def test_server_availability(self) -> Dict[str, Any]:
        """ทดสอบว่า Ollama server พร้อมใช้งานหรือไม่"""
        print(f"🔍 Testing Ollama server availability at {self.base_url}")
        
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                models = data.get('models', [])
                model_names = [model.get('name', 'unknown') for model in models]
                
                print(f"✅ Server is available!")
                print(f"📦 Available models: {model_names}")
                
                # Check if our target model exists
                if self.model in model_names:
                    print(f"✅ Target model '{self.model}' is available")
                else:
                    print(f"⚠️  Target model '{self.model}' not found!")
                
                return {
                    "success": True,
                    "models": model_names,
                    "target_model_available": self.model in model_names
                }
            else:
                print(f"❌ Server responded with HTTP {response.status_code}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Connection failed: {e}")
            return {"success": False, "error": str(e)}
    
    def test_simple_query(self, prompt: str = "สวัสดี คุณเป็นใคร?") -> Dict[str, Any]:
        """ทดสอบการส่ง query ธรรมดา"""
        print(f"\n🧪 Testing simple query: '{prompt}'")
        
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.7,
                "num_predict": 100
            }
        }
        
        try:
            start_time = time.time()
            response = requests.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=30
            )
            end_time = time.time()
            
            if response.status_code == 200:
                data = response.json()
                answer = data.get('response', '').strip()
                duration = end_time - start_time
                
                print(f"✅ Query successful!")
                print(f"📝 Response: {answer}")
                print(f"⏱️  Duration: {duration:.2f}s")
                print(f"🎯 Tokens: {data.get('eval_count', 0)}")
                
                return {
                    "success": True,
                    "response": answer,
                    "duration": duration,
                    "tokens": data.get('eval_count', 0)
                }
            else:
                print(f"❌ Query failed: HTTP {response.status_code}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
                
        except Exception as e:
            print(f"❌ Query error: {e}")
            return {"success": False, "error": str(e)}
    
    def test_siamtech_context(self) -> Dict[str, Any]:
        """ทดสอบด้วยบริบทของ SiamTech"""
        print(f"\n🏢 Testing SiamTech context query")
        
        prompt = """คุณเป็น AI Assistant ของบริษัท สยามเทค จำกัด สำนักงานใหญ่ กรุงเทพมหานคร

บริษัทมีพนักงาน 15 คน แบ่งเป็น:
- แผนก IT: 10 คน
- แผนก Sales: 3 คน  
- ผู้บริหาร: 2 คน

คำถาม: บริษัทเรามีพนักงานกี่คน?
ตอบ:"""
        
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.7,
                "num_predict": 150
            }
        }
        
        try:
            start_time = time.time()
            response = requests.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=30
            )
            end_time = time.time()
            
            if response.status_code == 200:
                data = response.json()
                answer = data.get('response', '').strip()
                duration = end_time - start_time
                
                print(f"✅ SiamTech context test successful!")
                print(f"📝 Response: {answer}")
                print(f"⏱️  Duration: {duration:.2f}s")
                
                return {
                    "success": True,
                    "response": answer,
                    "duration": duration
                }
            else:
                print(f"❌ SiamTech context test failed: HTTP {response.status_code}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
                
        except Exception as e:
            print(f"❌ SiamTech context test error: {e}")
            return {"success": False, "error": str(e)}
    
    async def test_async_connection(self) -> Dict[str, Any]:
        """ทดสอบการเชื่อมต่อแบบ async (สำหรับใช้ใน production)"""
        print(f"\n🔄 Testing async connection")
        
        payload = {
            "model": self.model,
            "prompt": "ทดสอบการเชื่อมต่อแบบ async",
            "stream": False,
            "options": {
                "temperature": 0.7,
                "num_predict": 50
            }
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                start_time = time.time()
                async with session.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    end_time = time.time()
                    
                    if response.status == 200:
                        data = await response.json()
                        answer = data.get('response', '').strip()
                        duration = end_time - start_time
                        
                        print(f"✅ Async connection successful!")
                        print(f"📝 Response: {answer}")
                        print(f"⏱️  Duration: {duration:.2f}s")
                        
                        return {
                            "success": True,
                            "response": answer,
                            "duration": duration
                        }
                    else:
                        print(f"❌ Async connection failed: HTTP {response.status}")
                        return {"success": False, "error": f"HTTP {response.status}"}
                        
        except Exception as e:
            print(f"❌ Async connection error: {e}")
            return {"success": False, "error": str(e)}
    
    def test_multi_tenant_scenarios(self) -> Dict[str, Any]:
        """ทดสอบสถานการณ์ multi-tenant"""
        print(f"\n🏢 Testing multi-tenant scenarios")
        
        scenarios = [
            {
                "tenant": "company-a",
                "name": "SiamTech Bangkok HQ",
                "prompt": "คุณเป็น AI Assistant ของ SiamTech สำนักงานใหญ่ กรุงเทพฯ มีพนักงาน 15 คน\nคำถาม: บริบทบริษัทเป็นอย่างไร?"
            },
            {
                "tenant": "company-b", 
                "name": "SiamTech Chiang Mai",
                "prompt": "คุณเป็น AI Assistant ของ SiamTech สาขาเชียงใหม่ เน้นงานท่องเที่ยว มีพนักงาน 10 คน\nคำถาม: สาขานี้ทำงานอะไร?"
            },
            {
                "tenant": "company-c",
                "name": "SiamTech International", 
                "prompt": "You are AI Assistant for SiamTech International division with 8 employees focusing on global projects.\nQuestion: What does this office do?"
            }
        ]
        
        results = {}
        
        for scenario in scenarios:
            print(f"\n--- Testing {scenario['name']} ---")
            
            payload = {
                "model": self.model,
                "prompt": scenario['prompt'],
                "stream": False,
                "options": {
                    "temperature": 0.7,
                    "num_predict": 200
                }
            }
            
            try:
                response = requests.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    answer = data.get('response', '').strip()
                    
                    print(f"✅ {scenario['name']}: Success")
                    print(f"📝 Response: {answer[:100]}...")
                    
                    results[scenario['tenant']] = {
                        "success": True,
                        "response": answer
                    }
                else:
                    print(f"❌ {scenario['name']}: Failed")
                    results[scenario['tenant']] = {
                        "success": False,
                        "error": f"HTTP {response.status_code}"
                    }
                    
            except Exception as e:
                print(f"❌ {scenario['name']}: Error - {e}")
                results[scenario['tenant']] = {
                    "success": False,
                    "error": str(e)
                }
        
        return results
    
    def run_full_test_suite(self):
        """รัน test ครบชุด"""
        print("🚀 SiamTech Multi-Tenant Ollama Connection Test")
        print("=" * 60)
        
        # Test 1: Server availability
        server_test = self.test_server_availability()
        if not server_test["success"]:
            print("💥 Server not available, stopping tests")
            return
        
        # Test 2: Simple query
        simple_test = self.test_simple_query()
        
        # Test 3: SiamTech context
        context_test = self.test_siamtech_context()
        
        # Test 4: Async connection
        print("\n🔄 Running async test...")
        async_test = asyncio.run(self.test_async_connection())
        
        # Test 5: Multi-tenant scenarios
        tenant_tests = self.test_multi_tenant_scenarios()
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 TEST SUMMARY")
        print("=" * 60)
        
        tests = [
            ("Server Availability", server_test["success"]),
            ("Simple Query", simple_test["success"]),
            ("SiamTech Context", context_test["success"]),
            ("Async Connection", async_test["success"])
        ]
        
        for test_name, success in tests:
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"{test_name:20} {status}")
        
        # Tenant tests
        print("\nTenant-specific tests:")
        for tenant, result in tenant_tests.items():
            status = "✅ PASS" if result["success"] else "❌ FAIL"
            print(f"  {tenant:15} {status}")
        
        # Overall status
        all_basic_pass = all(test[1] for test in tests)
        all_tenant_pass = all(result["success"] for result in tenant_tests.values())
        
        if all_basic_pass and all_tenant_pass:
            print(f"\n🎉 ALL TESTS PASSED! Ollama server is ready for SiamTech Multi-Tenant AI")
            print(f"🔗 Server: {self.base_url}")
            print(f"🤖 Model: {self.model}")
        else:
            print(f"\n⚠️  Some tests failed. Please check configuration.")
        
        return {
            "overall_success": all_basic_pass and all_tenant_pass,
            "server_test": server_test,
            "simple_test": simple_test,
            "context_test": context_test,
            "async_test": async_test,
            "tenant_tests": tenant_tests
        }


def main():
    """Main function to run tests"""
    tester = OllamaConnectionTester()
    results = tester.run_full_test_suite()
    
    # Return exit code based on results
    if results["overall_success"]:
        exit(0)
    else:
        exit(1)


if __name__ == "__main__":
    main()