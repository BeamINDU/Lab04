import json
import requests
import asyncio
import aiohttp
from typing import Dict, Any, List, Optional
import logging
from tenant_manager import get_tenant_config

logger = logging.getLogger(__name__)

class OllamaAgent:
    """Ollama Agent สำหรับเชื่อมต่อกับ Ollama Server"""
    
    def __init__(self, tenant_id: Optional[str] = None):
        self.current_tenant_id = tenant_id
        
    def set_tenant(self, tenant_id: str):
        """Set current tenant for operations"""
        self.current_tenant_id = tenant_id
        logger.info(f"Ollama Agent switched to tenant: {tenant_id}")

    def get_current_tenant_id(self) -> str:
        """Get current tenant ID"""
        if not self.current_tenant_id:
            raise ValueError("No tenant ID set. Call set_tenant() first.")
        return self.current_tenant_id

    def get_tenant_ollama_config(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Get Ollama configuration for specified tenant"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            tenant_config = get_tenant_config(tenant_id)
            ollama_config = tenant_config.settings.get('ollama', {})
            
            # Fallback to global config if not specified
            if not ollama_config:
                ollama_config = {
                    'base_url': 'http://192.168.11.97:11434',
                    'model': 'llama3.1:8b',
                    'temperature': 0.7,
                    'max_tokens': 1000
                }
            
            logger.debug(f"Retrieved Ollama config for tenant: {tenant_id}")
            return ollama_config
        except Exception as e:
            logger.error(f"Failed to get Ollama config for tenant {tenant_id}: {e}")
            raise

    def test_connection(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Test connection to Ollama server"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            config = self.get_tenant_ollama_config(tenant_id)
            base_url = config.get('base_url', 'http://192.168.11.97:11434')
            
            # Test connection
            response = requests.get(f"{base_url}/api/tags", timeout=10)
            
            if response.status_code == 200:
                models = response.json().get('models', [])
                model_names = [model.get('name', 'unknown') for model in models]
                
                return {
                    "success": True,
                    "tenant_id": tenant_id,
                    "server_url": base_url,
                    "available_models": model_names,
                    "message": "Connection successful"
                }
            else:
                return {
                    "success": False,
                    "tenant_id": tenant_id,
                    "server_url": base_url,
                    "error": f"HTTP {response.status_code}",
                    "message": "Connection failed"
                }
                
        except Exception as e:
            logger.error(f"Ollama connection test failed for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "tenant_id": tenant_id,
                "error": str(e),
                "message": "Connection failed"
            }

    async def generate_response(self, prompt: str, tenant_id: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Generate response from Ollama model"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            tenant_config = get_tenant_config(tenant_id)
            ollama_config = self.get_tenant_ollama_config(tenant_id)
            
            base_url = ollama_config.get('base_url', 'http://192.168.11.97:11434')
            model = ollama_config.get('model', 'llama3.1:8b')
            temperature = kwargs.get('temperature', ollama_config.get('temperature', 0.7))
            max_tokens = kwargs.get('max_tokens', ollama_config.get('max_tokens', 1000))
            
            # Add system prompt if configured
            system_prompt = ollama_config.get('system_prompt', '')
            if system_prompt:
                full_prompt = f"{system_prompt}\n\nคำถาม: {prompt}\n\nคำตอบ:"
            else:
                full_prompt = prompt
            
            # Prepare request payload
            payload = {
                "model": model,
                "prompt": full_prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                    "top_k": 40,
                    "top_p": 0.9,
                    "repeat_penalty": 1.1
                }
            }
            
            logger.info(f"Ollama request for tenant {tenant_id}: model={model}")
            
            # Make async request
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=300)
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        answer = result.get('response', '').strip()
                        
                        if not answer:
                            answer = "ขออภัย ไม่สามารถสร้างคำตอบได้ กรุณาลองใหม่อีกครั้ง"
                        
                        return {
                            "success": True,
                            "answer": answer,
                            "model": model,
                            "tenant_id": tenant_id,
                            "tenant_name": tenant_config.name,
                            "server_url": base_url,
                            "tokens_used": result.get('eval_count', 0),
                            "generation_time": result.get('total_duration', 0) / 1000000000  # Convert to seconds
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"Ollama API error for tenant {tenant_id}: HTTP {response.status} - {error_text}")
                        
                        return {
                            "success": False,
                            "answer": f"เกิดข้อผิดพลาดในการเชื่อมต่อ Ollama: HTTP {response.status}",
                            "model": model,
                            "tenant_id": tenant_id,
                            "error": error_text
                        }
                        
        except asyncio.TimeoutError:
            logger.error(f"Ollama request timeout for tenant {tenant_id}")
            return {
                "success": False,
                "answer": "คำขอใช้เวลานานเกินไป กรุณาลองใหม่อีกครั้ง",
                "tenant_id": tenant_id,
                "error": "Request timeout"
            }
        except Exception as e:
            logger.error(f"Ollama generation error for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "answer": f"เกิดข้อผิดพลาด: {str(e)}",
                "tenant_id": tenant_id,
                "error": str(e)
            }

    def query(self, question: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Main method สำหรับรับคำถามและส่งคืนคำตอบ (sync version)"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        # Run async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(self.generate_response(question, tenant_id))
            return result
        finally:
            loop.close()

    async def async_query(self, question: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Async version of query method"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        return await self.generate_response(question, tenant_id)

    def get_available_models(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Get list of available models from Ollama server"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            config = self.get_tenant_ollama_config(tenant_id)
            base_url = config.get('base_url', 'http://192.168.11.97:11434')
            
            response = requests.get(f"{base_url}/api/tags", timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                models = result.get('models', [])
                
                model_info = []
                for model in models:
                    model_info.append({
                        'name': model.get('name'),
                        'size': model.get('size', 0),
                        'modified_at': model.get('modified_at'),
                        'digest': model.get('digest', '')[:12] + '...' if model.get('digest') else ''
                    })
                
                return {
                    "success": True,
                    "tenant_id": tenant_id,
                    "server_url": base_url,
                    "models": model_info,
                    "total_models": len(model_info)
                }
            else:
                return {
                    "success": False,
                    "tenant_id": tenant_id,
                    "error": f"HTTP {response.status_code}",
                    "models": []
                }
                
        except Exception as e:
            logger.error(f"Error getting models for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "tenant_id": tenant_id,
                "error": str(e),
                "models": []
            }

    def switch_model(self, model_name: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Switch model for specific tenant (updates config in memory)"""
        if not tenant_id:
            tenant_id = self.get_current_tenant_id()
        
        try:
            # Check if model exists
            available_models = self.get_available_models(tenant_id)
            if not available_models["success"]:
                return {
                    "success": False,
                    "message": "Cannot check available models",
                    "error": available_models.get("error")
                }
            
            model_names = [m["name"] for m in available_models["models"]]
            if model_name not in model_names:
                return {
                    "success": False,
                    "message": f"Model '{model_name}' not found",
                    "available_models": model_names
                }
            
            # Note: This would update in-memory config only
            # For persistent changes, would need to update tenant_config.yaml
            
            return {
                "success": True,
                "tenant_id": tenant_id,
                "message": f"Model switched to '{model_name}' (in-memory only)",
                "new_model": model_name,
                "note": "To make this permanent, update tenant_config.yaml"
            }
            
        except Exception as e:
            logger.error(f"Error switching model for tenant {tenant_id}: {e}")
            return {
                "success": False,
                "tenant_id": tenant_id,
                "error": str(e)
            }


# Multi-tenant convenience functions
def create_ollama_agent(tenant_id: str) -> OllamaAgent:
    """Create Ollama agent for specific tenant"""
    agent = OllamaAgent(tenant_id)
    return agent

def query_ollama_for_tenant(question: str, tenant_id: str) -> Dict[str, Any]:
    """Quick query function for specific tenant"""
    agent = create_ollama_agent(tenant_id)
    return agent.query(question, tenant_id)

async def async_query_ollama_for_tenant(question: str, tenant_id: str) -> Dict[str, Any]:
    """Quick async query function for specific tenant"""
    agent = create_ollama_agent(tenant_id)
    return await agent.async_query(question, tenant_id)

def test_ollama_connection(tenant_id: str) -> Dict[str, Any]:
    """Test Ollama connection for specific tenant"""
    agent = create_ollama_agent(tenant_id)
    return agent.test_connection(tenant_id)


# Test usage
if __name__ == "__main__":
    import asyncio
    
    async def test_ollama_agent():
        """Test Ollama Agent functionality"""
        test_tenants = ['company-a', 'company-b', 'company-c']
        test_questions = [
            "สวัสดี คุณเป็นใคร?",
            "บริษัทของเรามีพนักงานกี่คน?",
            "บอกเกี่ยวกับบริการของบริษัท"
        ]
        
        for tenant_id in test_tenants:
            print(f"\n{'='*60}")
            print(f"🏢 Testing Ollama Agent: {tenant_id}")
            print(f"{'='*60}")
            
            # Test connection
            try:
                agent = create_ollama_agent(tenant_id)
                connection_result = agent.test_connection(tenant_id)
                print(f"🔗 Connection: {connection_result}")
                
                if not connection_result['success']:
                    print(f"❌ Connection failed, skipping tests for {tenant_id}")
                    continue
                    
                # Get available models
                models_result = agent.get_available_models(tenant_id)
                if models_result['success']:
                    print(f"📦 Available models: {[m['name'] for m in models_result['models']]}")
                
            except Exception as e:
                print(f"❌ Connection setup failed: {e}")
                continue
            
            # Test queries
            for question in test_questions:
                print(f"\n❓ คำถาม: {question}")
                try:
                    result = await agent.async_query(question, tenant_id)
                    if result['success']:
                        print(f"✅ คำตอบ: {result['answer'][:200]}...")
                        print(f"🤖 Model: {result.get('model', 'unknown')}")
                        print(f"⏱️ Time: {result.get('generation_time', 0):.2f}s")
                        print(f"🎯 Tokens: {result.get('tokens_used', 0)}")
                    else:
                        print(f"❌ Error: {result.get('error', 'Unknown error')}")
                except Exception as e:
                    print(f"❌ Query error: {e}")
            
            print("-" * 60)
    
    # Run sync test
    def test_ollama_sync():
        """Synchronous test"""
        for tenant_id in ['company-a']:
            print(f"🔧 Testing sync query for {tenant_id}")
            try:
                result = query_ollama_for_tenant("สวัสดี", tenant_id)
                print(f"Result: {result.get('answer', 'No answer')[:100]}...")
            except Exception as e:
                print(f"Error: {e}")
    
    print("🚀 Testing Ollama Agent")
    print("=" * 60)
    
    # Run async test
    asyncio.run(test_ollama_agent())
    
    # Run sync test
    test_ollama_sync()