import os
import re
import json
import asyncio
import time
import logging
import hashlib
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, date, timedelta
from decimal import Decimal
from collections import deque, defaultdict
import aiohttp
import psycopg2
from textwrap import dedent
from psycopg2.extras import RealDictCursor
from collections import Counter, defaultdict
logger = logging.getLogger(__name__)

class SimplifiedOllamaClient:
    """
    Client สำหรับเชื่อมต่อกับ Ollama API
    """
    def __init__(self):
        # ใช้ IP จริงของ Ollama server แทน host.docker.internal
        self.base_url = os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434')
        self.timeout = 120
        logger.info(f"🔗 Ollama client configured with: {self.base_url}")
    
    async def generate(self, prompt: str, model: str) -> str:
        """ส่ง prompt ไปยัง Ollama และรับคำตอบกลับมา"""
        payload = {
            'model': model,
            'prompt': prompt,
            'stream': False,
            'temperature': 0.1,  # ต่ำเพื่อความแม่นยำ
            'top_p': 0.9,
            'max_tokens': 500
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get('response', '')
                    else:
                        logger.error(f"Ollama API error: {response.status}")
                        return ""
        except asyncio.TimeoutError:
            logger.error("Ollama request timeout")
            return ""
        except Exception as e:
            logger.error(f"Ollama request failed: {e}")
            return ""
    
    async def test_connection(self) -> bool:
        """ทดสอบการเชื่อมต่อกับ Ollama server"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}/api/tags",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        models = data.get('models', [])
                        logger.info(f"✅ Ollama connected. Available models: {len(models)}")
                        for model in models[:3]:  # แสดงแค่ 3 models แรก
                            logger.info(f"   - {model.get('name', 'unknown')}")
                        return True
                    else:
                        logger.error(f"❌ Ollama connection failed: HTTP {response.status}")
                        return False
        except Exception as e:
            logger.error(f"❌ Cannot connect to Ollama at {self.base_url}: {e}")
            return False
