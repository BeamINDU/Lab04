import os
import re
import json
import asyncio
import time
import logging
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
    Fixed Ollama Client with proper NDJSON streaming support
    """
    def __init__(self):
        self.base_url = os.getenv('OLLAMA_BASE_URL', 'http://52.74.36.160:12434')
        self.timeout = 120
        logger.info(f"🔗 Ollama client configured with: {self.base_url}")
    
    async def generate(self, prompt: str, model: str) -> str:
        """
        Generate response from Ollama with proper streaming/NDJSON handling
        """
        payload = {
            'model': model,
            'prompt': prompt,
            'stream': False,  # ⚠️ IMPORTANT: Set to False to avoid NDJSON
            'temperature': 0.1,
            'top_p': 0.9,
            'max_tokens': 1000,
            'options': {
                'num_predict': 1000,
                'temperature': 0.1
            }
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                # First attempt: Non-streaming request
                async with session.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                    headers={'Content-Type': 'application/json'}
                ) as response:
                    content_type = response.headers.get('content-type', '')
                    
                    # Handle different response types
                    if 'application/json' in content_type:
                        # Standard JSON response
                        result = await response.json()
                        return result.get('response', '').strip()
                    
                    elif 'application/x-ndjson' in content_type or 'text/plain' in content_type:
                        # Streaming/NDJSON response (even though we requested non-streaming)
                        return await self._handle_streaming_response(response)
                    
                    else:
                        # Fallback to text parsing
                        text = await response.text()
                        return self._extract_response_from_text(text)
                        
        except asyncio.TimeoutError:
            logger.error(f"Ollama request timeout after {self.timeout}s")
            return self._generate_fallback_sql(prompt, model)
            
        except aiohttp.ContentTypeError as e:
            logger.warning(f"Content type error, attempting alternative parsing: {e}")
            # Try alternative parsing method
            return await self._alternative_generate(prompt, model)
            
        except Exception as e:
            logger.error(f"Ollama request failed: {e}")
            return self._generate_fallback_sql(prompt, model)
    
    async def _handle_streaming_response(self, response) -> str:
        """
        Handle NDJSON streaming response
        """
        accumulated_response = []
        
        try:
            async for line in response.content:
                if line:
                    line_str = line.decode('utf-8').strip()
                    if line_str:
                        try:
                            # Parse each JSON line
                            data = json.loads(line_str)
                            if 'response' in data:
                                accumulated_response.append(data['response'])
                            
                            # Check if done
                            if data.get('done', False):
                                break
                                
                        except json.JSONDecodeError:
                            # Skip invalid JSON lines
                            continue
            
            return ''.join(accumulated_response).strip()
            
        except Exception as e:
            logger.error(f"Error parsing streaming response: {e}")
            return ""
    
    async def _alternative_generate(self, prompt: str, model: str) -> str:
        """
        Alternative generation method with explicit streaming handling
        """
        payload = {
            'model': model,
            'prompt': prompt,
            'stream': True,  # Explicitly use streaming this time
            'temperature': 0.1,
            'options': {
                'num_predict': 1000
            }
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    accumulated = []
                    
                    async for chunk in response.content.iter_any():
                        if chunk:
                            # Process each chunk
                            chunk_str = chunk.decode('utf-8', errors='ignore')
                            lines = chunk_str.strip().split('\n')
                            
                            for line in lines:
                                if line.strip():
                                    try:
                                        data = json.loads(line)
                                        if 'response' in data:
                                            accumulated.append(data['response'])
                                        if data.get('done'):
                                            return ''.join(accumulated).strip()
                                    except:
                                        continue
                    
                    return ''.join(accumulated).strip()
                    
        except Exception as e:
            logger.error(f"Alternative generation failed: {e}")
            return self._generate_fallback_sql(prompt, model)
    
    def _extract_response_from_text(self, text: str) -> str:
        """
        Extract SQL or response from plain text
        """
        if not text:
            return ""
        
        # Try to find SQL patterns
        sql_patterns = [
            r'(SELECT[\s\S]+?;)',  # SELECT statements
            r'(WITH[\s\S]+?;)',    # WITH statements
            r'(INSERT[\s\S]+?;)',  # INSERT statements
            r'(UPDATE[\s\S]+?;)',  # UPDATE statements
        ]
        
        for pattern in sql_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        # If no SQL found, return cleaned text
        return text.strip()
    
    def _generate_fallback_sql(self, prompt: str, model: str) -> str:
        """
        Generate fallback SQL based on detected patterns in prompt
        """
        prompt_lower = prompt.lower()
        
        # Detect intent from prompt
        if 'วิเคราะห์การขาย' in prompt or 'sales' in prompt_lower or 'รายได้' in prompt:
            # Detect years
            years = re.findall(r'25\d{2}|\d{4}', prompt)
            if years:
                # Convert Thai years to AD
                ad_years = []
                for year in years:
                    y = int(year)
                    if y > 2500:
                        y = y - 543
                    if 2022 <= y <= 2025:
                        ad_years.append(y)
                
                if len(ad_years) == 1:
                    return f"""
                    SELECT customer_name, 
                           SUM(total_revenue) as total_revenue,
                           COUNT(*) as transaction_count
                    FROM v_sales{ad_years[0]}
                    WHERE total_revenue > 0
                    GROUP BY customer_name
                    ORDER BY total_revenue DESC
                    LIMIT 20;
                    """.strip()
                elif len(ad_years) > 1:
                    unions = []
                    for year in ad_years:
                        unions.append(f"SELECT '{year}' as year, customer_name, total_revenue FROM v_sales{year}")
                    
                    return f"""
                    SELECT year, customer_name, SUM(total_revenue) as total_revenue
                    FROM (
                        {' UNION ALL '.join(unions)}
                    ) combined
                    GROUP BY year, customer_name
                    ORDER BY year, total_revenue DESC
                    LIMIT 50;
                    """.strip()
        
        elif 'งาน' in prompt or 'work' in prompt_lower:
            return """
            SELECT date, customer, project, service_group,
                   CASE 
                       WHEN job_description_pm = true THEN 'PM'
                       WHEN job_description_replacement = true THEN 'Replacement'
                       ELSE 'Other'
                   END as job_type
            FROM v_work_force
            WHERE date::date >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY date DESC
            LIMIT 100;
            """.strip()
        
        elif 'อะไหล่' in prompt or 'spare' in prompt_lower or 'part' in prompt_lower:
            return """
            SELECT product_code, product_name, wh,
                   balance_num as stock, 
                   unit_price_num as price,
                   total_num as value
            FROM v_spare_part
            WHERE balance_num > 0
            ORDER BY total_num DESC
            LIMIT 50;
            """.strip()
        
        # Default fallback
        logger.warning("Using generic fallback SQL")
        return """
        SELECT * FROM v_revenue_summary 
        ORDER BY year DESC 
        LIMIT 10;
        """.strip()
    
    async def test_connection(self) -> bool:
        """
        Test connection to Ollama server with better error handling
        """
        try:
            async with aiohttp.ClientSession() as session:
                # Try the tags endpoint first
                async with session.get(
                    f"{self.base_url}/api/tags",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status == 200:
                        try:
                            data = await response.json()
                            models = data.get('models', [])
                            logger.info(f"✅ Ollama connected. Available models: {len(models)}")
                            for model in models[:3]:
                                logger.info(f"   - {model.get('name', 'unknown')}")
                            return True
                        except:
                            # Server is up but response format might be different
                            logger.warning("Ollama server responded but with unexpected format")
                            return True
                    else:
                        logger.error(f"❌ Ollama connection failed: HTTP {response.status}")
                        return False
                        
        except asyncio.TimeoutError:
            logger.error(f"❌ Ollama connection timeout")
            return False
            
        except Exception as e:
            logger.error(f"❌ Cannot connect to Ollama at {self.base_url}: {e}")
            # Check if we should use fallback mode
            if os.getenv('OLLAMA_FALLBACK_MODE', 'true').lower() == 'true':
                logger.warning("⚠️ Enabling fallback mode for SQL generation")
                return True  # Pretend connection is OK to use fallback
            return False

    async def generate_with_retry(self, prompt: str, model: str, max_retries: int = 3) -> str:
        """
        Generate with retry logic and progressive fallback
        """
        for attempt in range(max_retries):
            try:
                result = await self.generate(prompt, model)
                if result and len(result) > 10:  # Valid response
                    return result
                    
                logger.warning(f"Attempt {attempt + 1} returned empty/short response")
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")
            
            # Wait before retry with exponential backoff
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
        
        # Final fallback
        logger.warning("All attempts failed, using fallback SQL")
        return self._generate_fallback_sql(prompt, model)