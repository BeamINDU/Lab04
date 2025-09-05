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

class ConversationMemory:
    """
    ระบบจดจำบทสนทนา - เก็บประวัติการสนทนาและ context
    """
    def __init__(self, max_history: int = 20):
        self.conversations = defaultdict(lambda: deque(maxlen=max_history))
        self.user_preferences = defaultdict(dict)
        self.successful_patterns = defaultdict(list)
    
    def add_conversation(self, user_id: str, query: str, response: Dict[str, Any]):
        """บันทึกบทสนทนา"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'query': query,
            'intent': response.get('intent', 'unknown'),
            'entities': response.get('entities', {}),
            'success': response.get('success', False),
            'sql_query': response.get('sql_query'),
            'results_count': response.get('results_count', 0)
        }
        self.conversations[user_id].append(entry)
        
        # Track successful patterns
        if entry['success'] and entry['sql_query']:
            pattern_key = f"{entry['intent']}_{json.dumps(entry['entities'], sort_keys=True)}"
            self.successful_patterns[pattern_key].append(entry['sql_query'])
    
    def get_context(self, user_id: str, current_query: str) -> Dict[str, Any]:
        """ดึง context จากประวัติ"""
        recent = list(self.conversations[user_id])[-5:]
        
        context = {
            'conversation_count': len(self.conversations[user_id]),
            'recent_queries': [c['query'] for c in recent],
            'recent_intents': [c['intent'] for c in recent],
            'recent_entities': self._merge_recent_entities(recent),
            'has_history': len(recent) > 0
        }
        
        return context
    
    def _merge_recent_entities(self, conversations: List[Dict]) -> Dict:
        """รวม entities จากบทสนทนาล่าสุด"""
        merged = defaultdict(set)
        for conv in conversations:
            for key, value in conv.get('entities', {}).items():
                if isinstance(value, list):
                    merged[key].update(value)
                else:
                    merged[key].add(value)
        return {k: list(v) for k, v in merged.items()}
