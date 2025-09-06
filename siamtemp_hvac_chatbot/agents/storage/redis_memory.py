# agents/storage/redis_memory.py
"""
Redis-based conversation memory for scalability
Data persists across restarts and can be shared across instances
"""

import json
import redis
import pickle
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta , time
from dataclasses import dataclass, asdict
import asyncio

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class RedisConfig:
    """Redis configuration"""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    decode_responses: bool = False
    max_connections: int = 50
    socket_timeout: int = 5
    connection_pool_class: str = "redis.BlockingConnectionPool"

# =============================================================================
# REDIS-BASED CONVERSATION MEMORY
# =============================================================================

class ScalableConversationMemory:
    """
    Redis-backed conversation memory for horizontal scaling
    Features:
    - Persists across restarts
    - Shared across multiple instances
    - TTL for automatic cleanup
    - Supports clustering
    """
    
    def __init__(self, config: Optional[RedisConfig] = None):
        self.config = config or RedisConfig()
        self.redis_client = self._create_redis_client()
        self.ttl_seconds = 86400 * 7  # 7 days default TTL
        
        logger.info("✅ Scalable Redis memory initialized")
    
    def _create_redis_client(self) -> redis.Redis:
        """Create Redis client with connection pooling"""
        pool = redis.ConnectionPool(
            host=self.config.host,
            port=self.config.port,
            db=self.config.db,
            password=self.config.password,
            max_connections=self.config.max_connections,
            socket_timeout=self.config.socket_timeout,
            decode_responses=self.config.decode_responses
        )
        return redis.Redis(connection_pool=pool)
    
    # =========================================================================
    # CONVERSATION MANAGEMENT
    # =========================================================================
    
    def add_conversation(self, user_id: str, query: str, response: Dict[str, Any]):
        """Add conversation to Redis with TTL"""
        try:
            # Create conversation entry
            entry = {
                'timestamp': datetime.now().isoformat(),
                'query': query,
                'intent': response.get('intent', 'unknown'),
                'entities': response.get('entities', {}),
                'success': response.get('success', False),
                'processing_time': response.get('processing_time', 0)
            }
            
            # Key pattern: conv:user_id:timestamp
            key = f"conv:{user_id}:{datetime.now().timestamp()}"
            
            # Store in Redis with TTL
            self.redis_client.setex(
                key,
                self.ttl_seconds,
                json.dumps(entry)
            )
            
            # Add to user's conversation list
            list_key = f"conv_list:{user_id}"
            self.redis_client.lpush(list_key, key)
            self.redis_client.ltrim(list_key, 0, 99)  # Keep last 100
            self.redis_client.expire(list_key, self.ttl_seconds)
            
            # Update successful patterns
            if entry['success']:
                self._update_successful_pattern(entry['intent'], entry['entities'])
            
            logger.debug(f"Conversation added for user {user_id}")
            
        except Exception as e:
            logger.error(f"Failed to add conversation: {e}")
    
    def get_context(self, user_id: str, current_query: str) -> Dict[str, Any]:
        """Get conversation context from Redis"""
        try:
            list_key = f"conv_list:{user_id}"
            conversation_keys = self.redis_client.lrange(list_key, 0, 4)  # Last 5
            
            recent = []
            for key in conversation_keys:
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                    
                conv_data = self.redis_client.get(key)
                if conv_data:
                    if isinstance(conv_data, bytes):
                        conv_data = conv_data.decode('utf-8')
                    recent.append(json.loads(conv_data))
            
            context = {
                'conversation_count': self.redis_client.llen(list_key),
                'recent_queries': [c['query'] for c in recent],
                'recent_intents': [c['intent'] for c in recent],
                'recent_entities': self._merge_entities(recent),
                'has_history': len(recent) > 0
            }
            
            return context
            
        except Exception as e:
            logger.error(f"Failed to get context: {e}")
            return {'conversation_count': 0, 'has_history': False}
    
    def _merge_entities(self, conversations: List[Dict]) -> Dict:
        """Merge entities from recent conversations"""
        merged = {}
        for conv in conversations:
            for key, value in conv.get('entities', {}).items():
                if key not in merged:
                    merged[key] = set()
                if isinstance(value, list):
                    merged[key].update(value)
                else:
                    merged[key].add(value)
        
        return {k: list(v) for k, v in merged.items()}
    
    def _update_successful_pattern(self, intent: str, entities: Dict):
        """Track successful query patterns"""
        try:
            pattern_key = f"pattern:{intent}:{json.dumps(entities, sort_keys=True)}"
            self.redis_client.incr(f"pattern_count:{pattern_key}")
            self.redis_client.expire(f"pattern_count:{pattern_key}", self.ttl_seconds)
        except Exception as e:
            logger.error(f"Failed to update pattern: {e}")
    
    def get_successful_patterns(self, intent: str, limit: int = 5) -> List[Dict]:
        """Get top successful patterns for an intent"""
        try:
            pattern = f"pattern_count:pattern:{intent}:*"
            keys = self.redis_client.keys(pattern)
            
            patterns = []
            for key in keys[:limit]:
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                count = self.redis_client.get(key)
                if count:
                    patterns.append({
                        'pattern': key,
                        'count': int(count)
                    })
            
            return sorted(patterns, key=lambda x: x['count'], reverse=True)
            
        except Exception as e:
            logger.error(f"Failed to get patterns: {e}")
            return []
    
    # =========================================================================
    # USER SESSION MANAGEMENT
    # =========================================================================
    
    def get_active_users(self) -> int:
        """Get count of active users"""
        try:
            pattern = "conv_list:*"
            return len(self.redis_client.keys(pattern))
        except Exception as e:
            logger.error(f"Failed to get active users: {e}")
            return 0
    
    def clear_user_history(self, user_id: str):
        """Clear user's conversation history"""
        try:
            # Get all conversation keys
            list_key = f"conv_list:{user_id}"
            conversation_keys = self.redis_client.lrange(list_key, 0, -1)
            
            # Delete each conversation
            for key in conversation_keys:
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                self.redis_client.delete(key)
            
            # Delete the list
            self.redis_client.delete(list_key)
            
            logger.info(f"Cleared history for user {user_id}")
            
        except Exception as e:
            logger.error(f"Failed to clear history: {e}")

# =============================================================================
# SQL CACHE WITH REDIS
# =============================================================================

class ScalableSQLCache:
    """
    Redis-based SQL cache for query optimization
    Shared across all instances
    """
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client or self._create_default_client()
        self.cache_ttl = 3600  # 1 hour default
        self.stats_key = "sql_cache:stats"
        
    def _create_default_client(self) -> redis.Redis:
        """Create default Redis client"""
        return redis.Redis(host='localhost', port=6379, db=1)
    
    def get_cached_result(self, sql_hash: str) -> Optional[List[Dict]]:
        """Get cached SQL result"""
        try:
            cache_key = f"sql_cache:{sql_hash}"
            cached = self.redis_client.get(cache_key)
            
            if cached:
                # Update hit counter
                self.redis_client.hincrby(self.stats_key, "hits", 1)
                
                # Deserialize result
                if isinstance(cached, bytes):
                    cached = cached.decode('utf-8')
                return json.loads(cached)
            else:
                # Update miss counter
                self.redis_client.hincrby(self.stats_key, "misses", 1)
                return None
                
        except Exception as e:
            logger.error(f"Cache get failed: {e}")
            return None
    
    def cache_result(self, sql_hash: str, results: List[Dict]):
        """Cache SQL result with TTL"""
        try:
            cache_key = f"sql_cache:{sql_hash}"
            serialized = json.dumps(results, default=str)
            
            self.redis_client.setex(
                cache_key,
                self.cache_ttl,
                serialized
            )
            
            logger.debug(f"Cached SQL result: {sql_hash[:8]}...")
            
        except Exception as e:
            logger.error(f"Cache set failed: {e}")
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics"""
        try:
            stats = self.redis_client.hgetall(self.stats_key)
            return {
                'hits': int(stats.get(b'hits', 0)),
                'misses': int(stats.get(b'misses', 0)),
                'hit_rate': self._calculate_hit_rate(stats)
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {'hits': 0, 'misses': 0, 'hit_rate': 0}
    
    def _calculate_hit_rate(self, stats: Dict) -> float:
        """Calculate cache hit rate"""
        hits = int(stats.get(b'hits', 0))
        misses = int(stats.get(b'misses', 0))
        total = hits + misses
        return (hits / total * 100) if total > 0 else 0

# =============================================================================
# DISTRIBUTED LOCK FOR CONCURRENT ACCESS
# =============================================================================

class DistributedLock:
    """
    Redis-based distributed lock for concurrent access control
    """
    
    def __init__(self, redis_client: redis.Redis, key: str, timeout: int = 10):
        self.redis_client = redis_client
        self.key = f"lock:{key}"
        self.timeout = timeout
        self.identifier = None
    
    async def acquire(self) -> bool:
        """Acquire lock with timeout"""
        import uuid
        self.identifier = str(uuid.uuid4())
        
        end = time.time() + self.timeout
        while time.time() < end:
            if self.redis_client.set(self.key, self.identifier, nx=True, ex=self.timeout):
                return True
            await asyncio.sleep(0.001)
        
        return False
    
    def release(self):
        """Release lock if we own it"""
        if self.identifier:
            pipe = self.redis_client.pipeline(True)
            while True:
                try:
                    pipe.watch(self.key)
                    if pipe.get(self.key) == self.identifier:
                        pipe.multi()
                        pipe.delete(self.key)
                        pipe.execute()
                        return True
                    pipe.unwatch()
                    break
                except redis.WatchError:
                    pass
        return False

# =============================================================================
# INTEGRATION WITH MAIN SYSTEM
# =============================================================================

class ScalableStorageAdapter:
    """
    Adapter to integrate Redis storage with existing system
    Drop-in replacement for in-memory storage
    """
    
    def __init__(self, redis_config: Optional[RedisConfig] = None):
        self.memory = ScalableConversationMemory(redis_config)
        self.sql_cache = ScalableSQLCache(self.memory.redis_client)
        
    # Implement same interface as original ConversationMemory
    def add_conversation(self, user_id: str, query: str, response: Dict):
        return self.memory.add_conversation(user_id, query, response)
    
    def get_context(self, user_id: str, query: str) -> Dict:
        try:
            return self.memory.get_context(user_id, query)
        except Exception as e:
            logger.warning(f"Redis unavailable: {e}")
            # Return empty context if Redis fails
            return {
                'conversation_count': 0,
                'has_history': False,
                'recent_queries': [],
                'recent_intents': []
            }
    
    @property
    def conversations(self):
        """Compatibility property"""
        # Return a dict-like interface for backward compatibility
        return self
    
    def __getitem__(self, user_id: str):
        """Support dict-like access"""
        return self.memory.get_context(user_id, "")['recent_queries']