# Enhanced PromptManager with ALL 4 Features
# File: agents/nlp/prompt_manager.py

import json
import logging
import hashlib
import asyncio
import re
import time
import threading
from typing import Dict, List, Any, Optional, Tuple
from textwrap import dedent
from datetime import datetime, timedelta
from collections import defaultdict, deque
from functools import lru_cache

logger = logging.getLogger(__name__)

class PromptManager:
    """
    Enhanced PromptManager with 4 major features:
    1. Dynamic schema loading from database
    2. Learning from successful queries
    3. Intelligent Thai year conversion
    4. Prompt caching mechanism
    """
    
    def __init__(self, db_handler=None):
        self.db_handler = db_handler
        
        # Initialize with default schema (will be overridden by load_schema_from_db)
        self.VIEW_COLUMNS = {
            'v_sales2022': ['id', 'job_no', 'customer_name', 'description',
                           'overhaul_num', 'replacement_num', 'service_num',
                           'parts_num', 'product_num', 'solution_num', 'total_revenue'],
            'v_sales2023': ['id', 'job_no', 'customer_name', 'description',
                           'overhaul_num', 'replacement_num', 'service_num',
                           'parts_num', 'product_num', 'solution_num', 'total_revenue'],
            'v_sales2024': ['id', 'job_no', 'customer_name', 'description',
                           'overhaul_num', 'replacement_num', 'service_num',
                           'parts_num', 'product_num', 'solution_num', 'total_revenue'],
            'v_sales2025': ['id', 'job_no', 'customer_name', 'description',
                           'overhaul_num', 'replacement_num', 'service_num',
                           'parts_num', 'product_num', 'solution_num', 'total_revenue'],
            'v_work_force': ['id', 'date', 'customer', 'project', 'detail', 'duration',
                            'service_group', 'job_description_pm', 'job_description_replacement',
                            'job_description_overhaul', 'job_description_start_up',
                            'job_description_support_all', 'job_description_cpa',
                            'success', 'unsuccessful', 'failure_reason'],
            'v_spare_part': ['id', 'wh', 'product_code', 'product_name', 'unit',
                            'balance_num', 'unit_price_num', 'total_num',
                            'description', 'received'],
            'v_spare_part2': ['id', 'wh', 'product_code', 'product_name', 'unit',
                             'balance_num', 'unit_price_num', 'total_num',
                             'description', 'received']
        }
        
        # Load real-world optimized examples
        self.SQL_EXAMPLES = self._load_real_world_examples()
        
        # ===== FEATURE 2: Learning system for successful queries =====
        self.learned_examples = defaultdict(lambda: deque(maxlen=20))
        self.example_scores = defaultdict(float)
        self.max_learned_examples = 100
        
        # ===== FEATURE 1: Schema metadata cache =====
        self.schema_metadata = {}
        self.schema_last_updated = None
        
        # ===== FEATURE 3: Thai Year Conversion System =====
        self.thai_year_offset = 543
        self.current_year = datetime.now().year
        self.valid_year_range = (2020, 2030)  # Valid AD years for data
        
        # ===== FEATURE 4: Prompt Caching System =====
        self.prompt_cache = {}
        self.cache_max_size = 100
        self.cache_ttl_seconds = 3600  # 1 hour
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_lock = threading.Lock()
        
        # Simplified system prompt
        self.SQL_SYSTEM_PROMPT = self._get_simplified_prompt()
        
        # Try to load schema from database on init
        if self.db_handler:
            try:
                self.load_schema_from_db()
            except Exception as e:
                logger.warning(f"Could not load schema from DB on init: {e}")
    
    # =========================================================================
    # FEATURE 1: DYNAMIC SCHEMA LOADING
    # =========================================================================
    
    def load_schema_from_db(self) -> bool:
        """
        Load actual schema from database
        Returns True if successful
        """
        if not self.db_handler:
            logger.warning("No database handler available for schema loading")
            return False
        
        try:
            # Query to get all view columns
            schema_query = """
                SELECT 
                    table_name,
                    column_name,
                    data_type,
                    character_maximum_length,
                    is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                    AND table_name LIKE 'v_%'
                ORDER BY table_name, ordinal_position;
            """
            
            # Execute query (handle both sync and async)
            if hasattr(self.db_handler, 'execute_query'):
                if asyncio.iscoroutinefunction(self.db_handler.execute_query):
                    # Run async in sync context
                    loop = asyncio.new_event_loop()
                    results = loop.run_until_complete(self.db_handler.execute_query(schema_query))
                    loop.close()
                else:
                    # Sync version
                    results = self.db_handler.execute_query(schema_query)
            else:
                logger.warning("Database handler doesn't have execute_query method")
                return False
            
            # Parse results into VIEW_COLUMNS format
            new_schema = defaultdict(list)
            metadata = defaultdict(dict)
            
            for row in results:
                view_name = row.get('table_name')
                column_name = row.get('column_name')
                data_type = row.get('data_type')
                
                if view_name and column_name:
                    new_schema[view_name].append(column_name)
                    metadata[view_name][column_name] = {
                        'type': data_type,
                        'nullable': row.get('is_nullable') == 'YES',
                        'max_length': row.get('character_maximum_length')
                    }
            
            if new_schema:
                self.VIEW_COLUMNS = dict(new_schema)
                self.schema_metadata = dict(metadata)
                self.schema_last_updated = datetime.now()
                
                logger.info(f"✅ Loaded schema for {len(self.VIEW_COLUMNS)} views from database")
                logger.info(f"Views: {list(self.VIEW_COLUMNS.keys())}")
                return True
            else:
                logger.warning("No views found in database schema")
                return False
                
        except Exception as e:
            logger.error(f"Failed to load schema from database: {e}")
            return False
    
    def refresh_schema_if_needed(self, max_age_hours: int = 24) -> bool:
        """
        Refresh schema if it's older than max_age_hours
        """
        if not self.schema_last_updated:
            return self.load_schema_from_db()
        
        age = (datetime.now() - self.schema_last_updated).total_seconds() / 3600
        if age > max_age_hours:
            logger.info(f"Schema is {age:.1f} hours old, refreshing...")
            return self.load_schema_from_db()
        
        return True
    
    # =========================================================================
    # FEATURE 2: LEARNING FROM SUCCESSFUL QUERIES
    # =========================================================================
    
    def add_successful_query(self, intent: str, sql: str, results_count: int, 
                           execution_time: float, confidence: float = 1.0) -> None:
        """
        Add a successful query to the learning system
        """
        if not sql or results_count == 0:
            return
        
        quality_score = self._calculate_query_quality(
            results_count, execution_time, confidence
        )
        
        if quality_score > 0.7:
            example_entry = {
                'sql': self._normalize_sql(sql),
                'score': quality_score,
                'results_count': results_count,
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat(),
                'hash': hashlib.md5(sql.encode()).hexdigest()
            }
            
            existing = self._find_similar_learned_query(intent, sql)
            if existing:
                existing['score'] = (existing['score'] + quality_score) / 2
                existing['timestamp'] = datetime.now().isoformat()
            else:
                self.learned_examples[intent].append(example_entry)
                self._prune_learned_examples()
            
            logger.info(f"📚 Learned new SQL example for intent '{intent}' (score: {quality_score:.2f})")
    
    def get_learned_example(self, intent: str, entities: Dict) -> Optional[str]:
        """
        Get the best learned example for an intent
        """
        if intent not in self.learned_examples or not self.learned_examples[intent]:
            return None
        
        examples = sorted(
            self.learned_examples[intent],
            key=lambda x: (x['score'], x['timestamp']),
            reverse=True
        )
        
        if examples:
            logger.info(f"Using learned example for {intent} (score: {examples[0]['score']:.2f})")
            return examples[0]['sql']
        
        return None
    
    def _calculate_query_quality(self, results_count: int, execution_time: float, 
                                confidence: float) -> float:
        """Calculate quality score for a query"""
        # Results score
        if results_count == 0:
            results_score = 0
        elif 1 <= results_count <= 100:
            results_score = 1.0
        elif 100 < results_count <= 500:
            results_score = 0.8
        else:
            results_score = 0.5
        
        # Performance score
        if execution_time < 1:
            perf_score = 1.0
        elif execution_time < 3:
            perf_score = 0.8
        elif execution_time < 5:
            perf_score = 0.6
        else:
            perf_score = 0.3
        
        return (results_score * 0.4 + perf_score * 0.3 + confidence * 0.3)
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for comparison"""
        sql = re.sub(r'\s+', ' ', sql.strip())
        sql = sql.rstrip(';')
        
        keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 
                   'JOIN', 'LEFT', 'RIGHT', 'INNER', 'UNION', 'ALL', 'AND', 'OR']
        for keyword in keywords:
            sql = re.sub(rf'\b{keyword}\b', keyword, sql, flags=re.IGNORECASE)
        
        return sql
    
    def _find_similar_learned_query(self, intent: str, sql: str) -> Optional[Dict]:
        """Find similar query in learned examples"""
        normalized_sql = self._normalize_sql(sql)
        sql_hash = hashlib.md5(normalized_sql.encode()).hexdigest()
        
        for example in self.learned_examples[intent]:
            if example['hash'] == sql_hash:
                return example
            if self._calculate_sql_similarity(example['sql'], normalized_sql) > 0.9:
                return example
        
        return None
    
    def _calculate_sql_similarity(self, sql1: str, sql2: str) -> float:
        """Calculate similarity between SQLs"""
        tokens1 = set(sql1.lower().split())
        tokens2 = set(sql2.lower().split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1.intersection(tokens2)
        union = tokens1.union(tokens2)
        
        return len(intersection) / len(union)
    
    def _prune_learned_examples(self) -> None:
        """Remove low-quality learned examples"""
        total_examples = sum(len(examples) for examples in self.learned_examples.values())
        
        if total_examples > self.max_learned_examples:
            all_examples = []
            for intent, examples in self.learned_examples.items():
                for example in examples:
                    all_examples.append((intent, example))
            
            all_examples.sort(key=lambda x: x[1]['score'])
            to_remove = total_examples - self.max_learned_examples
            
            for intent, example in all_examples[:to_remove]:
                self.learned_examples[intent].remove(example)
            
            logger.info(f"Pruned {to_remove} low-quality learned examples")
    
    def get_learning_stats(self) -> Dict[str, Any]:
        """Get learning statistics"""
        stats = {
            'total_learned': sum(len(examples) for examples in self.learned_examples.values()),
            'by_intent': {},
            'average_scores': {}
        }
        
        for intent, examples in self.learned_examples.items():
            stats['by_intent'][intent] = len(examples)
            if examples:
                avg_score = sum(e['score'] for e in examples) / len(examples)
                stats['average_scores'][intent] = round(avg_score, 2)
        
        return stats
    
    # =========================================================================
    # FEATURE 3: INTELLIGENT THAI YEAR CONVERSION
    # =========================================================================
    
    def convert_thai_year(self, year_value: Any) -> Optional[int]:
        """
        Intelligently convert Thai Buddhist year to AD year
        
        Examples:
            "พ.ศ. 2567" → 2024
            "2567" → 2024
            "'67" → 2024
            "67" → 2024
            "2024" → 2024
        """
        try:
            # Convert to integer if string
            if isinstance(year_value, str):
                # Extract year from strings like "พ.ศ. 2567" or "ปี 2567"
                year_match = re.search(r'\d{2,4}', year_value)
                if year_match:
                    year = int(year_match.group())
                else:
                    return None
            else:
                year = int(year_value)
            
            # Determine if it's Thai year (BE) or AD
            if year > 2500:  # Likely Thai Buddhist Era
                ad_year = year - self.thai_year_offset
            elif year > 100:  # Likely already AD
                ad_year = year
            else:  # Two-digit year
                # Convert 67 -> 2567 -> 2024
                if year >= 50:
                    ad_year = 2500 + year - self.thai_year_offset
                else:
                    ad_year = 2600 + year - self.thai_year_offset
            
            # Validate year is in reasonable range
            if self.valid_year_range[0] <= ad_year <= self.valid_year_range[1]:
                return ad_year
            else:
                logger.warning(f"Year {ad_year} outside valid range {self.valid_year_range}")
                return None
                
        except (ValueError, TypeError) as e:
            logger.error(f"Failed to convert year '{year_value}': {e}")
            return None
    
    def extract_years_from_text(self, text: str) -> List[int]:
        """
        Extract and convert years from Thai text
        """
        years = []
        
        # Patterns to find years
        patterns = [
            r'(?:พ\.ศ\.|พ\.ศ|ปี)\s*(\d{4})',  # พ.ศ. 2567
            r'(?:ค\.ศ\.|ค\.ศ|AD)\s*(\d{4})',   # ค.ศ. 2024
            r'(?:25|26)\d{2}',                   # 2567, 2024
            r"'(\d{2})",                         # '67
            r'(?:ปี|year)\s*(\d{2,4})',         # ปี 67, year 2024
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                year = self.convert_thai_year(match)
                if year:
                    years.append(year)
        
        # Handle year ranges like "2567-2568" or "2024-2025"
        range_pattern = r'(\d{4})\s*[-–]\s*(\d{4})'
        range_matches = re.findall(range_pattern, text)
        for start_year, end_year in range_matches:
            start = self.convert_thai_year(start_year)
            end = self.convert_thai_year(end_year)
            if start and end:
                years.extend(range(start, end + 1))
        
        return sorted(list(set(years)))
    
    def convert_month_thai_to_number(self, month_text: str) -> Optional[int]:
        """Convert Thai/English month name to number"""
        month_map = {
            # Thai months
            'มกราคม': 1, 'ม.ค.': 1, 'มค': 1,
            'กุมภาพันธ์': 2, 'ก.พ.': 2, 'กพ': 2,
            'มีนาคม': 3, 'มี.ค.': 3, 'มีค': 3,
            'เมษายน': 4, 'เม.ย.': 4, 'เมย': 4,
            'พฤษภาคม': 5, 'พ.ค.': 5, 'พค': 5,
            'มิถุนายน': 6, 'มิ.ย.': 6, 'มิย': 6,
            'กรกฎาคม': 7, 'ก.ค.': 7, 'กค': 7,
            'สิงหาคม': 8, 'ส.ค.': 8, 'สค': 8,
            'กันยายน': 9, 'ก.ย.': 9, 'กย': 9,
            'ตุลาคม': 10, 'ต.ค.': 10, 'ตค': 10,
            'พฤศจิกายน': 11, 'พ.ย.': 11, 'พย': 11,
            'ธันวาคม': 12, 'ธ.ค.': 12, 'ธค': 12,
            # English months
            'january': 1, 'jan': 1,
            'february': 2, 'feb': 2,
            'march': 3, 'mar': 3,
            'april': 4, 'apr': 4,
            'may': 5,
            'june': 6, 'jun': 6,
            'july': 7, 'jul': 7,
            'august': 8, 'aug': 8,
            'september': 9, 'sep': 9, 'sept': 9,
            'october': 10, 'oct': 10,
            'november': 11, 'nov': 11,
            'december': 12, 'dec': 12,
        }
        
        month_lower = month_text.lower().strip()
        return month_map.get(month_lower)
    
    # =========================================================================
    # FEATURE 4: PROMPT CACHING MECHANISM
    # =========================================================================
    
    def _generate_cache_key(self, question: str, intent: str, entities: Dict) -> str:
        """Generate unique cache key for prompt"""
        key_data = {
            'q': question.lower().strip(),
            'i': intent,
            'e': json.dumps(entities, sort_keys=True)
        }
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _get_cached_prompt(self, cache_key: str) -> Optional[Tuple[str, float]]:
        """Get cached prompt if exists and not expired"""
        with self.cache_lock:
            if cache_key in self.prompt_cache:
                prompt, timestamp = self.prompt_cache[cache_key]
                
                # Check if expired
                age = time.time() - timestamp
                if age < self.cache_ttl_seconds:
                    self.cache_hits += 1
                    logger.debug(f"Cache hit for key {cache_key[:8]}... (age: {age:.1f}s)")
                    return prompt, timestamp
                else:
                    # Remove expired entry
                    del self.prompt_cache[cache_key]
                    logger.debug(f"Cache expired for key {cache_key[:8]}...")
            
            self.cache_misses += 1
            return None
    
    def _cache_prompt(self, cache_key: str, prompt: str) -> None:
        """Cache prompt with timestamp"""
        with self.cache_lock:
            # Check cache size limit
            if len(self.prompt_cache) >= self.cache_max_size:
                # Remove oldest entries (LRU)
                oldest_keys = sorted(
                    self.prompt_cache.keys(),
                    key=lambda k: self.prompt_cache[k][1]
                )[:10]  # Remove 10 oldest
                
                for key in oldest_keys:
                    del self.prompt_cache[key]
                
                logger.debug(f"Pruned {len(oldest_keys)} old cache entries")
            
            # Add new entry
            self.prompt_cache[cache_key] = (prompt, time.time())
            logger.debug(f"Cached prompt for key {cache_key[:8]}...")
    
    def clear_cache(self) -> Dict[str, int]:
        """Clear all cached prompts"""
        with self.cache_lock:
            stats = {
                'entries_cleared': len(self.prompt_cache),
                'total_hits': self.cache_hits,
                'total_misses': self.cache_misses,
                'hit_rate': self.cache_hits / (self.cache_hits + self.cache_misses) 
                            if (self.cache_hits + self.cache_misses) > 0 else 0
            }
            
            self.prompt_cache.clear()
            self.cache_hits = 0
            self.cache_misses = 0
            
            logger.info(f"Cache cleared: {stats['entries_cleared']} entries, "
                       f"hit rate was {stats['hit_rate']:.2%}")
            
            return stats
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self.cache_lock:
            total_requests = self.cache_hits + self.cache_misses
            
            return {
                'cache_size': len(self.prompt_cache),
                'max_size': self.cache_max_size,
                'ttl_seconds': self.cache_ttl_seconds,
                'hits': self.cache_hits,
                'misses': self.cache_misses,
                'total_requests': total_requests,
                'hit_rate': self.cache_hits / total_requests if total_requests > 0 else 0,
                'memory_usage_kb': sum(
                    len(k) + len(v[0]) for k, v in self.prompt_cache.items()
                ) / 1024 if self.prompt_cache else 0
            }
    
    # =========================================================================
    # EXISTING METHODS (ENHANCED)
    # =========================================================================
    
    def _load_real_world_examples(self) -> Dict[str, str]:
        """Load real-world tested and optimized SQL examples"""
        return {
            'work_plan_specific_date': dedent("""
                SELECT date, customer, project, detail, service_group,
                       CASE 
                           WHEN job_description_pm = true THEN 'PM'
                           WHEN job_description_replacement = true THEN 'Replacement'
                           WHEN job_description_overhaul IS NOT NULL THEN 'Overhaul'
                           ELSE 'Other'
                       END as job_type
                FROM v_work_force
                WHERE date = '2025-09-05'
                ORDER BY customer
                LIMIT 100;
            """).strip(),
            
            'customer_history_multi_year': dedent("""
                SELECT year_label, customer_name, 
                       COUNT(*) as transaction_count,
                       SUM(total_revenue) as total_amount,
                       SUM(overhaul_num) as overhaul,
                       SUM(replacement_num) as replacement,
                       SUM(service_num) as service
                FROM (
                    SELECT '2023' AS year_label, customer_name, total_revenue,
                           overhaul_num, replacement_num, service_num
                    FROM v_sales2023
                    WHERE customer_name ILIKE '%STANLEY%'
                    UNION ALL
                    SELECT '2024', customer_name, total_revenue,
                           overhaul_num, replacement_num, service_num
                    FROM v_sales2024
                    WHERE customer_name ILIKE '%STANLEY%'
                    UNION ALL
                    SELECT '2025', customer_name, total_revenue,
                           overhaul_num, replacement_num, service_num
                    FROM v_sales2025
                    WHERE customer_name ILIKE '%STANLEY%'
                ) combined
                GROUP BY year_label, customer_name
                ORDER BY year_label DESC;
            """).strip(),
            
            'repair_history': dedent("""
                SELECT date, customer, detail, service_group,
                       CASE 
                           WHEN success IS NOT NULL THEN 'สำเร็จ'
                           WHEN unsuccessful IS NOT NULL THEN 'ไม่สำเร็จ'
                           ELSE 'กำลังดำเนินการ'
                       END as status
                FROM v_work_force
                WHERE customer ILIKE '%STANLEY%'
                ORDER BY date DESC
                LIMIT 100;
            """).strip(),
            
            'spare_parts_price': dedent("""
                SELECT product_code, product_name, wh,
                       balance_num as stock,
                       unit_price_num as price,
                       total_num as value
                FROM v_spare_part
                WHERE product_name ILIKE '%EKAC460%'
                   OR product_code ILIKE '%EKAC460%'
                ORDER BY total_num DESC
                LIMIT 50;
            """).strip(),
            
            'sales_analysis_multi_year': dedent("""
                SELECT year_label,
                       SUM(overhaul_num) as overhaul,
                       SUM(replacement_num) as replacement,
                       SUM(service_num) as service,
                       SUM(parts_num) as parts,
                       SUM(product_num) as product,
                       SUM(solution_num) as solution,
                       SUM(total_revenue) as total
                FROM (
                    SELECT '2024' AS year_label, overhaul_num, replacement_num,
                           service_num, parts_num, product_num, solution_num, total_revenue
                    FROM v_sales2024
                    UNION ALL
                    SELECT '2025', overhaul_num, replacement_num,
                           service_num, parts_num, product_num, solution_num, total_revenue
                    FROM v_sales2025
                ) combined
                GROUP BY year_label
                ORDER BY year_label;
            """).strip(),
            
            'work_monthly_range': dedent("""
                SELECT date, customer, detail, service_group
                FROM v_work_force
                WHERE date::date BETWEEN '2025-08-01' AND '2025-09-30'
                ORDER BY date
                LIMIT 200;
            """).strip()
        }
    
    def _get_simplified_prompt(self) -> str:
        """Get simplified system prompt"""
        return dedent("""
        PostgreSQL expert for Siamtemp HVAC. Generate simple, clean SQL.
        
        KEY RULES:
        1. Views have clean data - use *_num columns directly
        2. No need for regexp_replace or complex cleaning
        3. Use ILIKE for text search
        4. Date format: WHERE date = 'YYYY-MM-DD' or date::date BETWEEN
        5. Year conversion handled automatically (Thai/AD)
        
        COLUMN NAMES:
        - Sales: customer_name, total_revenue, *_num fields
        - Work: customer (not customer_name!), date, detail
        - Parts: product_code, product_name, balance_num, unit_price_num
        
        Always add LIMIT. Return SQL only.
        """).strip()
    
    def build_sql_prompt(self, question: str, intent: str, entities: Dict,
                        context: Dict = None, examples_override: List[str] = None) -> str:
        """Build SQL generation prompt with ALL 4 features"""
        
        # ===== FEATURE 4: Check cache first =====
        cache_key = self._generate_cache_key(question, intent, entities)
        cached_prompt = self._get_cached_prompt(cache_key)
        
        if cached_prompt:
            prompt, _ = cached_prompt
            logger.info(f"Using cached prompt (hit rate: {self.cache_hits/(self.cache_hits+self.cache_misses):.2%})")
            return prompt
        
        # ===== FEATURE 2: Try to get learned example first =====
        learned_example = self.get_learned_example(intent, entities)
        
        if learned_example:
            example = learned_example
            logger.info(f"Using learned SQL example for {intent}")
        else:
            example = self._select_best_example(question, intent, entities)
        
        # Build entity-specific hints with Thai year conversion
        hints = self._build_sql_hints(entities, intent)
        
        # Add schema context if available
        schema_context = ""
        if intent in ['sales', 'sales_analysis'] and 'years' in entities:
            relevant_views = [f"v_sales{year}" for year in entities['years']]
            for view in relevant_views:
                if view in self.VIEW_COLUMNS:
                    schema_context += f"\n{view} columns: {', '.join(self.VIEW_COLUMNS[view][:10])}"
        
        # Create compact prompt
        prompt = dedent(f"""
        {self.SQL_SYSTEM_PROMPT}
        {schema_context}
        
        REAL WORKING EXAMPLE:
        {example}
        
        YOUR TASK:
        Question: {question}
        {hints}
        
        Generate simple SQL like the example:
        """).strip()
        
        # ===== FEATURE 4: Cache the generated prompt =====
        self._cache_prompt(cache_key, prompt)
        
        return prompt
  
    
    def _select_best_example(self, question: str, intent: str, entities: Dict) -> str:
        """Select most relevant example based on question pattern"""
        question_lower = question.lower()
        
        # Pattern matching for best example
        if 'แผนงานวันที่' in question or 'วันที่' in question and 'งาน' in question:
            return self.SQL_EXAMPLES['work_plan_specific_date']
        elif 'ย้อนหลัง' in question and 'ปี' in question:
            return self.SQL_EXAMPLES['customer_history_multi_year']
        elif 'ประวัติการซ่อม' in question:
            return self.SQL_EXAMPLES['repair_history']
        elif 'ราคาอะไหล่' in question or 'อะไหล่' in question:
            return self.SQL_EXAMPLES['spare_parts_price']
        elif 'วิเคราะห์การขาย' in question or (len(entities.get('years', [])) > 1 and 'sales' in intent):
            return self.SQL_EXAMPLES['sales_analysis_multi_year']
        elif 'เดือน' in question and entities.get('months'):
            return self.SQL_EXAMPLES['work_monthly_range']
        
        # Default based on intent
        intent_map = {
            'work_plan': self.SQL_EXAMPLES['work_plan_specific_date'],
            'work_force': self.SQL_EXAMPLES['work_monthly_range'],
            'customer_history': self.SQL_EXAMPLES['customer_history_multi_year'],
            'repair_history': self.SQL_EXAMPLES['repair_history'],
            'parts_price': self.SQL_EXAMPLES['spare_parts_price'],
            'spare_parts': self.SQL_EXAMPLES['spare_parts_price'],
            'sales_analysis': self.SQL_EXAMPLES['sales_analysis_multi_year']
        }
        
        return intent_map.get(intent, self.SQL_EXAMPLES['sales_analysis_multi_year'])
    
    def _build_sql_hints(self, entities: Dict, intent: str) -> str:
        """Build CORRECT hints based on intent and entities"""
        hints = []
        
        # Year hints - OK
        if entities.get('years'):
            years = entities['years']
            if len(years) == 1:
                hints.append(f"Single year: Use v_sales{years[0]}")
            else:
                views = [f"v_sales{y}" for y in years]
                hints.append(f"Multiple years: UNION ALL {', '.join(views)}")
        
        # Customer hints - แก้ไขใหม่ให้ชัดเจน
        if entities.get('customers'):
            customer = entities['customers'][0]
            
            # ✅ ระบุชัดเจนตาม intent
            if intent == 'customer_history':
                # customer_history ใช้ v_sales views = customer_name
                hints.append(f"Replace STANLEY with: {customer}")
                hints.append(f"Use: WHERE customer_name ILIKE '%{customer}%'")
            elif intent in ['work_force', 'work_plan', 'repair_history']:
                # work views ใช้ customer
                hints.append(f"Replace example customer with: {customer}")
                hints.append(f"Use: WHERE customer ILIKE '%{customer}%'")
            else:
                # Default for sales-related
                hints.append(f"Customer filter: WHERE customer_name ILIKE '%{customer}%'")
        
        # Date hints
        if entities.get('months'):
            months = entities['months']
            year = entities.get('years', [2025])[0]
            if len(months) == 1:
                month = months[0]
                hints.append(f"Month {month}: WHERE date::date BETWEEN '{year}-{month:02d}-01' AND '{year}-{month:02d}-31'")
        
        return '\n'.join(hints) if hints else ""
    
    # Other existing methods remain the same...
    def build_response_prompt(self, question: str, results: List[Dict],
                             sql_query: str, locale: str = "th") -> str:
        """Build response generation prompt"""
        if not results:
            return f"ไม่พบข้อมูลสำหรับคำถาม: {question}"
        
        stats = self._analyze_results(results)
        sample = results[:10]
        
        prompt = dedent(f"""
        สรุปข้อมูล HVAC สำหรับคำถาม: {question}
        
        พบข้อมูล: {len(results)} รายการ
        {stats}
        
        ตัวอย่าง:
        {json.dumps(sample, ensure_ascii=False, default=str)[:1000]}
        
        กรุณาสรุป:
        1. ข้อมูลที่พบ (จำนวน, ช่วงเวลา)
        2. รายละเอียดสำคัญ (ยอดเงิน, รายชื่อ)
        3. ข้อสังเกต/แนวโน้ม
        
        ตอบภาษาไทย กระชับ ชัดเจน:
        """).strip()
        
        return prompt
    
    def _analyze_results(self, results: List[Dict]) -> str:
        """Quick analysis of results"""
        if not results:
            return ""
        
        stats = []
        
        if 'total_revenue' in results[0] or 'total' in results[0]:
            field = 'total_revenue' if 'total_revenue' in results[0] else 'total'
            total = sum(float(r.get(field, 0) or 0) for r in results)
            if total > 0:
                stats.append(f"ยอดรวม: {total:,.0f} บาท")
        
        if 'year_label' in results[0] or 'year' in results[0]:
            years = set(r.get('year_label') or r.get('year') for r in results)
            if years:
                stats.append(f"ปีที่มีข้อมูล: {', '.join(sorted(str(y) for y in years if y))}")
        
        if 'customer_name' in results[0] or 'customer' in results[0]:
            field = 'customer_name' if 'customer_name' in results[0] else 'customer'
            customers = set(r.get(field) for r in results if r.get(field))
            if customers:
                stats.append(f"จำนวนลูกค้า: {len(customers)} ราย")
        
        return '\n'.join(stats)
    
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary with ALL 4 features"""
        return {
            'views': list(self.VIEW_COLUMNS.keys()),
            'examples': len(self.SQL_EXAMPLES),
            'learned_examples': sum(len(ex) for ex in self.learned_examples.values()),
            'schema_last_updated': self.schema_last_updated.isoformat() if self.schema_last_updated else None,
            'learning_stats': self.get_learning_stats(),
            'cache_stats': self.get_cache_stats(),
            'thai_year_config': {
                'current_year': self.current_year,
                'valid_range': self.valid_year_range,
                'offset': self.thai_year_offset
            },
            'optimized': True
        }
    
    # Export/Import methods for learned examples
    def export_learned_examples(self, filepath: str = None) -> Dict:
        """Export learned examples for backup"""
        export_data = {
            'version': '1.0',
            'exported_at': datetime.now().isoformat(),
            'learned_examples': {},
            'statistics': self.get_learning_stats()
        }
        
        for intent, examples in self.learned_examples.items():
            export_data['learned_examples'][intent] = list(examples)
        
        if filepath:
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Exported {export_data['statistics']['total_learned']} learned examples to {filepath}")
        
        return export_data
    
    def import_learned_examples(self, filepath: str = None, data: Dict = None) -> bool:
        """Import learned examples from backup"""
        try:
            if filepath:
                with open(filepath, 'r') as f:
                    import_data = json.load(f)
            elif data:
                import_data = data
            else:
                logger.error("No filepath or data provided for import")
                return False
            
            imported_count = 0
            for intent, examples in import_data.get('learned_examples', {}).items():
                self.learned_examples[intent] = deque(examples, maxlen=20)
                imported_count += len(examples)
            
            logger.info(f"Imported {imported_count} learned examples")
            return True
            
        except Exception as e:
            logger.error(f"Failed to import learned examples: {e}")
            return False
    
    # Other helper methods remain the same...
    def build_clarification_prompt(self, question: str, missing_info: List[str]) -> str:
        """Build clarification request"""
        examples = {
            'ระบุเดือน/ปี': 'เช่น "เดือนสิงหาคม 2568" หรือ "ปี 2567-2568"',
            'ชื่อบริษัท': 'เช่น "CLARION" หรือ "STANLEY"',
            'รหัสสินค้า': 'เช่น "EKAC460" หรือ "RCUG120"'
        }
        
        hints = [examples.get(info, info) for info in missing_info]
        
        return dedent(f"""
        ต้องการข้อมูลเพิ่มเติม:
        {chr(10).join(['• ' + h for h in hints])}
        
        กรุณาระบุให้ชัดเจน
        """).strip()
    
    def validate_column_usage(self, sql: str, view_name: str) -> tuple[bool, List[str]]:
        """Validate column usage in SQL"""
        issues = []
        sql_lower = sql.lower()
        
        if 'regexp_replace' in sql_lower:
            issues.append("Unnecessary regexp_replace - views have clean data")
        
        if view_name in self.VIEW_COLUMNS:
            if view_name.startswith('v_sales') and 'revenue' in sql_lower and 'total_revenue' not in sql_lower:
                issues.append("Use 'total_revenue' not 'revenue'")
            
            if view_name == 'v_work_force' and 'customer_name' in sql_lower:
                issues.append("Use 'customer' not 'customer_name' in v_work_force")
        
        return len(issues) == 0, issues
    
    def suggest_column_fix(self, invalid_column: str, view_name: str) -> Optional[str]:
        """Suggest correct column name"""
        fixes = {
            'revenue': 'total_revenue',
            'amount': 'total_revenue',
            'customer': 'customer_name' if view_name.startswith('v_sales') else 'customer',
            'balance': 'balance_num',
            'unit_price': 'unit_price_num',
            'price': 'unit_price_num'
        }
        return fixes.get(invalid_column.lower())
    
    def get_view_columns(self, view_name: str) -> List[str]:
        """Get columns for a view"""
        return self.VIEW_COLUMNS.get(view_name, [])
    
    def get_available_examples(self) -> List[str]:
        """Get available example keys"""
        return list(self.SQL_EXAMPLES.keys())