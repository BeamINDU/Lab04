# agents/core/context_handler.py
"""
Context Handler for Multi-turn Conversation
Handles reference resolution, entity inheritance, and conversation flow
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)

@dataclass
class ConversationTurn:
    """Single turn in conversation"""
    turn_id: int
    question: str
    resolved_question: Optional[str] = None
    intent: Optional[str] = None
    entities: Dict = field(default_factory=dict)
    sql_query: Optional[str] = None
    results_count: int = 0
    results_summary: Dict = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    references_resolved: Dict = field(default_factory=dict)

@dataclass
class ConversationState:
    """Current conversation state"""
    user_id: str
    session_id: str
    current_topic: Optional[str] = None
    state_type: str = 'querying'  # querying, clarifying, following_up, comparing
    active_entities: Dict = field(default_factory=dict)
    turn_count: int = 0

class ContextHandler:
    """Handle multi-turn conversation context"""
    
    def __init__(self):
        # Reference patterns ภาษาไทย
        self.temporal_refs = {
            'เดือนเดียวกัน': 'same_month',
            'ปีเดียวกัน': 'same_year',
            'ปีที่แล้ว': 'last_year',
            'ปีก่อน': 'last_year',
            'เดือนที่แล้ว': 'last_month',
            'เดือนก่อน': 'last_month',
            'วันเดียวกัน': 'same_date',
            'ช่วงเดียวกัน': 'same_period',
            'ช่วงเดิม': 'same_period'
        }
        
        self.entity_refs = {
            'บริษัทนั้น': 'that_company',
            'บริษัทเดิม': 'that_company',
            'ลูกค้านั้น': 'that_customer',
            'ลูกค้าเดิม': 'that_customer',
            'สินค้านั้น': 'that_product',
            'อะไหล่นั้น': 'that_part',
            'ทีมเดิม': 'that_team',
            'ทีมนั้น': 'that_team'
        }
        
        self.result_refs = {
            'ทั้งหมด': 'all_results',
            'ทั้งหมดนั้น': 'all_results',
            'รายการแรก': 'first_item',
            'อันดับ 1': 'top_one',
            'อันดับแรก': 'top_one',
            'top 5': 'top_five',
            '5 อันดับแรก': 'top_five'
        }
        
        # Follow-up indicators
        self.followup_indicators = {
            'drill_down': ['รายละเอียด', 'เจาะลึก', 'detail', 'เฉพาะ'],
            'comparison': ['เทียบ', 'เปรียบเทียบ', 'compare', 'กับ', 'vs'],
            'aggregation': ['รวม', 'ทั้งหมด', 'สรุป', 'total', 'sum'],
            'continuation': ['ต่อ', 'เพิ่ม', 'อีก', 'more', 'next'],
            'filtering': ['แยก', 'เฉพาะ', 'filter', 'only'],
            'temporal': ['เดือน', 'ปี', 'วันที่', 'ช่วง', 'period']
        }
        
        # State machine transitions
        self.state_transitions = {
            'querying': ['clarifying', 'following_up', 'comparing'],
            'clarifying': ['querying', 'following_up'],
            'following_up': ['querying', 'comparing', 'drilling_down'],
            'comparing': ['querying', 'drilling_down'],
            'drilling_down': ['querying', 'following_up']
        }
    
    def is_followup_query(self, question: str, history: List[ConversationTurn]) -> bool:
        """Check if this is a follow-up question"""
        if not history:
            return False
        
        question_lower = question.lower()
        
        # 1. Check for reference patterns
        for ref_pattern in {**self.temporal_refs, **self.entity_refs, **self.result_refs}:
            if ref_pattern in question:
                logger.info(f"Follow-up detected: reference pattern '{ref_pattern}'")
                return True
        
        # 2. Check for follow-up indicators
        for category, keywords in self.followup_indicators.items():
            if any(keyword in question_lower for keyword in keywords):
                logger.info(f"Follow-up detected: {category} indicator")
                return True
        
        # 3. Very short query (likely follow-up)
        if len(question.split()) <= 3 and not question.endswith('?'):
            logger.info("Follow-up detected: short query")
            return True
        
        # 4. No explicit subject (inherits from context)
        last_turn = history[-1]
        if last_turn.entities.get('customers') and not any(
            cust in question for cust in last_turn.entities['customers']
        ):
            # Question doesn't mention customer but previous did
            if any(word in question_lower for word in ['ยอด', 'รายได้', 'งาน', 'ซ่อม']):
                logger.info("Follow-up detected: implicit subject")
                return True
        
        return False
    
    def resolve_references(self, question: str, history: List[ConversationTurn]) -> Tuple[str, Dict]:
        """Resolve all references in the question"""
        if not history:
            return question, {}
        
        resolved_question = question
        resolutions = {}
        
        # Get relevant context
        last_turn = history[-1]
        last_two_turns = history[-2:] if len(history) >= 2 else history
        
        # 1. Resolve temporal references
        resolved_question, temporal_resolutions = self._resolve_temporal_refs(
            resolved_question, last_two_turns
        )
        resolutions.update(temporal_resolutions)
        
        # 2. Resolve entity references
        resolved_question, entity_resolutions = self._resolve_entity_refs(
            resolved_question, last_two_turns
        )
        resolutions.update(entity_resolutions)
        
        # 3. Resolve result references
        resolved_question, result_resolutions = self._resolve_result_refs(
            resolved_question, last_turn
        )
        resolutions.update(result_resolutions)
        
        logger.info(f"References resolved: {resolutions}")
        
        return resolved_question, resolutions
    
    def _resolve_temporal_refs(self, question: str, history: List[ConversationTurn]) -> Tuple[str, Dict]:
        """Resolve temporal references"""
        resolved = question
        resolutions = {}
        
        for turn in reversed(history):
            # Check each temporal reference
            for thai_ref, ref_type in self.temporal_refs.items():
                if thai_ref not in question:
                    continue
                
                if ref_type == 'same_month' and turn.entities.get('months'):
                    month = turn.entities['months'][0]
                    month_name = self._get_month_name(month)
                    resolved = resolved.replace(thai_ref, month_name)
                    resolutions[thai_ref] = month_name
                    
                elif ref_type == 'same_year' and turn.entities.get('years'):
                    year = turn.entities['years'][0]
                    resolved = resolved.replace(thai_ref, f'ปี {year}')
                    resolutions[thai_ref] = year
                    
                elif ref_type == 'last_year' and turn.entities.get('years'):
                    year = turn.entities['years'][0] - 1
                    resolved = resolved.replace(thai_ref, f'ปี {year}')
                    resolutions[thai_ref] = year
                    
                elif ref_type == 'last_month' and turn.entities.get('months'):
                    month = turn.entities['months'][0] - 1
                    if month < 1:
                        month = 12
                    month_name = self._get_month_name(month)
                    resolved = resolved.replace(thai_ref, month_name)
                    resolutions[thai_ref] = month_name
        
        return resolved, resolutions
    
    def _resolve_entity_refs(self, question: str, history: List[ConversationTurn]) -> Tuple[str, Dict]:
        """Resolve entity references"""
        resolved = question
        resolutions = {}
        
        for turn in reversed(history):
            for thai_ref, ref_type in self.entity_refs.items():
                if thai_ref not in question:
                    continue
                    
                if ref_type == 'that_company' and turn.entities.get('customers'):
                    customer = turn.entities['customers'][0]
                    resolved = resolved.replace(thai_ref, customer)
                    resolutions[thai_ref] = customer
                    
                elif ref_type == 'that_product' and turn.entities.get('products'):
                    product = turn.entities['products'][0]
                    resolved = resolved.replace(thai_ref, product)
                    resolutions[thai_ref] = product
                    
                elif ref_type == 'that_team' and turn.entities.get('service_groups'):
                    team = turn.entities['service_groups'][0]
                    resolved = resolved.replace(thai_ref, team)
                    resolutions[thai_ref] = team
        
        return resolved, resolutions
    
    def _resolve_result_refs(self, question: str, last_turn: ConversationTurn) -> Tuple[str, Dict]:
        """Resolve references to previous results"""
        resolved = question
        resolutions = {}
        
        # This would need actual result data to resolve properly
        # For now, just track what was referenced
        for thai_ref, ref_type in self.result_refs.items():
            if thai_ref in question:
                resolutions[thai_ref] = {
                    'type': ref_type,
                    'from_turn': last_turn.turn_id,
                    'results_count': last_turn.results_count
                }
        
        return resolved, resolutions
    
    def merge_entities(self, current_entities: Dict, history: List[ConversationTurn]) -> Dict:
        """Merge entities from conversation history"""
        merged = defaultdict(list)
        
        # Start with current entities
        for key, value in current_entities.items():
            if isinstance(value, list):
                merged[key].extend(value)
            else:
                merged[key].append(value)
        
        # Add from recent history (prioritize recent turns)
        for turn in reversed(history[-3:]):  # Last 3 turns
            for key, value in turn.entities.items():
                if key not in current_entities or not current_entities[key]:
                    # Only add if not in current
                    if isinstance(value, list):
                        for v in value:
                            if v not in merged[key]:
                                merged[key].append(v)
                    else:
                        if value not in merged[key]:
                            merged[key].append(value)
        
        # Clean up
        result = {}
        for key, values in merged.items():
            if values:
                # Remove duplicates while preserving order
                seen = set()
                unique_values = []
                for v in values:
                    if v not in seen:
                        seen.add(v)
                        unique_values.append(v)
                result[key] = unique_values
        
        return result
    
    def determine_query_type(self, question: str, history: List[ConversationTurn]) -> str:
        """Determine the type of follow-up query"""
        question_lower = question.lower()
        
        # Check each category
        scores = {}
        for category, keywords in self.followup_indicators.items():
            score = sum(1 for keyword in keywords if keyword in question_lower)
            if score > 0:
                scores[category] = score
        
        if not scores:
            return 'new_query'
        
        # Return category with highest score
        return max(scores, key=scores.get)
    
    def get_conversation_state(self, history: List[ConversationTurn]) -> str:
        """Determine current conversation state"""
        if not history:
            return 'greeting'
        
        last_turn = history[-1]
        
        # Check if clarifying
        if last_turn.results_count == 0:
            return 'clarifying'
        
        # Check query patterns
        if len(history) >= 2:
            query_types = [self.determine_query_type(turn.question, history[:i]) 
                          for i, turn in enumerate(history[-2:])]
            
            if 'comparison' in query_types:
                return 'comparing'
            elif 'drill_down' in query_types:
                return 'drilling_down'
        
        return 'querying'
    
    def suggest_followups(self, last_turn: ConversationTurn) -> List[str]:
        """Generate contextual follow-up suggestions"""
        suggestions = []
        
        intent_suggestions = {
            'sales': [
                "แยกตามประเภทบริการ",
                "เทียบกับปีที่แล้ว",
                "แสดง 5 อันดับแรก",
                "ดูแนวโน้มรายเดือน"
            ],
            'sales_analysis': [
                "รายละเอียดแต่ละเดือน",
                "เฉพาะ overhaul", 
                "ลูกค้าที่มียอดสูงสุด",
                "เปรียบเทียบกับปีก่อน"
            ],
            'work_force': [
                "เฉพาะงานที่สำเร็จ",
                "แยกตามทีม",
                "ดูรายละเอียดงาน",
                "สรุปตามประเภทงาน"
            ],
            'spare_parts': [
                "เฉพาะที่ใกล้หมด",
                "มูลค่ารวมทั้งหมด",
                "แยกตาม warehouse",
                "ดูประวัติการเบิก"
            ],
            'customer_history': [
                "ประวัติการซ่อม",
                "งานที่กำลังทำ",
                "ยอดซื้อรวม",
                "แนวโน้มการใช้บริการ"
            ]
        }
        
        # Get suggestions based on intent
        base_suggestions = intent_suggestions.get(
            last_turn.intent, 
            ["ดูรายละเอียด", "เทียบกับช่วงอื่น", "สรุปข้อมูล"]
        )
        
        # Customize based on entities
        if last_turn.entities.get('years'):
            if len(last_turn.entities['years']) == 1:
                base_suggestions.append("เทียบกับปีอื่น")
        
        if last_turn.entities.get('customers'):
            base_suggestions.append("ดูลูกค้าอื่น")
        
        # Return top 4 most relevant
        return base_suggestions[:4]
    
    def _get_month_name(self, month_num: int) -> str:
        """Get Thai month name"""
        months = {
            1: "มกราคม", 2: "กุมภาพันธ์", 3: "มีนาคม",
            4: "เมษายน", 5: "พฤษภาคม", 6: "มิถุนายน",
            7: "กรกฎาคม", 8: "สิงหาคม", 9: "กันยายน",
            10: "ตุลาคม", 11: "พฤศจิกายน", 12: "ธันวาคม"
        }
        return months.get(month_num, f"เดือน {month_num}")
    
    def create_context_summary(self, history: List[ConversationTurn]) -> str:
        """Create a summary of conversation context"""
        if not history:
            return ""
        
        summary_parts = []
        
        # Topic summary
        topics = set()
        for turn in history[-5:]:
            if turn.intent:
                topics.add(turn.intent)
        
        if topics:
            summary_parts.append(f"Topics discussed: {', '.join(topics)}")
        
        # Entity summary  
        all_entities = defaultdict(set)
        for turn in history[-5:]:
            for key, values in turn.entities.items():
                if isinstance(values, list):
                    all_entities[key].update(values)
                else:
                    all_entities[key].add(values)
        
        if all_entities:
            entity_summary = []
            if all_entities.get('customers'):
                entity_summary.append(f"Customers: {', '.join(all_entities['customers'])}")
            if all_entities.get('years'):
                entity_summary.append(f"Years: {', '.join(map(str, all_entities['years']))}")
            if entity_summary:
                summary_parts.append(' | '.join(entity_summary))
        
        return ' | '.join(summary_parts) if summary_parts else "New conversation"