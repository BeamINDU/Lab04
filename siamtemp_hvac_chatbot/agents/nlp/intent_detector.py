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

class ImprovedIntentDetector:
    """
    Enhanced Intent Detector - แก้ไขปัญหาจากการทดสอบ
    - Keywords ครอบคลุมคำถามจริง
    - Negative keywords ที่เหมาะสม
    - Business domain awareness
    - Better confidence calculation
    """
    
    def __init__(self):
        # =================================================================
        # ENHANCED INTENT KEYWORDS (แก้ไขปัญหาหลัก)
        # =================================================================
        self.intent_keywords = {
            'pricing': {
                'strong': ['ราคา', 'เสนอราคา', 'quotation', 'price', 'cost', 'quote'],
                'medium': ['Standard', 'งาน', 'สรุป', 'รายการ', 'ทั้งหมด'],  # เพิ่ม Standard!
                'weak': ['บาท', 'เงิน', 'ค่าใช้จ่าย'],
                'patterns': [
                    r'เสนอราคา.*งาน',
                    r'งาน.*Standard',  # เพิ่ม pattern สำคัญ
                    r'สรุป.*ราคา',
                    r'รายการ.*ราคา',
                    r'ราคา.*ทั้งหมด'
                ],
                'negative': ['อะไหล่', 'ช่าง', 'ทีม']  # เอา 'งาน' ออก
            },
            
            'sales': {
                'strong': ['รายได้', 'ยอดขาย', 'revenue', 'sales', 'income', 'การขาย'],
                'medium': ['overhaul', 'replacement', 'service', 'เสนอราคา', 'งาน'],  # เพิ่ม เสนอราคา
                'weak': ['รวม', 'ทั้งหมด', 'total', 'บาท'],
                'patterns': [
                    r'วิเคราะห์.*ขาย',
                    r'รายได้.*ปี',
                    r'ยอดขาย.*เดือน',
                    r'การขาย.*ของ'
                ],
                'negative': ['อะไหล่', 'ช่าง', 'ทีม', 'แผนงาน']  # เอา 'งาน' ออก
            },
            
            'sales_analysis': {
                'strong': ['วิเคราะห์', 'analysis', 'การขาย', 'รายงาน'],
                'medium': ['ยอดขาย', 'รายได้', 'revenue', 'เปรียบเทียบ'],
                'weak': ['ปี', 'เดือน', 'ช่วง'],
                'patterns': [
                    r'วิเคราะห์.*ขาย',
                    r'วิเคราะห์.*รายได้',
                    r'เปรียบเทียบ.*ปี'
                ],
                'negative': ['อะไหล่', 'แผนงาน']
            },
            
            'overhaul_report': {
                'strong': ['overhaul', 'โอเวอร์ฮอล', 'รายงาน'],
                'medium': ['compressor', 'คอมเพรสเซอร์', 'ยอดขาย', 'ซ่อม'],
                'weak': ['เครื่อง', 'งาน'],
                'patterns': [
                    r'overhaul.*compressor',
                    r'รายงาน.*overhaul',
                    r'ยอดขาย.*overhaul'
                ],
                'negative': ['อะไหล่', 'แผนงาน']
            },
            
            'work_force': {
                'strong': ['งาน', 'ทีม', 'ช่าง', 'service_group', 'พนักงาน'],
                'medium': ['project', 'โครงการ', 'success', 'สำเร็จ', 'ทำงาน'],
                'weak': ['เดือน', 'วันที่', 'ลูกค้า'],
                'patterns': [
                    r'งาน.*เดือน',
                    r'ทีม.*งาน',
                    r'งาน.*สำเร็จ',
                    r'งาน.*ทำ'
                ],
                'negative': ['ราคา', 'อะไหล่', 'รายได้', 'ยอดขาย', 'เสนอราคา']  # เพิ่ม เสนอราคา
            },
            
            'work_plan': {
                'strong': ['แผนงาน', 'วางแผน', 'plan', 'schedule', 'กำหนดการ'],
                'medium': ['วันที่', 'เดือน', 'งานอะไรบ้าง', 'มีอะไรบ้าง'],
                'weak': ['ล่วงหน้า', 'ต่อไป'],
                'patterns': [
                    r'แผนงาน.*เดือน',
                    r'วางแผน.*วันที่',
                    r'งาน.*วางแผน',
                    r'แผน.*อะไรบ้าง'
                ],
                'negative': ['ราคา', 'อะไหล่', 'รายได้', 'สำเร็จ']
            },
            
            'work_summary': {
                'strong': ['สรุปงาน', 'งานที่ทำ', 'summary'],
                'medium': ['เดือน', 'ช่วง', 'สำเร็จ', 'เสร็จ'],
                'weak': ['ผลงาน', 'ได้', 'แล้ว'],
                'patterns': [
                    r'สรุป.*งาน',
                    r'งาน.*ที่ทำ',
                    r'งาน.*เสร็จ'
                ],
                'negative': ['ราคา', 'อะไหล่', 'รายได้', 'แผนงาน']
            },
            
            'spare_parts': {
                'strong': ['อะไหล่', 'spare', 'part', 'ชิ้นส่วน'],
                'medium': ['stock', 'คงเหลือ', 'คลัง', 'เก็บ'],
                'weak': ['EK', 'model', 'HITACHI', 'เครื่อง'],
                'patterns': [
                    r'อะไหล่.*เครื่อง',
                    r'ชิ้นส่วน.*model',
                    r'spare.*part'
                ],
                'negative': ['งาน', 'ทีม', 'รายได้', 'แผนงาน']
            },
            
            'parts_price': {
                'strong': ['ราคา', 'อะไหล่', 'price'],
                'medium': ['เครื่อง', 'model', 'ทราบ', 'อยากรู้'],
                'weak': ['บาท', 'เท่าไหร่', 'cost'],
                'patterns': [
                    r'ราคา.*อะไหล่',
                    r'อะไหล่.*ราคา',
                    r'ทราบราคา.*เครื่อง',
                    r'อยากทราบ.*ราคา'
                ],
                'negative': ['งาน', 'ทีม', 'รายได้']
            },
            
            'inventory_value': {
                'strong': ['มูลค่า', 'คงคลัง', 'inventory', 'value'],
                'medium': ['สต็อก', 'คลัง', 'เก็บ', 'รวม'],
                'weak': ['ทั้งหมด', 'total'],
                'patterns': [
                    r'มูลค่า.*คงคลัง',
                    r'สต็อก.*คลัง'
                ],
                'negative': ['งาน', 'ทีม', 'ลูกค้า']
            },
            
            'customer_history': {
                'strong': ['ประวัติ', 'history', 'บริษัท'],
                'medium': ['ลูกค้า', 'customer', 'ซื้อขาย', 'การซื้อ'],
                'weak': ['เก่า', 'ผ่านมา', 'ย้อนหลัง'],
                'patterns': [
                    r'ประวัติ.*ลูกค้า',
                    r'บริษัท.*ประวัติ',
                    r'ลูกค้า.*ซื้อขาย',
                    r'การซื้อ.*ย้อนหลัง'
                ],
                'negative': ['งาน', 'ทีม', 'อะไหล่']
            },
            
            'repair_history': {
                'strong': ['ประวัติ', 'การซ่อม', 'ซ่อม', 'repair'],
                'medium': ['บริษัท', 'ลูกค้า', 'เครื่อง', 'อะไร'],
                'weak': ['เมื่อไหร่', 'เคย'],
                'patterns': [
                    r'ประวัติ.*ซ่อม',
                    r'การซ่อม.*บริษัท',
                    r'บริษัท.*ซ่อม'
                ],
                'negative': ['อะไหล่', 'ราคา', 'แผนงาน']
            },
            
            'top_customers': {
                'strong': ['ลูกค้า', 'Top', 'อันดับ', 'สูงสุด'],
                'medium': ['มากที่สุด', 'ใหญ่ที่สุด', 'หลัก'],
                'weak': ['5', '10', 'best'],
                'patterns': [
                    r'ลูกค้า.*Top',
                    r'Top.*ลูกค้า',
                    r'ลูกค้า.*สูงสุด'
                ],
                'negative': ['อะไหล่', 'งาน', 'ทีม']
            }
        }

        # =================================================================
        # ENHANCED MONTH MAPPING
        # =================================================================
        self.month_map = {
            # ชื่อเต็ม
            'มกราคม': 1, 'กุมภาพันธ์': 2, 'มีนาคม': 3,
            'เมษายน': 4, 'พฤษภาคม': 5, 'มิถุนายน': 6,
            'กรกฎาคม': 7, 'สิงหาคม': 8, 'กันยายน': 9,
            'ตุลาคม': 10, 'พฤศจิกายน': 11, 'ธันวาคม': 12,
            
            # ชื่อย่อ
            'ม.ค.': 1, 'ก.พ.': 2, 'มี.ค.': 3,
            'เม.ย.': 4, 'พ.ค.': 5, 'มิ.ย.': 6,
            'ก.ค.': 7, 'ส.ค.': 8, 'ก.ย.': 9,
            'ต.ค.': 10, 'พ.ย.': 11, 'ธ.ค.': 12,
            
            # English
            'january': 1, 'february': 2, 'march': 3,
            'april': 4, 'may': 5, 'june': 6,
            'july': 7, 'august': 8, 'september': 9,
            'october': 10, 'november': 11, 'december': 12,
            
            'jan': 1, 'feb': 2, 'mar': 3,
            'apr': 4, 'jun': 6, 'jul': 7,
            'aug': 8, 'sep': 9, 'oct': 10,
            'nov': 11, 'dec': 12
        }

        # =================================================================
        # BUSINESS-SPECIFIC TERMS
        # =================================================================
        self.business_terms = {
            'hvac_equipment': [
                'chiller', 'คิลเลอร์', 'แอร์', 'เครื่องปรับอากาศ',
                'compressor', 'คอมเพรสเซอร์', 'AHU', 'FCU'
            ],
            'service_types': [
                'PM', 'maintenance', 'บำรุงรักษา', 'overhaul', 
                'replacement', 'เปลี่ยน', 'ซ่อม', 'service'
            ],
            'brands': [
                'HITACHI', 'CLARION', 'EK', 'EKAC', 'RCUG', 
                'Sadesa', 'AGC', 'Honda'
            ]
        }

    # =================================================================
    # MAIN DETECTION METHOD
    # =================================================================
    
    def detect_intent_and_entities(self, question: str, 
                                previous_intent: Optional[str] = None) -> Dict[str, Any]:
        """
        FINAL FIX: Enhanced intent detection with proper entity extraction
        Addresses both customer detection and intent mapping issues
        """
        question_lower = question.lower().strip()
        
        # ============================================
        # PRIORITY CHECKS (ทำก่อนเสมอ)
        # ============================================
        
        # 1. CPA Work
        if 'cpa' in question_lower and 'งาน' in question_lower:
            return {'intent': 'cpa_work', 'confidence': 0.95, 'entities': self._extract_entities(question, 'cpa_work')}
        
        # 2. PM Work
        if 'pm' in question_lower and 'งาน' in question_lower:
            return {'intent': 'pm_work', 'confidence': 0.90, 'entities': self._extract_entities(question, 'pm_work')}
        
        # ============================================
        # 3. REPAIR HISTORY DETECTION (FIXED)
        # ============================================
        repair_patterns = [
            'ประวัติการซ่อม', 'ซ่อมอะไรบ้าง', 'repair history',
            'ประวัติบริการ', 'service history', 'มีประวัติการซ่อม',
            'เคยซ่อมอะไร', 'บริการอะไรบ้าง'
        ]

        if any(pattern in question_lower for pattern in repair_patterns):
            # ✅ FIX: Extract entities before returning
            entities = self._extract_entities(question, 'customer_repair_history')
            return {
                'intent': 'customer_repair_history', 
                'confidence': 0.95,
                'entities': entities  # ✅ ต้องมี entities!
            }
        
        # ============================================
        # 4. CUSTOMER HISTORY DETECTION (ENHANCED)
        # ============================================
        customer_history_patterns = [
            'ประวัติ', 'history', 'เคย.*ใช้', 'เคย.*ซื้อ', 
            'การซื้อขาย.*ย้อนหลัง', 'มีการซื้อขาย', 'ซื้อขาย.*ปี'
        ]
        
        if any(re.search(pattern, question_lower) for pattern in customer_history_patterns):
            entities = self._extract_entities(question, 'customer_history')
            if entities.get('customers'):
                # ✅ Differentiate between repair and sales history
                if any(word in question_lower for word in ['ซ่อม', 'repair', 'service', 'บริการ']):
                    return {
                        'intent': 'customer_repair_history',
                        'confidence': 0.92,
                        'entities': entities
                    }
                else:
                    return {
                        'intent': 'customer_history', 
                        'confidence': 0.90,
                        'entities': entities
                    }
        
        # ============================================
        # 5. OVERHAUL CONTEXT DETECTION
        # ============================================
        if 'overhaul' in question_lower:
            # Check for SALES context FIRST (higher priority)
            sales_indicators = [
                'ยอดขาย', 'รายงาน', 'sales', 'รายได้', 
                'revenue', 'มูลค่า', 'ราคา', 'บาท', 'income'
            ]
            
            # Check for WORK context
            work_indicators = [
                'การทำงาน', 'งานที่ทำ', 'work', 'ทำอะไร', 
                'รายละเอียด', 'detail', 'วันที่', 'date'
            ]
            
            # Priority 1: Sales context wins
            if any(word in question_lower for word in sales_indicators):
                return {
                    'intent': 'overhaul_sales',
                    'confidence': 0.95,
                    'entities': self._extract_entities(question, 'overhaul_sales')
                }
            
            # Priority 2: Explicit work context
            elif any(word in question_lower for word in work_indicators):
                return {
                    'intent': 'work_overhaul',
                    'confidence': 0.90,
                    'entities': self._extract_entities(question, 'work_overhaul')
                }
            
            # Priority 3: Just "งาน overhaul" without other context
            elif 'งาน' in question_lower:
                if 'รายงาน' in question_lower:
                    return {
                        'intent': 'overhaul_sales',
                        'confidence': 0.85,
                        'entities': self._extract_entities(question, 'overhaul_sales')
                    }
                else:
                    return {
                        'intent': 'work_overhaul',
                        'confidence': 0.85,
                        'entities': self._extract_entities(question, 'work_overhaul')
                    }
            
            # Priority 4: Just "overhaul" alone
            else:
                return {
                    'intent': 'overhaul_sales',
                    'confidence': 0.80,
                    'entities': self._extract_entities(question, 'overhaul_sales')
                }
        
        # ============================================
        # 6. OTHER SPECIFIC PATTERNS
        # ============================================
        
        # Employee queries
        employee_patterns = ['พนักงานชื่อ', 'ช่างชื่อ', 'ทีมของ', 'การทำงานของ']
        if any(pattern in question_lower for pattern in employee_patterns):
            entities = self._extract_entities(question, 'employee_work')
            if entities.get('employees'):
                return {
                    'intent': 'employee_work',
                    'confidence': 0.95,
                    'entities': entities
                }
        
        # ============================================
        # 7. NORMAL PROCESSING (existing logic)
        # ============================================
        
        # Preprocess question
        processed_question = self._preprocess_question(question_lower)
        
        # Calculate intent scores
        intent_scores = self._calculate_intent_scores(processed_question, previous_intent)
        
        # Get best intent with confidence
        best_intent, confidence = self._get_best_intent_with_confidence(intent_scores, processed_question)
        
        # Extract entities
        entities = self._extract_entities(question, best_intent)
        
        # Post-process and validate
        final_intent, final_confidence = self._post_process_intent(
            best_intent, confidence, entities, processed_question
        )
        
        # ============================================
        # 8. FINAL VALIDATION & EMERGENCY FALLBACK
        # ============================================
        
        # Critical validation for customer-related intents
        if final_intent in ['customer_history', 'customer_repair_history']:
            if not entities.get('customers'):
                logger.warning(f"⚠️ {final_intent} intent but no customers found!")
                
                # Try emergency customer extraction
                emergency_customers = self._emergency_customer_extraction(question)
                if emergency_customers:
                    entities['customers'] = emergency_customers
                    logger.info(f"🚨 Emergency extraction found: {emergency_customers}")
                else:
                    logger.warning("⚠️ No customers found even with emergency extraction")
                    # Keep the intent but mark as low confidence
                    final_confidence = max(final_confidence - 0.2, 0.3)
        
        return {
            'intent': final_intent,
            'confidence': final_confidence,
            'entities': entities,
            'scores': intent_scores,
            'debug_info': {
                'original_question': question,
                'processed_question': processed_question,
                'customers_found': len(entities.get('customers', [])),
                'emergency_extraction_used': bool(entities.get('customers')) and final_intent in ['customer_history', 'customer_repair_history']
            }
        }

    def _emergency_customer_extraction(self, question: str) -> List[str]:
        """
        Emergency customer extraction for failed cases
        Enhanced patterns for Thai/foreign mixed names
        """
        logger.info("🚨 Starting emergency customer extraction...")
        
        emergency_patterns = [
            # Pattern 1: บริษัท + ทุกอย่างจนถึง action words
            r'บริษัท\s*([^,\.\?!]{3,30}?)(?:\s+(?:มี|เคย|ซ่อม|ประวัติ|บริการ)|$)',
            
            # Pattern 2: คลีนิค + ทุกอย่างจนถึง action words  
            r'คลีนิค\s*([^,\.\?!]{3,30}?)(?:\s+(?:มี|เคย|ซ่อม|ประวัติ|บริการ)|$)',
            
            # Pattern 3: Thai/foreign mixed names directly
            r'(แซด\s*คูโรดา|ซาด\s*คูโรดา|แซดคูโรดา|ซาดคูโรดา)',
            
            # Pattern 4: Common business names
            r'(STANLEY|CLARION|HONDA|AGC|HITACHI)\s+(?:มี|เคย|ประวัติ)',
            
            # Pattern 5: Generic company extraction before action
            r'([A-Z]{2,}(?:\s+[A-Z][a-z]+)*)\s+(?:มี|เคย|ประวัติ)',
        ]
        
        customers = []
        for i, pattern in enumerate(emergency_patterns):
            matches = re.findall(pattern, question, re.IGNORECASE)
            for match in matches:
                clean = match.strip()
                if len(clean) > 2:
                    customers.append(clean)
                    logger.info(f"🚨 Emergency pattern {i+1} found: '{clean}'")
        
        # Add hardcoded known problematic names
        known_problematic = ['แซด คูโรดา']
        for name in known_problematic:
            if name in question.lower():
                customers.append(name)
                logger.info(f"🚨 Known problematic name found: '{name}'")
        
        return list(set(customers))

    def _preprocess_question(self, question_lower: str) -> str:
        """Preprocess question for better matching"""
        # Remove extra spaces
        processed = re.sub(r'\s+', ' ', question_lower).strip()
        
        # Normalize Thai-English mixed terms
        normalizations = {
            'standardทั้งหมด': 'standard ทั้งหมด',
            'pmงาน': 'pm งาน',
            'overhaul compressor': 'overhaul compressor',
        }
        
        for old, new in normalizations.items():
            processed = processed.replace(old.lower(), new.lower())
        
        return processed
    
    def _calculate_intent_scores(self, question: str, previous_intent: Optional[str] = None) -> Dict[str, float]:
        """Calculate scores for all intents"""
        scores = {}
        
        for intent, keywords in self.intent_keywords.items():
            score = 0.0
            
            # Strong keywords (high weight)
            for keyword in keywords.get('strong', []):
                if keyword.lower() in question:
                    score += 10.0
                    # Bonus for exact word match
                    if re.search(rf'\b{re.escape(keyword.lower())}\b', question):
                        score += 2.0
            
            # Medium keywords
            for keyword in keywords.get('medium', []):
                if keyword.lower() in question:
                    score += 5.0
                    if re.search(rf'\b{re.escape(keyword.lower())}\b', question):
                        score += 1.0
            
            # Weak keywords
            for keyword in keywords.get('weak', []):
                if keyword.lower() in question:
                    score += 2.0
            
            # Pattern matching (high value)
            for pattern in keywords.get('patterns', []):
                if re.search(pattern, question, re.IGNORECASE):
                    score += 8.0
            
            # Negative keywords (penalty)
            for neg_keyword in keywords.get('negative', []):
                if neg_keyword.lower() in question:
                    score -= 3.0
            
            # Business domain bonus
            score += self._calculate_domain_bonus(question, intent)
            
            # Previous intent bonus (continuity)
            if previous_intent == intent:
                score += 3.0
            
            scores[intent] = max(0.0, score)
        
        return scores
    
    def _calculate_domain_bonus(self, question: str, intent: str) -> float:
        """Calculate domain-specific bonus"""
        bonus = 0.0
        
        # HVAC equipment terms
        for term in self.business_terms['hvac_equipment']:
            if term.lower() in question:
                if intent in ['spare_parts', 'parts_price', 'repair_history']:
                    bonus += 2.0
                elif intent in ['sales', 'overhaul_report']:
                    bonus += 1.0
        
        # Service type terms
        for term in self.business_terms['service_types']:
            if term.lower() in question:
                if intent in ['work_force', 'work_plan', 'work_summary']:
                    bonus += 2.0
                elif intent in ['sales', 'overhaul_report']:
                    bonus += 1.0
        
        # Brand terms
        for term in self.business_terms['brands']:
            if term.lower() in question:
                if intent in ['customer_history', 'repair_history']:
                    bonus += 3.0
                elif intent in ['spare_parts', 'parts_price']:
                    bonus += 2.0
        
        return bonus
    
    def _get_best_intent_with_confidence(self, scores: Dict[str, float], question: str) -> Tuple[str, float]:
        """Get best intent with calculated confidence"""
        if not scores:
            return 'unknown', 0.0
        
        # Find top 2 intents
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        best_intent, best_score = sorted_scores[0]
        second_score = sorted_scores[1][1] if len(sorted_scores) > 1 else 0.0
        
        # Calculate confidence based on score and separation
        if best_score == 0:
            confidence = 0.0
        else:
            # Base confidence from score
            base_confidence = min(best_score / 30.0, 1.0)  # Normalize to max 30 points
            
            # Separation bonus (how much better than second best)
            if second_score > 0:
                separation = (best_score - second_score) / best_score
                separation_bonus = separation * 0.3  # Up to 30% bonus
            else:
                separation_bonus = 0.3  # Max bonus if only one intent scored
            
            confidence = min(base_confidence + separation_bonus, 1.0)
        
        return best_intent, confidence
    
    def _post_process_intent(self, intent: str, confidence: float, entities: Dict, question: str) -> Tuple[str, float]:
        """Post-process intent based on entities and context"""
        
        # Special rules for pricing detection
        if 'เสนอราคา' in question or 'standard' in question.lower():
            if intent not in ['pricing', 'parts_price']:
                # Force pricing intent if clear pricing indicators
                if 'standard' in question.lower() and 'งาน' in question:
                    return 'pricing', max(0.8, confidence)
                elif 'อะไหล่' in question and 'ราคา' in question:
                    return 'parts_price', max(0.8, confidence)
        
        # Boost confidence for clear entity matches
        if entities.get('years') and intent in ['sales', 'sales_analysis', 'customer_history']:
            confidence = min(confidence + 0.1, 1.0)
        
        if entities.get('months') and intent in ['work_plan', 'work_summary', 'sales']:
            confidence = min(confidence + 0.1, 1.0)
        
        if entities.get('products') and intent in ['spare_parts', 'parts_price']:
            confidence = min(confidence + 0.15, 1.0)
        
        # Reduce confidence for ambiguous cases
        if confidence < 0.3 and not entities.get('years') and not entities.get('months'):
            confidence = max(confidence - 0.1, 0.0)
        
        return intent, confidence

    # =================================================================
    # ENTITY EXTRACTION (ปรับปรุงแล้ว)
    # =================================================================
    
    def _extract_entities(self, question: str, intent: str) -> Dict[str, Any]:
        """Enhanced entity extraction"""
        entities = {
            'years': [],
            'months': [],
            'dates': [],
            'products': [],
            'customers': [],
            'amounts': [],
            'job_types': [],
            'brands': []
        }
        
        # Extract years with better patterns
        entities['years'] = self._extract_years(question)
        
        # Extract months
        entities['months'] = self._extract_months(question)
        
        # Extract dates
        entities['dates'] = self._extract_dates(question)
        
        # Extract products/models
        entities['products'] = self._extract_products(question)
        
        # Extract customers/brands
        entities['customers'] = self._extract_customers(question)
        entities['brands'] = self._extract_brands(question)
        
        # Extract job types
        entities['job_types'] = self._extract_job_types(question)
        
        # Extract amounts
        entities['amounts'] = self._extract_amounts(question)
        
        lookback_pattern = r'ย้อนหลัง\s*(\d+)\s*ปี'
        match = re.search(lookback_pattern, question)
        if match:
            years_back = int(match.group(1))
            current_year = 2025  
            # Generate years list
            for i in range(years_back):
                year = current_year - i
                if year >= 2022:  # มีข้อมูลตั้งแต่ 2022
                    entities['years'].append(year)

        # Clean duplicates and validate
        entities = self._clean_and_validate_entities(entities)
        
        return entities
    
    def _extract_years(self, question: str) -> List[int]:
        """Extract years with improved patterns"""
        years = []
        
        # Thai year patterns (พ.ศ.)
        thai_patterns = [
            (r'ปี\s*(\d{4})', lambda m: int(m.group(1))),
            (r'(\d{4})', lambda m: int(m.group(1))),
            (r'25(\d{2})', lambda m: 2000 + int(m.group(1))),  # 2567 -> 67
        ]
        
        for pattern, converter in thai_patterns:
            matches = re.findall(pattern, question)
            for match in matches:
                try:
                    year = int(match) if isinstance(match, str) else converter(re.match(pattern, str(match)))
                    # Convert Thai year to AD
                    if year > 2500:
                        year = year - 543
                    if 2020 <= year <= 2030:
                        years.append(year)
                except:
                    continue
        
        # Handle ranges like 2567-2568
        range_pattern = r'(\d{4})\s*[-–]\s*(\d{4})'
        range_matches = re.findall(range_pattern, question)
        for start_year, end_year in range_matches:
            try:
                start = int(start_year)
                end = int(end_year)
                if start > 2500:
                    start -= 543
                if end > 2500:
                    end -= 543
                if 2020 <= start <= 2030 and 2020 <= end <= 2030:
                    years.extend(range(start, end + 1))
            except:
                continue
        
        return list(set(years))
    
    def _extract_months(self, question: str) -> List[int]:
        """Extract months from question"""
        months = []
        
        for month_name, month_num in self.month_map.items():
            if month_name.lower() in question.lower():
                months.append(month_num)
        
        # Handle ranges like สิงหาคม-กันยายน
        range_pattern = r'(\w+)\s*[-–]\s*(\w+)'
        range_matches = re.findall(range_pattern, question)
        for start_month, end_month in range_matches:
            start_num = self.month_map.get(start_month.lower())
            end_num = self.month_map.get(end_month.lower())
            if start_num and end_num:
                if start_num <= end_num:
                    months.extend(range(start_num, end_num + 1))
                else:
                    # Cross year range (e.g., Nov-Jan)
                    months.extend(range(start_num, 13))
                    months.extend(range(1, end_num + 1))
        
        return list(set(months))
    
    def _extract_dates(self, question: str) -> List[str]:
        """Extract specific dates"""
        dates = []
        
        # Date patterns
        date_patterns = [
            r'\d{1,2}/\d{1,2}/\d{4}',  # DD/MM/YYYY
            r'\d{4}-\d{2}-\d{2}',      # YYYY-MM-DD
            r'\d{1,2}\s+\w+\s+\d{4}'   # DD Month YYYY
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, question)
            dates.extend(matches)
        
        return dates
    
    def _extract_products(self, question: str) -> List[str]:
        """Extract product/model names"""
        products = []
        
        # Common product patterns
        product_patterns = [
            r'EKAC\d+',
            r'RCUG\d+[A-Z]*\d*',
            r'17[A-C]\d{5}[A-Z]?',
            r'EK\s+model\s+(\w+)',
            r'model\s+(\w+)'
        ]
        
        for pattern in product_patterns:
            matches = re.findall(pattern, question, re.IGNORECASE)
            products.extend(matches)
        
        return list(set(products))
    
    def _extract_customers(self, question: str) -> List[str]:
        """
        FIXED: Enhanced customer extraction with robust Thai/foreign name handling
        Addresses the specific issue: "บริษัทแซด คูโรดา" not being detected
        """
        
        # Early exit for non-customer queries
        non_customer_patterns = [
            r'มีลูกค้า.*กี่', r'ลูกค้า.*กี่', r'จำนวนลูกค้า',
            r'ซื้อ.*กี่ครั้ง', r'ลูกค้า.*ทั้งหมด', r'รวม.*ลูกค้า'
        ]
        
        question_lower = question.lower()
        logger.info(f"🔍 Question lower: '{question_lower}'")
        
        for pattern in non_customer_patterns:
            if re.search(pattern, question_lower):
                logger.info(f"🚫 EARLY EXIT: Non-customer query detected")
                return []
        
        customers = []
        question_original = question
        
        # ========================================
        # PHASE 1: ENHANCED KNOWN CUSTOMERS DATABASE
        # ========================================
        
        known_customers = {
            # คลีนิค variations
            'คลีนิคประกอบโรคศิลป์': [
                'คลีนิคประกอบโรคศิลป์ฯ', 'คลีนิคการประกอบโรคศิลปะ', 
                'คลีนิคประกอบโรคศิลปะ'
            ],
            
            # ✅ FIX: Add missing Thai/foreign companies
            'แซด คูโรดา': ['แซด คูโรดา', 'แซดคูโรดา', 'SADEKURODA'],
            
            # English companies
            'CLARION': ['CLARION', 'CLARION ASIA', 'CLARION ASIA (THAILAND)'],
            'STANLEY': ['STANLEY', 'STANLEY ELECTRIC'],
            'HONDA': ['HONDA', 'HONDA AUTOMOBILE', 'ฮอนด้า'],
            'SADESA': ['SADESA', 'Sadesa', 'SADESA (THAILAND)', 'ซาเดซ่า'],
            'AGC': ['AGC', 'เอจีซี', 'บริษัทเอจีซี แฟลทกลาส'],
            
            # Thai companies with foreign names
            'ชินอิทซึ': ['ชินอิทซึ แมกเนติค', 'ชินอิทซึ แม็คเนติคส์'],
            'สหกล': ['สหกลอิควิปเมนท์', 'สหกลอิควีปเม้นท์'],
            
            # Government entities
            'กระทรวงกลาโหม': ['สำนักงานปลัดกระทรวงกลาโหม', 'กลาโหม'],
            'การไฟฟ้า': ['การไฟฟ้านครหลวง', 'การไฟฟ้าฝ่ายผลิต'],
        }
        
        # Check for known customers first (with fuzzy matching)
        for key, variations in known_customers.items():
            for variation in variations:
                # ✅ Case-insensitive and space-tolerant matching
                variation_clean = re.sub(r'\s+', ' ', variation.lower().strip())
                question_clean = re.sub(r'\s+', ' ', question_lower.strip())
                
                if variation_clean in question_clean:
                    customers.extend([variation, key])
                    logger.info(f"✅ Found known customer via fuzzy match: {variation} -> {key}")
        
        if customers:
            logger.info(f"✅ Found known customers: {list(set(customers))}")
            return list(set(customers))
        
        # ========================================
        # PHASE 2: ENHANCED PATTERN EXTRACTION
        # ========================================
        
        # Only extract if there are clear customer indicators
        customer_indicators = [
            'บริษัท', 'บ.', 'บจก.', 'บมจ.', 'หจก.',
            'คลีนิค', 'โรงพยาบาล', 'รพ.', 
            'คุณ', 'สำนัก', 'กรม',
            'ประวัติลูกค้า', 'ลูกค้าชื่อ'
        ]
        
        has_customer_indicator = any(indicator in question for indicator in customer_indicators)
        
        if not has_customer_indicator:
            logger.info(f"🚫 No customer indicators found")
            return []
        
        # ✅ ENHANCED Pattern 1: บริษัท (with flexible suffix handling)
        thai_company_patterns = [
            # Original patterns with required suffixes
            r'บริษัท\s*([^\s].{2,30}?)(?:\s+(?:จำกัด|จก\.|ฯ))',
            r'บ\.\s*([^\s].{2,20}?)(?:\s+(?:จก\.|จำกัด|ฯ))',
            
            # ✅ NEW: Flexible patterns without required suffixes
            r'บริษัท\s*([^\s][^,\.\?!]{2,30}?)(?=\s+(?:มี|ได้|เคย|ซ่อม|ประวัติ|กี่)|$)',
            r'บ\.\s*([^\s][^,\.\?!]{2,20}?)(?=\s+(?:มี|ได้|เคย|ซ่อม|ประวัติ|กี่)|$)',
            
            # ✅ NEW: Thai/foreign mixed names
            r'บริษัท\s*([ก-๙A-Za-z\s]{3,25})(?=\s|$)',
        ]
        
        for pattern in thai_company_patterns:
            matches = re.findall(pattern, question, re.IGNORECASE)
            for match in matches:
                clean = match.strip()
                # ✅ Better validation (allow Thai/foreign mixed names)
                if (len(clean) > 2 and 
                    not any(word in clean.lower() for word in ['มี', 'ได้', 'ไหม', 'กี่', 'ทั้งหมด', 'การ', 'ซื้อ', 'ขาย'])):
                    customers.append(clean)
                    logger.info(f"✅ Extracted via Thai company pattern: '{clean}'")
        
        # ✅ ENHANCED Pattern 2: English Company Names (more flexible)
        if re.search(r'\b[A-Z]{2,}', question):  # At least 2 caps
            eng_patterns = [
                r'\b([A-Z][A-Za-z0-9\s\-&.,()]{2,30}(?:CO\.|LTD\.|LIMITED|INC\.|CORP\.))',
                r'\b([A-Z]{2,}(?:\s+[A-Z]+)*)\b',  # Multiple caps
                r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b'  # Title case
            ]
            
            for pattern in eng_patterns:
                matches = re.findall(pattern, question_original)
                for match in matches:
                    clean = match.strip()
                    # Enhanced exclusion list
                    if (clean.upper() not in ['HVAC', 'FCU', 'AHU', 'VRF', 'THE', 'AND', 'PM', 'OR', 'BUT'] and
                        len(clean) > 2):
                        customers.append(clean)
                        logger.info(f"✅ Extracted via English pattern: '{clean}'")
        
        # ✅ ENHANCED Pattern 3: Thai foreign names (without company prefix)
        if 'บริษัท' in question and not customers:
            # More aggressive extraction for Thai/foreign mixed names
            thai_foreign_patterns = [
                r'บริษัท\s*([ก-๙A-Za-z\s]+)',  # Allow mixed Thai/English
                r'บริษัท\s*([^\s][^ๆ]{2,25})',  # General pattern
            ]
            
            for pattern in thai_foreign_patterns:
                matches = re.findall(pattern, question)
                for match in matches:
                    clean_name = match.strip()
                    # Enhanced filtering
                    excluded_words = ['มี', 'ได้', 'ไหม', 'การ', 'ประวัติ', 'ซ่อม', 'บ้าง', 'อะไร', 'เท่า', 'ไหร่']
                    if (clean_name not in excluded_words and 
                        len(clean_name) > 3 and
                        not any(word in clean_name.lower() for word in excluded_words)):
                        customers.append(clean_name)
                        logger.info(f"✅ Extracted Thai foreign name: '{clean_name}'")
        
        # ✅ ENHANCED Emergency fallbacks for common problematic names
        emergency_fallbacks = [
            ('แซด คูโรดา', ['แซด คูโรดา', 'แซดคูโรดา']),
            ('ชินอิทซึ', ['ชินอิทซึ', 'SHINETSU']),
            ('สหกล', ['สหกล', 'สหกลอิควิป']),
        ]
        
        if not customers:
            for canonical_name, variations in emergency_fallbacks:
                for variant in variations:
                    if variant in question:
                        customers.append(canonical_name)
                        logger.info(f"✅ Emergency fallback: {variant} -> {canonical_name}")
                        break
        
        # ✅ Pattern 4: คลีนิค (enhanced)
        if 'คลีนิค' in question and not customers:
            clinic_patterns = [
                r'(คลีนิค[ก-๙\s]+?)(?=\s+(?:มี|เคย|ได้|ซ่อม|ประวัติ|กี่)|$)',
                r'(คลีนิค[^กี่ทั้งหมดการซื้อขาย]{3,30}?)(?=\s|$)',
            ]
            
            for pattern in clinic_patterns:
                matches = re.findall(pattern, question)
                for match in matches:
                    clinic_name = match.strip()
                    if len(clinic_name) > 6:  # คลีนิค = 6 chars + name
                        customers.append(clinic_name)
                        logger.info(f"✅ Extracted clinic: '{clinic_name}'")
        
        # ========================================
        # PHASE 3: CLEAN AND VALIDATE
        # ========================================
        
        cleaned_customers = []
        for customer in customers:
            clean = customer.strip()
            clean = re.sub(r'\s+', ' ', clean)  # Normalize spaces
            clean = clean.rstrip(',.;:?!-')
            
            # Final validation
            if (len(clean) > 2 and 
                not any(bad_word in clean.lower() for bad_word in 
                    ['กี่ครั้ง', 'ทั้งหมด', 'มากที่สุด', 'การซื้อขาย', 'วิเคราะห์', 'มีอะไร'])):
                cleaned_customers.append(clean)
        
        # Remove duplicates while preserving order
        cleaned_customers = list(dict.fromkeys(cleaned_customers))
        
        # ========================================
        # LOGGING AND RETURN
        # ========================================
        
        if cleaned_customers:
            logger.info(f"✅ Final extracted customers: {cleaned_customers}")
        else:
            logger.info(f"ℹ️ No customers extracted - will be treated as general query")
        
        return cleaned_customers


    
    def _extract_brands(self, question: str) -> List[str]:
        """Extract brand names"""
        brands = []
        
        for brand in self.business_terms['brands']:
            if brand.lower() in question.lower():
                brands.append(brand)
        
        return list(set(brands))
    
    def _extract_job_types(self, question: str) -> List[str]:
        """Extract job types"""
        job_types = []
        
        job_keywords = {
            'overhaul': ['overhaul', 'โอเวอร์ฮอล', 'ซ่อมใหญ่'],
            'replacement': ['replacement', 'เปลี่ยน', 'แทนที่'],
            'PM': ['pm', 'บำรุงรักษา', 'maintenance'],
            'service': ['service', 'บริการ', 'ซ่อม']
        }
        
        for job_type, keywords in job_keywords.items():
            for keyword in keywords:
                if keyword.lower() in question.lower():
                    job_types.append(job_type)
                    break
        
        return list(set(job_types))
    
    def _extract_amounts(self, question: str) -> List[str]:
        """Extract amounts/numbers"""
        amounts = []
        
        # Number patterns
        amount_patterns = [
            r'\d{1,3}(?:,\d{3})*\s*บาท',
            r'\$\d{1,3}(?:,\d{3})*',
            r'\d+\s*เท่าไหร่'
        ]
        
        for pattern in amount_patterns:
            matches = re.findall(pattern, question)
            amounts.extend(matches)
        
        return amounts
    
    def _clean_and_validate_entities(self, entities: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and validate extracted entities"""
        cleaned = {}
        
        for key, value in entities.items():
            if isinstance(value, list):
                # Remove duplicates and empty values
                cleaned_list = list(set([v for v in value if v and str(v).strip()]))
                
                # Sort for consistency
                if key in ['years', 'months']:
                    cleaned_list.sort()
                
                cleaned[key] = cleaned_list
            else:
                cleaned[key] = value
        
        return cleaned

    # =================================================================
    # UTILITY METHODS
    # =================================================================
    
    def get_intent_confidence_report(self, question: str) -> Dict[str, Any]:
        """Get detailed confidence report for debugging"""
        result = self.detect_intent_and_entities(question)
        
        # Sort scores by value
        sorted_scores = sorted(result['scores'].items(), key=lambda x: x[1], reverse=True)
        
        return {
            'question': question,
            'detected_intent': result['intent'],
            'confidence': result['confidence'],
            'entities': result['entities'],
            'all_scores': sorted_scores,
            'top_3_intents': sorted_scores[:3]
        }
    
    def test_intent_accuracy(self, test_cases: List[Tuple[str, str]]) -> Dict[str, Any]:
        """Test intent detection accuracy with known cases"""
        correct = 0
        total = len(test_cases)
        results = []
        
        for question, expected_intent in test_cases:
            result = self.detect_intent_and_entities(question)
            detected = result['intent']
            confidence = result['confidence']
            
            is_correct = detected == expected_intent
            if is_correct:
                correct += 1
            
            results.append({
                'question': question,
                'expected': expected_intent,
                'detected': detected,
                'confidence': confidence,
                'correct': is_correct
            })
        
        return {
            'accuracy': correct / total if total > 0 else 0,
            'correct_count': correct,
            'total_count': total,
            'results': results
        }
  