# tests/unit/test_intent_detector.py
import pytest
from agents.nlp.intent_detector import ImprovedIntentDetector

class TestIntentDetector:
    
    def test_sales_intent(self):
        """Test sales intent detection"""
        detector = ImprovedIntentDetector()
        
        result = detector.detect_intent_and_entities("รายได้ปี 2024 เท่าไหร่")
        
        assert result['intent'] in ['sales', 'sales_analysis']
        assert 2024 in result['entities']['years']
        assert result['confidence'] > 0.5
    
    def test_work_force_intent(self):
        """Test work force intent detection"""
        detector = ImprovedIntentDetector()
        
        result = detector.detect_intent_and_entities("มีงาน PM เดือนสิงหาคม")
        
        assert result['intent'] in ['work_force', 'work_plan']
        assert 8 in result['entities']['months']
    
    def test_parts_price_intent(self):
        """Test parts price intent detection"""
        detector = ImprovedIntentDetector()
        
        result = detector.detect_intent_and_entities("ราคาอะไหล่ EKAC460")
        
        assert result['intent'] in ['parts_price', 'spare_parts']
        assert 'EKAC460' in str(result['entities'].get('products', []))