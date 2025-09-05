# tests/integration/test_full_flow.py
import pytest
from agents.core.orchestrator import ImprovedDualModelDynamicAISystem

class TestFullFlow:
    
    @pytest.fixture
    async def ai_system(self):
        """Real system with real connections"""
        system = ImprovedDualModelDynamicAISystem()
        # Test connection first
        connected = await system.ollama_client.test_connection()
        if not connected:
            pytest.skip("Ollama not available")
        return system
    
    @pytest.mark.integration
    async def test_sales_query(self, ai_system):
        """Test complete sales query flow"""
        result = await ai_system.process_any_question(
            "รายได้ของ CLARION ปี 2024 เท่าไหร่"
        )
        
        assert result['success'] == True
        assert result['intent'] == 'sales_analysis'
        assert 'CLARION' in result['entities'].get('customers', [])[0]
        assert 2024 in result['entities'].get('years', [])
        assert len(result['answer']) > 50
    
    @pytest.mark.integration  
    async def test_work_force_query(self, ai_system):
        """Test work force query"""
        result = await ai_system.process_any_question(
            "มีงาน PM เดือนสิงหาคม 2025 กี่งาน"
        )
        
        assert result['success'] == True
        assert result['intent'] in ['work_force', 'work_plan']
        assert 8 in result['entities'].get('months', [])