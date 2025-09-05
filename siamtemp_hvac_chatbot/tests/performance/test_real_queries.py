# tests/integration/test_real_queries.py
import pytest
from agents.core.orchestrator import ImprovedDualModelDynamicAISystem

@pytest.mark.integration
class TestRealQueries:
    
    @pytest.fixture(scope="class")
    async def ai_system(self):
        """Real system with actual connections"""
        system = ImprovedDualModelDynamicAISystem()
        # Check if services are available
        try:
            await system.ollama_client.test_connection()
            await system.db_handler.execute_query("SELECT 1")
        except:
            pytest.skip("External services not available")
        return system
    
    @pytest.mark.parametrize("question,expected_intent", [
        ("รายได้ปี 2024", "sales_analysis"),
        ("งาน PM เดือนนี้", "work_force"),
        ("ราคาอะไหล่ EKAC460", "parts_price"),
        ("ประวัติลูกค้า CLARION", "customer_history"),
    ])
    async def test_real_questions(self, ai_system, question, expected_intent):
        """Test with real questions"""
        result = await ai_system.process_any_question(question)
        
        assert result['success'] == True
        assert result['intent'] == expected_intent
        assert len(result['answer']) > 50