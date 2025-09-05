# tests/unit/test_orchestrator.py - Version ที่แก้ bugs แล้ว
import pytest
from unittest.mock import Mock, AsyncMock, MagicMock
from agents.core.orchestrator import ImprovedDualModelDynamicAISystem, QueryContext

class TestOrchestrator:
    
    @pytest.fixture
    def ai_system(self):
        """Create system with mocked dependencies"""
        system = ImprovedDualModelDynamicAISystem()
        
        # Mock all external dependencies to avoid real connections
        system.ollama_client = AsyncMock()
        system.db_handler = AsyncMock()
        system.intent_detector = MagicMock()
        system.sql_validator = MagicMock()
        system.data_cleaner = MagicMock()
        
        # Mock the database connection that's failing
        system.db_handler._connect = MagicMock()
        
        return system
    
    async def test_intent_detection(self, ai_system):
        """Test intent detection step"""
        # Mock intent detector response
        ai_system.intent_detector.detect_intent_and_entities.return_value = {
            'intent': 'sales_analysis',
            'entities': {'years': [2024]},
            'confidence': 0.95
        }
        
        # Initialize stats to avoid division by zero
        ai_system.stats['total_queries'] = 1
        
        context = QueryContext("รายได้ปี 2024", "company-a", "test")
        await ai_system._detect_intent(context)
        
        assert context.intent == 'sales_analysis'
        assert context.confidence == 0.95
        assert context.entities.get('years') == [2024]
    
    async def test_missing_info_identification(self, ai_system):
        """Test missing info detection"""
        context = QueryContext("งานเดือนไหน", "company-a", "test")
        context.entities = {}
        
        missing = ai_system._identify_missing_info(context)
        assert len(missing) > 0
        assert "ระบุเดือน/ปี" in missing[0]
    
    async def test_sql_generation(self, ai_system):
        """Test SQL generation with mock"""
        # Mock Ollama response
        ai_system.ollama_client.generate.return_value = "SELECT * FROM v_sales2024 LIMIT 10;"
        
        # Mock SQL validator
        ai_system.sql_validator.validate_and_fix.return_value = (
            True, 
            "SELECT * FROM v_sales2024 LIMIT 10;",
            []
        )
        
        context = QueryContext("รายได้ปี 2024", "company-a", "test")
        context.intent = 'sales_analysis'
        context.entities = {'years': [2024]}
        
        sql = await ai_system._generate_sql(context)
        assert "SELECT" in sql
        assert "v_sales2024" in sql
        
    async def test_error_handling(self, ai_system):
        """Test error handling"""
        # Mock to raise error
        ai_system.ollama_client.test_connection.side_effect = Exception("Connection failed")
        
        result = await ai_system.process_any_question("test query")
        
        assert result['success'] == False
        assert 'error' in result
        assert result['error'] == "Connection failed"
    
    async def test_full_flow_success(self, ai_system):
        """Test successful full flow"""
        # Mock all steps
        ai_system.ollama_client.test_connection.return_value = True
        ai_system.intent_detector.detect_intent_and_entities.return_value = {
            'intent': 'sales_analysis',
            'entities': {'years': [2024]},
            'confidence': 0.95
        }
        ai_system.ollama_client.generate.side_effect = [
            "SELECT * FROM v_sales2024;",  # SQL generation
            "พบรายได้ทั้งหมด 10 ล้านบาท"  # Response generation
        ]
        ai_system.sql_validator.validate_and_fix.return_value = (
            True, "SELECT * FROM v_sales2024;", []
        )
        ai_system.db_handler.execute_query.return_value = [
            {'total_revenue': 10000000}
        ]
        ai_system.data_cleaner.clean_results.return_value = (
            [{'total_revenue': 10000000}], {}
        )
        ai_system.data_cleaner.create_summary_insights.return_value = {}
        
        result = await ai_system.process_any_question("รายได้ปี 2024")
        
        assert result['success'] == True
        assert result['intent'] == 'sales_analysis'
        assert len(result['answer']) > 0