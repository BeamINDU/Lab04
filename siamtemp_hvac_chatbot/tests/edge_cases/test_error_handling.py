# tests/edge_cases/test_error_handling.py

import asyncio
from unittest.mock import AsyncMock


class TestEdgeCases:
    
    async def test_empty_query(self, ai_system):
        """Test empty question handling"""
        result = await ai_system.process_any_question("")
        assert result['success'] == False
        assert 'error' in result
    
    async def test_sql_injection_attempt(self, ai_system):
        """Test SQL injection prevention"""
        malicious = "'; DROP TABLE users; --"
        result = await ai_system.process_any_question(malicious)
        # Should either fail safely or clean the input
        assert "DROP TABLE" not in result.get('sql_query', '')
    
    async def test_thai_encoding_issues(self, ai_system):
        """Test Thai character handling"""
        thai_query = "ราคาอะไหล่ ËÒÁÔ¤¹ÔËµ"  # Broken encoding
        result = await ai_system.process_any_question(thai_query)
        # Should handle gracefully
        assert result is not None
    
    async def test_timeout_handling(self, ai_system):
        """Test timeout scenarios"""
        # Mock slow response
        ai_system.ollama_client.generate = AsyncMock(
            side_effect=asyncio.TimeoutError()
        )
        
        result = await ai_system.process_any_question("test")
        assert result['success'] == False
        assert 'timeout' in result.get('error', '').lower()