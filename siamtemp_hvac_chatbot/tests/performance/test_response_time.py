# tests/performance/test_response_time.py
import time
import asyncio
import statistics
from agents import (
    ImprovedDualModelDynamicAISystem 
)
async def test_response_times():
    """Measure response times for different query types"""
    system = ImprovedDualModelDynamicAISystem()
    
    test_queries = [
        ("Simple", "รายได้ปี 2024"),
        ("Medium", "งาน overhaul ของ CLARION ปี 2024-2025"),
        ("Complex", "เปรียบเทียบรายได้ทุกปีแยกตามประเภทบริการ")
    ]
    
    results = {}
    for query_type, question in test_queries:
        times = []
        for _ in range(5):  # Run 5 times
            start = time.time()
            await system.process_any_question(question)
            times.append(time.time() - start)
        
        results[query_type] = {
            'avg': statistics.mean(times),
            'median': statistics.median(times),
            'max': max(times),
            'min': min(times)
        }
    
    # Assert performance thresholds
    assert results['Simple']['avg'] < 3.0  # Simple < 3s
    assert results['Medium']['avg'] < 5.0  # Medium < 5s
    assert results['Complex']['avg'] < 10.0  # Complex < 10s
    
    return results