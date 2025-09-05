# tests/performance/test_baseline.py
import time
import pytest
from statistics import mean, stdev

@pytest.mark.performance
async def test_response_time_baseline(ai_system):
    """Establish performance baseline"""
    
    queries = [
        "รายได้ปี 2024",  # Simple
        "งาน overhaul ของ CLARION ปี 2024-2025",  # Medium
        "เปรียบเทียบรายได้ทุกปีแยกตามประเภท"  # Complex
    ]
    
    baselines = {
        "Simple": 3.0,   # seconds
        "Medium": 5.0,
        "Complex": 10.0
    }
    
    for i, query in enumerate(queries):
        times = []
        for _ in range(3):
            start = time.time()
            await ai_system.process_any_question(query)
            times.append(time.time() - start)
        
        avg_time = mean(times)
        query_type = ["Simple", "Medium", "Complex"][i]
        
        assert avg_time < baselines[query_type], \
            f"{query_type} query took {avg_time:.2f}s (limit: {baselines[query_type]}s)"
        
        print(f"{query_type}: {avg_time:.2f}s ± {stdev(times):.2f}s")