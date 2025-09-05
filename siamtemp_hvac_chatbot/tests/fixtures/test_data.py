# tests/fixtures/test_data.py

import pytest


TEST_DATA = {
    'sales_queries': [
        "รายได้ปี 2024",
        "ยอดขาย overhaul เดือนมิถุนายน",
        "Top 10 ลูกค้าปี 2025"
    ],
    'work_queries': [
        "งานเดือนนี้",
        "แผนงาน PM เดือนหน้า",
        "งานที่ยังไม่เสร็จ"
    ],
    'parts_queries': [
        "ราคาอะไหล่ EKAC460",
        "stock คงเหลือ",
        "อะไหล่ที่หมด"
    ]
}

@pytest.fixture
def sample_query_set():
    """Provide test queries"""
    return TEST_DATA