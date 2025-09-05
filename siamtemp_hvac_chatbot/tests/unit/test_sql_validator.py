# tests/unit/test_sql_validator.py
import pytest
from agents.sql.validator import SQLValidator, ValidationResult

class TestSQLValidator:
    
    def test_dangerous_operation_detection(self):
        """Test dangerous SQL detection"""
        validator = SQLValidator()
        
        dangerous_sql = "DROP TABLE users;"
        is_valid, fixed_sql, issues = validator.validate_and_fix(dangerous_sql)
        
        assert is_valid == False
        assert "Dangerous operation" in str(issues)
    
    def test_date_format_fix(self):
        """Test date format fixing"""
        validator = SQLValidator()
        
        sql = "SELECT * FROM v_work_force WHERE date LIKE '%2025-08%'"
        is_valid, fixed_sql, issues = validator.validate_and_fix(sql)
        
        assert "BETWEEN" in fixed_sql
        assert "'2025-08-01'" in fixed_sql
        assert "'2025-08-31'" in fixed_sql
    
    def test_add_limit_clause(self):
        """Test LIMIT clause addition"""
        validator = SQLValidator()
        
        sql = "SELECT * FROM v_sales2024"
        is_valid, fixed_sql, issues = validator.validate_and_fix(sql)
        
        assert "LIMIT" in fixed_sql