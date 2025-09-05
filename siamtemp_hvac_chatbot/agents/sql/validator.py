# agents/sql/validator.py
"""
Refactored SQLValidator using Chain of Responsibility Pattern
Modular validation rules that are easy to maintain and extend
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple, Union
from abc import ABC, abstractmethod
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# =============================================================================
# VALIDATION RESULT MODEL
# =============================================================================

@dataclass
class ValidationResult:
    """Result of SQL validation"""
    is_valid: bool
    sql: str
    issues: List[str]
    fixes_applied: List[str]

# =============================================================================
# VALIDATOR INTERFACE
# =============================================================================

class SQLValidationRule(ABC):
    """Abstract base class for validation rules"""
    
    @abstractmethod
    def validate(self, sql: str) -> ValidationResult:
        """Validate and fix SQL according to this rule"""
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """Get rule name for logging"""
        pass

# =============================================================================
# CONCRETE VALIDATION RULES
# =============================================================================

class DangerousOperationValidator(SQLValidationRule):
    """Check for dangerous SQL operations"""
    
    DANGEROUS_OPS = [
        'DROP', 'DELETE', 'TRUNCATE', 'ALTER',
        'CREATE', 'UPDATE', 'INSERT', 'GRANT', 'REVOKE'
    ]
    
    def validate(self, sql: str) -> ValidationResult:
        sql_upper = sql.upper()
        
        for op in self.DANGEROUS_OPS:
            if re.search(rf'\b{op}\b', sql_upper):
                return ValidationResult(
                    is_valid=False,
                    sql=sql,
                    issues=[f"Dangerous operation detected: {op}"],
                    fixes_applied=[]
                )
        
        return ValidationResult(is_valid=True, sql=sql, issues=[], fixes_applied=[])
    
    def get_name(self) -> str:
        return "DangerousOperationValidator"


class DateFormatValidator(SQLValidationRule):
    """Fix date format issues especially for v_work_force"""
    
    def validate(self, sql: str) -> ValidationResult:
        issues = []
        fixes = []
        fixed_sql = sql
        
        # Fix LIKE patterns for dates in v_work_force
        if 'v_work_force' in sql.lower():
            # Pattern: date LIKE '%2025-08%' → date::date BETWEEN '2025-08-01' AND '2025-08-31'
            like_pattern = r"date\s+LIKE\s+'%(\d{4})-(\d{2})%'"
            matches = re.findall(like_pattern, sql, re.IGNORECASE)
            
            for year, month in matches:
                old_pattern = f"date LIKE '%{year}-{month}%'"
                new_pattern = f"date::date BETWEEN '{year}-{month}-01' AND '{year}-{month}-31'"
                fixed_sql = fixed_sql.replace(old_pattern, new_pattern)
                fixes.append(f"Fixed date LIKE pattern to BETWEEN")
        
        # Convert Thai years (2567 → 2024)
        thai_years = {'2565': '2022', '2566': '2023', '2567': '2024', '2568': '2025'}
        for thai_year, ad_year in thai_years.items():
            if thai_year in fixed_sql:
                fixed_sql = fixed_sql.replace(thai_year, ad_year)
                fixes.append(f"Converted Thai year {thai_year} to {ad_year}")
        
        return ValidationResult(
            is_valid=True,
            sql=fixed_sql,
            issues=issues,
            fixes_applied=fixes
        )
    
    def get_name(self) -> str:
        return "DateFormatValidator"


class ColumnExistenceValidator(SQLValidationRule):
    """Validate columns exist in views"""
    
    def __init__(self, schema_config: Dict):
        self.view_columns = schema_config.get('VIEW_COLUMNS', {})
        self.column_fixes = {
            # Common mistakes
            'job_type': None,  # Needs CASE statement
            'amount': 'total_num',
            'price': 'unit_price_num',
            'quantity': 'balance_num'
        }
    
    def validate(self, sql: str) -> ValidationResult:
        issues = []
        fixes = []
        fixed_sql = sql
        
        # Extract view names
        views = self._extract_views(sql)
        
        for view_name in views:
            if view_name not in self.view_columns:
                continue
            
            valid_columns = self.view_columns[view_name]
            invalid_columns = self._find_invalid_columns(sql, valid_columns)
            
            for invalid_col in invalid_columns:
                if invalid_col in self.column_fixes:
                    replacement = self.column_fixes[invalid_col]
                    if replacement:
                        fixed_sql = re.sub(rf'\b{invalid_col}\b', replacement, fixed_sql, flags=re.IGNORECASE)
                        fixes.append(f"Fixed column: {invalid_col} → {replacement}")
                    else:
                        issues.append(f"Column '{invalid_col}' needs special handling")
                else:
                    issues.append(f"Invalid column '{invalid_col}' in {view_name}")
        
        return ValidationResult(
            is_valid=len(issues) == 0,
            sql=fixed_sql,
            issues=issues,
            fixes_applied=fixes
        )
    
    def _extract_views(self, sql: str) -> List[str]:
        """Extract view names from SQL"""
        pattern = r'(?:FROM|JOIN)\s+(v_\w+)'
        matches = re.findall(pattern, sql, re.IGNORECASE)
        return list(set(matches))
    
    def _find_invalid_columns(self, sql: str, valid_columns: List[str]) -> List[str]:
        """Find columns that don't exist"""
        # This is simplified - real implementation would be more sophisticated
        words = re.findall(r'\b\w+\b', sql)
        invalid = []
        
        for word in words:
            # Skip SQL keywords
            if word.upper() in ['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'GROUP', 'BY', 'ORDER', 'LIMIT']:
                continue
            # Check if it looks like a column and is not valid
            if word.lower() not in [c.lower() for c in valid_columns] and '_' in word:
                invalid.append(word)
        
        return invalid
    
    def get_name(self) -> str:
        return "ColumnExistenceValidator"


class JobTypeValidator(SQLValidationRule):
    """Handle job_type references in v_work_force"""
    
    JOB_TYPE_CASE = """
    CASE 
        WHEN job_description_pm = true THEN 'PM'
        WHEN job_description_replacement = true THEN 'Replacement'
        WHEN job_description_overhaul IS NOT NULL THEN 'Overhaul'
        WHEN job_description_start_up IS NOT NULL THEN 'Start Up'
        WHEN job_description_support_all IS NOT NULL THEN 'Support'
        WHEN job_description_cpa = true THEN 'CPA'
        ELSE 'Other'
    END"""
    
    def validate(self, sql: str) -> ValidationResult:
        fixes = []
        fixed_sql = sql
        
        if 'v_work_force' in sql.lower() and 'job_type' in sql.lower():
            # Replace job_type with CASE statement
            pattern = r'\bjob_type\b'
            replacement = f"({self.JOB_TYPE_CASE.strip()}) AS job_type"
            fixed_sql = re.sub(pattern, replacement, fixed_sql, flags=re.IGNORECASE)
            fixes.append("Replaced job_type with CASE statement")
        
        return ValidationResult(
            is_valid=True,
            sql=fixed_sql,
            issues=[],
            fixes_applied=fixes
        )
    
    def get_name(self) -> str:
        return "JobTypeValidator"


class BasicSQLStructureValidator(SQLValidationRule):
    """Ensure basic SQL structure is valid"""
    
    def validate(self, sql: str) -> ValidationResult:
        issues = []
        fixes = []
        fixed_sql = sql
        
        # Check basic structure
        sql_upper = sql.upper()
        if 'SELECT' not in sql_upper:
            issues.append("Missing SELECT statement")
            return ValidationResult(False, sql, issues, [])
        
        if 'FROM' not in sql_upper:
            issues.append("Missing FROM clause")
            return ValidationResult(False, sql, issues, [])
        
        # Add LIMIT if missing
        if 'LIMIT' not in sql_upper:
            fixed_sql = fixed_sql.rstrip().rstrip(';') + ' LIMIT 1000;'
            fixes.append("Added LIMIT clause")
        
        # Add semicolon if missing
        if not fixed_sql.rstrip().endswith(';'):
            fixed_sql = fixed_sql.rstrip() + ';'
            fixes.append("Added semicolon")
        
        # Fix parentheses
        open_count = fixed_sql.count('(')
        close_count = fixed_sql.count(')')
        if open_count != close_count:
            if open_count > close_count:
                fixed_sql += ')' * (open_count - close_count)
                fixes.append(f"Added {open_count - close_count} closing parentheses")
            else:
                issues.append(f"Too many closing parentheses")
        
        return ValidationResult(
            is_valid=len(issues) == 0,
            sql=fixed_sql,
            issues=issues,
            fixes_applied=fixes
        )
    
    def get_name(self) -> str:
        return "BasicSQLStructureValidator"

# =============================================================================
# MAIN SQL VALIDATOR - REFACTORED
# =============================================================================

class SQLValidator:
    """
    Refactored SQL Validator using Chain of Responsibility
    Easy to add/remove/modify validation rules
    """
    
    def __init__(self, prompt_manager=None):
        """Initialize with optional prompt manager for schema info"""
        # Get schema config from prompt manager if available
        schema_config = {}
        if prompt_manager and hasattr(prompt_manager, 'VIEW_COLUMNS'):
            schema_config['VIEW_COLUMNS'] = prompt_manager.VIEW_COLUMNS
        else:
            schema_config['VIEW_COLUMNS'] = self._get_default_schema()
        
        # Initialize validation chain
        self.validators = self._initialize_validators(schema_config)
        
        logger.info(f"SQL Validator initialized with {len(self.validators)} rules")
    
    def _initialize_validators(self, schema_config: Dict) -> List[SQLValidationRule]:
        """Initialize all validation rules in order"""
        return [
            DangerousOperationValidator(),
            DateFormatValidator(),
            ColumnExistenceValidator(schema_config),
            JobTypeValidator(),
            BasicSQLStructureValidator()
        ]
    
    def validate_and_fix(self, sql: str) -> Tuple[bool, str, List[str]]:
        """
        Main validation method - clean and simple
        """
        if not sql:
            return False, sql, ["Empty SQL query"]
        
        all_issues = []
        all_fixes = []
        current_sql = sql.strip()
        
        # Run through validation chain
        for validator in self.validators:
            logger.debug(f"Running {validator.get_name()}")
            
            result = validator.validate(current_sql)
            
            # Collect issues and fixes
            all_issues.extend(result.issues)
            all_fixes.extend(result.fixes_applied)
            
            # Update SQL with fixes
            current_sql = result.sql
            
            # Stop if critical validation fails
            if not result.is_valid and isinstance(validator, DangerousOperationValidator):
                return False, current_sql, all_issues
        
        # Log summary if fixes were applied
        if all_fixes:
            logger.info(f"Applied {len(all_fixes)} fixes: {all_fixes[:3]}...")
        
        return len(all_issues) == 0, current_sql, all_issues + all_fixes
    
    def add_validator(self, validator: SQLValidationRule, position: Optional[int] = None):
        """Add a new validation rule"""
        if position is None:
            self.validators.append(validator)
        else:
            self.validators.insert(position, validator)
        logger.info(f"Added validator: {validator.get_name()}")
    
    def remove_validator(self, validator_name: str) -> bool:
        """Remove a validation rule by name"""
        for i, validator in enumerate(self.validators):
            if validator.get_name() == validator_name:
                del self.validators[i]
                logger.info(f"Removed validator: {validator_name}")
                return True
        return False
    
    def get_validation_report(self, sql: str) -> Dict[str, Any]:
        """Get detailed validation report"""
        is_valid, fixed_sql, issues = self.validate_and_fix(sql)
        
        return {
            'original_sql': sql,
            'fixed_sql': fixed_sql,
            'is_valid': is_valid,
            'issues_count': len(issues),
            'issues': issues,
            'sql_changed': sql != fixed_sql,
            'validators_used': [v.get_name() for v in self.validators]
        }
    
    def _get_default_schema(self) -> Dict:
        """Get default schema if prompt manager not available"""
        return {
            'v_sales2024': [
                'id', 'job_no', 'customer_name', 'description',
                'overhaul_num', 'replacement_num', 'service_num',
                'parts_num', 'product_num', 'solution_num', 'total_revenue'
            ],
            'v_work_force': [
                'id', 'date', 'customer', 'project', 'detail', 
                'service_group', 'success', 'unsuccessful'
            ],
            'v_spare_part': [
                'id', 'wh', 'product_code', 'product_name',
                'balance_num', 'unit_price_num', 'total_num'
            ]
        }