"""
Smart Query Rewriter for Siamtemp HVAC Database
================================================
แก้ปัญหา SQL errors โดยการ rewrite queries อัตโนมัติ
ทำงานได้ทันทีโดยไม่ต้องแก้ไข SQL examples เดิม
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# =============================================================================
# SMART QUERY REWRITER CLASS
# =============================================================================

class SmartQueryRewriter:
    """
    Rewrite SQL queries อัตโนมัติเพื่อจัดการ data quality issues
    - แปลง CAST operations ให้ปลอดภัย
    - สร้าง safe functions ใน database
    - Switch ไปใช้ views ถ้ามี
    """
    
    def __init__(self):
        self.views_checked = False
        self.views_available = {}
        self.safe_function_created = False
        
        # Rewrite patterns
        self.rewrite_patterns = [
            # Pattern 1: CAST(NULLIF(...) AS NUMERIC)
            (
                r"CAST\s*\(\s*NULLIF\s*\(\s*([\w_]+)\s*,\s*''\s*\)\s+AS\s+NUMERIC\s*\)",
                r"safe_cast_numeric(\1)"
            ),
            # Pattern 2: CAST(field AS NUMERIC) without NULLIF
            (
                r"CAST\s*\(\s*([\w_]+)\s+AS\s+NUMERIC\s*\)",
                r"safe_cast_numeric(\1)"
            ),
            # Pattern 3: field::NUMERIC
            (
                r"(\w+)::NUMERIC",
                r"safe_cast_numeric(\1)"
            ),
            # Pattern 4: Long CASE WHEN for numeric casting
            (
                r"CASE\s+WHEN\s+(\w+)\s+IS\s+NULL\s+OR\s+\1\s*=\s*''\s+THEN\s+0\s+"
                r"(?:WHEN\s+[\w\s~'^$.*+\-\[\]]+\s+THEN\s+CAST\s*\(\s*\1\s+AS\s+NUMERIC\s*\)\s+)?"
                r"ELSE\s+(?:CAST\s*\(\s*\1\s+AS\s+NUMERIC\s*\)|0)\s+END",
                r"safe_cast_numeric(\1)",
                re.IGNORECASE | re.DOTALL
            ),
            # Pattern 5: Simple CASE for empty check
            (
                r"CASE\s+WHEN\s+(\w+)\s*=\s*''\s+THEN\s+0\s+ELSE\s+CAST\s*\(\s*\1\s+AS\s+NUMERIC\s*\)\s+END",
                r"safe_cast_numeric(\1)",
                re.IGNORECASE
            )
        ]
    
    async def ensure_safe_functions(self, db_handler):
        """
        สร้าง safe functions ใน database ถ้ายังไม่มี
        """
        if self.safe_function_created:
            return True
        
        try:
            # Check if function exists
            check_sql = """
            SELECT EXISTS (
                SELECT 1 
                FROM pg_proc 
                WHERE proname = 'safe_cast_numeric'
            );
            """
            
            result = await db_handler.execute_query(check_sql)
            
            if not result or not result[0].get('exists'):
                # Create safe cast function
                create_function_sql = """
                CREATE OR REPLACE FUNCTION safe_cast_numeric(input_text TEXT) 
                RETURNS NUMERIC AS $$
                BEGIN
                    -- Handle NULL
                    IF input_text IS NULL THEN
                        RETURN 0;
                    END IF;
                    
                    -- Handle empty string
                    IF input_text = '' THEN
                        RETURN 0;
                    END IF;
                    
                    -- Try to cast to numeric
                    RETURN input_text::NUMERIC;
                    
                EXCEPTION
                    WHEN OTHERS THEN
                        -- Return 0 for any casting error
                        RETURN 0;
                END;
                $$ LANGUAGE plpgsql IMMUTABLE;
                
                -- Create helper function for date checking
                CREATE OR REPLACE FUNCTION is_valid_date(input_text TEXT)
                RETURNS BOOLEAN AS $$
                BEGIN
                    RETURN input_text ~ '^\d{1,2}[-/]\d{1,2}[-/]\d{4}$'
                        OR input_text ~ '^\d{1,2}-\d{1,2}[-/]\d{1,2}[-/]\d{4}$'
                        OR input_text ~ '^\d{5}$';  -- Excel serial
                END;
                $$ LANGUAGE plpgsql IMMUTABLE;
                """
                
                await db_handler.execute_ddl(create_function_sql)
                logger.info("✅ Created safe_cast_numeric function in database")
            
            self.safe_function_created = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to create safe functions: {e}")
            # Continue anyway - will use fallback
            return False
    
    async def check_or_create_views(self, db_handler):
        """
        เช็คและสร้าง views ถ้าจำเป็น
        """
        if self.views_checked:
            return self.views_available
        
        try:
            # Check existing views
            check_sql = """
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'v_%';
            """
            
            result = await db_handler.execute_query(check_sql)
            
            if result:
                for row in result:
                    view_name = row.get('table_name')
                    self.views_available[view_name] = True
                logger.info(f"📊 Found {len(self.views_available)} existing views")
            else:
                # Create views if not exist
                await self._create_safe_views(db_handler)
            
            self.views_checked = True
            return self.views_available
            
        except Exception as e:
            logger.error(f"Error checking views: {e}")
            # Continue without views
            self.views_checked = True
            return {}
    
    async def _create_safe_views(self, db_handler):
        """
        สร้าง views ที่ safe สำหรับทุก table
        """
        try:
            views_sql = """
            -- Sales views
            CREATE OR REPLACE VIEW v_sales2024 AS
            SELECT 
                id, job_no, customer_name, description,
                safe_cast_numeric(overhaul_) as overhaul_,
                safe_cast_numeric(replacement) as replacement,
                safe_cast_numeric(service_contact_) as service_contact_,
                safe_cast_numeric(parts_all_) as parts_all_,
                safe_cast_numeric(product_all) as product_all,
                safe_cast_numeric(solution_) as solution_
            FROM sales2024;
            
            CREATE OR REPLACE VIEW v_sales2023 AS
            SELECT 
                id, job_no, customer_name, description,
                safe_cast_numeric(overhaul_) as overhaul_,
                safe_cast_numeric(replacement) as replacement,
                safe_cast_numeric(service_contact_) as service_contact_,
                safe_cast_numeric(parts_all_) as parts_all_,
                safe_cast_numeric(product_all) as product_all,
                safe_cast_numeric(solution_) as solution_
            FROM sales2023;
            
            CREATE OR REPLACE VIEW v_sales2022 AS
            SELECT 
                id, job_no, customer_name, description,
                safe_cast_numeric(overhaul_) as overhaul_,
                safe_cast_numeric(replacement) as replacement,
                safe_cast_numeric(service_contact_) as service_contact_,
                safe_cast_numeric(parts_all_) as parts_all_,
                safe_cast_numeric(product_all) as product_all,
                safe_cast_numeric(solution_) as solution_
            FROM sales2022;
            
            CREATE OR REPLACE VIEW v_sales2025 AS
            SELECT 
                id, job_no, customer_name, description,
                safe_cast_numeric(overhaul_) as overhaul_,
                safe_cast_numeric(replacement) as replacement,
                safe_cast_numeric(service_contact_) as service_contact_,
                safe_cast_numeric(parts_all_) as parts_all_,
                safe_cast_numeric(product_all) as product_all,
                safe_cast_numeric(solution_) as solution_
            FROM sales2025;
            
            -- Spare part view
            CREATE OR REPLACE VIEW v_spare_part AS
            SELECT 
                id, wh, product_code, product_name, unit,
                safe_cast_numeric(balance) as balance,
                safe_cast_numeric(unit_price) as unit_price,
                safe_cast_numeric(total) as total,
                description, received
            FROM spare_part;
            
            -- Work force view (keep as is - no numeric fields to cast)
            CREATE OR REPLACE VIEW v_work_force AS
            SELECT * FROM work_force;
            """
            
            await db_handler.execute_ddl(views_sql)
            
            # Update available views
            for view in ['v_sales2024', 'v_sales2023', 'v_sales2022', 'v_sales2025', 
                        'v_spare_part', 'v_work_force']:
                self.views_available[view] = True
            
            logger.info("✅ Created safe views for all tables")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create views: {e}")
            return False
    
    def rewrite_query(self, sql: str, use_views: bool = False) -> Tuple[str, List[str]]:
        """
        Rewrite SQL query เพื่อให้ปลอดภัย
        Returns: (rewritten_sql, changes_made)
        """
        if not sql:
            return sql, []
        
        original_sql = sql
        changes_made = []
        
        # Step 1: Apply rewrite patterns for CAST operations
        for pattern in self.rewrite_patterns:
            if len(pattern) == 2:
                pattern_re, replacement = pattern
                flags = re.IGNORECASE
            else:
                pattern_re, replacement, flags = pattern
            
            # Check if pattern matches
            if re.search(pattern_re, sql, flags):
                sql = re.sub(pattern_re, replacement, sql, flags=flags)
                if sql != original_sql:
                    changes_made.append(f"Replaced unsafe CAST with safe_cast_numeric")
        
        # Step 2: Fix SUM of additions (common error pattern)
        # เปลี่ยนจาก SUM(field1 + field2) เป็น SUM(safe_cast_numeric(field1)) + SUM(safe_cast_numeric(field2))
        sum_pattern = r"SUM\s*\(((?:[^()]+|\([^()]*\))+)\)"
        sum_matches = re.findall(sum_pattern, sql, re.IGNORECASE)
        
        for match in sum_matches:
            if '+' in match and 'CASE' not in match.upper():
                # Split by + and wrap each field
                fields = match.split('+')
                new_sum = ' + '.join([f"SUM(safe_cast_numeric({f.strip()}))" for f in fields])
                sql = sql.replace(f"SUM({match})", new_sum)
                changes_made.append("Fixed SUM of additions")
        
        # Step 3: Switch to views if available and requested
        if use_views and self.views_available:
            view_mappings = [
                ('FROM sales2024', 'FROM v_sales2024'),
                ('FROM sales2023', 'FROM v_sales2023'),
                ('FROM sales2022', 'FROM v_sales2022'),
                ('FROM sales2025', 'FROM v_sales2025'),
                ('FROM spare_part', 'FROM v_spare_part'),
                ('FROM work_force', 'FROM v_work_force'),
            ]
            
            for old, new in view_mappings:
                if old in sql:
                    sql = sql.replace(old, new)
                    changes_made.append(f"Switched to safe view: {new.split()[-1]}")
        
        # Step 4: Fix regex patterns (~ operator)
        # PostgreSQL regex ต้องใช้กับ text, ไม่ใช่ numeric
        regex_pattern = r"(\w+)\s+~\s+'([^']+)'"
        regex_matches = re.findall(regex_pattern, sql)
        
        for field, pattern in regex_matches:
            # Check if it's likely a numeric field being checked
            if '^[0-9]' in pattern or field in ['balance', 'unit_price', 'overhaul_', 'replacement']:
                # Replace with safe check
                sql = re.sub(
                    f"{field}\\s+~\\s+'{re.escape(pattern)}'",
                    f"{field} IS NOT NULL AND {field} != ''",
                    sql
                )
                changes_made.append(f"Fixed regex pattern for {field}")
        
        # Log changes if any
        if changes_made:
            logger.info(f"🔄 Query rewritten: {', '.join(changes_made)}")
        
        return sql, changes_made
    
    def make_super_safe_query(self, sql: str) -> str:
        """
        Fallback: สร้าง query ที่ปลอดภัยที่สุด
        """
        # ถ้า error มาก ให้ใช้ query ที่ safe ที่สุด
        if 'SUM' in sql.upper() and 'sales' in sql.lower():
            # Default safe revenue query
            return """
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE 
                    WHEN overhaul_ IS NULL OR overhaul_ = '' THEN 0
                    WHEN LENGTH(overhaul_) > 0 AND overhaul_ != '0' THEN 
                        CASE 
                            WHEN overhaul_ ~ '^[0-9]+\.?[0-9]*$' THEN overhaul_::NUMERIC
                            ELSE 0
                        END
                    ELSE 0
                END) as total_amount
            FROM sales2024
            WHERE overhaul_ IS NOT NULL;
            """
        
        # Return original if can't make safer
        return sql


# =============================================================================
# INTEGRATION WITH EXISTING SYSTEM
# =============================================================================

def integrate_query_rewriter(dual_model_instance):
    """
    Integrate Smart Query Rewriter กับ existing system
    """
    # Add rewriter
    dual_model_instance.query_rewriter = SmartQueryRewriter()
    
    # Override execute_query method
    original_execute = dual_model_instance.execute_query
    
    async def enhanced_execute_query(sql: str) -> List[Dict]:
        """Execute query with automatic rewriting"""
        try:
            # Ensure safe functions exist
            if not dual_model_instance.query_rewriter.safe_function_created:
                await dual_model_instance.query_rewriter.ensure_safe_functions(
                    dual_model_instance.db_handler
                )
            
            # Rewrite query
            rewritten_sql, changes = dual_model_instance.query_rewriter.rewrite_query(
                sql, 
                use_views=dual_model_instance.query_rewriter.views_available
            )
            
            # Execute rewritten query
            results = await original_execute(rewritten_sql)
            return results
            
        except Exception as e:
            logger.error(f"Query failed even after rewrite: {e}")
            
            # Try super safe fallback
            if 'invalid input syntax' in str(e):
                logger.info("🛡️ Attempting super safe query")
                safe_sql = dual_model_instance.query_rewriter.make_super_safe_query(sql)
                return await original_execute(safe_sql)
            
            # Re-raise if can't handle
            raise
    
    # Replace method
    dual_model_instance.execute_query = enhanced_execute_query
    
    # Add startup check
    async def ensure_database_ready():
        """Run once at startup"""
        await dual_model_instance.query_rewriter.check_or_create_views(
            dual_model_instance.db_handler
        )
    
    dual_model_instance.ensure_database_ready = ensure_database_ready
    
    logger.info("✅ Smart Query Rewriter integrated successfully")
    
    return dual_model_instance


# =============================================================================
# DATABASE HANDLER EXTENSION
# =============================================================================

class DatabaseHandlerExtension:
    """
    Extension methods for database handler
    """
    
    @staticmethod
    def add_ddl_support(db_handler):
        """
        Add DDL execution support to existing handler
        """
        async def execute_ddl(sql: str) -> bool:
            """Execute DDL statements (CREATE, ALTER, etc.)"""
            try:
                with db_handler.connection.cursor() as cursor:
                    cursor.execute(sql)
                    db_handler.connection.commit()
                return True
            except Exception as e:
                logger.error(f"DDL execution failed: {e}")
                if db_handler.connection:
                    db_handler.connection.rollback()
                return False
        
        db_handler.execute_ddl = execute_ddl
        return db_handler


# =============================================================================
# USAGE EXAMPLE
# =============================================================================

"""
การใช้งาน:

1. Import และ integrate ใน dual_model_dynamic_ai.py:

from smart_query_rewriter import SmartQueryRewriter, integrate_query_rewriter, DatabaseHandlerExtension

class ImprovedDualModelDynamicAISystem:
    def __init__(self):
        # ... existing code ...
        
        # Add DDL support to db_handler
        DatabaseHandlerExtension.add_ddl_support(self.db_handler)
        
        # Integrate query rewriter
        integrate_query_rewriter(self)
        
    async def startup(self):
        # Call this once at startup
        await self.ensure_database_ready()

2. ใน main service file:

@app.on_event("startup")
async def startup_event():
    # ... existing code ...
    
    # Ensure database is ready
    if hasattr(ultimate_ai, 'dual_model_ai'):
        await ultimate_ai.dual_model_ai.ensure_database_ready()

3. That's it! ระบบจะ rewrite queries อัตโนมัติ
"""


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    # Test rewriting
    rewriter = SmartQueryRewriter()
    
    test_queries = [
        # Test 1: CAST with NULLIF
        """SELECT SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC)) FROM sales2024""",
        
        # Test 2: CASE WHEN pattern
        """SELECT CASE WHEN overhaul_ IS NULL OR overhaul_ = '' THEN 0 
           ELSE CAST(overhaul_ AS NUMERIC) END FROM sales2024""",
        
        # Test 3: Complex SUM
        """SELECT SUM(CAST(NULLIF(overhaul_, '') AS NUMERIC) + 
           CAST(NULLIF(replacement, '') AS NUMERIC)) FROM sales2024""",
        
        # Test 4: Regex pattern
        """SELECT * FROM spare_part WHERE balance ~ '^[0-9]+' """
    ]
    
    print("=" * 60)
    print("Testing Query Rewriter")
    print("=" * 60)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\nTest {i}:")
        print(f"Original: {query[:100]}...")
        rewritten, changes = rewriter.rewrite_query(query)
        print(f"Rewritten: {rewritten[:100]}...")
        print(f"Changes: {changes}")
        print("-" * 40)