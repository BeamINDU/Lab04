
import re
import logging

logger = logging.getLogger("agents.smart_query_rewriter")

class SmartQueryRewriter:
    """
    Safer SQL rewriter (no UDF). Key behaviors:
    - Force views (v_sales20xx, v_spare_part) instead of raw tables
    - Map raw sales/spare fields to *_num
    - Remove comparisons of numeric columns with '' and LENGTH() on numerics
    - Split SUM(a+b) into SUM(a)+SUM(b)
    - Light TH->AD year literal normalization inside LIKE patterns (2567->2024, 2568->2025)
    """
    def __init__(self):
        self.safe_function_created = True

    async def ensure_safe_functions(self, db_handler):
        """No-op (UDFs not required)."""
        self.safe_function_created = True
        return True

    async def _create_safe_views(self, db_handler):
        logger.info("ℹ️ Skip auto-creating views; expecting v_sales20xx/v_spare_part/v_work_force to exist.")
        return True

    def _thai_year_normalize(self, s: str) -> str:
        # Turn month/year literal filters with TH years into AD years in LIKE patterns.
        # Examples: '%/08/2568%' -> '%/08/2025%'; '%2025-08%' untouched
        mapping = {'2567': '2024', '2568': '2025', '2566': '2023'}
        out = s
        for th, ad in mapping.items():
            out = re.sub(rf'(/|-){th}\b', rf'\1{ad}', out)
            out = re.sub(rf'\b{th}(/|-)', rf'{ad}\1', out)
            out = re.sub(rf'\b{th}\b', ad, out)
        return out

    def rewrite_query(self, sql: str, use_views: bool = True):
        """
        Returns: (rewritten_sql, changes_made)
        """
        if not sql:
            return sql, []
        changes_made = []
        original = sql

        # 0) normalize TH year literals in LIKE filters
        sql = self._thai_year_normalize(sql)

        # 1) enforce views
        mapping = {
            r'\bFROM\s+sales2022\b': 'FROM v_sales2022',
            r'\bFROM\s+sales2023\b': 'FROM v_sales2023',
            r'\bFROM\s+sales2024\b': 'FROM v_sales2024',
            r'\bFROM\s+sales2025\b': 'FROM v_sales2025',
            r'\bJOIN\s+sales2022\b': 'JOIN v_sales2022',
            r'\bJOIN\s+sales2023\b': 'JOIN v_sales2023',
            r'\bJOIN\s+sales2024\b': 'JOIN v_sales2024',
            r'\bJOIN\s+sales2025\b': 'JOIN v_sales2025',
            r'\bFROM\s+spare_part\b': 'FROM v_spare_part',
            r'\bJOIN\s+spare_part\b': 'JOIN v_spare_part',
        }
        tmp = sql
        for pat, rep in mapping.items():
            new = re.sub(pat, rep, tmp, flags=re.IGNORECASE)
            if new != tmp:
                changes_made.append(f"Switched to {rep.split()[-1]}")
            tmp = new
        sql = tmp

        # 2) map raw fields -> *_num (apply only if using sales/spare views)
        if re.search(r'\bv_sales20(2[2-5])\b', sql, flags=re.IGNORECASE):
            sales_map = {
                r'\boverhaul_\b': 'overhaul_num',
                r'\breplacement\b': 'replacement_num',
                r'\bservice_contact_\b': 'service_num',
                r'\bparts_all_\b': 'parts_num',
                r'\bproduct_all\b': 'product_num',
                r'\bsolution_\b': 'solution_num',
            }
            for pat, rep in sales_map.items():
                new = re.sub(pat, rep, sql, flags=re.IGNORECASE)
                if new != sql:
                    changes_made.append(f"Field {pat} -> {rep}")
                sql = new

        if re.search(r'\bv_spare_part\b', sql, flags=re.IGNORECASE):
            spare_map = {
                r'\bbalance\b': 'balance_num',
                r'\bunit_price\b': 'unit_price_num',
                r'\btotal\b': 'total_num',
            }
            for pat, rep in spare_map.items():
                new = re.sub(pat, rep, sql, flags=re.IGNORECASE)
                if new != sql:
                    changes_made.append(f"Field {pat} -> {rep}")
                sql = new

        # 3) strip dangerous string comparisons on numerics
        new = re.sub(r'(\b\w+\b)\s*(=|<>|!=)\s*''', 'TRUE', sql, flags=re.IGNORECASE)
        if new != sql:
            changes_made.append("Removed empty-string comparisons on numeric fields")
        sql = new

        new = re.sub(r'LENGTH\s*\(\s*\w+\s*\)\s*>\s*0', 'TRUE', sql, flags=re.IGNORECASE)
        if new != sql:
            changes_made.append("Removed LENGTH() checks on numeric fields")
        sql = new

        out2 = re.sub(r"(\b\w+\b)\s*~\s*'[^']+'", r"\1 IS NOT NULL", out, flags=re.IGNORECASE)

        if new != sql:
            changes_made.append("Removed regex checks on numeric fields")
        sql = new

        # 4) split SUM(a+b) => SUM(a)+SUM(b) for plain additions
        def repl_sum(m):
            inside = m.group(1)
            if '+' in inside and 'CASE' not in inside.upper():
                parts = [p.strip() for p in inside.split('+')]
                return ' + '.join([f"SUM({p})" for p in parts])
            return m.group(0)
        new = re.sub(r'SUM\s*\(\s*([^()]+)\s*\)', repl_sum, sql, flags=re.IGNORECASE)
        if new != sql:
            changes_made.append("Split SUM(a+b) into SUM(a)+SUM(b)")
        sql = new

        sql = re.sub(r'\s+', ' ', sql).strip()
        return sql, changes_made

    def make_super_safe_query(self, sql: str) -> str:
        up = sql.upper()
        if 'SUM' in up and 'SALES' in up:
            target_view = 'v_sales2024' if '2024' in up else 'v_sales2025' if '2025' in up else 'v_sales2024'
            return f"""
            SELECT 
                COUNT(*) as total_records,
                SUM(overhaul_num) +
                SUM(replacement_num) +
                SUM(service_num) +
                SUM(parts_num) +
                SUM(product_num) +
                SUM(solution_num) AS total_amount
            FROM {target_view}
            WHERE total_revenue IS NOT NULL;
            """
        return sql


def integrate_query_rewriter(system):
    """Wrap system.execute_query to apply rewriting safely."""
    rewriter = SmartQueryRewriter()
    if not hasattr(system, 'execute_query'):
        logger.error("System has no execute_query to wrap.")
        return

    original_execute = system.execute_query

    async def enhanced_execute_query(sql: str):
        rewritten_sql = sql
        try:
            rewritten_sql, changes = rewriter.rewrite_query(sql)
            if changes:
                logger.info(f"[Rewriter] Changes: {changes}")
        except Exception as e:
            logger.exception("Rewrite step failed; using original SQL")
            rewritten_sql = sql

        try:
            return await original_execute(rewritten_sql)
        except Exception as e:
            logger.error(f"Query failed even after rewrite: {e}")
            # fallback super safe
            fallback_sql = rewriter.make_super_safe_query(rewritten_sql)
            logger.info(f"[Rewriter] Fallback SQL: {fallback_sql}")
            return await original_execute(fallback_sql)

    # monkey-patch
    system.execute_query = enhanced_execute_query
    logger.info("✅ Smart Query Rewriter integrated successfully")


# Back-compat extension hook if your code expects DatabaseHandlerExtension
class DatabaseHandlerExtension:
    @staticmethod
    def add_ddl_support(db_handler):
        # No-op hook to match previous behavior
        return db_handler
