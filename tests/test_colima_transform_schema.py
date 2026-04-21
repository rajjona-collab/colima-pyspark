"""
Schema contract tests for colima_transform.py.

Two layers:
1. Parse colima_transform.py, extract MERGE/INSERT SQL blocks, validate all
   column references against the DDL schema — catches column name mismatches.
2. Explicit guard tests for every class of runtime error we've already hit.

Run: pytest tests/test_colima_transform_schema.py -v
"""

import re
import os
import pytest


TRANSFORM_SCRIPT = os.path.join(
    os.path.dirname(__file__), "..", "scripts", "colima_transform.py"
)

# Ground truth from colima_create_catalog.py DDL
MART_SCHEMA = {
    "dim_policyholder": {
        "policyholder_key", "policyholder_business_key", "policyholder_name",
        "ssn_hash", "dob_hash", "is_current", "effective_date", "end_date",
        "created_date", "updated_date",
    },
    "dim_policy": {
        "policy_key", "policy_number", "org_code", "policyholder_key",
        "policy_status", "issue_date", "effective_date", "termination_date",
        "is_current", "effective_date_scd", "end_date_scd",
        "created_date", "updated_date",
    },
    "dim_address": {
        "address_key", "policy_key", "address_type", "street_address",
        "city", "state", "zip_code", "country", "is_current",
        "created_date", "updated_date",
    },
    "dim_payment_method": {
        "payment_method_key", "policy_key", "payment_method_type",
        "account_number_last_4", "bank_name", "is_current",
        "created_date", "updated_date",
    },
    "fact_premiums": {
        "premium_key", "transaction_id", "policy_key", "org_key",
        "premium_amount", "transaction_date", "payment_status",
        "created_date", "batch_date",
    },
}

# Columns/tokens that appear in SQL syntax but are not target table column names
SOURCE_ALIASES = {
    "s", "t", "rn", "batch_date", "TRUE", "FALSE", "NULL",
    "CURRENT_TIMESTAMP", "DATE_SUB", "CAST", "COALESCE", "CONCAT",
}


def load_transform_script() -> str:
    with open(TRANSFORM_SCRIPT, "r") as f:
        return f.read()


def extract_sql_blocks(script: str) -> list[tuple[str, str]]:
    """
    Extract (target_table, sql_block) pairs from spark.sql(f\"\"\"...\"\"\") calls.
    """
    results = []
    pattern = re.compile(r'spark\.sql\s*\(\s*f?"""(.*?)"""\s*\)', re.DOTALL)
    for m in pattern.finditer(script):
        sql = m.group(1)
        tbl_match = re.search(
            r"(?:MERGE INTO|INSERT INTO)\s+pg_jdbc_catalog\.\w+\.(\w+)",
            sql, re.IGNORECASE,
        )
        if tbl_match:
            results.append((tbl_match.group(1), sql))
    return results


def extract_target_columns_from_sql(sql: str) -> set[str]:
    """
    Extract column names used on the *target* side only:
    - INSERT (...) column list
    - UPDATE SET left-hand side, scoped to the UPDATE SET block
    """
    cols = set()

    insert_match = re.search(r"INSERT\s*\(([^)]+)\)", sql, re.IGNORECASE)
    if insert_match:
        for col in insert_match.group(1).split(","):
            cols.add(col.strip())

    # Scope UPDATE SET extraction to avoid picking up ON/JOIN clause assignments
    update_block = re.search(
        r"UPDATE\s+SET\s+(.*?)(?=WHEN\s+(?:NOT\s+)?MATCHED|$)",
        sql, re.IGNORECASE | re.DOTALL
    )
    if update_block:
        for m in re.finditer(r"\b(\w+)\s*=\s*\S", update_block.group(1)):
            cols.add(m.group(1).strip())

    sql_keywords = {
        "SET", "TRUE", "FALSE", "NULL", "AND", "OR", "NOT", "IS",
        "WHEN", "THEN", "MATCHED", "UNMATCHED",
    }
    cols -= sql_keywords
    cols -= SOURCE_ALIASES
    return cols


def get_sql_for_table(table: str) -> str | None:
    script = load_transform_script()
    for tbl, sql in extract_sql_blocks(script):
        if tbl == table:
            return sql
    return None


# ── Layer 1: Dynamic column reference validation ──────────────────────────────

class TestTransformScriptColumnRefs:
    """For every MERGE/INSERT in transform.py, all target column refs must exist in DDL."""

    @pytest.fixture(scope="class")
    def sql_blocks(self):
        return extract_sql_blocks(load_transform_script())

    def test_script_has_all_expected_tables(self, sql_blocks):
        found = {tbl for tbl, _ in sql_blocks}
        expected = {"dim_policyholder", "dim_policy", "dim_address", "dim_payment_method", "fact_premiums"}
        assert not (expected - found), f"transform.py missing MERGE/INSERT for: {expected - found}"

    def test_all_target_columns_exist_in_ddl(self, sql_blocks):
        errors = []
        for table, sql in sql_blocks:
            if table not in MART_SCHEMA:
                continue
            invalid = extract_target_columns_from_sql(sql) - MART_SCHEMA[table]
            if invalid:
                errors.append(f"{table}: unknown columns {invalid}")
        assert not errors, "Column mismatches:\n" + "\n".join(errors)


# ── Layer 2: Cardinality violation guards ─────────────────────────────────────

class TestCardinalityViolationGuards:
    """MERGE source must be deduplicated per match key to avoid MERGE_CARDINALITY_VIOLATION."""

    def test_dim_policyholder_has_row_number_dedup(self):
        """One policyholder has multiple policies — source must dedup by business_key."""
        sql = get_sql_for_table("dim_policyholder")
        assert sql, "dim_policyholder MERGE not found"
        assert re.search(r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(", sql, re.IGNORECASE), \
            "dim_policyholder MERGE must use ROW_NUMBER() to dedup source by business_key"
        assert re.search(r"PARTITION BY.*COALESCE.*ssn_hash", sql, re.IGNORECASE), \
            "dim_policyholder ROW_NUMBER must PARTITION BY business_key expression"

    def test_dim_policy_has_row_number_dedup(self):
        """Same policy_number may appear multiple times across orgs — dedup required."""
        sql = get_sql_for_table("dim_policy")
        assert sql, "dim_policy MERGE not found"
        assert re.search(r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(", sql, re.IGNORECASE), \
            "dim_policy MERGE must use ROW_NUMBER() to dedup source by policy_number"
        assert re.search(r"PARTITION BY\s+policy_number", sql, re.IGNORECASE), \
            "dim_policy ROW_NUMBER must PARTITION BY policy_number"

    def test_dim_payment_method_has_row_number_dedup(self):
        """stg_bankinfo may have multiple rows per policy — dedup before MERGE on policy_key."""
        sql = get_sql_for_table("dim_payment_method")
        assert sql, "dim_payment_method MERGE not found"
        assert re.search(r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(", sql, re.IGNORECASE), \
            "dim_payment_method MERGE must use ROW_NUMBER() to dedup source by policy_number"


# ── Layer 3: Schema regression guards ────────────────────────────────────────

class TestSchemaRegressions:
    """Explicit guards for every column-name bug we've hit in production."""

    def test_dim_policy_uses_end_date_scd_not_end_date(self):
        assert "end_date" not in MART_SCHEMA["dim_policy"]
        sql = get_sql_for_table("dim_policy")
        update_cols = extract_target_columns_from_sql(sql)
        assert "end_date" not in update_cols, "dim_policy MERGE uses bare end_date — must be end_date_scd"
        assert "end_date_scd" in update_cols, "dim_policy MERGE missing end_date_scd"

    def test_dim_policy_insert_includes_policyholder_key(self):
        """policyholder_key FK must be populated in dim_policy INSERT — not left NULL."""
        sql = get_sql_for_table("dim_policy")
        insert_match = re.search(r"INSERT\s*\(([^)]+)\)", sql, re.IGNORECASE)
        assert insert_match, "dim_policy MERGE has no INSERT clause"
        insert_cols = {c.strip() for c in insert_match.group(1).split(",")}
        assert "policyholder_key" in insert_cols, \
            "dim_policy INSERT missing policyholder_key FK — join to dim_policyholder required"

    def test_dim_address_insert_uses_policy_key_not_policy_number(self):
        sql = get_sql_for_table("dim_address")
        insert_match = re.search(r"INSERT\s*\(([^)]+)\)", sql, re.IGNORECASE)
        assert insert_match
        cols = {c.strip() for c in insert_match.group(1).split(",")}
        assert "policy_number" not in cols, "dim_address INSERT must use policy_key, not policy_number"
        assert "policy_key" in cols, "dim_address INSERT missing policy_key"

    def test_dim_payment_method_insert_uses_policy_key_not_policy_number(self):
        sql = get_sql_for_table("dim_payment_method")
        insert_match = re.search(r"INSERT\s*\(([^)]+)\)", sql, re.IGNORECASE)
        assert insert_match
        cols = {c.strip() for c in insert_match.group(1).split(",")}
        assert "policy_number" not in cols, "dim_payment_method INSERT must use policy_key"
        assert "policy_key" in cols, "dim_payment_method INSERT missing policy_key"


# ── Layer 4: fact_premiums specific guards ────────────────────────────────────

class TestFactPremiumsGuards:

    def test_explicit_column_list_avoids_arity_mismatch(self):
        """INSERT INTO ... SELECT must name columns; premium_key is surrogate and omitted."""
        sql = get_sql_for_table("fact_premiums")
        assert re.search(r"INSERT\s+INTO\s+[^\s(]+\s*\(", sql, re.IGNORECASE), \
            "fact_premiums INSERT INTO must have explicit column list"

    def test_no_org_code_in_insert(self):
        """fact_premiums uses org_key (surrogate), not org_code."""
        sql = get_sql_for_table("fact_premiums")
        insert_match = re.search(r"INSERT\s+INTO\s+[^\s(]+\s*\(([^)]+)\)", sql, re.IGNORECASE)
        assert insert_match
        cols = {c.strip() for c in insert_match.group(1).split(",")}
        assert "org_code" not in cols, "fact_premiums INSERT must use org_key not org_code"
        assert "org_key" in cols

    def test_no_policy_number_in_insert(self):
        """fact_premiums uses policy_key (surrogate), not policy_number."""
        sql = get_sql_for_table("fact_premiums")
        insert_match = re.search(r"INSERT\s+INTO\s+[^\s(]+\s*\(([^)]+)\)", sql, re.IGNORECASE)
        assert insert_match
        cols = {c.strip() for c in insert_match.group(1).split(",")}
        assert "policy_number" not in cols, "fact_premiums INSERT must use policy_key not policy_number"
        assert "policy_key" in cols

    def test_dedup_uses_left_join_not_not_in(self):
        """NOT IN subquery is O(n²); dedup must use LEFT JOIN / IS NULL pattern."""
        sql = get_sql_for_table("fact_premiums")
        assert "NOT IN" not in sql.upper(), \
            "fact_premiums dedup must use LEFT JOIN ... IS NULL, not NOT IN subquery"
        assert re.search(r"LEFT\s+JOIN.*fact_premiums", sql, re.IGNORECASE), \
            "fact_premiums dedup missing LEFT JOIN to fact_premiums for idempotent insert"

    def test_dim_organization_alias_is_not_reserved_word(self):
        """'do' is a SQL reserved word — dim_organization alias must be dorg or similar."""
        sql = get_sql_for_table("fact_premiums")
        # Check that 'do' is not used as a standalone alias (e.g. "dim_organization do")
        assert not re.search(r"dim_organization\s+do\b", sql, re.IGNORECASE), \
            "dim_organization alias 'do' is a SQL reserved word — use dorg"
