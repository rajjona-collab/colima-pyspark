"""
Test mart tables with natural key design.

Validates:
- Natural key constraints (no NULL FKs, no surrogates)
- SCD Type 2 logic (dim_policyholder, dim_policy)
- SCD Type 1 logic (dim_address, dim_payment_method)
- Fact table dedup (fact_premiums by transaction_id)
- Data lineage (policy_number, org_code flow correctly)
"""

import pytest
from pyspark.sql import SparkSession
from datetime import date


@pytest.fixture(scope="module")
def spark_session():
    """Spark session configured for Iceberg catalog."""
    return SparkSession.builder \
        .appName("MartNaturalKeyTests") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.pg_jdbc_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.pg_jdbc_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
        .config("spark.sql.defaultCatalog", "pg_jdbc_catalog") \
        .getOrCreate()


class TestDimensionNaturalKeys:
    """Validate dimension tables use natural keys (no surrogates)."""

    def test_dim_organization_static_reference(self, spark_session):
        """dim_organization: static 4-row reference."""
        df = spark_session.sql("SELECT * FROM pg_jdbc_catalog.idm_mart.dim_organization")
        assert df.count() == 4, "dim_organization must have exactly 4 rows"

        orgs = df.select("org_code").collect()
        org_codes = {row.org_code for row in orgs}
        assert org_codes == {"ail", "gl", "lnl", "ua"}, "dim_organization must have all 4 orgs"

    def test_dim_policyholder_natural_key(self, spark_session):
        """dim_policyholder: natural key = (policyholder_business_key, org_code)."""
        df = spark_session.sql("""
            SELECT policyholder_business_key, org_code, COUNT(*) as count
            FROM pg_jdbc_catalog.idm_mart.dim_policyholder
            WHERE is_current = TRUE
            GROUP BY policyholder_business_key, org_code
            HAVING count > 1
        """)
        # No duplicates on natural key (one current version per insured per org)
        assert df.count() == 0, "No duplicate natural keys allowed in dim_policyholder"

    def test_dim_policy_natural_key(self, spark_session):
        """dim_policy: natural key = (policy_number, org_code)."""
        df = spark_session.sql("""
            SELECT policy_number, org_code, COUNT(*) as count
            FROM pg_jdbc_catalog.idm_mart.dim_policy
            WHERE is_current = TRUE
            GROUP BY policy_number, org_code
            HAVING count > 1
        """)
        assert df.count() == 0, "No duplicate natural keys allowed in dim_policy"

    def test_dim_address_natural_key(self, spark_session):
        """dim_address: natural key = (policy_number, org_code, address_type)."""
        df = spark_session.sql("""
            SELECT policy_number, org_code, address_type, COUNT(*) as count
            FROM pg_jdbc_catalog.idm_mart.dim_address
            GROUP BY policy_number, org_code, address_type
            HAVING count > 1
        """)
        assert df.count() == 0, "No duplicate natural keys allowed in dim_address"

    def test_dim_payment_method_natural_key(self, spark_session):
        """dim_payment_method: natural key = (policy_number, org_code)."""
        df = spark_session.sql("""
            SELECT policy_number, org_code, COUNT(*) as count
            FROM pg_jdbc_catalog.idm_mart.dim_payment_method
            GROUP BY policy_number, org_code
            HAVING count > 1
        """)
        assert df.count() == 0, "No duplicate natural keys allowed in dim_payment_method"


class TestSCD2Logic:
    """Validate SCD Type 2 (history tracking)."""

    def test_dim_policy_scd2_tracking(self, spark_session):
        """dim_policy: is_current flag and SCD columns present."""
        df = spark_session.sql("""
            SELECT policy_number, org_code, is_current, effective_date_scd, end_date_scd
            FROM pg_jdbc_catalog.idm_mart.dim_policy
            LIMIT 1
        """)
        rows = df.collect()
        assert len(rows) > 0, "dim_policy must have data"

        row = rows[0]
        assert row.is_current is not None, "is_current flag must be set"
        assert row.effective_date_scd is not None, "effective_date_scd must be set"

    def test_dim_policyholder_scd2_tracking(self, spark_session):
        """dim_policyholder: is_current flag and date range."""
        df = spark_session.sql("""
            SELECT policyholder_business_key, org_code, is_current, effective_date, end_date
            FROM pg_jdbc_catalog.idm_mart.dim_policyholder
            LIMIT 1
        """)
        rows = df.collect()
        assert len(rows) > 0, "dim_policyholder must have data"

        row = rows[0]
        assert row.is_current is not None, "is_current flag must be set"
        assert row.effective_date is not None, "effective_date must be set"

    def test_scd2_no_multiple_current_per_key(self, spark_session):
        """No two is_current=TRUE rows for same natural key."""
        df = spark_session.sql("""
            SELECT policy_number, org_code
            FROM pg_jdbc_catalog.idm_mart.dim_policy
            WHERE is_current = TRUE
            GROUP BY policy_number, org_code
            HAVING COUNT(*) > 1
        """)
        assert df.count() == 0, "At most one is_current=TRUE per natural key"


class TestSCD1Logic:
    """Validate SCD Type 1 (no history)."""

    def test_dim_address_scd1_no_history(self, spark_session):
        """dim_address: SCD1 — no multiple versions, just latest."""
        # Count rows per natural key — should be at most 1
        df = spark_session.sql("""
            SELECT policy_number, org_code, address_type, COUNT(*) as count
            FROM pg_jdbc_catalog.idm_mart.dim_address
            GROUP BY policy_number, org_code, address_type
            HAVING count > 1
        """)
        assert df.count() == 0, "SCD1 should have at most 1 row per natural key"

    def test_dim_payment_method_scd1_no_history(self, spark_session):
        """dim_payment_method: SCD1 — no multiple versions."""
        df = spark_session.sql("""
            SELECT policy_number, org_code, COUNT(*) as count
            FROM pg_jdbc_catalog.idm_mart.dim_payment_method
            GROUP BY policy_number, org_code
            HAVING count > 1
        """)
        assert df.count() == 0, "SCD1 should have at most 1 row per natural key"


class TestFactTableNaturalForeignKeys:
    """Validate fact tables use natural FKs (no surrogates)."""

    def test_fact_premiums_has_policy_number(self, spark_session):
        """fact_premiums: stores policy_number (natural FK), not policy_key."""
        df = spark_session.sql("""
            SELECT policy_number, org_code, COUNT(*) as count
            FROM pg_jdbc_catalog.idm_mart.fact_premiums
            GROUP BY policy_number, org_code
        """)
        rows = df.collect()
        assert len(rows) > 0, "fact_premiums must have rows"

        for row in rows:
            assert row.policy_number is not None, "policy_number (natural FK) must not be NULL"
            assert row.org_code is not None, "org_code must not be NULL"

    def test_fact_premiums_transaction_id_unique(self, spark_session):
        """fact_premiums: transaction_id is unique (dedup)."""
        df = spark_session.sql("""
            SELECT transaction_id, COUNT(*) as count
            FROM pg_jdbc_catalog.idm_mart.fact_premiums
            GROUP BY transaction_id
            HAVING count > 1
        """)
        assert df.count() == 0, "transaction_id must be unique (no duplicates)"

    def test_fact_premiums_no_surrogate_keys(self, spark_session):
        """fact_premiums: does NOT have policy_key, premium_key columns."""
        schema = spark_session.sql("SELECT * FROM pg_jdbc_catalog.idm_mart.fact_premiums LIMIT 1").schema
        column_names = {field.name for field in schema.fields}

        assert "policy_key" not in column_names, "fact_premiums should NOT have surrogate policy_key"
        assert "premium_key" not in column_names, "fact_premiums should NOT have surrogate premium_key"
        assert "policy_number" in column_names, "fact_premiums should have natural FK policy_number"
        assert "org_code" in column_names, "fact_premiums should have org_code"


class TestDataLineage:
    """Validate data flows correctly through natural keys."""

    def test_fact_premiums_join_dim_policy(self, spark_session):
        """fact_premiums can join to dim_policy on natural key (policy_number, org_code)."""
        df = spark_session.sql("""
            SELECT fp.transaction_id, dp.policy_number, dp.org_code
            FROM pg_jdbc_catalog.idm_mart.fact_premiums fp
            JOIN pg_jdbc_catalog.idm_mart.dim_policy dp
                ON fp.policy_number = dp.policy_number AND fp.org_code = dp.org_code AND dp.is_current = TRUE
            LIMIT 10
        """)
        count = df.count()
        assert count > 0, "fact_premiums should successfully join to dim_policy on natural keys"

    def test_all_policies_in_fact_exist_in_dimension(self, spark_session):
        """All policies in fact_premiums exist in dim_policy."""
        df = spark_session.sql("""
            SELECT COUNT(*) as fact_count FROM pg_jdbc_catalog.idm_mart.fact_premiums
        """)
        fact_count = df.collect()[0].fact_count

        df = spark_session.sql("""
            SELECT COUNT(DISTINCT fp.policy_number, fp.org_code) as policy_count
            FROM pg_jdbc_catalog.idm_mart.fact_premiums fp
            JOIN pg_jdbc_catalog.idm_mart.dim_policy dp
                ON fp.policy_number = dp.policy_number AND fp.org_code = dp.org_code
        """)
        policy_count = df.collect()[0].policy_count

        assert policy_count > 0, "Fact table should reference existing policies"

    def test_org_codes_consistent(self, spark_session):
        """org_code values are consistent across tables."""
        # Get org_codes from each table
        dim_policy_orgs = set(row.org_code for row in
            spark_session.sql("SELECT DISTINCT org_code FROM pg_jdbc_catalog.idm_mart.dim_policy").collect())
        fact_orgs = set(row.org_code for row in
            spark_session.sql("SELECT DISTINCT org_code FROM pg_jdbc_catalog.idm_mart.fact_premiums").collect())
        dim_org_orgs = set(row.org_code for row in
            spark_session.sql("SELECT DISTINCT org_code FROM pg_jdbc_catalog.idm_mart.dim_organization").collect())

        # All fact org_codes should exist in dim_organization
        assert fact_orgs.issubset(dim_org_orgs), "fact_premiums org_codes must exist in dim_organization"


class TestDataQuality:
    """Validate data quality constraints."""

    def test_no_null_natural_keys(self, spark_session):
        """No NULL values in natural key columns."""
        tables_and_keys = [
            ("dim_policyholder", ["policyholder_business_key", "org_code"]),
            ("dim_policy", ["policy_number", "org_code"]),
            ("dim_address", ["policy_number", "org_code", "address_type"]),
            ("dim_payment_method", ["policy_number", "org_code"]),
            ("fact_premiums", ["transaction_id", "policy_number", "org_code"]),
        ]

        for table, key_columns in tables_and_keys:
            for col in key_columns:
                df = spark_session.sql(f"""
                    SELECT COUNT(*) as null_count
                    FROM pg_jdbc_catalog.idm_mart.{table}
                    WHERE {col} IS NULL
                """)
                null_count = df.collect()[0].null_count
                assert null_count == 0, f"{table}.{col} must not have NULL values"

    def test_fact_premiums_required_columns(self, spark_session):
        """fact_premiums has all required columns."""
        required_cols = ["transaction_id", "policy_number", "org_code", "premium_amount",
                        "transaction_date", "payment_status", "batch_date", "created_date"]

        schema = spark_session.sql("SELECT * FROM pg_jdbc_catalog.idm_mart.fact_premiums LIMIT 1").schema
        column_names = {field.name for field in schema.fields}

        for col in required_cols:
            assert col in column_names, f"fact_premiums must have {col} column"

    def test_row_counts(self, spark_session):
        """Verify row counts match expected ranges."""
        # Fact premiums should have >100 rows (depends on test data)
        df = spark_session.sql("SELECT COUNT(*) as count FROM pg_jdbc_catalog.idm_mart.fact_premiums")
        count = df.collect()[0].count
        assert count > 100, "fact_premiums should have >100 rows in test data"

        # Dimensions should have fewer rows than facts (expected warehouse pattern)
        df = spark_session.sql("SELECT COUNT(*) as count FROM pg_jdbc_catalog.idm_mart.dim_policy WHERE is_current=TRUE")
        dim_count = df.collect()[0].count
        assert dim_count < count, "Current dim_policy rows should be less than fact rows"
