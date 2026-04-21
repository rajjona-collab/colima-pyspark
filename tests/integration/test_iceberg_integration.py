"""
test_iceberg_integration.py — Live integration tests for Iceberg DDL/DML.

Tests the full lifecycle against a running local stack (port-forwards required):
  - Postgres at localhost:25432
  - MinIO  at localhost:9000
  - Trino  at localhost:8999

PySpark tests use pg_jdbc_catalog; Trino tests use the iceberg catalog
(same JDBC backend, different access path — validates catalog is shared).

Run:
    pytest tests/integration/ -v
    pytest tests/integration/ -v -k trino   # only Trino tests
    pytest tests/integration/ -v -k spark   # only PySpark tests
"""

import pytest
from decimal import Decimal
from datetime import date

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, DecimalType
)

# ── Shared test data ──────────────────────────────────────────────────────────

SPARK_SCHEMA = "test_pyspark"
SPARK_TABLE  = "employees"
SPARK_FULL   = f"pg_jdbc_catalog.{SPARK_SCHEMA}.{SPARK_TABLE}"
SPARK_WH     = f"s3://lakehouse/sg-life-idm/{SPARK_SCHEMA}"

TRINO_SCHEMA = "test_trino"
TRINO_TABLE  = "products"
TRINO_FULL   = f"iceberg.{TRINO_SCHEMA}.{TRINO_TABLE}"

EMPLOYEE_ROWS = [
    (1, "Alice Johnson", "Engineering", Decimal("95000.00"), date(2020, 1, 15)),
    (2, "Bob Smith",     "Sales",       Decimal("75000.00"), date(2019, 6,  1)),
    (3, "Carol White",   "Engineering", Decimal("92000.00"), date(2021, 3, 10)),
]

EMPLOYEE_SCHEMA = StructType([
    StructField("emp_id",     IntegerType(),     False),
    StructField("emp_name",   StringType(),      False),
    StructField("department", StringType(),      False),
    StructField("salary",     DecimalType(10,2), False),
    StructField("hire_date",  DateType(),        False),
])


# ══════════════════════════════════════════════════════════════════════════════
# PySpark DDL tests
# ══════════════════════════════════════════════════════════════════════════════

class TestSparkDDL:
    """CREATE DATABASE and CREATE TABLE via PySpark → pg_jdbc_catalog."""

    def test_01_create_schema(self, spark):
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS pg_jdbc_catalog.{SPARK_SCHEMA}
            LOCATION '{SPARK_WH}/'
        """)
        schemas = [r.namespace for r in spark.sql(
            "SHOW DATABASES IN pg_jdbc_catalog"
        ).collect()]
        assert SPARK_SCHEMA in schemas, f"{SPARK_SCHEMA} not found after CREATE DATABASE"

    def test_02_create_table(self, spark):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {SPARK_FULL} (
                emp_id     INT,
                emp_name   STRING,
                department STRING,
                salary     DECIMAL(10,2),
                hire_date  DATE
            )
            USING iceberg
            LOCATION '{SPARK_WH}/{SPARK_TABLE}'
            PARTITIONED BY (department)
        """)
        tables = [r.tableName for r in spark.sql(
            f"SHOW TABLES IN pg_jdbc_catalog.{SPARK_SCHEMA}"
        ).collect()]
        assert SPARK_TABLE in tables, f"{SPARK_TABLE} not found after CREATE TABLE"

    def test_03_table_schema_matches(self, spark):
        cols = {r.col_name for r in spark.sql(f"DESCRIBE {SPARK_FULL}").collect()
                if not r.col_name.startswith("#")}
        expected = {"emp_id", "emp_name", "department", "salary", "hire_date"}
        assert expected <= cols, f"Missing columns: {expected - cols}"


# ══════════════════════════════════════════════════════════════════════════════
# PySpark DML tests
# ══════════════════════════════════════════════════════════════════════════════

class TestSparkDML:
    """INSERT, SELECT, TRUNCATE via PySpark against Iceberg table."""

    def test_04_insert_rows(self, spark):
        df = spark.createDataFrame(EMPLOYEE_ROWS, schema=EMPLOYEE_SCHEMA)
        df.createOrReplaceTempView("_tmp_employees")
        spark.sql(f"INSERT INTO {SPARK_FULL} SELECT * FROM _tmp_employees")
        spark.catalog.dropTempView("_tmp_employees")
        count = spark.sql(f"SELECT COUNT(*) FROM {SPARK_FULL}").collect()[0][0]
        assert count == len(EMPLOYEE_ROWS), f"Expected {len(EMPLOYEE_ROWS)} rows, got {count}"

    def test_05_select_filter(self, spark):
        rows = spark.sql(f"""
            SELECT emp_name FROM {SPARK_FULL}
            WHERE department = 'Engineering'
            ORDER BY emp_id
        """).collect()
        names = [r.emp_name for r in rows]
        assert names == ["Alice Johnson", "Carol White"], f"Unexpected result: {names}"

    def test_06_select_aggregate(self, spark):
        result = spark.sql(f"""
            SELECT COUNT(*) as cnt, MAX(salary) as max_sal
            FROM {SPARK_FULL}
        """).collect()[0]
        assert result.cnt == 3
        assert result.max_sal == Decimal("95000.00")

    def test_07_truncate_empties_table(self, spark):
        spark.sql(f"TRUNCATE TABLE {SPARK_FULL}")
        count = spark.sql(f"SELECT COUNT(*) FROM {SPARK_FULL}").collect()[0][0]
        assert count == 0, f"Expected 0 rows after TRUNCATE, got {count}"

    def test_08_insert_after_truncate(self, spark):
        """Verify TRUNCATE is idempotent — can re-insert cleanly."""
        df = spark.createDataFrame(EMPLOYEE_ROWS[:2], schema=EMPLOYEE_SCHEMA)
        df.createOrReplaceTempView("_tmp_employees2")
        spark.sql(f"INSERT INTO {SPARK_FULL} SELECT * FROM _tmp_employees2")
        spark.catalog.dropTempView("_tmp_employees2")  # noqa
        count = spark.sql(f"SELECT COUNT(*) FROM {SPARK_FULL}").collect()[0][0]
        assert count == 2

    def test_09_iceberg_snapshot_history(self, spark):
        """Iceberg maintains snapshot history for TRUNCATE and INSERTs."""
        snapshots = spark.sql(f"SELECT * FROM {SPARK_FULL}.snapshots").collect()
        assert len(snapshots) >= 2, f"Expected >=2 snapshots, got {len(snapshots)}"


# ══════════════════════════════════════════════════════════════════════════════
# Trino DDL + DML tests
# ══════════════════════════════════════════════════════════════════════════════

class TestTrinoDDL:
    """CREATE SCHEMA and TABLE via Trino CLI → same JDBC catalog backend."""

    def test_10_trino_show_catalogs(self, trino):
        catalogs = trino("SHOW CATALOGS")
        assert "iceberg" in catalogs, f"iceberg catalog not visible in Trino: {catalogs}"

    def test_11_trino_create_schema(self, trino):
        trino(f"""
            CREATE SCHEMA IF NOT EXISTS iceberg.{TRINO_SCHEMA}
            WITH (location = 's3://lakehouse/sg-life-idm/{TRINO_SCHEMA}/')
        """)
        schemas = trino("SHOW SCHEMAS", catalog="iceberg")
        assert TRINO_SCHEMA in schemas, f"{TRINO_SCHEMA} not in Trino schemas: {schemas}"

    def test_12_trino_create_table(self, trino):
        trino(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_FULL} (
                product_id   INTEGER,
                product_name VARCHAR,
                category     VARCHAR,
                price        DECIMAL(10,2)
            ) WITH (
                location = 's3://lakehouse/sg-life-idm/{TRINO_SCHEMA}/{TRINO_TABLE}',
                format   = 'PARQUET'
            )
        """)
        tables = trino(f"SHOW TABLES", catalog="iceberg", schema=TRINO_SCHEMA)
        assert TRINO_TABLE in tables, f"{TRINO_TABLE} not found after Trino CREATE TABLE"

    def test_13_trino_insert(self, trino):
        trino(f"""
            INSERT INTO {TRINO_FULL} VALUES
                (1, 'Widget A', 'Hardware', 19.99),
                (2, 'Widget B', 'Hardware', 29.99),
                (3, 'Gadget X', 'Electronics', 99.99)
        """)

    def test_14_trino_select_count(self, trino):
        result = trino(f"SELECT COUNT(*) FROM {TRINO_FULL}")
        assert result == ["3"], f"Expected 3 rows, Trino returned: {result}"

    def test_15_trino_select_filter(self, trino):
        rows = trino(f"""
            SELECT product_name FROM {TRINO_FULL}
            WHERE category = 'Hardware'
            ORDER BY product_id
        """)
        assert rows == ["Widget A", "Widget B"], f"Unexpected rows: {rows}"

    def test_16_trino_drop_table(self, trino):
        trino(f"DROP TABLE IF EXISTS {TRINO_FULL}")
        tables = trino("SHOW TABLES", catalog="iceberg", schema=TRINO_SCHEMA)
        assert TRINO_TABLE not in tables


# ══════════════════════════════════════════════════════════════════════════════
# Cross-validation: PySpark writes, Trino reads (shared catalog)
# ══════════════════════════════════════════════════════════════════════════════

class TestCrossValidation:
    """Verify pg_jdbc_catalog and Trino iceberg catalog share the same metastore."""

    def test_17_trino_sees_spark_schema(self, trino):
        schemas = trino("SHOW SCHEMAS", catalog="iceberg")
        assert SPARK_SCHEMA in schemas, \
            f"Trino cannot see PySpark-created schema {SPARK_SCHEMA}: {schemas}"

    def test_18_trino_sees_spark_table(self, trino):
        tables = trino("SHOW TABLES", catalog="iceberg", schema=SPARK_SCHEMA)
        assert SPARK_TABLE in tables, \
            f"Trino cannot see PySpark-created table {SPARK_TABLE}: {tables}"

    def test_19_trino_reads_spark_data(self, trino):
        """Trino can query data inserted by PySpark (test_08 left 2 rows)."""
        result = trino(f"SELECT COUNT(*) FROM iceberg.{SPARK_SCHEMA}.{SPARK_TABLE}")
        assert result == ["2"], f"Trino count mismatch: {result}"

    def test_20_trino_reads_spark_partition(self, trino):
        rows = trino(f"""
            SELECT emp_name
            FROM iceberg.{SPARK_SCHEMA}.{SPARK_TABLE}
            WHERE department = 'Engineering'
            ORDER BY emp_id
        """)
        assert rows == ["Alice Johnson"], f"Unexpected partition read: {rows}"


# ══════════════════════════════════════════════════════════════════════════════
# Cleanup: DROP via PySpark
# ══════════════════════════════════════════════════════════════════════════════

class TestSparkCleanup:
    """Drop test tables and schemas — run last to leave clean state."""

    def test_21_drop_spark_table(self, spark):
        spark.sql(f"DROP TABLE IF EXISTS {SPARK_FULL}")
        tables = [r.tableName for r in spark.sql(
            f"SHOW TABLES IN pg_jdbc_catalog.{SPARK_SCHEMA}"
        ).collect()]
        assert SPARK_TABLE not in tables

    def test_22_drop_spark_schema(self, spark):
        spark.sql(f"DROP DATABASE IF EXISTS pg_jdbc_catalog.{SPARK_SCHEMA}")
        schemas = [r.namespace for r in spark.sql(
            "SHOW DATABASES IN pg_jdbc_catalog"
        ).collect()]
        assert SPARK_SCHEMA not in schemas

    def test_23_drop_trino_schema(self, trino):
        trino(f"DROP SCHEMA IF EXISTS iceberg.{TRINO_SCHEMA}")
        schemas = trino("SHOW SCHEMAS", catalog="iceberg")
        assert TRINO_SCHEMA not in schemas
