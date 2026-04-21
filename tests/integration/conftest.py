"""
Integration test fixtures — real SparkSession, not mocked.

Parent conftest.py mocks pyspark globally; undo that here so integration
tests get a real SparkSession connected to local port-forwards.
"""

import sys
import os
import glob
import subprocess
import pytest

# Set PySpark Python environment (driver + executors must match)
py312_path = "/Users/rajani/miniforge3/envs/py312/bin/python"
os.environ["PYSPARK_PYTHON"] = py312_path
os.environ["PYSPARK_DRIVER_PYTHON"] = py312_path

# Remove PySpark mocks installed by parent conftest before importing pyspark
for _key in list(sys.modules.keys()):
    if _key.startswith("pyspark") or _key == "requests":
        del sys.modules[_key]

from pyspark.sql import SparkSession  # noqa: E402 — must come after mock removal


# ── Config from env (port-forwards must be active) ────────────────────────────

JDBC_URL   = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:25432/metastore")
DB_USER    = os.getenv("POSTGRES_USER",     "dbadmin")
DB_PASS    = os.getenv("POSTGRES_PASSWORD", "")
S3_EP      = os.getenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
S3_KEY     = os.getenv("AWS_ACCESS_KEY_ID",   "lakehouse-etl")
S3_SECRET  = os.getenv("AWS_SECRET_ACCESS_KEY", "")
WH         = "s3://lakehouse/sg-life-idm"
TRINO_CLI  = os.getenv("TRINO_CLI", os.path.expanduser("~/scripts/trino-cli-480"))
TRINO_URL  = os.getenv("TRINO_URL", "http://localhost:8999")

# JARs from scratch/jars/ (required for S3A + Iceberg locally)
_JARS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scratch", "jars"))
_JARS = ",".join(glob.glob(os.path.join(_JARS_DIR, "*.jar"))) if os.path.isdir(_JARS_DIR) else ""


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession with Iceberg JDBC catalog + S3A."""
    session = (SparkSession.builder
        .appName("IntegrationTest")
        .master("local[2]")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.pg_jdbc_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.pg_jdbc_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.pg_jdbc_catalog.uri", JDBC_URL)
        .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.user", DB_USER)
        .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.password", DB_PASS)
        .config("spark.sql.catalog.pg_jdbc_catalog.warehouse", f"{WH}/")
        .config("spark.jars", _JARS)
        .config("spark.hadoop.fs.s3a.endpoint", S3_EP)
        .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate())
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_schemas(spark):
    """Drop test schemas before AND after the session to ensure clean state."""
    for schema in ("test_pyspark", "test_trino"):
        _drop_schema_if_exists(spark, schema)
    yield
    for schema in ("test_pyspark", "test_trino"):
        _drop_schema_if_exists(spark, schema)


def _drop_schema_if_exists(spark, schema):
    """Drop all tables in schema then drop the schema."""
    full = f"pg_jdbc_catalog.{schema}"
    try:
        tables = spark.sql(f"SHOW TABLES IN {full}").collect()
        for row in tables:
            spark.sql(f"DROP TABLE IF EXISTS {full}.{row.tableName}")
        spark.sql(f"DROP DATABASE IF EXISTS {full}")
    except Exception:
        pass


def trino_exec(sql, catalog="iceberg", schema=None, output_format="TSV"):
    """Run a SQL statement via Trino CLI, return stdout lines."""
    cmd = [
        "java", "-jar", TRINO_CLI,
        "--server", TRINO_URL,
        "--catalog", catalog,
        "--output-format", output_format,
        "--execute", sql,
    ]
    if schema:
        cmd += ["--schema", schema]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"Trino CLI error:\n{result.stderr.strip()}")
    return [line for line in result.stdout.strip().splitlines() if line]


@pytest.fixture(scope="session")
def trino():
    """Return the trino_exec helper for use in tests."""
    return trino_exec
