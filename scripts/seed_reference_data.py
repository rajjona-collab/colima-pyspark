#!/usr/bin/env python
"""
seed_reference_data.py — Seed reference data (dim_organization, org configs).

Runs locally with .env/local.env for testing/setup.
In production, use Airflow DAG task instead.

Usage:
    source ~/.env/local.env
    cd ~/src/colima-pyspark
    python scripts/seed_reference_data.py
"""

import os
import glob as _glob

# Set PySpark Python environment
py312_path = "/Users/rajani/miniforge3/envs/py312/bin/python"
os.environ["PYSPARK_PYTHON"] = py312_path
os.environ["PYSPARK_DRIVER_PYTHON"] = py312_path

from pyspark.sql import SparkSession
from datetime import date

# Load .env/local.env
_env_file = os.path.join(os.path.dirname(__file__), "..", ".env", "local.env")
if os.path.exists(_env_file):
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key, val = line.split("=", 1)
                os.environ.setdefault(key, val)

# Load env vars
POSTGRES_JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:25432/metastore")
POSTGRES_USER = os.getenv("POSTGRES_USER", "dbadmin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
AWS_ENDPOINT_URL_S3 = os.getenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "lakehouse-etl")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# Auto-load JARs
_JARS_DIR = os.path.join(os.path.dirname(__file__), "..", "scratch", "jars")
_JARS = ",".join(_glob.glob(os.path.join(_JARS_DIR, "*.jar"))) if os.path.isdir(_JARS_DIR) else ""

if not _JARS:
    print(f"[ERROR] No JARs found in {os.path.abspath(_JARS_DIR)}")
    exit(1)

# Create SparkSession
spark = (SparkSession.builder
    .appName("SeedReferenceData")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.pg_jdbc_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.pg_jdbc_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
    .config("spark.sql.catalog.pg_jdbc_catalog.uri", POSTGRES_JDBC_URL)
    .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.user", POSTGRES_USER)
    .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.password", POSTGRES_PASSWORD)
    .config("spark.sql.catalog.pg_jdbc_catalog.warehouse", "s3://lakehouse/sg-life-idm/")
    .config("spark.jars", _JARS)
    .config("spark.hadoop.fs.s3a.endpoint", AWS_ENDPOINT_URL_S3)
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())

try:
    print("\n=== Seeding dim_organization ===")

    # Seed reference organizations (idempotent: DELETE then INSERT, or use MERGE)
    spark.sql("""
        DELETE FROM pg_jdbc_catalog.idm_mart.dim_organization
    """)

    spark.sql("""
        INSERT INTO pg_jdbc_catalog.idm_mart.dim_organization
            (org_key, org_code, org_name, org_type, active_flag, created_date, updated_date)
        VALUES
            (1, 'ail', 'American Income Life',     'subsidiary', true, current_date(), current_date()),
            (2, 'gl',  'Globe Life',               'subsidiary', true, current_date(), current_date()),
            (3, 'lnl', 'Liberty National Life',    'subsidiary', true, current_date(), current_date()),
            (4, 'ua',  'United American Insurance','subsidiary', true, current_date(), current_date())
    """)
    print("✓ Seeded 4 organizations")

    # Verify
    count = spark.sql("SELECT COUNT(*) as cnt FROM pg_jdbc_catalog.idm_mart.dim_organization").collect()[0][0]
    print(f"✓ Total org records: {count}")

    spark.sql("SELECT org_key, org_code, org_name FROM pg_jdbc_catalog.idm_mart.dim_organization ORDER BY org_key").show()

finally:
    spark.stop()
