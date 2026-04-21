#!/usr/bin/env python
"""
truncate_all.py — Truncate all staging and mart tables for complete data reset.

Idempotent cleanup script. Use for full reload (parse + transform from scratch).

Usage (local with port-forwards):
    source .env/local.env
    python scripts/truncate_all.py

Usage (in-pod via spark-submit):
    spark-submit scripts/truncate_all.py
    (Requires env vars: POSTGRES_JDBC_URL, POSTGRES_USER, POSTGRES_PASSWORD,
     AWS_ENDPOINT_URL_S3, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
"""

import os
import glob as _glob
from pyspark.sql import SparkSession

# Load env vars
# Local defaults: localhost:25432 (port-forward to k8s postgres)
# In-pod should use k8s DNS: postgres.postgres.svc.cluster.local:5432
JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:25432/metastore")
DB_USER = os.getenv("POSTGRES_USER", "dbadmin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "")
# Local defaults: http://localhost:9000 (port-forward to k8s MinIO)
# In-pod should use k8s DNS: http://minio.minio.svc.cluster.local:9000
S3_EP = os.getenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
S3_KEY = os.getenv("AWS_ACCESS_KEY_ID", "lakehouse-etl")
S3_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "")
WH = "s3://lakehouse/sg-life-idm"

# Auto-load JARs
_JARS_DIR = os.path.join(os.path.dirname(__file__), "..", "scratch", "jars")
_JARS = ",".join(_glob.glob(os.path.join(_JARS_DIR, "*.jar"))) if os.path.isdir(_JARS_DIR) else ""

if not _JARS:
    print(f"[ERROR] No JARs found in {os.path.abspath(_JARS_DIR)}")
    exit(1)

spark = (SparkSession.builder
    .appName("TruncateAll")
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
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())

try:
    print("\n=== Truncating staging tables ===")
    staging_tables = ["stg_basic", "stg_address", "stg_bankinfo", "stg_premiums", "stg_rejected"]
    for tbl in staging_tables:
        try:
            spark.sql(f"TRUNCATE TABLE pg_jdbc_catalog.idm_staging.{tbl}")
            print(f"  ✓ {tbl}")
        except Exception as e:
            print(f"  ✗ {tbl}: {e}")

    print("\n=== Truncating mart tables ===")
    # Exclude dim_organization (static reference data)
    mart_tables = ["dim_policyholder", "dim_policy", "dim_address", "dim_payment_method", "fact_premiums"]
    for tbl in mart_tables:
        try:
            spark.sql(f"TRUNCATE TABLE pg_jdbc_catalog.idm_mart.{tbl}")
            print(f"  ✓ {tbl}")
        except Exception as e:
            print(f"  ✗ {tbl}: {e}")

    print("\n[OK] All tables truncated (dim_organization untouched).")

finally:
    spark.stop()
