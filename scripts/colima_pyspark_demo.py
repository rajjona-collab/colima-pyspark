#!/usr/bin/env python
"""
colima_pyspark_demo.py — Create catalog, DB, table, insert data (host OS).

Usage:
    source ~/src/colima-pyspark/.env/local.env
    cd ~/src/colima-pyspark
    /Users/rajani/miniforge3/envs/py312/bin/python colima_pyspark_demo.py
"""

import os
import glob as _glob

# Set PySpark to use py312 consistently (driver + executors)
py312_path = "/Users/rajani/miniforge3/envs/py312/bin/python"
os.environ["PYSPARK_PYTHON"] = py312_path
os.environ["PYSPARK_DRIVER_PYTHON"] = py312_path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date

# Load .env/local.env (optional, for local testing)
_env_file = os.path.join(os.path.dirname(__file__), "..", ".env", "local.env")
if os.path.exists(_env_file):
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key, val = line.split("=", 1)
                os.environ.setdefault(key, val)

# Load env vars (from .env file or shell environment)
POSTGRES_JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:25432/metastore")
POSTGRES_USER = os.getenv("POSTGRES_USER", "dbadmin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
AWS_ENDPOINT_URL_S3 = os.getenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "lakehouse-etl")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# Auto-load JARs from scratch/jars/ (relative to this script's parent dir)
_JARS_DIR = os.path.join(os.path.dirname(__file__), "..", "scratch", "jars")
_JARS = ",".join(_glob.glob(os.path.join(_JARS_DIR, "*.jar"))) if os.path.isdir(_JARS_DIR) else ""

if not _JARS:
    print(f"[ERROR] No JARs found in {os.path.abspath(_JARS_DIR)}")
    print("[ERROR] Expected: iceberg-spark-runtime, postgresql, hadoop-aws, aws-java-sdk-bundle, awssdk-bundle")
    exit(1)
print(f"[OK] Loaded {len(_JARS.split(','))} JARs")
# Create SparkSession with Iceberg + JDBC catalog
spark = (SparkSession.builder
    .appName("DemoIcebergDDL")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.pg_jdbc_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.pg_jdbc_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
    .config("spark.sql.catalog.pg_jdbc_catalog.uri", POSTGRES_JDBC_URL)
    .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.user", POSTGRES_USER)
    .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.password", POSTGRES_PASSWORD)
    .config("spark.sql.catalog.pg_jdbc_catalog.warehouse", "s3://lakehouse/demo/")
    .config("spark.jars", _JARS)
    # S3A config for MinIO
    .config("spark.hadoop.fs.s3a.endpoint", AWS_ENDPOINT_URL_S3)
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())

try:
    # 1. CREATE DATABASE
    print("\n=== Creating testdb schema ===")
    spark.sql("""
        CREATE DATABASE IF NOT EXISTS pg_jdbc_catalog.testdb
        LOCATION 's3://lakehouse/demo/testdb/'
    """)
    print("✓ Schema created")

    # 2. CREATE TABLE
    print("\n=== Creating employees table ===")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.testdb.employees (
            emp_id     INT,
            emp_name   STRING,
            department STRING,
            hire_date  DATE
        )
        USING iceberg
        LOCATION 's3://lakehouse/demo/testdb/employees'
    """)
    print("✓ Table created")

    # 3. INSERT DATA (via temp view + SQL — works for non-default catalog)
    print("\n=== Inserting data ===")
    data = [
        (1, "Alice", "Engineering", date(2020, 1, 15)),
        (2, "Bob", "Sales", date(2019, 6, 1)),
        (3, "Carol", "Engineering", date(2021, 3, 10)),
    ]
    schema = StructType([
        StructField("emp_id", IntegerType()),
        StructField("emp_name", StringType()),
        StructField("department", StringType()),
        StructField("hire_date", DateType()),
    ])
    df = spark.createDataFrame(data, schema=schema)

    # Temp view + SQL INSERT is the reliable pattern for non-default catalogs
    df.createOrReplaceTempView("_tmp_employees")
    spark.sql("INSERT INTO pg_jdbc_catalog.testdb.employees SELECT * FROM _tmp_employees")
    spark.catalog.dropTempView("_tmp_employees")
    print(f"✓ Inserted {df.count()} rows")

    # 4. VERIFY
    print("\n=== Verification ===")
    spark.sql("SELECT * FROM pg_jdbc_catalog.testdb.employees").show()
    count = spark.sql("SELECT COUNT(*) as cnt FROM pg_jdbc_catalog.testdb.employees").collect()[0][0]
    print(f"✓ Total rows: {count}")

    # 5. Check via Trino (optional)
    print("\n=== Via Trino ===")
    print("Run: java -jar ~/scripts/trino-cli-480 --server http://localhost:8999")
    print("  > SELECT * FROM iceberg.testdb.employees;")

finally:
    spark.stop()
