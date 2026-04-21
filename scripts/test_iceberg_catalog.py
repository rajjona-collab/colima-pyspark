#!/usr/bin/env python3
"""Test Iceberg JDBC catalog: create schema, table, insert, verify."""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
import os
from datetime import date
from decimal import Decimal

# Config from environment
JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres.postgres.svc.cluster.local:5432/metastore")
DB_USER = os.getenv("POSTGRES_USER", "dbadmin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "")
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL_S3", "http://minio.minio.svc.cluster.local:9000")
S3_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
S3_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "")

spark = SparkSession.builder \
    .appName("test_iceberg_catalog") \
    .config("spark.sql.catalog.pg_jdbc_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.pg_jdbc_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
    .config("spark.sql.catalog.pg_jdbc_catalog.uri", JDBC_URL) \
    .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.user", DB_USER) \
    .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.password", DB_PASS) \
    .config("spark.sql.catalog.pg_jdbc_catalog.warehouse", "s3://lakehouse/sg-life-idm/") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

try:
    # 1. CREATE DATABASE
    print("\n=== Step 1: Create Database ===")
    spark.sql("CREATE DATABASE IF NOT EXISTS pg_jdbc_catalog.test_iceberg LOCATION 's3://lakehouse/sg-life-idm/test_iceberg/'")
    print("✓ Database created: pg_jdbc_catalog.test_iceberg")

    # 2. CREATE TABLE
    print("\n=== Step 2: Create Table ===")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.test_iceberg.employees (
        emp_id INT,
        emp_name STRING,
        department STRING,
        salary DECIMAL(10, 2),
        hire_date DATE
    )
    USING iceberg
    LOCATION 's3://lakehouse/sg-life-idm/test_iceberg/employees'
    PARTITIONED BY (department)
    """
    spark.sql(create_table_sql)
    print("✓ Table created: pg_jdbc_catalog.test_iceberg.employees")

    # 3. INSERT TEST RECORDS
    print("\n=== Step 3: Insert Test Records ===")
    test_data = [
        (1, "Alice Johnson", "Engineering", Decimal("95000.00"), date(2020, 1, 15)),
        (2, "Bob Smith", "Sales", Decimal("75000.00"), date(2019, 6, 1)),
        (3, "Carol White", "Engineering", Decimal("92000.00"), date(2021, 3, 10)),
        (4, "David Brown", "HR", Decimal("70000.00"), date(2018, 11, 20)),
        (5, "Eve Davis", "Sales", Decimal("78000.00"), date(2020, 9, 5)),
    ]

    schema = StructType([
        StructField("emp_id", IntegerType(), False),
        StructField("emp_name", StringType(), False),
        StructField("department", StringType(), False),
        StructField("salary", DecimalType(10, 2), False),
        StructField("hire_date", DateType(), False),
    ])

    df = spark.createDataFrame(test_data, schema=schema)
    df.write.mode("append").insertInto("pg_jdbc_catalog.test_iceberg.employees")
    print(f"✓ Inserted {len(test_data)} records")

    # 4. VERIFY DATA
    print("\n=== Step 4: Verify Data ===")
    result_df = spark.sql("SELECT * FROM pg_jdbc_catalog.test_iceberg.employees ORDER BY emp_id")
    print(f"Total rows: {result_df.count()}")
    result_df.show(truncate=False)

    # 5. CHECK PARTITION METADATA
    print("\n=== Step 5: Check Partition Metadata ===")
    spark.sql("SELECT COUNT(*) as count_by_dept, department FROM pg_jdbc_catalog.test_iceberg.employees GROUP BY department").show()

    print("\n✅ All tests passed!")

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)
finally:
    spark.stop()
