#!/usr/bin/env python3
"""truncate Iceberg catalogs: idm_mart schemas + tables."""

from pyspark.sql import SparkSession
import os

# Config from environment
JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres.postgres.svc.cluster.local:5432/metastore")
DB_USER = os.getenv("POSTGRES_USER", "dbadmin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "")
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL_S3", "http://minio.minio.svc.cluster.local:9000")
S3_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
S3_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "")

spark = SparkSession.builder \
    .appName("colima_create_catalog") \
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


#         "pg_jdbc_catalog.idm_mart.dim_organization" should not be truncated

def run_ddl():
    mart_tables=[
        "pg_jdbc_catalog.idm_mart.dim_policyholder",
        "pg_jdbc_catalog.idm_mart.dim_policy",
        "pg_jdbc_catalog.idm_mart.dim_address",
        "pg_jdbc_catalog.idm_mart.dim_payment_method",
        "pg_jdbc_catalog.idm_mart.fact_premiums"
    ]

    try:
        for table in mart_tables:
            print(f"\n=== Truncating table {table} ===")
            spark.sql(f"TRUNCATE TABLE {table}")

        # ========== VERIFICATION ==========
        print("\n=== Verification ===")
        staging_tables = spark.sql("SHOW TABLES IN pg_jdbc_catalog.idm_staging")
        print(f"✓ Staging tables created:")
        staging_tables.show(truncate=False)

        mart_tables = spark.sql("SHOW TABLES IN pg_jdbc_catalog.idm_mart")
        print(f"✓ Mart tables created:")
        mart_tables.show(truncate=False)

        print("\n✅ All DDL initialization completed successfully!")

    except Exception as e:
        print(f"\n❌ Error during DDL execution: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_ddl()
