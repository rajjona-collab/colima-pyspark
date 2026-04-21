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



def run_ddl():
    WAREHOUSE_ROOT = "s3://lakehouse/sg-life-idm"
    try:
        spark.sql("""
            DROP TABLE pg_jdbc_catalog.idm_mart.dim_organization
        """)
        # dim_organization (static reference)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_mart.dim_organization (
                org_key   INT NOT NULL,
                org_code  STRING NOT NULL,
                org_name  STRING NOT NULL,
                created_at TIMESTAMP DEFAULT current_timestamp()
            )
            USING iceberg
            LOCATION '{WAREHOUSE_ROOT}/idm_mart/dim_organization/'
        """.format(WAREHOUSE_ROOT=WAREHOUSE_ROOT))
        spark.sql("""
            INSERT INTO pg_jdbc_catalog.idm_mart.dim_organization (org_key, org_code, org_name)
            VALUES (1, 'ail', 'Org A'), (2, 'gl', 'Org G'), (3, 'lnl', 'Org L'), (4, 'ua', 'Org U')
        """)
        print("  ✓ dim_organization table created + seeded")


    except Exception as e:
        print(f"\n❌ Error during DDL execution: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_ddl()
