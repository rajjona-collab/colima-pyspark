"""
colima_adhoc 

Run inside spark-debug pod:
    kubectl exec -it spark-debug -n spark -- bash
    spark-submit /path/to/ddl/colima_create_catalog.py

Creates:
  - pg_jdbc_catalog.idm_staging.{stg_basic, stg_address, stg_bankinfo, stg_premiums}
  - pg_jdbc_catalog.idm_mart.{dim_organization, dim_policyholder, dim_policy, dim_address, dim_payment_method, fact_premiums}
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, DecimalType, BooleanType
)

# Environment (set by pod)
POSTGRES_JDBC_URL = os.environ.get("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres.postgres.svc.cluster.local:5432/metastore")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "dbadmin")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "e2TApDQrA3L9K8s7")
MINIO_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://minio.minio.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "lakehouse-etl")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "PiDiigd17yVJOlpYFRl+bRAhg9aLueMchdSO9IKv")

WAREHOUSE_ROOT = "s3://lakehouse/sg-life-idm"


def create_spark_session():
    """Create SparkSession with Iceberg + S3 config."""
    return (SparkSession.builder
        .appName("IcebergCreateCatalog")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.pg_jdbc_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.pg_jdbc_catalog.type", "jdbc")
        .config("spark.sql.catalog.pg_jdbc_catalog.url", POSTGRES_JDBC_URL)
        .config("spark.sql.catalog.pg_jdbc_catalog.user", POSTGRES_USER)
        .config("spark.sql.catalog.pg_jdbc_catalog.password", POSTGRES_PASSWORD)
        .config("spark.sql.catalog.pg_jdbc_catalog.driver", "org.postgresql.Driver")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())


def create_mart_tables(spark):
    """Create mart schema and tables."""
    print("\n[MART SCHEMA]")

    # Create schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS pg_jdbc_catalog.idm_mart LOCATION '{WAREHOUSE_ROOT}/idm_mart/'")
    print("  ✓ idm_mart schema created")

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


if __name__ == "__main__":
    spark = create_spark_session()
    print(f"\n[INFO] Spark session created. Warehouse: {WAREHOUSE_ROOT}")

    create_mart_tables(spark)

    # Verify
    print("\n[VERIFY]")
    tables = spark.sql("SHOW TABLES IN pg_jdbc_catalog.idm_staging").collect()
    print(f"  Staging tables: {len(tables)}")
    tables = spark.sql("SHOW TABLES IN pg_jdbc_catalog.idm_mart").collect()
    print(f"  Mart tables: {len(tables)}")

    spark.stop()
    print("\n[OK] Catalog creation complete.")
