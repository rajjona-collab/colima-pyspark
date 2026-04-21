"""
colima_create_catalog.py — DDL runner for Iceberg schemas + tables.

Designed to run in spark-debug pod (in-cluster) or locally via port-forwards.
Catalog name: pg_jdbc_catalog (Iceberg JDBC, backed by Postgres metastore).

Usage (in spark-debug pod):
    spark-submit /path/to/ddl/colima_create_catalog.py

Usage (local, with port-forwards active):
    export POSTGRES_JDBC_URL=jdbc:postgresql://localhost:25432/metastore
    export POSTGRES_USER=dbadmin POSTGRES_PASSWORD=<pw>
    export AWS_ENDPOINT_URL_S3=http://localhost:9000
    export AWS_ACCESS_KEY_ID=lakehouse-etl AWS_SECRET_ACCESS_KEY=<key>
    /Users/rajani/miniforge3/envs/py312/bin/python ddl/colima_create_catalog.py
"""

import os
import glob as _glob
from pyspark.sql import SparkSession

# Load env vars from .env/local.env if not already set
if not os.getenv("POSTGRES_JDBC_URL"):
    env_file = ".env/local.env"
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    key, val = line.strip().split('=', 1)
                    if key not in os.environ:
                        os.environ[key] = val.strip('\'"')

JDBC_URL  = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres.postgres.svc.cluster.local:5432/metastore")
DB_USER   = os.getenv("POSTGRES_USER",     "dbadmin")
DB_PASS   = os.getenv("POSTGRES_PASSWORD", "")
S3_EP     = os.getenv("AWS_ENDPOINT_URL_S3", "http://minio.minio.svc.cluster.local:9000")
S3_KEY    = os.getenv("AWS_ACCESS_KEY_ID",   "lakehouse-etl")
S3_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "")
WH        = "s3://lakehouse/sg-life-idm"

# Locate JARs: use scratch/jars/ when running locally (not in pod)
_JARS_DIR = os.path.join(os.path.dirname(__file__), "..", "scratch", "jars")
_JARS = ",".join(_glob.glob(os.path.join(_JARS_DIR, "*.jar"))) if os.path.isdir(_JARS_DIR) else ""

spark = (SparkSession.builder
    .appName("IcebergCreateCatalog")
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
    .getOrCreate())


def run_ddl():
    try:
        # ── Staging schema ─────────────────────────────────────────────────────
        print("\n=== Creating idm_staging schema ===")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS pg_jdbc_catalog.idm_staging LOCATION '{WH}/idm_staging/'")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_staging.stg_basic (
            org_code          STRING NOT NULL,
            batch_date        DATE NOT NULL,
            policy_number     STRING NOT NULL,
            policyholder_name STRING,
            ssn_hash          STRING,
            match_method      STRING,
            policy_status     STRING,
            issue_date        DATE,
            effective_date    DATE,
            load_timestamp    TIMESTAMP NOT NULL
        )
        USING iceberg
        LOCATION '{WH}/idm_staging/stg_basic'
        PARTITIONED BY (org_code, batch_date)
        """)
        print("  ✓ stg_basic")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_staging.stg_address (
            org_code       STRING NOT NULL,
            batch_date     DATE NOT NULL,
            policy_number  STRING NOT NULL,
            address_type   STRING NOT NULL,
            street_address STRING,
            city           STRING,
            state          STRING,
            zip_code       STRING,
            country        STRING,
            load_timestamp TIMESTAMP NOT NULL
        )
        USING iceberg
        LOCATION '{WH}/idm_staging/stg_address'
        PARTITIONED BY (org_code, batch_date)
        """)
        print("  ✓ stg_address")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_staging.stg_bankinfo (
            org_code              STRING NOT NULL,
            batch_date            DATE NOT NULL,
            policy_number         STRING NOT NULL,
            payment_method_type   STRING,
            account_number_last_4 STRING,
            bank_name             STRING,
            routing_number_hash   STRING,
            load_timestamp        TIMESTAMP NOT NULL
        )
        USING iceberg
        LOCATION '{WH}/idm_staging/stg_bankinfo'
        PARTITIONED BY (org_code, batch_date)
        """)
        print("  ✓ stg_bankinfo")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_staging.stg_premiums (
            org_code         STRING NOT NULL,
            batch_date       DATE NOT NULL,
            transaction_id   STRING NOT NULL,
            policy_number    STRING NOT NULL,
            premium_amount   DECIMAL(12, 2),
            transaction_date DATE,
            payment_status   STRING,
            load_timestamp   TIMESTAMP NOT NULL
        )
        USING iceberg
        LOCATION '{WH}/idm_staging/stg_premiums'
        PARTITIONED BY (org_code, batch_date)
        """)
        print("  ✓ stg_premiums")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_staging.stg_rejected (
            org_code         STRING NOT NULL,
            batch_date       DATE NOT NULL,
            file_type        STRING NOT NULL,
            rejection_reason STRING,
            raw_record       STRING NOT NULL,
            load_timestamp   TIMESTAMP NOT NULL
        )
        USING iceberg
        LOCATION '{WH}/idm_staging/stg_rejected'
        PARTITIONED BY (org_code, batch_date)
        """)
        print("  ✓ stg_rejected")

        # ── Mart schema ────────────────────────────────────────────────────────
        print("\n=== Creating idm_mart schema ===")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS pg_jdbc_catalog.idm_mart LOCATION '{WH}/idm_mart/'")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_mart.dim_organization (
            org_key      INT NOT NULL,
            org_code     STRING NOT NULL,
            org_name     STRING,
            org_type     STRING,
            active_flag  BOOLEAN,
            created_date DATE NOT NULL,
            updated_date DATE NOT NULL
        )
        USING iceberg
        LOCATION '{WH}/idm_mart/dim_organization'
        """)
        spark.sql("""
        INSERT INTO pg_jdbc_catalog.idm_mart.dim_organization
            (org_key, org_code, org_name, org_type, active_flag, created_date, updated_date)
        VALUES
            (1, 'ail', 'American Income Life',    'subsidiary', true, current_date(), current_date()),
            (2, 'gl',  'Globe Life',              'subsidiary', true, current_date(), current_date()),
            (3, 'lnl', 'Liberty National Life',   'subsidiary', true, current_date(), current_date()),
            (4, 'ua',  'United American Insurance','subsidiary', true, current_date(), current_date())
        """)
        print("  ✓ dim_organization (seeded)")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_mart.dim_policyholder (
            policyholder_business_key STRING NOT NULL,
            org_code                  STRING NOT NULL,
            policyholder_name         STRING,
            ssn_hash                  STRING,
            dob_hash                  STRING,
            is_current                BOOLEAN NOT NULL,
            effective_date            DATE NOT NULL,
            end_date                  DATE,
            created_date              DATE NOT NULL,
            updated_date              DATE NOT NULL
        )
        USING iceberg
        LOCATION '{WH}/idm_mart/dim_policyholder'
        PARTITIONED BY (org_code)
        """)
        print("  ✓ dim_policyholder")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_mart.dim_policy (
            policy_number      STRING NOT NULL,
            org_code           STRING NOT NULL,
            policy_status      STRING,
            issue_date         DATE,
            effective_date     DATE,
            termination_date   DATE,
            is_current         BOOLEAN NOT NULL,
            effective_date_scd DATE NOT NULL,
            end_date_scd       DATE,
            created_date       DATE NOT NULL,
            updated_date       DATE NOT NULL
        )
        USING iceberg
        LOCATION '{WH}/idm_mart/dim_policy'
        PARTITIONED BY (org_code)
        """)
        print("  ✓ dim_policy")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_mart.dim_address (
            policy_number  STRING NOT NULL,
            org_code       STRING NOT NULL,
            address_type   STRING NOT NULL,
            street_address STRING,
            city           STRING,
            state          STRING,
            zip_code       STRING,
            country        STRING,
            is_current     BOOLEAN NOT NULL,
            created_date   DATE NOT NULL,
            updated_date   DATE NOT NULL
        )
        USING iceberg
        LOCATION '{WH}/idm_mart/dim_address'
        PARTITIONED BY (org_code)
        """)
        print("  ✓ dim_address")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_mart.dim_payment_method (
            policy_number         STRING NOT NULL,
            org_code              STRING NOT NULL,
            payment_method_type   STRING,
            account_number_last_4 STRING,
            bank_name             STRING,
            is_current            BOOLEAN NOT NULL,
            created_date          DATE NOT NULL,
            updated_date          DATE NOT NULL
        )
        USING iceberg
        LOCATION '{WH}/idm_mart/dim_payment_method'
        PARTITIONED BY (org_code)
        """)
        print("  ✓ dim_payment_method")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pg_jdbc_catalog.idm_mart.fact_premiums (
            transaction_id   STRING NOT NULL,
            policy_number    STRING NOT NULL,
            org_code         STRING NOT NULL,
            premium_amount   DECIMAL(12, 2),
            transaction_date DATE,
            payment_status   STRING,
            created_date     DATE NOT NULL,
            batch_date       DATE NOT NULL
        )
        USING iceberg
        LOCATION '{WH}/idm_mart/fact_premiums'
        PARTITIONED BY (org_code, batch_date)
        """)
        print("  ✓ fact_premiums")

        # ── Verify ─────────────────────────────────────────────────────────────
        print("\n=== Verification ===")
        spark.sql("SHOW TABLES IN pg_jdbc_catalog.idm_staging").show(truncate=False)
        spark.sql("SHOW TABLES IN pg_jdbc_catalog.idm_mart").show(truncate=False)
        print("\n[OK] Catalog creation complete.")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback; traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_ddl()
