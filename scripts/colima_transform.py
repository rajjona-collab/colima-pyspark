"""
colima_transform.py — Phase 2: Iceberg staging → IDM mart with SCD Type 2.

Uses Iceberg MERGE INTO for SCD2 logic:
  1. basic → dim_policyholder + dim_policy (SCD Type 2, upsert by business key)
  2. address → dim_address (SCD Type 1, merge)
  3. bankinfo → dim_payment_method (SCD Type 1, merge)
  4. premiums → fact_premiums (append + dedup by transaction_id)

Usage (in pod):
    spark-submit colima_transform.py --batch_date 2026-03-31
"""

import os
import sys
import glob
import argparse
import logging
import traceback
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configure structured logging with level prefix (parseable by Airflow)
logging.basicConfig(
    format='[%(levelname)s] %(message)s',
    level=logging.INFO,
    stream=sys.stdout,
    force=True  # Override any existing config
)
logger = logging.getLogger(__name__)

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

# Auto-load JARs from scratch/jars/
_JARS_DIR = os.path.join(os.path.dirname(__file__), "..", "scratch", "jars")
_JARS = ",".join(glob.glob(os.path.join(_JARS_DIR, "*.jar"))) if os.path.isdir(_JARS_DIR) else ""


def get_spark_session():
    """Create SparkSession with Iceberg + S3 config."""
    return (SparkSession.builder
        .appName("IcebergTransform")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.pg_jdbc_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.pg_jdbc_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.pg_jdbc_catalog.uri", os.environ.get("POSTGRES_JDBC_URL"))
        .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.user", os.environ.get("POSTGRES_USER"))
        .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.password", os.environ.get("POSTGRES_PASSWORD"))
        .config("spark.sql.catalog.pg_jdbc_catalog.warehouse", "s3://lakehouse/sg-life-idm/")
        .config("spark.jars", _JARS)
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("AWS_ENDPOINT_URL_S3"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())


def transform_basic(spark, batch_date):
    """Load basic → upsert dim_policyholder, dim_policy (SCD Type 2 via MERGE INTO)."""
    logger.info(f"Transform stg_basic → dim_policyholder + dim_policy (SCD2)")

    # Step 1: SCD2 merge for dim_policyholder
    # Business key: COALESCE(ssn_hash, CONCAT(policyholder_name, match_method))
    # Dedup by business_key (one policyholder per batch) using ROW_NUMBER()
    # Match only current records, expire on attribute changes, insert new version

    logger.info(" Merging dim_policyholder...")
    spark.sql(f"""
        MERGE INTO pg_jdbc_catalog.idm_mart.dim_policyholder t
        USING (
            SELECT
                business_key,
                org_code,
                ssn_hash,
                policyholder_name,
                effective_date,
                match_method,
                batch_date
            FROM (
                SELECT
                    COALESCE(ssn_hash, CONCAT(policyholder_name, '_', match_method)) AS business_key,
                    org_code,
                    ssn_hash,
                    policyholder_name,
                    effective_date,
                    match_method,
                    batch_date,
                    ROW_NUMBER() OVER (PARTITION BY org_code, COALESCE(ssn_hash, CONCAT(policyholder_name, '_', match_method)) ORDER BY effective_date DESC) AS rn
                FROM pg_jdbc_catalog.idm_staging.stg_basic
                WHERE batch_date = '{batch_date}'
            )
            WHERE rn = 1
        ) s
        ON t.policyholder_business_key = s.business_key AND t.org_code = s.org_code AND t.is_current = TRUE

        -- SCD2: When record exists and attributes changed, expire old and insert new
        WHEN MATCHED AND (
            t.policyholder_name != s.policyholder_name
            OR COALESCE(t.ssn_hash, '') != COALESCE(s.ssn_hash, '')
        ) THEN
            UPDATE SET
                is_current = FALSE,
                end_date = DATE_SUB(CAST('{batch_date}' AS DATE), 1),
                updated_date = CURRENT_TIMESTAMP()

        -- New record: insert with current flag
        WHEN NOT MATCHED THEN
            INSERT (
                policyholder_business_key, org_code, policyholder_name, ssn_hash, dob_hash,
                is_current, effective_date, end_date, created_date, updated_date
            )
            VALUES (
                s.business_key, s.org_code, s.policyholder_name, s.ssn_hash, NULL,
                TRUE, CAST('{batch_date}' AS DATE), NULL,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
    """)
    logger.info(" dim_policyholder merged (SCD2)")

    # Step 2: SCD2 merge for dim_policy
    # Natural key: (policy_number, org_code)
    # Dedup by policy_number within org (ROW_NUMBER) to prevent cardinality violation.
    logger.info(" Merging dim_policy...")
    spark.sql(f"""
        MERGE INTO pg_jdbc_catalog.idm_mart.dim_policy t
        USING (
            SELECT
                policy_number,
                org_code,
                policy_status,
                issue_date,
                effective_date
            FROM (
                SELECT
                    policy_number, org_code, policy_status, issue_date, effective_date,
                    ROW_NUMBER() OVER (PARTITION BY org_code, policy_number ORDER BY issue_date DESC) AS rn
                FROM pg_jdbc_catalog.idm_staging.stg_basic
                WHERE batch_date = '{batch_date}'
            ) s
            WHERE rn = 1
        ) s
        ON t.policy_number = s.policy_number AND t.org_code = s.org_code AND t.is_current = TRUE

        -- SCD2: Expire old record if attributes changed
        WHEN MATCHED AND (
            t.policy_status != s.policy_status
            OR t.issue_date != s.issue_date
        ) THEN
            UPDATE SET
                is_current = FALSE,
                end_date_scd = DATE_SUB(CAST('{batch_date}' AS DATE), 1),
                updated_date = CURRENT_TIMESTAMP()

        -- New record: insert with natural key
        WHEN NOT MATCHED THEN
            INSERT (
                policy_number, org_code, policy_status, issue_date, effective_date, termination_date,
                is_current, effective_date_scd, end_date_scd,
                created_date, updated_date
            )
            VALUES (
                s.policy_number, s.org_code, s.policy_status, s.issue_date, s.effective_date, NULL,
                TRUE, CAST('{batch_date}' AS DATE), NULL,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
    """)
    logger.info(" dim_policy merged (SCD2)")


def transform_address(spark, batch_date):
    """Load address → merge dim_address (SCD Type 1)."""
    print(f"\n[ADDRESS] Transform stg_address → dim_address (SCD1)")

    # Natural key: (policy_number, org_code, address_type)
    spark.sql(f"""
        MERGE INTO pg_jdbc_catalog.idm_mart.dim_address t
        USING (
            SELECT
                a.policy_number,
                a.org_code,
                a.address_type,
                a.street_address,
                a.city,
                a.state,
                a.zip_code,
                a.country
            FROM pg_jdbc_catalog.idm_staging.stg_address a
            WHERE a.batch_date = '{batch_date}'
        ) s
        ON t.policy_number = s.policy_number AND t.org_code = s.org_code AND t.address_type = s.address_type

        -- SCD1: Update all fields (no history)
        WHEN MATCHED THEN
            UPDATE SET
                street_address = s.street_address,
                city = s.city,
                state = s.state,
                zip_code = s.zip_code,
                country = s.country,
                updated_date = CURRENT_TIMESTAMP()

        -- New record: insert
        WHEN NOT MATCHED THEN
            INSERT (
                policy_number, org_code, address_type, street_address, city, state, zip_code, country,
                is_current, created_date, updated_date
            )
            VALUES (
                s.policy_number, s.org_code, s.address_type, s.street_address, s.city, s.state,
                s.zip_code, s.country, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
    """)
    logger.info(" dim_address merged (SCD1)")


def transform_bankinfo(spark, batch_date):
    """Load bankinfo → merge dim_payment_method (SCD Type 1)."""
    print(f"\n[BANKINFO] Transform stg_bankinfo → dim_payment_method (SCD1)")

    # Natural key: (policy_number, org_code)
    # Dedup by policy_number within org to prevent cardinality violation if stg_bankinfo has duplicates.
    spark.sql(f"""
        MERGE INTO pg_jdbc_catalog.idm_mart.dim_payment_method t
        USING (
            SELECT
                b.policy_number,
                b.org_code,
                b.payment_method_type,
                b.account_number_last_4,
                b.bank_name
            FROM (
                SELECT
                    policy_number, org_code, payment_method_type, account_number_last_4, bank_name,
                    ROW_NUMBER() OVER (PARTITION BY org_code, policy_number ORDER BY policy_number) AS rn
                FROM pg_jdbc_catalog.idm_staging.stg_bankinfo
                WHERE batch_date = '{batch_date}'
            ) b
            WHERE b.rn = 1
        ) s
        ON t.policy_number = s.policy_number AND t.org_code = s.org_code

        -- SCD1: Update all fields
        WHEN MATCHED THEN
            UPDATE SET
                payment_method_type = s.payment_method_type,
                account_number_last_4 = s.account_number_last_4,
                bank_name = s.bank_name,
                updated_date = CURRENT_TIMESTAMP()

        -- New record: insert
        WHEN NOT MATCHED THEN
            INSERT (
                policy_number, org_code, payment_method_type, account_number_last_4, bank_name,
                is_current, created_date, updated_date
            )
            VALUES (
                s.policy_number, s.org_code, s.payment_method_type, s.account_number_last_4, s.bank_name,
                TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
    """)
    logger.info(" dim_payment_method merged (SCD1)")


def transform_premiums(spark, batch_date):
    """Load premiums → append fact_premiums (append-only, dedup by transaction_id)."""
    print(f"\n[PREMIUMS] Transform stg_premiums → fact_premiums (append-only)")

    # Natural key: transaction_id
    # Dedup via LEFT JOIN to fact_premiums (avoids O(n²) NOT IN subquery).
    spark.sql(f"""
        INSERT INTO pg_jdbc_catalog.idm_mart.fact_premiums
            (transaction_id, policy_number, org_code, premium_amount, transaction_date, payment_status, batch_date, created_date)
        SELECT DISTINCT
            p.transaction_id,
            p.policy_number,
            p.org_code,
            p.premium_amount,
            p.transaction_date,
            p.payment_status,
            CAST('{batch_date}' AS DATE) AS batch_date,
            CURRENT_TIMESTAMP() AS created_date
        FROM pg_jdbc_catalog.idm_staging.stg_premiums p
        LEFT JOIN pg_jdbc_catalog.idm_mart.fact_premiums fp
            ON fp.transaction_id = p.transaction_id
        WHERE p.batch_date = '{batch_date}'
        AND fp.transaction_id IS NULL
    """)
    logger.info(" fact_premiums appended (dedup by transaction_id)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_date", required=True)
    args = parser.parse_args()

    batch_date = args.batch_date
    logger.info(f" Transform batch_date={batch_date}")

    spark = get_spark_session()

    try:
        transform_basic(spark, batch_date)
        transform_address(spark, batch_date)
        transform_bankinfo(spark, batch_date)
        transform_premiums(spark, batch_date)

        print(f"\n[OK] Transform complete for batch_date={batch_date}")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        logger.exception("Transform failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
