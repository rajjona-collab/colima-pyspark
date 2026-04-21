"""
colima_parse_csv.py — Phase 1: CSV validation, PII hashing, Iceberg staging load.

Reads CSV from MinIO, validates, hashes PII (SSN), writes Iceberg stg_* tables.
Schema validation via config JSON from MinIO.

Spark pod env vars (set by KubernetesPodOperator):
  AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
  POSTGRES_JDBC_URL, POSTGRES_USER, POSTGRES_PASSWORD

Usage (in pod):
    spark-submit colima_parse_csv.py \
      --org ail --file_type basic --batch_date 2026-03-31 \
      --config_path s3://lakehouse/sg-life-idm/config/ail/basic.json \
      --s3_input s3://lakehouse/sg-life-idm/landing/ail/ailbasic.csv
"""

import hashlib
import json
import os
import re
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, DecimalType
import requests


def send_zoom_alert(org, file_type, batch_date, validation_rejected, schema_rejected):
    """Send rejection alert to Zoom webhook (via Slack)."""
    webhook_url = os.environ.get("ZOOM_WEBHOOK_URL")
    if not webhook_url:
        print(f"[ALERT_SKIPPED] ZOOM_WEBHOOK_URL not set")
        return

    message = {
        "text": f"⚠️ IDM Parse CSV Rejections",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*IDM Parse CSV - Rejections Detected*\n\n*Org:* {org}\n*File Type:* {file_type}\n*Batch Date:* {batch_date}\n\n*Validation Rejections:* {validation_rejected}\n*Schema Mismatches:* {schema_rejected}\n\nCheck `pg_jdbc_catalog.idm_staging.stg_rejected` for details."
                }
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=message, timeout=5)
        if response.status_code == 200:
            print(f"[ALERT] Sent Zoom notification for {org}/{file_type}")
        else:
            print(f"[ALERT_FAILED] Zoom webhook returned {response.status_code}")
    except Exception as e:
        print(f"[ALERT_ERROR] Failed to send Zoom alert: {e}")


def get_spark_session():
    """Create SparkSession with Iceberg + S3 config."""
    postgres_url = os.environ.get("POSTGRES_JDBC_URL")
    postgres_user = os.environ.get("POSTGRES_USER")
    postgres_pass = os.environ.get("POSTGRES_PASSWORD")
    s3_endpoint = os.environ.get("AWS_ENDPOINT_URL_S3")
    s3_key = os.environ.get("AWS_ACCESS_KEY_ID")
    s3_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")

    print(f"[DEBUG] Spark config: POSTGRES_URL={postgres_url[:50] if postgres_url else None}...")
    print(f"[DEBUG] Spark config: S3_ENDPOINT={s3_endpoint}")

    return (SparkSession.builder
        .appName("IcebergParseCSV")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.pg_jdbc_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.pg_jdbc_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.pg_jdbc_catalog.uri", postgres_url)
        .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.user", postgres_user)
        .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.password", postgres_pass)
        .config("spark.sql.catalog.pg_jdbc_catalog.warehouse", "s3://lakehouse/sg-life-idm/")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())


def load_config(spark, config_path):
    """Load schema config from S3."""
    if config_path.startswith("s3://") or config_path.startswith("s3a://"):
        bucket, key = config_path.split("://", 1)[1].split("/", 1)
        from pyspark.sql.functions import col
        # Read as text and parse JSON manually
        lines = spark.read.text(config_path).collect()
        return json.loads("".join([row[0] for row in lines]))
    else:
        with open(config_path) as f:
            return json.load(f)


def map_columns_to_table(df, file_type):
    """Map config columns to stg_* table schema. File-type-specific transformations."""
    if file_type == "basic":
        # Combine first_name + last_name → policyholder_name, dob → effective_date
        if "first_name" in df.columns and "last_name" in df.columns:
            df = df.withColumn(
                "policyholder_name",
                F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
            ).drop("first_name", "last_name")
        if "dob" in df.columns:
            df = df.withColumn("effective_date", F.col("dob")).drop("dob")
        # Drop columns not in stg_basic schema
        extra_cols = [c for c in df.columns if c in ["face_amount", "policy_type", "gender", "ssn_field"]]
        if extra_cols:
            df = df.drop(*extra_cols)
    elif file_type == "address":
        # street1 → street_address, zip → zip_code, drop street2
        if "street1" in df.columns:
            df = df.withColumn("street_address", F.col("street1")).drop("street1")
        if "street2" in df.columns:
            df = df.drop("street2")
        if "zip" in df.columns:
            df = df.withColumn("zip_code", F.col("zip")).drop("zip")
        # Add country as NULL (not in config)
        if "country" not in df.columns:
            df = df.withColumn("country", F.lit(None).cast(StringType()))
        # Drop effective_date if present (not in stg_address)
        if "effective_date" in df.columns:
            df = df.drop("effective_date")
    elif file_type == "bankinfo":
        # account_type → payment_method_type, extract last 4 of account_number, hash routing_number
        if "account_type" in df.columns:
            df = df.withColumn("payment_method_type", F.col("account_type")).drop("account_type")
        if "account_number" in df.columns:
            df = df.withColumn(
                "account_number_last_4",
                F.when(F.col("account_number").isNotNull(),
                       F.substring(F.col("account_number"), -4, 4))
                .otherwise(None)
            ).drop("account_number")
        if "routing_number" in df.columns:
            sha256_udf = F.udf(
                lambda s: hashlib.sha256(s.strip().encode()).hexdigest() if s and s.strip() else None,
                StringType()
            )
            df = df.withColumn(
                "routing_number_hash",
                sha256_udf(F.col("routing_number"))
            ).drop("routing_number")
        # Drop effective_date if present (not in stg_bankinfo)
        if "effective_date" in df.columns:
            df = df.drop("effective_date")
    elif file_type == "premiums":
        # premium_date → transaction_date, amount → premium_amount, status → payment_status
        if "premium_date" in df.columns:
            df = df.withColumn("transaction_date", F.col("premium_date")).drop("premium_date")
        if "amount" in df.columns:
            df = df.withColumn("premium_amount", F.col("amount")).drop("amount")
        if "status" in df.columns:
            df = df.withColumn("payment_status", F.col("status")).drop("status")
        # Drop payment_method if present (not in stg_premiums)
        if "payment_method" in df.columns:
            df = df.drop("payment_method")

    return df


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--org", required=True)
    parser.add_argument("--file_type", required=True)
    parser.add_argument("--batch_date", required=True)
    parser.add_argument("--config_path", required=True)
    parser.add_argument("--s3_input", required=True)
    args = parser.parse_args()

    org = args.org
    file_type = args.file_type
    batch_date = args.batch_date
    config_path = args.config_path
    s3_input = args.s3_input

    print(f"[START] org={org} file_type={file_type} batch_date={batch_date}")
    print(f"        input={s3_input}")

    spark = get_spark_session()
    config = load_config(spark, config_path)

    col_names = [c["name"] for c in config["columns"]]
    not_null_cols = config.get("not_null_check", [])
    ssn_field = config.get("ssn_field")
    date_cols = [c["name"] for c in config["columns"] if c["type"] == "date"]

    print(f"[CONFIG] columns={col_names}")

    # Read CSV
    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("delimiter", config.get("delimiter", ","))
        .csv(s3_input)
    )

    total_count = df_raw.count()
    print(f"[READ] {total_count} rows from {s3_input}")

    # Column presence check
    actual_cols = set(df_raw.columns)
    expected_cols = set(col_names)
    missing = expected_cols - actual_cols

    if missing:
        raise ValueError(f"[FATAL] Missing required columns: {missing}")

    df = df_raw.select(*col_names)

    # Standardize strings
    strip_punct = F.udf(lambda s: re.sub(r"[^A-Z\s]", "", s.upper()).strip() if s else s, StringType())

    if "first_name" in col_names:
        df = df.withColumn("first_name", strip_punct(F.col("first_name")))
    if "last_name" in col_names:
        df = df.withColumn("last_name", strip_punct(F.col("last_name")))

    # Org code check
    df = df.withColumn(
        "_org_mismatch",
        F.when(F.col("org_code") != F.lit(org), F.lit(f"org_code mismatch: expected {org}")).otherwise(F.lit(None))
    )

    # Not-null checks
    null_conditions = []
    for col in not_null_cols:
        null_conditions.append(
            F.when(
                F.col(col).isNull() | (F.trim(F.col(col)) == ""),
                F.lit(f"null_or_empty: {col}")
            )
        )

    rejection_expr = F.coalesce(*null_conditions, F.col("_org_mismatch")) if null_conditions else F.col("_org_mismatch")
    df = df.withColumn("_null_rejection", rejection_expr)

    # Date validation
    date_pattern = r"^\d{4}-\d{2}-\d{2}$"
    date_rejection_exprs = []
    for dc in date_cols:
        date_rejection_exprs.append(
            F.when(
                F.col(dc).isNotNull()
                & (F.trim(F.col(dc)) != "")
                & (~F.col(dc).rlike(date_pattern)),
                F.lit(f"invalid_date_format: {dc}")
            )
        )

    if date_rejection_exprs:
        date_rejection = F.coalesce(*date_rejection_exprs)
        df = df.withColumn("_date_rejection", date_rejection)
    else:
        df = df.withColumn("_date_rejection", F.lit(None).cast(StringType()))

    df = df.withColumn(
        "rejection_reason",
        F.coalesce(F.col("_null_rejection"), F.col("_date_rejection"))
    )

    # SSN handling
    if ssn_field and ssn_field in col_names:
        sha256_udf = F.udf(
            lambda s: hashlib.sha256(s.strip().encode()).hexdigest() if s and s.strip() else None,
            StringType()
        )

        df = (
            df
            .withColumn("ssn_hash", sha256_udf(F.col(ssn_field)))
            .withColumn(
                "match_method",
                F.when(F.col(ssn_field).isNull() | (F.trim(F.col(ssn_field)) == ""), F.lit("NAME_DOB"))
                 .otherwise(F.lit("SSN"))
            )
            .drop(ssn_field)
        )
        print(f"[SSN] Hashed {ssn_field} → ssn_hash; raw column dropped")

    # Audit columns (load_timestamp required by all stg_* tables)
    df = (
        df
        .withColumn("batch_date", F.to_date(F.lit(batch_date), "yyyy-MM-dd"))
        .withColumn("load_timestamp", F.current_timestamp())
    )

    # Split valid / rejected
    df_valid = df.filter(F.col("rejection_reason").isNull()) \
                .drop("rejection_reason", "_null_rejection", "_date_rejection", "_org_mismatch")
    df_rejected = df.filter(F.col("rejection_reason").isNotNull()) \
                .drop("_null_rejection", "_date_rejection", "_org_mismatch")

    valid_count = df_valid.count()
    rejected_count = df_rejected.count()

    print(f"[SPLIT] total={total_count}  valid={valid_count}  rejected={rejected_count}")

    # Cast to proper types
    for _col in config["columns"]:
        _name = _col["name"]
        if _name not in df_valid.columns:
            continue
        if _col["type"] == "date":
            df_valid = df_valid.withColumn(_name, F.to_date(F.col(_name), "yyyy-MM-dd"))
        elif _col["type"] == "decimal":
            df_valid = df_valid.withColumn(_name, F.col(_name).cast(DecimalType(15, 2)))

    # Map config columns to stg_* table schema (handles transformations, renames, drops)
    df_valid = map_columns_to_table(df_valid, file_type)

    # Select columns in the exact order expected by the target table
    table_column_order = {
        "basic": ["org_code", "batch_date", "policy_number", "policyholder_name", "ssn_hash", "match_method", "policy_status", "issue_date", "effective_date", "load_timestamp"],
        "address": ["org_code", "batch_date", "policy_number", "address_type", "street_address", "city", "state", "zip_code", "country", "load_timestamp"],
        "bankinfo": ["org_code", "batch_date", "policy_number", "payment_method_type", "account_number_last_4", "bank_name", "routing_number_hash", "load_timestamp"],
        "premiums": ["org_code", "batch_date", "transaction_id", "policy_number", "premium_amount", "transaction_date", "payment_status", "load_timestamp"]
    }

    if file_type in table_column_order:
        df_valid = df_valid.select(*table_column_order[file_type])

    # Write to Iceberg staging table
    table_name = f"pg_jdbc_catalog.idm_staging.stg_{file_type}"
    schema_mismatch_count = 0
    print("  ->Truncate stg_{file_type} table...")
    spark.sql(f"""
        TRUNCATE TABLE pg_jdbc_catalog.idm_staging.stg_{file_type}              
              """)

    try:
        # temp view + SQL INSERT works reliably for non-default catalog 3-part names
        df_valid.createOrReplaceTempView("_stg_insert_tmp")
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM _stg_insert_tmp")
        spark.catalog.dropTempView("_stg_insert_tmp")
        print(f"[WRITE] {valid_count} rows → {table_name}")
    except Exception as e:
        error_msg = str(e)
        if "too many data columns" in error_msg.lower() or "column arity" in error_msg.lower():
            print(f"[SCHEMA_MISMATCH] {valid_count} rows cannot be inserted to {table_name}")
            schema_mismatch_count = valid_count
            # Write mismatched records to stg_rejected table
            df_mismatch = df_valid.select(
                F.lit(org).alias("org_code"),
                F.to_date(F.lit(batch_date), "yyyy-MM-dd").alias("batch_date"),
                F.lit(file_type).alias("file_type"),
                F.lit("Schema mismatch: source columns don't match table schema").alias("rejection_reason"),
                F.to_json(F.struct("*")).alias("raw_record"),
                F.current_timestamp().alias("load_timestamp")
            )
            try:
                df_mismatch.createOrReplaceTempView("_stg_reject_tmp")
                spark.sql("INSERT INTO pg_jdbc_catalog.idm_staging.stg_rejected SELECT * FROM _stg_reject_tmp")
                spark.catalog.dropTempView("_stg_reject_tmp")
                print(f"[REJECT_SCHEMA] {valid_count} rows → stg_rejected")
            except Exception as reject_err:
                print(f"[REJECT_ERROR] Failed to write to stg_rejected: {reject_err}")
        else:
            print(f"[ERROR] {error_msg[:200]}")
            raise

    # Write validation-rejected to S3
    if rejected_count > 0:
        rejected_path = f"s3://lakehouse/sg-life-idm/rejected/{org}/{file_type}/{batch_date}/"
        df_rejected.write.mode("overwrite").option("header", "true").csv(rejected_path)
        print(f"[REJECT_VALIDATION] {rejected_count} rows → {rejected_path}")

    # Send Zoom alert if there are any rejections
    total_rejected = rejected_count + schema_mismatch_count
    if total_rejected > 0:
        send_zoom_alert(org, file_type, batch_date, rejected_count, schema_mismatch_count)

    print("[DONE]")


if __name__ == "__main__":
    main()
