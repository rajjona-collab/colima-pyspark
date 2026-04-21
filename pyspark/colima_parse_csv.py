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
from pyspark.sql.types import StringType


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
        .config("spark.sql.catalog.pg_jdbc_catalog.type", "jdbc")
        .config("spark.sql.catalog.pg_jdbc_catalog.url", postgres_url)
        .config("spark.sql.catalog.pg_jdbc_catalog.user", postgres_user)
        .config("spark.sql.catalog.pg_jdbc_catalog.password", postgres_pass)
        .config("spark.sql.catalog.pg_jdbc_catalog.driver", "org.postgresql.Driver")
        .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.schema", "PUBLIC")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())


def load_config(spark, config_path):
    """Load schema config from S3."""
    if config_path.startswith("s3a://"):
        bucket, key = config_path[6:].split("/", 1)
        from pyspark.sql.functions import col
        # Read as text and parse JSON manually
        lines = spark.read.text(config_path).collect()
        return json.loads("".join([row[0] for row in lines]))
    else:
        with open(config_path) as f:
            return json.load(f)


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

    # Audit columns
    df = (
        df
        .withColumn("batch_date", F.lit(batch_date).cast("date"))
        .withColumn("source_file", F.lit(s3_input))
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
    from pyspark.sql.types import DateType, DecimalType

    for _col in config["columns"]:
        _name = _col["name"]
        if _name not in df_valid.columns:
            continue
        if _col["type"] == "date":
            df_valid = df_valid.withColumn(_name, F.to_date(F.col(_name), "yyyy-MM-dd"))
        elif _col["type"] == "decimal":
            df_valid = df_valid.withColumn(_name, F.col(_name).cast(DecimalType(15, 2)))

    # Write to Iceberg staging table
    table_name = f"pg_jdbc_catalog.idm_staging.stg_{file_type}"
    (
        df_valid
        .write
        .mode("append")
        .option("merge-schema", "true")
        .insertInto(table_name)
    )
    print(f"[WRITE] {valid_count} rows → {table_name}")

    # Write rejected to S3
    if rejected_count > 0:
        rejected_path = f"s3://lakehouse/sg-life-idm/rejected/{org}/{file_type}/{batch_date}/"
        df_rejected.write.mode("overwrite").option("header", "true").csv(rejected_path)
        print(f"[REJECT] {rejected_count} rows → {rejected_path}")

    print("[DONE]")


if __name__ == "__main__":
    main()
