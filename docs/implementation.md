# Implementation Guide

**Status:** ✅ Production Ready | **Last Updated:** 2026-04-21

---

## Quick Start

### Prerequisites
- Colima Kubernetes running (`colima start`)
- Port-forwards active: `postgres.postgres.svc.cluster.local:25432`, `minio:9000`, `trino:8999`
- `.env/local.env` configured with credentials
- py312 conda env with PySpark 3.5.8

### One-Time Setup

```bash
cd ~/src/colima-pyspark

# 1. Create Iceberg schemas and tables
source .env/local.env
/Users/rajani/miniforge3/envs/py312/bin/python ddl/colima_create_catalog.py

# 2. Generate test data and upload to MinIO
python scripts/colima_generate_data.py
python scripts/colima_setup_minio.py

# 3. Verify schema and data
pytest tests/test_colima_transform_schema.py -v
```

### Run Pipeline

```bash
# Trigger DAG in Airflow (http://localhost:8080)
# Conf: {"batch_date": "2026-03-31"}
# Or via CLI:
kubectl exec -n airflow airflow-scheduler-0 -- \
  airflow dags trigger colima_idm_dag --conf '{"batch_date":"2026-03-31"}'
```

Expected: Parse → Transform → Notify completes in ~2 minutes, 1,359 premium facts inserted.

---

## Architecture

### Schema Design: Natural Keys

All tables use **business keys** instead of surrogates. No policy_key, policyholder_key, etc.

**Why?**
- Iceberg has no IDENTITY support (no auto-increment)
- Natural keys provide stable lineage (policy_number is meaningful)
- Resilient to schema resets (business keys always valid)
- Simpler—no surrogate generation complexity

**Table Keys:**

```
dim_organization:    org_code (static 4-row reference)
dim_policyholder:    (policyholder_business_key, org_code) — SCD2
dim_policy:          (policy_number, org_code) — SCD2
dim_address:         (policy_number, org_code, address_type) — SCD1
dim_payment_method:  (policy_number, org_code) — SCD1
fact_premiums:       (transaction_id) with (policy_number, org_code) stored
```

### Table Structure

#### Staging (idm_staging)
Partition by (org_code, batch_date)

| Table | Columns | Purpose |
|-------|---------|---------|
| stg_basic | policy_number, policyholder_name, ssn_hash, match_method, policy_status, issue_date, effective_date | Raw policy + insured |
| stg_address | policy_number, address_type, street_address, city, state, zip_code, country | Address details |
| stg_bankinfo | policy_number, payment_method_type, account_number_last_4, bank_name | Payment info |
| stg_premiums | transaction_id, policy_number, premium_amount, transaction_date, payment_status | Premium records |

#### Mart (idm_mart)
Partition by (org_code) or (org_code, batch_date) for facts

| Table | SCD Type | Columns |
|-------|----------|---------|
| dim_organization | Reference | org_key (FK), org_code, org_name, org_type, active_flag |
| dim_policyholder | SCD2 | policyholder_business_key, org_code (NK), policyholder_name, ssn_hash, is_current, effective_date, end_date |
| dim_policy | SCD2 | policy_number, org_code (NK), policy_status, issue_date, effective_date_scd, end_date_scd, is_current |
| dim_address | SCD1 | policy_number, org_code, address_type (NK), street_address, city, state, zip_code, country, is_current |
| dim_payment_method | SCD1 | policy_number, org_code (NK), payment_method_type, account_number_last_4, bank_name, is_current |
| fact_premiums | Append | transaction_id (NK), policy_number, org_code (NFK), premium_amount, transaction_date, payment_status, batch_date |

**NK** = Natural Key (no surrogate)  
**NFK** = Natural Foreign Key (references policy_number + org_code in dim_policy, not policy_key)

---

## Data Pipeline Execution

### Phase 1: Parse CSV (colima_parse_csv.py)

**Input:** CSVs from MinIO landing/{org}/{org}{file_type}.csv  
**Output:** Staging tables in idm_staging  
**Duration:** ~15 sec per org

**Steps:**
1. Read CSV file from MinIO
2. Load schema config from MinIO ({org}/{file_type}.json)
3. Validate columns:
   - Column presence (not_null, expected columns)
   - Type compatibility (date, decimal, string)
   - Value ranges (org_code in [ail, gl, lnl, ua])
4. PII hashing: SSN → SHA-256 hash (drop raw SSN)
5. Split records:
   - Valid → INSERT INTO stg_* (incremental append)
   - Invalid → s3://lakehouse/sg-life-idm/rejected/{org}/{file_type}/{batch_date}/
6. Log summary: "Loaded N rows, rejected M rows"

**Idempotency:** TRUNCATE stg_{file_type} before loading (safe for reruns)

### Phase 2: Transform to IDM Mart (colima_transform.py)

**Input:** Staging tables (stg_*)  
**Output:** Mart tables (dim_*, fact_*)  
**Duration:** ~9 sec total

**SCD Type 2 (dim_policyholder, dim_policy):**

```python
spark.sql("""
MERGE INTO dim_policyholder t
USING (
    SELECT business_key, org_code, policyholder_name, ssn_hash, effective_date
    FROM stg_basic
    WHERE ROW_NUMBER() OVER (PARTITION BY org_code, business_key ORDER BY effective_date DESC) = 1
) s
ON t.policyholder_business_key = s.business_key AND t.org_code = s.org_code AND t.is_current = TRUE

WHEN MATCHED AND (attribute_changed) THEN
    UPDATE SET is_current = FALSE, end_date = batch_date - 1

WHEN NOT MATCHED THEN
    INSERT (policyholder_business_key, org_code, ..., is_current, effective_date, ...)
    VALUES (s.business_key, s.org_code, ..., TRUE, batch_date, ...)
""")
```

**Dedup Strategy:** ROW_NUMBER() in USING clause prevents cardinality violations if same business_key appears multiple times in staging.

**SCD Type 1 (dim_address, dim_payment_method):**

Same MERGE, but no is_current/end_date history—just update or insert.

**Fact Insert (fact_premiums):**

```python
spark.sql("""
INSERT INTO fact_premiums (transaction_id, policy_number, org_code, ...)
SELECT DISTINCT
    p.transaction_id,
    p.policy_number,
    p.org_code,
    p.premium_amount,
    p.transaction_date,
    p.payment_status,
    batch_date,
    CURRENT_TIMESTAMP()
FROM stg_premiums p
LEFT JOIN fact_premiums fp ON fp.transaction_id = p.transaction_id
WHERE p.batch_date = '{batch_date}' AND fp.transaction_id IS NULL  -- Dedup
""")
```

**Natural Foreign Key:** fact_premiums stores (policy_number, org_code) directly. No policy_key surrogate needed.

**Result:** 1,359 rows inserted (verified with recent DAG run).

### Phase 3: Notify Completion

**Zoom Alert:** Sent to #pipeline-alerts channel via `zoom_webhook_conn1` connection.

**Payload Format:**
```json
{
  "name": "colima-idm",
  "level": "Info",
  "message": "colima-idm batch_date=2026-03-31 completed successfully"
}
```

**Header:** Authorization token from Airflow connection  
**Endpoint:** `{webhook_url}?format=fields`

---

## PySpark Configuration

**All scripts include:**

```python
spark = (SparkSession.builder
    .appName("...")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.pg_jdbc_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.pg_jdbc_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
    .config("spark.sql.catalog.pg_jdbc_catalog.uri", POSTGRES_JDBC_URL)
    .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.user", POSTGRES_USER)
    .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.password", POSTGRES_PASSWORD)
    .config("spark.sql.catalog.pg_jdbc_catalog.warehouse", "s3://lakehouse/sg-life-idm/")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())
```

**JARs (in scratch/jars/):**
- iceberg-spark-runtime-3.5_2.12-1.10.1.jar
- postgresql-42.7.10.jar
- hadoop-aws-3.3.4.jar (NOT 3.4.x — breaks BulkDelete API)
- aws-java-sdk-bundle-1.12.797.jar
- awssdk-bundle-2.42.20.jar

---

## Testing

### Unit: Schema Contract Tests

```bash
pytest tests/test_colima_transform_schema.py -v
```

Verifies:
- Column names match SELECT statements (avoids runtime mismatch errors)
- MERGE ON clauses reference correct columns
- SCD2 columns (is_current, effective_date_scd, end_date_scd) exist

**Status:** 14/14 passing

### Integration: End-to-End Tests

```bash
pytest tests/integration/test_iceberg_integration.py -v
```

Verifies:
- DDL creates schemas/tables
- Staging tables accept inserts
- SCD2 MERGE logic works correctly
- Fact table dedup prevents duplicates
- Trino can query results

**Status:** 23/23 passing (after natural key refactor)

### Smoke Test: Full Pipeline

```bash
# 1. Setup (one-time)
/Users/rajani/miniforge3/envs/py312/bin/python ddl/colima_create_catalog.py
python scripts/colima_generate_data.py
python scripts/colima_setup_minio.py

# 2. Trigger DAG
kubectl exec -n airflow airflow-scheduler-0 -- \
  airflow dags trigger colima_idm_dag --conf '{"batch_date":"2026-03-31"}'

# 3. Verify
# - All tasks succeed in Airflow UI
# - Zoom alerts sent to #pipeline-alerts
# - Trino query returns 1,359 fact_premiums rows
```

---

## Troubleshooting

| Issue | Diagnosis | Resolution |
|-------|-----------|-----------|
| `MERGE cardinality violation` | Duplicate rows in staging with same business key | Add ROW_NUMBER() dedup in USING clause |
| `NULL policy_number in fact_premiums` | Missing JOIN to dim_policy or stale data | Check stg_premiums has data; verify policy_number values |
| `Zoom alert not received` | Webhook not configured or payload format wrong | Verify `zoom_webhook_conn1` connection; use `{name, level, message}` format with Authorization header |
| `S3A connection timeout` | Network issue or wrong endpoint | Use k8s in-cluster DNS: `minio.minio.svc.cluster.local:9000` |
| `Postgres JDBC connection refused` | Wrong credentials or port-forward missing | Verify `POSTGRES_JDBC_URL` and port-forward: `kubectl port-forward svc/postgres 25432:5432 -n postgres` |
| `Iceberg table not found` | Table never created or dropped | Run `ddl/colima_create_catalog.py` to recreate all tables |
| `Data type mismatch in MERGE` | Column order in VALUES doesn't match column list | Ensure INSERT columns and VALUES are in same order |

---

## Performance Tuning

**Current Baseline (batch_date=2026-03-31, 2 orgs, 4 file_types):**

| Task | Rows | Duration |
|------|------|----------|
| parse_csv_ail (4 files sequential) | 1,300 | ~20 sec |
| parse_csv_lnl (4 files sequential) | 700 | ~15 sec |
| transform_to_idm | 1,359 facts | ~9 sec |
| **Total DAG** | 1,359 | **~2 min** |

**Optimization Opportunities:**
- Parallel org parsing (current: sequential ail → lnl)
- Larger Spark executor memory for large batches
- Partition pruning on Trino queries (add WHERE org_code filters)

---

## Files & Paths

| Resource | Path |
|----------|------|
| DAG | `~/src/airflow/dags/colima_idm_dag.py` |
| DDL | `~/src/colima-pyspark/ddl/colima_create_catalog.py` |
| Phase 1 Script | `~/src/colima-pyspark/scripts/colima_parse_csv.py` |
| Phase 2 Script | `~/src/colima-pyspark/scripts/colima_transform.py` |
| Utilities | `~/src/colima-pyspark/scripts/colima_generate_data.py`, `colima_setup_minio.py` |
| Tests | `~/src/colima-pyspark/tests/` |
| Config | `~/src/colima-pyspark/.env/local.env` |

---

## Git Repository

### Initial Push to GitHub

**Prerequisites:**
- GitHub CLI installed: `which gh`
- Authenticated: `gh auth status`
- Repository org: `rajjona-collab`

**Setup & Push (one-time):**

```bash
cd ~/src/colima-pyspark

# 1. Initialize git repo
git init

# 2. Set git user config (if not already global)
git config user.email "rajjona@gmail.com"
git config user.name "Rajani"

# 3. Stage all files
git add .

# 4. Commit with context
git commit -m "Initial commit: Iceberg pipeline with natural key schema

- Natural key design: policy_number + org_code as business keys
- SCD Type 2 for dim_policyholder and dim_policy
- SCD Type 1 for dim_address and dim_payment_method
- Fact table dedup via transaction_id
- Zoom webhook integration for alerts
- Full test coverage: unit, schema contract, integration, and natural key validation
- Production-ready with 1,359 premium facts verified"

# 5. Create repo on GitHub and push (single command)
gh repo create colima-pyspark --public --source=. --remote=origin --push --org rajjona-collab
```

**Alternative: Manual Push (if repo already exists)**

```bash
git remote add origin https://github.com/rajjona-collab/colima-pyspark.git
git branch -M main
git push -u origin main
```

### .gitignore

Protects secrets and temporary files:
- `.env` and `.env/` — credentials
- `CLAUDE.md` — local instructions
- `scratch/` — temporary work
- `__pycache__/`, `*.pyc` — Python cache
- `.pytest_cache/`, `.coverage`, `htmlcov/` — test artifacts
- `build/`, `dist/`, `*.egg-info/` — package build artifacts
- `.DS_Store` — macOS system files

---

## Rollback & Reset

**Truncate Staging Only** (before re-running parse_csv):
```bash
python scripts/truncate_staging.py
```

**Truncate Mart Only** (before re-running transform):
```bash
python scripts/truncate_marts.py
```

**Full Reset** (remove all data):
```bash
python scripts/truncate_all.py
```

**Recreate Schema** (drop + recreate all tables):
```bash
python scripts/drop_recreate_marts.py
```

---

## Compliance & Monitoring

**Data Lineage:** Natural keys (policy_number, org_code, transaction_id) provide stable traceability.

**SCD Audit:** is_current and end_date flags in dimensions track historical changes.

**Dedup Audit:** LEFT JOIN dedup logic in fact_premiums prevents duplicate transactions.

**Zoom Alerts:** Critical failures and completion status sent to #pipeline-alerts.
