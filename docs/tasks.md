# colima-pyspark — Task Confirmation & Execution Plan

_Date: 2026-04-20_

---

## Confirmed Decisions

| # | Question | Decision | Rationale |
|---|---|---|---|
| 1a | Parse/Transform for k8s or local .env? | **k8s only** — keep hardcoded k8s DNS | Production execution via KubernetesPodOperator |
| 1b | Reference SparkSession in demo? | **No** — demo.py sufficient | Local testing is separate concern |
| 2a | Add `defaultCatalog` to all builders? | **Yes** — fix INSERT blocker | Required for non-default Iceberg catalog |
| 2b | Consolidate DDL scripts? | **Delete scripts/colima_create_catalog.py** | Single source in ddl/colima_create_catalog.py |
| 3a | TRUNCATE idempotency? | **Confirmed idempotent** | Staging table TRUNCATE before load |
| 4 | Transform MERGE strategy? | **Use MERGE** — SCD2 + dedup | Existing logic with ROW_NUMBER() |
| 5 | Warehouse paths? | **Keep current structure** | `s3a://lakehouse/sg-life-idm/{schema}/{table}` |
| 6 | Org seeding? | **Standalone scripts, local env** | Created `seed_reference_data.py` |

---

## Warehouse Architecture

**Catalog:** `pg_jdbc_catalog` (JDBC → Postgres `metastore` DB)

**Warehouse Root:** `s3a://lakehouse/sg-life-idm/`

### **Schema: idm_staging** (Raw/Validated)

| Table | Partitioning | Records | Source |
|---|---|---|---|
| `stg_basic` | `org_code, batch_date` | Policy + policyholder (ssn_hash) | Landing CSV |
| `stg_address` | `org_code, batch_date` | Address records | Landing CSV |
| `stg_bankinfo` | `org_code, batch_date` | Payment methods (routed hash) | Landing CSV |
| `stg_premiums` | `org_code, batch_date` | Premium transactions | Landing CSV |
| `stg_rejected` | `org_code, batch_date` | Validation failures (JSON raw_record) | parse_csv errors |

**Behavior:**
- `TRUNCATE stg_{type}` before each CSV load (idempotent reruns)
- Invalid rows → `stg_rejected` (no DAG failure)

### **Schema: idm_mart** (Curated/Dimensional)

| Table | Type | Key | Partitioned | Records | Source |
|---|---|---|---|---|---|
| `dim_organization` | Ref | `org_key` | — | 4 rows (seeded) | `seed_reference_data.py` |
| `dim_policyholder` | SCD2 | `policyholder_business_key` | — | N rows | MERGE from stg_basic |
| `dim_policy` | SCD2 | `policy_number` | — | N rows | MERGE from stg_basic |
| `dim_address` | SCD1 | `policy_key, address_type` | — | N rows | MERGE from stg_address |
| `dim_payment_method` | SCD1 | `policy_key` | — | N rows | MERGE from stg_bankinfo |
| `fact_premiums` | Append | `transaction_id` | `org_key, batch_date` | N rows | INSERT from stg_premiums (deduped) |

**Behavior:**
- SCD2 tables: MERGE with `is_current` flag, effective/end dates
- SCD1 tables: MERGE (no history, update-in-place)
- Fact table: INSERT with LEFT JOIN dedup on transaction_id (idempotent)

---

## Implementation Tasks

### **Phase 1: Fix INSERT Blocker (Required for all scripts)**

**Task 1.1:** Add `spark.sql.defaultCatalog=pg_jdbc_catalog` to SparkSession builders

**Files to update:**
- [ ] `ddl/colima_create_catalog.py` — Line ~34 (after warehouse config)
- [ ] `scripts/colima_parse_csv.py` — Line ~73 (in get_spark_session())
- [ ] `scripts/colima_transform.py` — Line ~28 (SparkSession builder)
- [ ] `tests/integration/conftest.py` — Line ~45 (fixture)

**Pattern:**
```python
spark = (SparkSession.builder
    # ... existing configs ...
    .config("spark.sql.defaultCatalog", "pg_jdbc_catalog")  # ← ADD THIS
    .config("spark.jars", _JARS)
    # ... rest of configs ...
    .getOrCreate())
```

**Validation:** After fix, run integration tests:
```bash
pytest tests/integration/test_iceberg_integration.py -v
# Expected: 23/23 passing (was 16/23 with INSERT blocker)
```

---

### **Phase 2: Consolidate DDL & Verify Schema**

**Task 2.1:** Verify `ddl/colima_create_catalog.py` is the single source

**Files:**
- [x] `scripts/colima_create_catalog.py` — DELETED ✓
- [✓] `ddl/colima_create_catalog.py` — Active

**What it does:**
1. Auto-loads JARs from `scratch/jars/`
2. Creates `idm_staging` schema + 5 tables
3. Creates `idm_mart` schema + 6 tables
4. Seeds `dim_organization` with 4 orgs

**Run (local testing):**
```bash
source ~/.env/local.env
cd ~/src/colima-pyspark
/Users/rajani/miniforge3/envs/py312/bin/python ddl/colima_create_catalog.py
```

**In-pod (KubernetesPodOperator):**
```python
spark-submit /path/to/ddl/colima_create_catalog.py \
  --driver-memory 2g \
  --executor-memory 2g
```

---

### **Phase 3: Create Standalone Seed Scripts**

**Task 3.1:** Verify `seed_reference_data.py` for org seeding

**Files:**
- [✓] `scripts/seed_reference_data.py` — CREATED ✓

**What it does:**
- Loads `.env/local.env` (local testing)
- Connects to Iceberg JDBC catalog
- Seeds `dim_organization` (DELETE → INSERT for idempotency)
- Verifies via SELECT

**Run (local):**
```bash
source ~/.env/local.env
python scripts/seed_reference_data.py
```

**In production (Airflow):**
- Use `KubernetesPodOperator` with spark-submit
- Pass env vars from Airflow connections (same as parse_csv/transform)

---

### **Phase 4: Verify Parse & Transform Scripts (k8s)**

**Task 4.1:** Confirm `colima_parse_csv.py` uses k8s DNS + TRUNCATE

**File:** `scripts/colima_parse_csv.py`

**Expected behaviors:**
- ✓ Reads CSV from `s3a://lakehouse/sg-life-idm/landing/{org}/{org}{file_type}.csv`
- ✓ Validates against config JSON from `s3a://lakehouse/sg-life-idm/config/{org}/{file_type}.json`
- ✓ Hashes PII (SSN → SHA-256)
- ✓ **TRUNCATE idm_staging.stg_{file_type} before INSERT** (idempotent)
- ✓ On schema mismatch → writes to stg_rejected (no DAG failure)
- ✓ Temp view + SQL INSERT pattern (for non-default catalog)

**In-pod execution (KubernetesPodOperator):**
```python
KubernetesPodOperator(
    name="parse_csv_ail_basic",
    image="spark-local:0.0.4",
    cmds=["spark-submit"],
    arguments=[
        "s3a://lakehouse/sg-life-idm/scripts/colima_parse_csv.py",
        "--org", "ail",
        "--file_type", "basic",
        "--batch_date", "{{ ds }}",
    ],
    env_vars={
        "POSTGRES_JDBC_URL": "jdbc:postgresql://postgres.postgres.svc.cluster.local:5432/metastore",
        "POSTGRES_USER": "dbadmin",
        "POSTGRES_PASSWORD": "{{ postgres_password }}",
        "AWS_ENDPOINT_URL_S3": "http://minio.minio.svc.cluster.local:9000",
        "AWS_ACCESS_KEY_ID": "lakehouse-etl",
        "AWS_SECRET_ACCESS_KEY": "{{ minio_secret }}",
    }
)
```

---

### **Phase 5: Verify Transform Script (k8s)**

**Task 5.1:** Confirm `colima_transform.py` uses MERGE + k8s DNS

**File:** `scripts/colima_transform.py`

**Expected behaviors:**
- ✓ Reads from idm_staging (stg_*)
- ✓ MERGE INTO dim_policyholder (SCD2 with ROW_NUMBER() dedup)
- ✓ MERGE INTO dim_policy (SCD2)
- ✓ MERGE INTO dim_address (SCD1)
- ✓ MERGE INTO dim_payment_method (SCD1)
- ✓ INSERT fact_premiums (dedup via LEFT JOIN on transaction_id)
- ✓ Handles surrogate keys (policy_key, org_key from dim_policy, dim_organization)

**Known issues fixed:**
- ✓ Uses `end_date_scd` not `end_date` in dim_policy MERGE
- ✓ Uses `policy_key` not `policy_number` in dim_address/dim_payment_method
- ✓ Uses `org_key` not `org_code` in fact_premiums

**Schema contract tests (before merge):**
```bash
pytest tests/test_colima_transform_schema.py -v
# Expected: 14/14 passing
```

---

### **Phase 6: Integration Testing**

**Task 6.1:** Run full integration test suite

```bash
source ~/.env/local.env
pytest tests/integration/test_iceberg_integration.py -v
# After Phase 1 fix: 23/23 passing
```

**Task 6.2:** Run schema contract tests

```bash
pytest tests/test_colima_transform_schema.py -v
# Expected: 14/14 passing (no regressions in transform.py)
```

---

### **Phase 7: End-to-End Pipeline (Local Testing)**

**Setup (one-time):**
```bash
# 1. Start Colima + port-forwards
~/src/platform/start.sh

# 2. Create catalog & tables
source ~/.env/local.env
python ddl/colima_create_catalog.py

# 3. Seed reference data
python scripts/seed_reference_data.py

# 4. Generate test data
python scripts/colima_generate_data.py

# 5. Upload to MinIO
python scripts/colima_setup_minio.py
```

**Smoke test (CSV → staging → mart):**
```bash
# Simulating parse_csv for ail/basic (batch_date=2026-03-31)
# In real DAG: KubernetesPodOperator would run this
source ~/.env/local.env
/Users/rajani/miniforge3/envs/py312/bin/python scripts/colima_parse_csv.py \
  --org ail --file_type basic --batch_date 2026-03-31

# Verify staging data
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Verify').getOrCreate()
spark.sql('SELECT COUNT(*) FROM pg_jdbc_catalog.idm_staging.stg_basic').show()
"
```

---

## Success Criteria

| Task | Success Criteria | Status |
|---|---|---|
| Phase 1 | INSERT works for non-default catalog | ⏳ Pending |
| Phase 2 | Single DDL script, scripts/ cleaned up | ✅ Done |
| Phase 3 | Seed script works with local .env | ✅ Done |
| Phase 4 | parse_csv TRUNCATE + temp view INSERT | ⏳ Verify |
| Phase 5 | transform MERGE with SCD logic + dedup | ⏳ Verify |
| Phase 6 | Integration tests 23/23 passing | ⏳ Pending |
| Phase 7 | E2E pipeline: CSV → staging → mart → Trino | ⏳ Pending |

---

## Reference: SparkSession Builder Template

```python
# Local testing (colima_pyspark_demo.py pattern)
spark = (SparkSession.builder
    .appName("MyApp")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.pg_jdbc_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.pg_jdbc_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
    .config("spark.sql.catalog.pg_jdbc_catalog.uri", POSTGRES_JDBC_URL)
    .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.user", POSTGRES_USER)
    .config("spark.sql.catalog.pg_jdbc_catalog.jdbc.password", POSTGRES_PASSWORD)
    .config("spark.sql.catalog.pg_jdbc_catalog.warehouse", "s3a://lakehouse/sg-life-idm/")
    .config("spark.sql.defaultCatalog", "pg_jdbc_catalog")  # ← FIX INSERT BLOCKER
    .config("spark.jars", _JARS)
    .config("spark.hadoop.fs.s3a.endpoint", AWS_ENDPOINT_URL_S3)
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())
```

**For in-pod (KubernetesPodOperator):** Same, but env vars come from Airflow connections (not .env file).

---

## Next Steps (Immediate)

1. **Phase 1:** Apply `spark.sql.defaultCatalog` fix to 4 files
2. **Phase 6:** Verify integration tests pass (23/23)
3. **Phase 7:** Run E2E smoke test with real CSV → mart pipeline
4. **Phase 8:** Trigger Airflow DAG manually to verify KubernetesPodOperator integration

Ready to proceed? Any blockers?
