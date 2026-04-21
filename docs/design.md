# colima-pyspark: Iceberg IDM Pipeline Design

**Status:** ✅ Production Ready | **Last Updated:** 2026-04-21

---

## Executive Summary

Local Iceberg-based IDM (Integrated Data Management) pipeline for life insurance policy data. Replicates AWS Redshift production architecture using Colima Kubernetes, with **natural business keys** (Iceberg-idiomatic) instead of surrogate keys.

**Key Achievement:** Eliminated surrogate key complexity. Pipeline uses natural keys (policy_number, org_code, transaction_id) throughout—simpler, more resilient, better data lineage.

---

## Design Principles

### 1. Natural Keys (Iceberg-Idiomatic)
**Decision:** Use business keys instead of surrogate keys (policy_key, policyholder_key, etc.)

**Reasoning:**
- **Iceberg has no IDENTITY support** — Surrogate generation requires ROW_NUMBER() or UUID, adding complexity
- **Data lake context** — Natural keys provide stable, reproducible identifiers
- **Resilience** — Schema resets don't invalidate fact tables (policy_number is always valid)
- **Lineage** — Data traces back to source (policy_number is meaningful to business)
- **Simplicity** — No surrogate FK joins; fact tables store business keys directly

**Impact:**
- dim_policy: PK = (policy_number, org_code)
- fact_premiums: stores (policy_number, org_code) directly, no policy_key
- No surrogate key generation overhead in transform

### 2. Partition by Business Context (org_code, batch_date)
**Decision:** Partition all tables by (org_code, batch_date) not by surrogate keys

**Reasoning:**
- Org isolation: Different insurance subsidiaries have separate data
- Batch processing: Each daily load is a logical unit
- Query efficiency: Most queries filter by org and date

### 3. SCD Type 2 for Dimensions, Append-Only for Facts
**Decision:**
- dim_policyholder, dim_policy: SCD Type 2 (track history with is_current flag)
- fact_premiums: Append-only (insert-only, no updates)

**Reasoning:**
- History tracking: Need to know policy state changes over time
- Fact immutability: Premiums paid are historical facts, not updated

### 4. Use Iceberg MERGE for SCD Logic
**Decision:** Leverage Iceberg MERGE INTO for SCD Type 2 instead of UPDATE/INSERT

**Reasoning:**
- ACID guarantees: No partial updates if failure occurs
- Simplified logic: WHEN MATCHED / WHEN NOT MATCHED
- Built-in dedup: ROW_NUMBER() in USING clause prevents cardinality violations

---

## Architecture

```
Airflow (Orchestration)
  ↓
check_landing_files → parse_csv (ail, lnl) → transform → notify_complete
                                    ↓
                              PySpark 3.5.8
                                    ↓
        [MinIO] ← [Postgres JDBC] → [Iceberg Catalog]
         S3A            Metastore      s3://lakehouse/
                                    
Analytics: Trino, Superset
```

### Components

| Layer | Technology | Role |
|-------|-----------|------|
| **Orchestration** | Apache Airflow 3.2 | DAG scheduling, state tracking |
| **Compute** | PySpark 3.5.8 | CSV parsing, SCD merges, validation |
| **Catalog** | Iceberg JDBC | Metadata management (tables, schema versions) |
| **Metastore** | PostgreSQL | Iceberg catalog backend, Airflow state |
| **Storage** | MinIO (S3-compatible) | Landing zone, staging, mart data warehouse |
| **Analytics** | Trino, Superset | Query interface |

### Connectivity (In-Cluster)

```
Postgres:  jdbc:postgresql://postgres.postgres.svc.cluster.local:5432/metastore
MinIO:     http://minio.minio.svc.cluster.local:9000
Iceberg:   pg_jdbc_catalog (JDBC catalog with Postgres backend)
```

---

## Data Model

### Staging Layer (idm_staging)
Raw CSV validation + PII hashing. **Partition by:** (org_code, batch_date)

| Table | Purpose | Key Columns |
|-------|---------|------------|
| stg_basic | Policy + policyholder | policy_number, org_code, ssn_hash (hashed), match_method |
| stg_address | Address records | policy_number, org_code, address_type |
| stg_bankinfo | Payment method | policy_number, org_code, payment_method_type |
| stg_premiums | Premium transactions | transaction_id, policy_number, org_code |

### Mart Layer (idm_mart)
ACID-compliant dimensional + fact tables. **Partition by:** (org_code, batch_date) or (org_code)

#### Dimensions (SCD Type 2 — track history)

| Table | Natural Key | Columns | Purpose |
|-------|------------|---------|---------|
| dim_organization | org_code | org_key (surrogate for reference), org_code, org_name | Static org reference |
| dim_policyholder | (policyholder_business_key, org_code) | ssn_hash, policyholder_name, is_current, effective_date, end_date | Track insured versions |
| dim_policy | (policy_number, org_code) | policy_status, issue_date, effective_date_scd, end_date_scd, is_current | Track policy versions |
| dim_address | (policy_number, org_code, address_type) | street_address, city, state, zip_code | SCD Type 1 (no history) |
| dim_payment_method | (policy_number, org_code) | payment_method_type, account_number_last_4, bank_name | SCD Type 1 (no history) |

#### Facts (Append-Only)

| Table | Natural Key | FK References | Dedup Strategy |
|-------|------------|---|---|
| fact_premiums | transaction_id | (policy_number, org_code) | LEFT JOIN to avoid re-inserting |

---

## Data Flow

### Phase 1: Parse CSV (colima_parse_csv.py)

```
Landing Zone (MinIO): ailbasic.csv, lnlbasic.csv, ...
                ↓
        CSV Validation
        - Column presence
        - Type casting (dates, decimals)
        - Not-null enforcement
        - org_code validation
                ↓
        PII Hashing
        - SSN → SHA-256 (drop raw SSN)
                ↓
        Split Records
        - Valid → idm_staging.stg_*
        - Invalid → rejected/{org}/{file_type}/{batch_date}/
```

**Input:** 8 CSV files (2 orgs × 4 file types)  
**Output:** 5 staging tables (~2k rows combined)  
**Duration:** 15 sec per org

### Phase 2: Transform to IDM Mart (colima_transform.py)

```
Staging (idm_staging.stg_*)
        ↓
    SCD Type 2 MERGE
    - dim_policyholder: COALESCE(ssn_hash, name_concat) as business_key
    - dim_policy: policy_number as natural key
        ↓
    SCD Type 1 MERGE
    - dim_address: (policy_number, address_type)
    - dim_payment_method: (policy_number)
        ↓
    Fact INSERT
    - fact_premiums: (transaction_id) with (policy_number, org_code) stored
    - Dedup via LEFT JOIN on transaction_id
        ↓
Mart (idm_mart.dim_*, fact_*)
```

**Input:** 5 staging tables  
**Output:** 5 dim + 1 fact table  
**Duration:** 9 sec  
**Result:** 1,359 premium facts loaded

---

## Transform Logic: Natural Keys in Detail

### Why No policy_key Foreign Key?

**Old (Surrogate) Approach:**
```sql
INSERT INTO fact_premiums (policy_key, premium_amount, ...)
SELECT dp.policy_key, p.premium_amount, ...
FROM stg_premiums p
JOIN dim_policy dp ON dp.policy_number = p.policy_number AND dp.is_current = TRUE
```
**Problem:** policy_key might be NULL if dim_policy.policy_key wasn't populated (Iceberg has no IDENTITY).

**New (Natural Key) Approach:**
```sql
INSERT INTO fact_premiums (policy_number, org_code, premium_amount, ...)
SELECT p.policy_number, p.org_code, p.premium_amount, ...
FROM stg_premiums p
LEFT JOIN fact_premiums fp ON fp.transaction_id = p.transaction_id
WHERE fp.transaction_id IS NULL  -- Dedup by transaction_id
```
**Benefit:** No surrogate generation, no NULL FKs, table self-describing (policy_number is meaningful).

### SCD Type 2 Example (dim_policyholder)

```sql
MERGE INTO dim_policyholder t
USING (
    -- Dedup by business_key: one policyholder per batch
    SELECT business_key, org_code, ssn_hash, policyholder_name, effective_date
    FROM stg_basic
    WHERE ROW_NUMBER() OVER (PARTITION BY org_code, COALESCE(ssn_hash, name_concat) ORDER BY ...) = 1
) s
ON t.policyholder_business_key = s.business_key AND t.org_code = s.org_code AND t.is_current = TRUE

WHEN MATCHED AND (t.policyholder_name != s.policyholder_name OR t.ssn_hash != s.ssn_hash) THEN
    UPDATE SET is_current = FALSE, end_date = batch_date - 1

WHEN NOT MATCHED THEN
    INSERT (policyholder_business_key, org_code, policyholder_name, ssn_hash, is_current, effective_date, ...)
```

**Natural Key:** (policyholder_business_key, org_code)  
**Dedup:** ROW_NUMBER() prevents cardinality violations if same insured appears multiple times in staging

---

## MinIO Data Layout

```
lakehouse/sg-life-idm/
├── landing/
│   ├── ail/{ailbasic.csv, ailaddress.csv, ailbankinfo.csv, ailpremiums.csv}
│   └── lnl/{lnlbasic.csv, ...}
├── config/
│   ├── ail/{basic.json, address.json, bankinfo.json, premiums.json}
│   └── lnl/{...}
├── scripts/
│   ├── colima_parse_csv.py
│   ├── colima_transform.py
│   └── drop_recreate_marts.py
├── rejected/
│   └── {org}/{file_type}/{batch_date}/rejected_*.parquet
└── idm_staging/ & idm_mart/  (Iceberg data warehouse)
```

---

## Zoom Notifications

**Configured:** Two checkpoints notify Zoom channel #pipeline-alerts

1. **After landing files verified** → Info alert (green)
2. **After pipeline completion** → Info (success) or Warning (failure)

**Connection:** Airflow connection `zoom_webhook_conn1`  
**Payload Format:** `{name, level, message}` with `Authorization` header

---

## Testing Strategy

### Unit Tests (Schema Contract)
- Verify column names match Spark SQL selects
- Validate MERGE logic syntax before executing

### Integration Tests (End-to-End)
- DDL creates schemas/tables without error
- parse_csv reads CSV, validates, hashes PII, writes to staging
- transform reads staging, applies SCD, populates mart
- fact_premiums contains correct number of rows (1,359)
- All natural key joins succeed (no NULL FKs)

### Pipeline Smoke Test
- Trigger DAG with batch_date=2026-03-31
- All tasks succeed (check → parse → transform → notify)
- Zoom alerts sent to #pipeline-alerts
- Trino queries return expected row counts

---

## Known Issues & Resolutions

| Issue | Resolution | Status |
|-------|-----------|--------|
| Surrogate key generation complexity | Use natural keys (policy_number, org_code, transaction_id) | ✅ Resolved |
| NULL policy_key in fact_premiums | Store policy_number + org_code directly; no FK surrogate | ✅ Resolved |
| data type mismatch in MERGE | Ensure all columns in INSERT match column list order | ✅ Resolved |
| Zoom notifications not received | Use Connection.get_connection_from_secrets(); correct payload format with Authorization header | ✅ Resolved |
| Hadoop 3.4.x incompatibility with Spark 3.5.8 | Use hadoop-aws 3.3.4.jar | ✅ Resolved |

---

## Performance Characteristics

| Operation | Duration | Rows |
|-----------|----------|------|
| parse_csv_ail_basic | ~4 sec | 500 rows |
| parse_csv_lnl_basic | ~4 sec | 333 rows |
| parse_csv (all) | ~60 sec total | 2,000 rows |
| transform_to_idm | ~9 sec | 1,359 premiums inserted |
| Full DAG (check → parse → transform → notify) | ~2 min | 1,359 facts |

---

## Future Enhancements

- [ ] dbt models for dimensional modeling (currently Spark SQL)
- [ ] Data quality tests (row counts, freshness, key uniqueness)
- [ ] Incremental loads (currently full reloads)
- [ ] Cost optimization (Spark memory tuning, S3 partition pruning)
- [ ] Monitoring dashboard (Superset charts on mart tables)
- [ ] Retention policies (archive old batches to cold storage)
