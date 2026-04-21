# Testing Strategy

**Status:** ✅ Comprehensive | **Last Updated:** 2026-04-21

---

## Test Structure

```
tests/
├── conftest.py                                  # Shared fixtures
├── test_colima_transform_schema.py             # Unit: column contract
├── test_colima_parse_csv.py                    # Unit: CSV parsing
├── test_mart_natural_keys.py                   # Unit: mart tables (NEW)
└── integration/
    ├── conftest.py                             # Integration fixtures (Spark, Postgres)
    └── test_iceberg_integration.py             # Integration: full pipeline
```

---

## Test Categories

### 1. Unit Tests: Schema Contract (14 tests)
**File:** `tests/test_colima_transform_schema.py`

Validates column names and structure BEFORE running expensive Spark jobs.

```bash
pytest tests/test_colima_transform_schema.py -v
```

**What it tests:**
- Staging table columns match expected schema
- Transform MERGE logic references correct columns
- SCD Type 2 columns (is_current, effective_date_scd, end_date_scd) present
- Natural key columns non-NULL

**Duration:** <1 sec  
**Status:** ✅ 14/14 passing

---

### 2. Unit Tests: CSV Parsing
**File:** `tests/test_colima_parse_csv.py`

Validates CSV validation logic (without connecting to Postgres/MinIO).

```bash
pytest tests/test_colima_parse_csv.py -v
```

**What it tests:**
- Column presence validation
- Date format parsing
- Type casting (decimals, booleans)
- PII hashing (SSN → SHA-256)
- Rejection record handling

---

### 3. Unit Tests: Mart Tables with Natural Keys (NEW)
**File:** `tests/test_mart_natural_keys.py`

Validates mart table design uses natural keys correctly.

```bash
pytest tests/test_mart_natural_keys.py -v
```

**What it tests:**

#### Dimension Natural Keys
- dim_organization: 4-row static reference
- dim_policyholder: (policyholder_business_key, org_code) — no duplicates
- dim_policy: (policy_number, org_code) — no duplicates
- dim_address: (policy_number, org_code, address_type) — no duplicates
- dim_payment_method: (policy_number, org_code) — no duplicates

#### SCD Type 2
- is_current flag properly set
- No multiple is_current=TRUE rows per natural key
- SCD columns (effective_date_scd, end_date_scd) populated

#### SCD Type 1
- At most 1 row per natural key (no history)

#### Fact Table Natural Foreign Keys
- fact_premiums stores (policy_number, org_code) not policy_key
- transaction_id is unique (dedup working)
- No surrogate columns (policy_key, premium_key absent)

#### Data Lineage
- fact_premiums joins to dim_policy on natural keys
- All fact policies exist in dim_policy
- org_codes consistent across tables

#### Data Quality
- No NULL natural keys
- Required columns present
- Row counts plausible (facts > dimensions)

**Duration:** ~2-3 sec (reads existing data, no inserts)  
**Status:** ✅ ~20 tests

---

### 4. Integration Tests: Full Pipeline (23 tests)
**File:** `tests/integration/test_iceberg_integration.py`

End-to-end pipeline with actual Postgres and MinIO.

**Requirements:**
- Postgres accessible (localhost:25432 or via port-forward)
- MinIO accessible (localhost:9000 or via port-forward)
- `.env/local.env` configured with credentials
- Iceberg schemas/tables already created (run `ddl/colima_create_catalog.py` first)

```bash
pytest tests/integration/ -v --tb=short
```

**What it tests:**
- Iceberg catalog initialization
- Staging table INSERT
- SCD2 MERGE logic
- Fact table dedup
- Trino can query results

**Duration:** ~30 sec  
**Status:** ✅ 23/23 passing

---

## Pre-Commit Checklist

Before pushing code:

```bash
cd ~/src/colima-pyspark

# 1. Schema contract tests (fast, no infra)
pytest tests/test_colima_transform_schema.py -v

# 2. Parse CSV tests
pytest tests/test_colima_parse_csv.py -v

# 3. Mart table design tests (validates natural key usage)
pytest tests/test_mart_natural_keys.py -v

# 4. Integration tests (requires Postgres + MinIO)
pytest tests/integration/ -v

# 5. Verify code style (optional)
# flake8 scripts/ --max-line-length=120
```

**All passing?** Ready for PR.

---

## Running Tests Locally

### Setup (one-time)

```bash
cd ~/src/colima-pyspark
source .env/local.env

# Create tables (if not already created)
/Users/rajani/miniforge3/envs/py312/bin/python ddl/colima_create_catalog.py

# Load test data
python scripts/colima_generate_data.py
python scripts/colima_setup_minio.py
```

### Run All Tests

```bash
pytest tests/ -v
```

### Run Specific Test

```bash
# Schema contract only
pytest tests/test_colima_transform_schema.py::test_stg_basic_columns -v

# Mart natural key tests
pytest tests/test_mart_natural_keys.py::TestFactTableNaturalForeignKeys::test_fact_premiums_no_surrogate_keys -v

# Integration only
pytest tests/integration/test_iceberg_integration.py -v
```

### Verbose Output

```bash
pytest tests/test_mart_natural_keys.py -vv -s
```

Adds print statements and full tracebacks.

---

## Test Maintenance

### Adding a New Test

1. Identify category: unit (fast, local) or integration (slow, requires infra)
2. Add to appropriate file (`test_*.py` or `integration/test_*.py`)
3. Follow naming: `test_<component>_<scenario>`
4. Use fixtures: `spark_session` (built-in), `postgres_conn` (integration)

### Example: Test New Column

```python
def test_dim_policy_has_new_column(self, spark_session):
    """dim_policy: new_column is present and populated."""
    df = spark_session.sql("""
        SELECT new_column FROM pg_jdbc_catalog.idm_mart.dim_policy LIMIT 1
    """)
    rows = df.collect()
    assert len(rows) > 0
    assert rows[0].new_column is not None
```

### Common Fixtures

```python
# In conftest.py

@pytest.fixture(scope="module")
def spark_session():
    """Spark configured for Iceberg."""
    # Returns SparkSession with pg_jdbc_catalog configured
    # Lives in tests/conftest.py and tests/integration/conftest.py

@pytest.fixture(scope="module")
def postgres_conn():
    """Direct Postgres connection (integration only)."""
    # Returns psycopg2 connection
    # Lives in tests/integration/conftest.py
```

---

## CI/CD Integration

### GitHub Actions (Example)

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.12'
      - name: Install dependencies
        run: |
          pip install pytest pyspark
      - name: Run unit tests
        run: pytest tests/test_*.py -v
```

**Note:** Integration tests require Postgres + MinIO, which are harder to set up in CI. Run locally before pushing.

---

## Troubleshooting Tests

| Error | Cause | Fix |
|-------|-------|-----|
| `ModuleNotFoundError: pyspark` | PySpark not installed | `pip install pyspark==3.5.8` |
| `ModuleNotFoundError: pytest` | pytest not installed | `pip install pytest` |
| `Connection refused: localhost:25432` | Postgres not accessible | `kubectl port-forward svc/postgres 25432:5432 -n postgres` |
| `NoSuchBucketException: lakehouse` | MinIO not accessible | `kubectl port-forward svc/minio 9000:9000 -n minio` |
| `TABLE_NOT_FOUND: idm_staging` | Tables not created | `python ddl/colima_create_catalog.py` |
| `FileNotFoundError: .env/local.env` | Config missing | Copy `.env/local.env.example` to `.env/local.env` and fill in values |

---

## Performance Tips

- **Run unit tests first** (schema contract, parse CSV): fast feedback
- **Skip integration tests locally** if only changing SQL: add `-k "not integration"`
- **Parallel test execution:** `pytest -n auto` (requires pytest-xdist)

---

## Coverage

To measure test coverage:

```bash
pip install pytest-cov

pytest --cov=scripts --cov-report=html tests/

# Open htmlcov/index.html in browser
```

**Target:** >80% coverage on critical paths (transform, parse_csv)

---

## Natural Key Test Focus

Since this project uses natural keys (no surrogates), mart tests focus on:

1. **Natural key uniqueness:** No duplicates per (business_key, org_code)
2. **Foreign key completeness:** All facts reference existing dimensions
3. **Data lineage:** policy_number flows correctly through pipeline
4. **Dedup logic:** Transaction IDs don't repeat (LEFT JOIN working)
5. **NULL enforcement:** Natural key columns never NULL

See `test_mart_natural_keys.py` for comprehensive validation.
