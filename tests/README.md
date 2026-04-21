# colima-pyspark Tests

Unit and integration tests for the IDM pipeline scripts.

## Unit Tests (pytest)

### Run All Tests
```bash
cd /Users/rajani/src/colima-pyspark
pytest tests/ -v
```

### Run Specific Test Class
```bash
pytest tests/test_colima_parse_csv.py::TestValidation -v
```

### Run with Coverage
```bash
pytest tests/ --cov=scripts --cov-report=html
```

## Test Structure

### Unit Tests (Local, No Spark)
- **TestSSNHashing**: PII hashing consistency
- **TestValidation**: Date format, org_code, required fields
- **TestConfigLoading**: JSON schema validation
- **TestCSVParsing**: Header extraction, row counting, null handling
- **TestRejectionLogic**: Validation failure scenarios
- **TestSchemaHandling**: Schema mismatch detection, JSON serialization
- **TestAlertGeneration**: Zoom alert message format
- **TestBatchProcessing**: Batch date parsing and consistency

### Coverage
- ✅ Validation logic (dates, org codes, required fields)
- ✅ PII hashing (SSN → SHA256)
- ✅ CSV parsing and null handling
- ✅ Rejection criteria
- ✅ Schema mismatch detection
- ✅ Zoom alert formatting
- ⏳ Spark DataFrame operations (requires Spark)
- ⏳ Iceberg table writes (requires Iceberg + Postgres)

## Integration Tests (With Spark/Iceberg)

### Local Spark Cluster
```bash
# Start local Spark and test parse_csv end-to-end
cd /Users/rajani/src/colima-pyspark
export PYTHONPATH="/path/to/spark/python:${PYTHONPATH}"

# Run parse_csv with local CSV
python scripts/colima_parse_csv.py \
  --org lnl \
  --file_type basic \
  --batch_date 2026-03-31 \
  --config_path config/lnl/basic.json \
  --s3_input /tmp/test_data/lnl/lnlbasic.csv
```

### Colima Kubernetes Cluster
```bash
# Test in spark-debug pod
kubectl exec -it spark-debug -n spark -- bash -c "
export AWS_ENDPOINT_URL_S3='http://minio.minio.svc.cluster.local:9000'
export AWS_ACCESS_KEY_ID='lakehouse-etl'
export AWS_SECRET_ACCESS_KEY='PiDiigd17yVJOlpYFRl+bRAhg9aLueMchdSO9IKv'
export POSTGRES_JDBC_URL='jdbc:postgresql://postgres.postgres.svc.cluster.local:5432/metastore'
export POSTGRES_USER='dbadmin'
export POSTGRES_PASSWORD='e2TApDQrA3L9K8s7'

spark-submit --master 'local[*]' --driver-memory 3g \
  s3a://lakehouse/sg-life-idm/scripts/colima_parse_csv.py \
  --org lnl --file_type basic --batch_date 2026-03-31 \
  --config_path s3a://lakehouse/sg-life-idm/config/lnl/basic.json \
  --s3_input s3a://lakehouse/sg-life-idm/landing/lnl/lnlbasic.csv
"
```

## Test Data

Synthetic test data is in `scratch/data/`:
- `ail/ailbasic.csv` (740 rows)
- `ail/ailaddress.csv` (740 rows)
- `ail/ailbankinfo.csv` (740 rows)
- `ail/ailpremiums.csv` (2,220 rows)
- `lnl/lnlbasic.csv` (453 rows)
- `lnl/lnladdress.csv` (453 rows)
- `lnl/lnlbankinfo.csv` (453 rows)
- `lnl/lnlpremiums.csv` (1,359 rows)

Generate fresh test data:
```bash
python scripts/colima_generate_data.py --org all
```

## Expected Test Results

### Unit Tests
All tests should pass without Spark/Iceberg:
```
tests/test_colima_parse_csv.py::TestSSNHashing::test_ssn_hash_consistent PASSED
tests/test_colima_parse_csv.py::TestSSNHashing::test_ssn_hash_different_inputs PASSED
tests/test_colima_parse_csv.py::TestValidation::test_date_validation_valid PASSED
tests/test_colima_parse_csv.py::TestValidation::test_date_validation_invalid_format PASSED
tests/test_colima_parse_csv.py::TestValidation::test_date_validation_invalid_date PASSED
tests/test_colima_parse_csv.py::TestValidation::test_org_code_validation PASSED
tests/test_colima_parse_csv.py::TestConfigLoading::test_load_config_basic PASSED
tests/test_colima_parse_csv.py::TestConfigLoading::test_config_required_fields PASSED
tests/test_colima_parse_csv.py::TestCSVParsing::test_parse_csv_header_extraction PASSED
tests/test_colima_parse_csv.py::TestCSVParsing::test_parse_csv_row_count PASSED
tests/test_colima_parse_csv.py::TestCSVParsing::test_parse_csv_null_values PASSED
tests/test_colima_parse_csv.py::TestRejectionLogic::test_null_ssn_and_name_rejection PASSED
tests/test_colima_parse_csv.py::TestRejectionLogic::test_date_format_validation_rejection PASSED
tests/test_colima_parse_csv.py::TestRejectionLogic::test_org_code_mismatch_rejection PASSED
tests/test_colima_parse_csv.py::TestSchemaHandling::test_stg_rejected_schema PASSED
tests/test_colima_parse_csv.py::TestSchemaHandling::test_mismatch_error_detection PASSED
tests/test_colima_parse_csv.py::TestSchemaHandling::test_json_serialization PASSED
tests/test_colima_parse_csv.py::TestAlertGeneration::test_alert_message_format PASSED
tests/test_colima_parse_csv.py::TestAlertGeneration::test_alert_only_when_rejections PASSED
tests/test_colima_parse_csv.py::TestBatchProcessing::test_batch_date_parsing PASSED
tests/test_colima_parse_csv.py::TestBatchProcessing::test_batch_date_in_rejection PASSED

======================== 21 passed in 0.25s ========================
```

### Integration Tests
When run in spark-debug pod with real Iceberg:
- CSV reads from MinIO ✅ (453 rows)
- Validation logic executes ✅ (0 rejected by validation rules)
- Schema mismatch caught gracefully ✅ (453 rows → stg_rejected)
- stg_rejected table populated ✅ (verified with SQL query)
- Zoom alert skipped (ZOOM_WEBHOOK_URL not set) ✅
