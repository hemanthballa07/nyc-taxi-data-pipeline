# Plan: Ingestion Script

**Status:** Awaiting approval
**Last updated:** 2026-04-11

---

## Goal
Write `scripts/ingest.py` — a CLI tool that downloads a monthly NYC Yellow Taxi Parquet file from TLC, validates it, bulk-loads it into `raw.yellow_taxi_trips`, and logs the run to `raw.ingestion_log`. Zone lookup ingestion is out of scope for V1. Idempotent: re-running the same month deletes and re-loads that month's rows.

---

## Files to Create or Modify

| File                   | Action       | Purpose                                                 |
|------------------------|--------------|---------------------------------------------------------|
| `scripts/ingest.py`    | Create       | CLI — download, validate, load trips + DB connection    |
| `tests/test_ingest.py` | Create.      | Unit tests for validation logic                         |
| `data/raw/`            | Auto-created | Download destination (gitignored)                       |

`scripts/db.py` is deferred — DB connection lives inside `ingest.py` for now. Extract only if a second script needs it.

---

## Dependencies

- Postgres must be running with `raw` schema and tables from `docker/init-db.sql` ✅
- `.env` must have `DATABASE_URL` or individual `POSTGRES_*` vars ✅ (`.env.example` exists)
- Python packages installed: `requests`, `pyarrow`, `pandas`, `psycopg2-binary`, `click`, `python-dotenv` ✅ (in `requirements.txt`)

---

## Implementation Steps

1. **`scripts/ingest.py`** — single-file CLI, structured in three sections:

   **a. DB connection** (module-level helper, not a separate file)
   - `get_connection()` reads `.env` via `python-dotenv`, returns a `psycopg2` connection
   - Reads `DATABASE_URL` if set, otherwise falls back to individual `POSTGRES_*` vars

   **b. CLI** — one command via `click`:
   - `python scripts/ingest.py trips YEAR MONTH [--force]`

   **c. `ingest_trips(year, month, force)` flow:**
   1. Construct URL: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YEAR}-{MM:02d}.parquet`
   2. Skip download if file exists in `data/raw/` (unless `--force`)
   3. Download with streaming + progress logging (never load full file into memory during download)
   4. Read Parquet with `pyarrow` → validate (see below)
   5. Rename Parquet columns → DB column names (see map below)
   6. Delete existing rows for this year/month from `raw.yellow_taxi_trips`, then insert fresh rows (delete-and-reload idempotency). This is the right default for monthly batch data: each file is a complete, authoritative snapshot of that month, so a partial load is always worse than a full reload. Upsert/merge would add complexity with no benefit since there's no stable row-level key in the raw TLC data.
   7. Bulk load via `psycopg2.copy_expert` with an in-memory CSV buffer — never `pandas.to_sql`
   8. Insert row into `raw.ingestion_log` (file, year, month, rows_loaded, status)
   9. On any unhandled exception: catch at the top level, insert a `status = 'failed'` row into `raw.ingestion_log` with the error message in `notes`, then re-raise so the process exits non-zero

2. **Validation rules** — warnings, not hard failures, unless clearly fatal:

   | Check                    | Threshold                          | On failure                                                             |
   |--------------------------|------------------------------------|------------------------------------------------------------------------|
   | Required columns present | All 19 expected columns            | Raise — can't load without schema                                      |
   | Row count                | > 0                                | Raise — empty file is broken                                           |
   | Row count sanity         | < 10,000                           | Warn + log, don't abort — TLC occasionally publishes small corrections |
   | File size                | > 1 MB                             | Warn — not a hard stop, but suspicious                                 |
   | Date range               | ≥ 80% of pickups in expected month | Warn — TLC files sometimes have spillover rows from adjacent months.   |

   Rationale: the original 100k row floor and 95% date threshold would false-positive on legitimate small correction files and on months where TLC retroactively patches data. Warnings surface the anomaly without blocking the load.

3. **Column name mapping** (Parquet → DB):

   | Parquet column | DB column |
   |---|---|
   | VendorID | vendor_id |
   | tpep_pickup_datetime | pickup_datetime |
   | tpep_dropoff_datetime | dropoff_datetime |
   | passenger_count | passenger_count |
   | trip_distance | trip_distance |
   | RatecodeID | rate_code_id |
   | store_and_fwd_flag | store_and_fwd_flag |
   | PULocationID | pickup_location_id |
   | DOLocationID | dropoff_location_id |
   | payment_type | payment_type |
   | fare_amount | fare_amount |
   | extra | extra |
   | mta_tax | mta_tax |
   | tip_amount | tip_amount |
   | tolls_amount | tolls_amount |
   | improvement_surcharge | improvement_surcharge |
   | total_amount | total_amount |
   | congestion_surcharge | congestion_surcharge |
   | airport_fee | airport_fee |

---

## Tests to Write (`tests/test_ingest.py`)

- `test_validate_columns_ok` — df with all required columns passes without error
- `test_validate_columns_missing` — df missing a column raises `ValueError`
- `test_validate_empty_file` — df with 0 rows raises `ValueError`
- `test_validate_small_file_warns` — df with <10k rows logs a warning but does not raise
- `test_validate_date_range_warn` — df where <80% of pickups are in the expected month logs a warning but does not raise
- `test_column_rename` — after applying the column map, output df has DB column names and no Parquet column names
- `test_build_url` — correct URL constructed for YEAR=2024, MONTH=1

All tests use small in-memory DataFrames — no DB, no network calls.

---

## Makefile Integration

`make ingest YEAR=2024 MONTH=1` should map to:
```
python scripts/ingest.py trips $(YEAR) $(MONTH)
```
The Makefile target already exists in the scaffold — confirm it calls the right entrypoint after the script is written.

---

## Out of Scope for V1
- Zone lookup ingestion (`ingest zones`) — add in V1.1 if dbt needs it before Airflow is set up
- `scripts/db.py` helper — extract only when a second script needs the connection

---

## Definition of Done

- [ ] `make ingest YEAR=2024 MONTH=1` completes without error
- [ ] `raw.yellow_taxi_trips` has rows loaded (count logged to stdout)
- [ ] `raw.ingestion_log` has 1 row with `status = 'success'`
- [ ] Running the same command again succeeds and row count stays the same (idempotent)
- [ ] `make test` passes all unit tests
- [ ] `ruff check scripts/` passes with no errors
