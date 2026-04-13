# Implementation Plan

_This is a living document. Claude MUST update task checkboxes after completing work._
_Mark tasks: `[x]` when done, `[~]` if partially done, `[ ]` if not started._
_Add new tasks under the correct phase if scope changes._

**Current Phase: 7 — Data Quality (Complete)**
**Last Updated: 2026-04-12**

---

## Phase 1: Infrastructure (Days 1-2)
- [x] Set up Docker Compose with PostgreSQL
- [x] Verify Postgres connection and create schemas (`raw`, `staging`, `marts`)
- [x] Create `Makefile` with common commands
- [x] Set up Python virtual environment and `requirements.txt`
- [x] Set up `.gitignore`, `.env.example`
- [x] Initialize git repo with first commit

## Phase 2: Data Ingestion (Days 3-5)
- [x] Write `scripts/ingest.py` — download Parquet from TLC
- [x] Add validation (schema check, row count, date range)
- [x] Implement bulk loading into `raw.yellow_taxi_trips` via `COPY`
- [x] Download taxi zone lookup CSV and load into `raw.taxi_zone_lookup` (via --zones-only flag)
- [x] Create `raw.ingestion_log` table to track loads (DDL was in init-db.sql)
- [x] Test: load January 2024 data successfully (2,964,606 rows)
- [ ] Test: re-running for same month is idempotent
- [x] Write unit tests in `tests/test_ingest.py`

## Phase 3: dbt Transformations (Days 6-10)
- [x] Initialize dbt project: `dbt init nyc_taxi`
- [x] Configure `profiles.yml` for local Postgres
- [x] Create `sources.yml` pointing to raw schema
- [x] Build `stg_yellow_taxi_trips` (rename, cast, filter)
- [x] Build `dim_date` (generate date spine)
- [x] Build `dim_location` (from taxi zone lookup)
- [x] Build `dim_payment_type` (static seed or model)
- [x] Build `dim_rate_code` (static seed or model)
- [x] Build `fact_trips` (join dims, compute duration, flag anomalies)
- [x] Build `fact_hourly_summary` (aggregate by hour and zone)
- [x] Write `schema.yml` for all models with tests
- [x] Add custom tests (positive fares, valid durations)
- [ ] Run `dbt docs generate` and verify documentation

## Phase 4: Orchestration (Days 11-14)
- [x] Add Airflow to Docker Compose
- [x] Write DAG `dags/nyc_taxi_monthly.py`
- [x] Implement tasks: ingest_trips → dbt_seed → dbt_run → dbt_test
- [x] Add error handling and retries (retries=1 on all except dbt_test)
- [x] Test DAG execution via Airflow UI — all 4 tasks success for year=2024, month=1
- [ ] Verify idempotency (run DAG twice for same month)

## Phase 5: Dashboard (Days 15-16)
- [x] Add Metabase to Docker Compose
- [x] Switch Metabase internal DB to Postgres (persistent config)
- [x] Connect Metabase to Postgres marts schema
- [x] Build chart: trip volume by borough (bar)
- [x] Build chart: average fare + tip by borough (bar)
- [x] Build chart: hourly trip patterns by hour of day (line)
- [x] Build chart: data quality anomaly rate by day (line)
- [x] Assemble all 4 into "NYC Taxi — January 2024" dashboard
- [x] Save dashboard screenshot to docs/dashboard_screenshot.png

## Phase 7: Data Quality (Complete)
- [x] Brainstorm GE design — approved Option 1 (standalone script, soft fail)
- [x] Write implementation plan to docs/plans/great-expectations.md
- [x] Write scripts/run_ge.py — 10 expectations, EphemeralDataContext, HTML report
- [x] Write tests/test_run_ge.py — 7 unit tests, all passing
- [x] Add ge_validate task to nyc_taxi_monthly DAG (between ingest_trips and dbt_seed)
- [x] Add great-expectations==0.18.22 to requirements.txt and docker-compose.yml
- [x] Gitignore docs/ge_report/ (generated at runtime)

## Phase 6: Polish (Days 17-18)
- [ ] Write comprehensive README.md
- [ ] Export architecture diagram as image
- [ ] Record 2-minute demo walkthrough
- [ ] Final code review and cleanup
- [ ] Push to GitHub

---

## Notes
- Phase 1 is considered complete because Postgres is running, schemas exist, the virtual environment is set up, `.env` and `.env.example` exist, and the repository scaffold is already in place.
- The immediate next task is `scripts/ingest.py` for downloading one month of TLC Yellow Taxi data and loading it into `raw.yellow_taxi_trips`.
- Task-specific implementation plans should be written to `docs/plans/` via the Claude `/plan` workflow before coding new features.