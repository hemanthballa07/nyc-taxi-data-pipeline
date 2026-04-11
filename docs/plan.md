# Implementation Plan

_This is a living document. Claude MUST update task checkboxes after completing work._
_Mark tasks: `[x]` when done, `[~]` if partially done, `[ ]` if not started._
_Add new tasks under the correct phase if scope changes._

**Current Phase: 1 — Infrastructure**
**Last Updated: 2026-04-11**

---

## Phase 1: Infrastructure (Days 1-2)
- [ ] Set up Docker Compose with PostgreSQL
- [ ] Verify Postgres connection and create schemas (`raw`, `staging`, `marts`)
- [ ] Create `Makefile` with common commands
- [ ] Set up Python virtual environment and `requirements.txt`
- [ ] Set up `.gitignore`, `.env.example`
- [ ] Initialize git repo with first commit

## Phase 2: Data Ingestion (Days 3-5)
- [ ] Write `scripts/ingest.py` — download Parquet from TLC
- [ ] Add validation (schema check, row count, date range)
- [ ] Implement bulk loading into `raw.yellow_taxi_trips` via `COPY`
- [ ] Download taxi zone lookup CSV and load into `raw.taxi_zone_lookup`
- [ ] Create `raw.ingestion_log` table to track loads
- [ ] Test: load January 2024 data successfully
- [ ] Test: re-running for same month is idempotent
- [ ] Write unit tests in `tests/test_ingest.py`

## Phase 3: dbt Transformations (Days 6-10)
- [ ] Initialize dbt project: `dbt init nyc_taxi`
- [ ] Configure `profiles.yml` for local Postgres
- [ ] Create `sources.yml` pointing to raw schema
- [ ] Build `stg_yellow_taxi_trips` (rename, cast, filter)
- [ ] Build `dim_date` (generate date spine)
- [ ] Build `dim_location` (from taxi zone lookup)
- [ ] Build `dim_payment_type` (static seed or model)
- [ ] Build `dim_rate_code` (static seed or model)
- [ ] Build `fact_trips` (join dims, compute duration, flag anomalies)
- [ ] Build `fact_hourly_summary` (aggregate by hour and zone)
- [ ] Write `schema.yml` for all models with tests
- [ ] Add custom tests (positive fares, valid durations)
- [ ] Run `dbt docs generate` and verify documentation

## Phase 4: Orchestration (Days 11-14)
- [ ] Add Airflow to Docker Compose
- [ ] Write DAG `dags/nyc_taxi_monthly.py`
- [ ] Implement tasks: download → validate → load → dbt_run → dbt_test
- [ ] Add error handling and retries
- [ ] Test DAG execution via Airflow UI
- [ ] Verify idempotency (run DAG twice for same month)

## Phase 5: Dashboard (Days 15-16)
- [ ] Add Metabase to Docker Compose
- [ ] Connect Metabase to Postgres
- [ ] Build chart: trip volume by hour-of-day heatmap
- [ ] Build chart: average fare by borough
- [ ] Build chart: monthly trip volume trend
- [ ] Build chart: data quality (records dropped per stage)

## Phase 6: Polish (Days 17-18)
- [ ] Write comprehensive README.md
- [ ] Export architecture diagram as image
- [ ] Record 2-minute demo walkthrough
- [ ] Final code review and cleanup
- [ ] Push to GitHub
