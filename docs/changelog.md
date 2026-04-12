# Changelog

All significant changes to this project are logged here. Claude Code MUST append to this file after every code change.

Format: `[YYYY-MM-DD] CATEGORY: description (files affected)`

Categories: `INFRA`, `INGEST`, `DBT`, `AIRFLOW`, `DASHBOARD`, `DOCS`, `FIX`, `REFACTOR`

---

## Log

[2026-04-11] INFRA: Initial project scaffold created — CLAUDE.md, docker-compose.yml, Makefile, skills, commands, documentation (all files)
[2026-04-11] INGEST: scripts/ingest.py — download, validate, bulk-load monthly Yellow Taxi Parquet into raw.yellow_taxi_trips; idempotent delete-and-reload; failure logging to raw.ingestion_log (scripts/ingest.py, tests/test_ingest.py)
[2026-04-11] FIX: Remapped Docker postgres to host port 5433 to avoid collision with local Postgres on 5432 (docker-compose.yml, .env, .env.example)
[2026-04-11] FIX: Corrected TLC column name Airport_fee (capital A) and cast float-encoded integer columns before COPY; added rollback before failure log write (scripts/ingest.py)
[2026-04-11] DBT: Initialized dbt project (nyc_taxi) with Python 3.13 venv due to Python 3.14 incompatibility in mashumaro. Built stg_yellow_taxi_trips — 2,964,606 rows, 8/8 tests passing (dbt/dbt_project.yml, dbt/profiles.yml, dbt/macros/generate_schema_name.sql, dbt/models/staging/)
[2026-04-11] INGEST: Added --zones-only flag to scripts/ingest.py; loaded 265 rows into raw.taxi_zone_lookup
[2026-04-11] DBT: Built full marts layer — dim_date (31), dim_location (265), dim_payment_type (7), dim_rate_code (7), fact_trips (2,789,040), fact_hourly_summary (72,042). 25/25 tests pass including 2 singular tests (dbt/seeds/, dbt/models/marts/, dbt/tests/)
[2026-04-11] AIRFLOW: Built nyc_taxi_monthly DAG orchestrating full pipeline (ingest_trips → dbt_seed → dbt_run → dbt_test). dbt installed via _PIP_ADDITIONAL_REQUIREMENTS; profiles.yml updated with env_var() for container portability. DAG triggered for year=2024, month=1 — all 4 tasks succeeded in ~2.5 minutes (dags/nyc_taxi_monthly.py, docker-compose.yml, dbt/profiles.yml)
[2026-04-11] DASHBOARD: Switched Metabase internal DB from H2 to Postgres for persistence; connected to marts schema; built "NYC Taxi — January 2024" dashboard with 4 charts: Trip Volume by Borough, Average Fare by Borough, Hourly Trip Patterns, Data Quality Anomaly Rate. Screenshot saved to docs/dashboard_screenshot.png (docker-compose.yml)
[2026-04-11] DOCS: Rewrote README.md as portfolio-ready doc — architecture diagram, tech stack table, setup instructions, dashboard insights, design decisions (README.md)
[2026-04-11] FIX: Added max_active_runs=1 to DAG to prevent concurrent-run race condition on delete-and-reload (dags/nyc_taxi_monthly.py)
[2026-04-11] INFRA: Removed obsolete version attribute from docker-compose.yml (docker-compose.yml)
[2026-04-12] INGEST: Loaded all 12 months of 2024 Yellow Taxi data. Months 2–11 were already present from prior session. Month 12 (Dec 2024) ingested via make ingest YEAR=2024 MONTH=12 → 3,668,337 rows. DAG nyc_taxi_monthly triggered for Dec 2024 — all 4 tasks (ingest_trips, dbt_seed, dbt_run, dbt_test) succeeded. Final counts: raw.yellow_taxi_trips = 41,169,300 rows, marts.fact_trips = 36,472,952 rows.
