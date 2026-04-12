# NYC Taxi Data Pipeline
## Who
Data engineering learner, completed crash course. Comfortable with basic SQL and Python. Learning dbt, Airflow, and Docker through this project. Explain new concepts briefly when they come up — don't assume prior knowledge of dbt macros, Airflow operators, or Docker networking.
## What
An end-to-end data engineering pipeline that ingests NYC Yellow Taxi trip data, transforms it through a star schema, and serves analytics dashboards. Built as a portfolio project demonstrating production-grade DE patterns.

## Architecture
```
NYC TLC (Parquet) → Python Ingestion → PostgreSQL (raw) → dbt (staging→marts) → Metabase Dashboard
                                                          ↑
                                          Airflow orchestrates everything
```

## Tech Stack
- **Language**: Python 3.11+
- **Database**: PostgreSQL 16 (Dockerized)
- **Transformations**: dbt-core with dbt-postgres adapter
- **Orchestration**: Apache Airflow 2.x (Dockerized)
- **Dashboard**: Metabase (Dockerized)
- **Containerization**: Docker Compose
- **Data format**: Parquet (source), SQL (warehouse)

## Project Structure
```
├── scripts/          # Python ingestion & utility scripts
├── dbt/              # dbt project (models, tests, macros)
├── dags/             # Airflow DAG definitions
├── docker/           # Custom Dockerfiles if needed
├── tests/            # Python tests for ingestion scripts
├── data/             # Local data (gitignored)
│   ├── raw/          # Downloaded Parquet files
│   └── processed/    # Intermediate outputs
├── docs/             # Architecture docs, decisions, notes
└── docker-compose.yml
```

## Key Commands
- `make up` — start all services (Postgres, Airflow, Metabase)
- `make down` — stop all services
- `make ingest YEAR=2024 MONTH=1` — download and load one month of taxi data
- `make dbt-run` — run all dbt models
- `make dbt-test` — run dbt tests
- `make test` — run Python unit tests
- `docker compose ps` — check service status

## Data Source
NYC TLC Yellow Taxi Trip Records: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Parquet format, ~3M rows/month, ~50MB/file
- Key columns: pickup/dropoff datetime, locations (zone IDs), fare, tip, payment type
- Zone lookup CSV: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

## Code Standards
- Python: use `ruff` for linting, type hints on all function signatures
- SQL (dbt): lowercase keywords, CTEs over subqueries, one model per file
- All scripts must have `if __name__ == "__main__":` guards
- Use `logging` module, never bare `print()` for operational output
- Every dbt model needs a `schema.yml` entry with description and tests

## Git Workflow
- Branch from `main` for each feature: `feat/ingestion`, `feat/dbt-models`, etc.
- Write descriptive commit messages: `feat: add hourly trip aggregation model`
- Never commit data files, credentials, or `.env` files

## Database Schema (Target)
- `raw.yellow_taxi_trips` — raw ingested data, append-only
- `staging.stg_yellow_taxi_trips` — cleaned, typed, filtered (dbt)
- `marts.dim_date` — date dimension
- `marts.dim_location` — taxi zone lookup
- `marts.dim_payment_type` — payment type dimension
- `marts.dim_rate_code` — rate code dimension
- `marts.fact_trips` — cleaned trips with dimension keys
- `marts.fact_hourly_summary` — hourly aggregations per zone

## ⚠️ MANDATORY: Living Documentation Rules
Before writing ANY code, read these source-of-truth files:
- `docs/architecture.md` — current system design, schema, and design decisions
- `docs/plan.md` — implementation checklist (what's done, what's next)
- `docs/changelog.md` — log of every significant change made to the project

After making ANY code change, you MUST:
1. Update `docs/changelog.md` with: date, what changed, why, files affected
2. Update `docs/plan.md` — check off completed tasks, add new ones if scope changed
3. Update `docs/architecture.md` — if schema, pipeline flow, or design decisions changed
4. If a new file was created, verify it appears in the correct section of this CLAUDE.md project structure
5. If you find a discrepancy between the docs and the actual code/database state, fix the docs to match reality and note the correction in `docs/changelog.md`. Docs always reflect what IS, not what was planned.

These docs are the project's memory. Without updating them, future sessions lose context. This is non-negotiable.

## Do Not
- Do not install packages without adding them to `requirements.txt`
- Do not create files outside the project structure listed above
- Do not use `pandas.to_sql()` — use `psycopg2.copy_expert()` for bulk loads
- Do not skip writing tests — every new script gets a test file
- Do not use `print()` — use the `logging` module
- Do not modify `docker/init-db.sql` after Postgres has been initialized (use migration scripts instead)
- Do not start implementing without reading `docs/plan.md` first

## Important Notes
- PostgreSQL runs on port 5432 (user: `nyctaxi`, password: `nyctaxi`, db: `nyctaxi`)
- Airflow webserver runs on port 8080 (user: `airflow`, password: `airflow`)
- Metabase runs on port 3000
- Data downloads go to `data/raw/` — this directory is gitignored
- For dbt docs, see `dbt/README.md`
- All database operations must use context managers (`with conn:`) or explicit try/finally
- All scripts must exit with non-zero status on failure (use `sys.exit(1)`)
- Log errors with `logger.error()` before raising exceptions
