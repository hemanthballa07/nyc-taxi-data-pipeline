# NYC Taxi Data Pipeline

An end-to-end data engineering portfolio project that ingests NYC Yellow Taxi trip records, transforms them through a star schema, orchestrates the pipeline with Airflow, and serves analytics through a Metabase dashboard — all running locally on Docker.

---

## Architecture

```
┌─────────────────┐     ┌─────────────────────┐     ┌──────────────────────────────┐
│  NYC TLC Site   │────▶│  Python Ingestion   │────▶│  PostgreSQL 16               │
│  (Parquet ~3M   │     │  scripts/ingest.py  │     │                              │
│   rows/month)   │     │  - PyArrow + pandas │     │  raw.yellow_taxi_trips       │
└─────────────────┘     │  - psycopg2 COPY    │     │  raw.taxi_zone_lookup        │
                        │  - Idempotent load  │     │  raw.ingestion_log           │
                        └─────────────────────┘     └──────────────┬───────────────┘
                                                                   │
                                                           dbt run │
                                                                   ▼
                                                    ┌──────────────────────────────┐
                                                    │  staging                     │
                                                    │  stg_yellow_taxi_trips       │
                                                    │  (cast, clean, flag anomaly) │
                                                    └──────────────┬───────────────┘
                                                                   │
                                                           dbt run │
                                                                   ▼
                                                    ┌──────────────────────────────┐
                                                    │  marts (star schema)         │
                                                    │  dim_date · dim_location     │
                                                    │  dim_payment_type            │
                                                    │  dim_rate_code               │
                                                    │  fact_trips (2.8M rows)      │
                                                    │  fact_hourly_summary (72K)   │
                                                    └──────────────┬───────────────┘
                                                                   │
                                                                   ▼
                                                    ┌──────────────────────────────┐
                                                    │  Metabase Dashboard          │
                                                    │  Trip volume · Fares         │
                                                    │  Hourly patterns · DQ metrics│
                                                    └──────────────────────────────┘

                        ┌─────────────────────────────────────────┐
                        │  Apache Airflow 2.x                     │
                        │  nyc_taxi_monthly DAG (manual trigger)  │
                        │  ingest → seed → dbt run → dbt test     │
                        └─────────────────────────────────────────┘
```

---

## Tech Stack

| Layer            | Tool                    | Version     | Purpose                                 |
|------------------|-------------------------|-------------|-----------------------------------------|
| Ingestion        | Python + PyArrow        | 3.11 / 15.x | Download + bulk-load Parquet → Postgres |
| Warehouse        | PostgreSQL              | 16          | Star schema data warehouse              |
| Transformation   | dbt-core + dbt-postgres | 1.11        | SQL ELT: staging → dims → facts         |
| Orchestration    | Apache Airflow          | 2.9.3       | DAG scheduling, retries, observability  |
| Dashboard        | Metabase                | 0.59        | Self-serve BI on top of mart tables     |
| Containerization | Docker Compose          | —           | Single-command local environment        |

---

## Dashboard

![NYC Taxi Dashboard](docs/dashboard_screenshot.png)

**Key insights from January 2024:**
- **Manhattan dominance**: 2.49M of 2.79M clean trips (89%) originate in Manhattan
- **Airport premium**: EWR and N/A zones (airport routes) average ~$95–100 fares vs. $13 in Manhattan
- **Commuter peaks**: Trip volume troughs at 5am and peaks at 6pm — classic weekday pattern
- **Data quality**: Anomaly rate spikes to ~14% on Jan 1 (New Year's edge cases), settles to 4–8% for the rest of the month

---

## Data Model

```
             dim_date
                │
dim_location ──▶├── fact_trips ──▶ dim_payment_type
                │
                └──────────────▶ dim_rate_code

fact_hourly_summary (pre-aggregated from fact_trips for dashboard performance)
```

| Schema  | Table                  | Rows (Jan 2024) |
|---------|------------------------|-----------------|
| raw     | yellow_taxi_trips      | 2,964,606       |
| staging | stg_yellow_taxi_trips  | 2,964,606       |
| marts   | fact_trips             | 2,789,040       |
| marts   | fact_hourly_summary    | 72,042          |
| marts   | dim_date               | 31              |
| marts   | dim_location           | 265             |

---

## Quick Start

### Prerequisites
- Docker Desktop with 4GB+ RAM allocated
- `make` (comes with Xcode Command Line Tools on Mac)

```bash
git clone https://github.com/YOUR_USERNAME/nyc-taxi-data-pipeline.git
cd nyc-taxi-data-pipeline

# Start all services (Postgres, Airflow, Metabase)
make up

# Wait ~2 minutes for Airflow to initialize, then load January 2024 data
make ingest YEAR=2024 MONTH=1

# Run dbt transformations + tests
make dbt-run
make dbt-test
```

### Access
| Service    | URL                   | Credentials                      |
|------------|-----------------------|----------------------------------|
| Airflow    | http://localhost:8080 | airflow / airflow                |
| Metabase   | http://localhost:3000 | admin@nyctaxi.local / Admin1234! |
| PostgreSQL | localhost:5433        | nyctaxi / nyctaxi / nyctaxi      |

### Run via Airflow DAG (recommended)

The DAG runs the full pipeline end-to-end for a given month:

```bash
# Trigger via CLI
docker compose exec airflow-scheduler \
  airflow dags trigger nyc_taxi_monthly --conf '{"year": 2024, "month": 1}'

# Or use the Airflow UI → Trigger DAG w/ config
```

Tasks: `ingest_trips` → `dbt_seed` → `dbt_run` → `dbt_test`

### Load additional months

```bash
make ingest YEAR=2024 MONTH=2
make dbt-run && make dbt-test
```

---

## Project Structure

```
├── dags/                   # Airflow DAG definitions
│   └── nyc_taxi_monthly.py
├── dbt/                    # dbt project
│   ├── models/
│   │   ├── staging/        # stg_yellow_taxi_trips
│   │   └── marts/          # dims + facts
│   ├── seeds/              # dim_payment_type, dim_rate_code CSVs
│   ├── tests/              # singular tests (no negative fares, no negative durations)
│   └── profiles.yml
├── docker/
│   └── init-db.sql         # Schema DDL (raw, staging, marts)
├── docs/
│   ├── architecture.md
│   ├── plan.md
│   ├── changelog.md
│   └── dashboard_screenshot.png
├── scripts/
│   └── ingest.py           # Download + validate + bulk-load TLC Parquet
├── tests/
│   └── test_ingest.py      # 8 unit tests (no DB/network required)
├── docker-compose.yml
├── Makefile
└── requirements.txt
```

---

## Design Decisions

**Idempotent ingestion**: `ingest.py` deletes all rows for the target year/month before loading. Re-running for the same month always produces the same row count. The DAG enforces `max_active_runs=1` to prevent concurrent runs from causing a race on the delete step.

**dbt table materialization**: All models are materialized as tables (not views) so Metabase queries hit pre-computed results rather than re-scanning 3M raw rows on every dashboard load.

**Anomaly flagging**: Rows with negative fares, null passengers, or extreme durations are flagged `is_anomaly=true` in staging and kept for auditing. `fact_trips` excludes them — this is the "clean" analytical layer.

**PostgreSQL over BigQuery/Snowflake**: Free and local. The dbt adapter is the only thing that changes when moving to a cloud warehouse — all SQL and pipeline logic is identical.

---

## Testing

```bash
# Python unit tests (no Docker required)
make test

# dbt schema + singular tests (requires running Postgres)
make dbt-test
```

33 dbt tests cover uniqueness, not-null, accepted values, and custom business rules (no negative fares, no negative trip durations).

---

## Data Source

[NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — published monthly in Parquet format. ~3M rows/month, ~50MB/file.
