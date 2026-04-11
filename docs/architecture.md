# Architecture

## Current State
_Last updated: 2026-04-11_

| Component         | Status       | Notes                              |
|-------------------|--------------|------------------------------------|
| Docker Compose    | ✅ Defined   | Postgres, Airflow, Metabase        |
| PostgreSQL schemas| ✅ Defined   | raw, staging, marts in init-db.sql |
| Ingestion script  | ⬜ Not started |                                  |
| dbt project       | ⬜ Not started |                                  |
| Airflow DAGs      | ⬜ Not started |                                  |
| Metabase dashboard| ⬜ Not started |                                  |
| Tests             | ⬜ Not started |                                  |

### Tables in Database
| Schema   | Table                  | Status       | Row Count |
|----------|------------------------|--------------|-----------|
| raw      | yellow_taxi_trips      | ✅ DDL ready | 0         |
| raw      | taxi_zone_lookup       | ✅ DDL ready | 0         |
| raw      | ingestion_log          | ✅ DDL ready | 0         |
| staging  | stg_yellow_taxi_trips  | ⬜ Not built |           |
| marts    | dim_date               | ⬜ Not built |           |
| marts    | dim_location           | ⬜ Not built |           |
| marts    | dim_payment_type       | ⬜ Not built |           |
| marts    | dim_rate_code          | ⬜ Not built |           |
| marts    | fact_trips             | ⬜ Not built |           |
| marts    | fact_hourly_summary    | ⬜ Not built |           |

### Files Registry
_Claude MUST update this when creating new files._

| File | Purpose | Created |
|------|---------|---------|
| `scripts/ingest.py` | Download + load TLC data | — |
| `dbt/models/staging/stg_yellow_taxi_trips.sql` | Clean raw trips | — |
| `dbt/models/marts/dim_date.sql` | Date dimension | — |
| `dbt/models/marts/dim_location.sql` | Zone lookup dimension | — |
| `dbt/models/marts/fact_trips.sql` | Core fact table | — |
| `dags/nyc_taxi_monthly.py` | Airflow DAG | — |

---

## Pipeline Overview

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────────────┐
│   NYC TLC Site   │────▶│  Python      │────▶│   PostgreSQL            │
│   (Parquet)      │     │  Ingestion   │     │                         │
└─────────────────┘     │  Script      │     │  raw.yellow_taxi_trips  │
                        └──────────────┘     │  raw.ingestion_log      │
                                             └────────────┬────────────┘
                                                          │
                                                    dbt run│
                                                          ▼
                                             ┌─────────────────────────┐
                                             │   staging               │
                                             │  stg_yellow_taxi_trips  │
                                             └────────────┬────────────┘
                                                          │
                                                    dbt run│
                                                          ▼
                                             ┌─────────────────────────┐
                                             │   marts                 │
                                             │  dim_date               │
                                             │  dim_location           │
                                             │  dim_payment_type       │
                                             │  dim_rate_code          │
                                             │  fact_trips             │
                                             │  fact_hourly_summary    │
                                             └────────────┬────────────┘
                                                          │
                                                          ▼
                                             ┌─────────────────────────┐
                                             │   Metabase Dashboard    │
                                             │  - Trip volume heatmap  │
                                             │  - Fare trends by zone  │
                                             │  - Data quality metrics │
                                             └─────────────────────────┘

                        ┌──────────────────────────────────┐
                        │  Apache Airflow                   │
                        │  Orchestrates the full pipeline   │
                        │  on a monthly schedule            │
                        └──────────────────────────────────┘
```

## Star Schema

```
                    ┌──────────────┐
                    │  dim_date    │
                    │──────────────│
                    │  date_key    │
                    │  full_date   │
                    │  year        │
                    │  month       │
                    │  day         │
                    │  day_of_week │
                    │  is_weekend  │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌──────────────────┐
│ dim_location │    │  fact_trips  │    │ dim_payment_type │
│──────────────│    │──────────────│    │──────────────────│
│ location_key │◀───│ pickup_key   │───▶│ payment_type_key │
│ zone_id      │    │ dropoff_key  │    │ payment_type_id  │
│ borough      │    │ date_key     │    │ description      │
│ zone_name    │    │ payment_key  │    └──────────────────┘
│ service_zone │    │ rate_key     │
└──────────────┘    │ vendor_id    │    ┌──────────────────┐
                    │ passenger_ct │    │ dim_rate_code    │
                    │ trip_distance│    │──────────────────│
                    │ fare_amount  │───▶│ rate_code_key    │
                    │ tip_amount   │    │ rate_code_id     │
                    │ total_amount │    │ description      │
                    │ trip_duration│    └──────────────────┘
                    │ is_anomaly   │
                    └──────────────┘
```

## Design Decisions

### Why PostgreSQL over BigQuery/Snowflake?
Free, local, no cloud account needed. The pipeline patterns are identical — if you swap the dbt adapter from `dbt-postgres` to `dbt-bigquery`, everything else works the same. PostgreSQL is a stand-in for a cloud warehouse.

### Why dbt over raw SQL?
dbt gives us version-controlled transformations, automated testing, documentation generation, and dependency management between models. These are standard in production DE teams.

### Why Metabase over Superset?
Simpler to set up in Docker. Single container, auto-detects Postgres tables, no complex configuration. For a portfolio project, the dashboard tool matters less than the data models behind it.

### Why Parquet as source format?
NYC TLC publishes in Parquet. It's columnar, compressed, and schema-aware — representative of how data moves in production pipelines. Reading Parquet with PyArrow is a valuable skill.

### Why monthly granularity?
Yellow taxi data is ~3M rows/month. Loading 6 months gives us ~18M rows — enough to be meaningful without overwhelming a local Postgres instance. The pipeline is designed to handle incremental monthly loads.
