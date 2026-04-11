---
name: data-quality
description: Add or modify data quality checks. Use when implementing validations, dbt tests, data profiling, or investigating data issues.
---

# Data Quality Skill

## Quality Check Layers

### 1. Ingestion-time checks (Python, in `scripts/ingest.py`)
- Schema validation: expected columns present, correct dtypes
- Volume check: row count within expected range (100K–5M per month)
- Freshness check: pickup dates within expected month
- Null rate check: critical columns (datetime, location, fare) < 5% null

### 2. dbt tests (SQL, in `dbt/models/*/schema.yml`)
- `unique` on surrogate keys
- `not_null` on required fields
- `accepted_values` on categorical fields (payment_type, rate_code)
- `relationships` to verify foreign keys match dimension tables
- Custom tests in `dbt/tests/` for business logic:
  - `fare_amount >= 0`
  - `trip_distance >= 0`
  - `dropoff_datetime > pickup_datetime`
  - `trip_duration_minutes between 1 and 720` (no 0-second or 30-day trips)

### 3. Post-load profiling (optional, in `scripts/profile.py`)
- Value distribution summaries per column
- Outlier detection (e.g., fares > $500, distances > 200 miles)
- Compare current month stats to previous month (drift detection)

## Custom dbt Test Example
```sql
-- tests/assert_positive_fare.sql
select *
from {{ ref('stg_yellow_taxi_trips') }}
where fare_amount < 0
```
If this returns rows, the test fails — meaning negative fares exist.

## Quality Dashboard Metrics
Track these in a dbt model or Metabase chart:
- Records ingested vs records in staging (drop rate)
- Null rate per column per month
- Anomaly count per month (fare outliers, distance outliers)
- Data freshness (latest pickup_datetime in warehouse)
