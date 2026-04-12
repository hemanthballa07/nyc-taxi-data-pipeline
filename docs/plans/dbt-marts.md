# Plan: dbt Marts Layer

**Status:** Approved — ready to implement
**Last updated:** 2026-04-11
**Scope:** All 6 mart models + prerequisite zone CSV load. Staging is complete and verified.

---

## Goal

Build the star schema mart layer on top of `staging.stg_yellow_taxi_trips`:
four dimension tables and two fact tables. Metabase will query these directly.

---

## Approved Design Decisions (from brainstorm)

| Decision                         | Choice                                                               |
|----------------------------------|----------------------------------------------------------------------|
| dim_location source              | Load zone CSV first, build from `raw.taxi_zone_lookup`               |
| dim_date generation              | `generate_series` SQL, date range derived from staging min/max       |
| dim_payment_type / dim_rate_code | dbt seeds (CSV files)                                                |
| Fact table keys                  | Natural IDs — no surrogate keys                                      |
| fact_hourly_summary source       | Aggregate from `fact_trips`                                          |
| Anomaly rows in facts            | Filter `WHERE NOT is_anomaly` in fact_trips; summary inherits filter |

---

## Files to Create

| File                                       | Purpose                                                                |
|--------------------------------------------|------------------------------------------------------------------------|
| `scripts/ingest.py`                        | Add `--zones-only` flag to load `raw.taxi_zone_lookup` from TLC CSV    |
| `dbt/seeds/dim_payment_type.csv`           | 7 rows: payment type codes + descriptions                              |
| `dbt/seeds/dim_rate_code.csv`              | 8 rows: rate code codes + descriptions (including code 99)             |
| `dbt/models/marts/dim_date.sql`            | Date spine from staging date range                                     |
| `dbt/models/marts/dim_location.sql`        | Zone lookup from `raw.taxi_zone_lookup`                                |
| `dbt/models/marts/fact_trips.sql`          | One row per clean trip, natural key joins                              |
| `dbt/models/marts/fact_hourly_summary.sql` | Hourly aggregations from fact_trips                                    |
| `dbt/models/marts/schema.yml`              | Descriptions + tests for all 4 models above                            |

Seeds live under `dbt/seeds/` and are loaded by `dbt seed`, not `dbt run`.

---

## Step 0 — Load Zone CSV (prerequisite)

`raw.taxi_zone_lookup` currently has 0 rows. `dim_location` cannot have borough or zone names
without it. This must be done before building marts.

**Implementation:** Add a `--zones-only` flag to `scripts/ingest.py`:
1. Download `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
2. Parse CSV — expected columns: `LocationID`, `Borough`, `Zone`, `service_zone`
3. `TRUNCATE raw.taxi_zone_lookup` then bulk-load via `COPY`
4. Log to `raw.ingestion_log` with `source_file = 'taxi_zone_lookup.csv'`, `year = 0`, `month = 0`

Makefile already has `ingest-zones` target pointing to `--zones-only`. Run:
```
make ingest-zones
```

Expected result: 265 rows in `raw.taxi_zone_lookup`.

---

## Step 1 — dbt Seeds

Two seed CSV files. Run once with `dbt seed`, re-runnable safely (full replace).

### `dbt/seeds/dim_payment_type.csv`

```
payment_type_id,description
0,Unknown/Anomaly
1,Credit card
2,Cash
3,No charge
4,Dispute
5,Unknown
6,Voided trip
```

Note: `payment_type = 0` appears on 140k anomalous rows in Jan 2024. `5` and `6` appear
in the TLC data dictionary but not in Jan 2024 data — include for completeness.

### `dbt/seeds/dim_rate_code.csv`

```
rate_code_id,description
1,Standard rate
2,JFK
3,Newark
4,Nassau or Westchester
5,Negotiated fare
6,Group ride
99,Unknown
```

`rate_code_id = 99` appears on ~28,663 rows in Jan 2024. Not in the TLC spec — treat as Unknown.
NULLs (140k anomalous rows) will not join to this seed; that's acceptable since those rows
are filtered out of `fact_trips` by the `is_anomaly` condition.

---

## Step 2 — dim_date

**File:** `dbt/models/marts/dim_date.sql`
**Materialization:** table

Date range is derived dynamically from staging to auto-extend as new months load:
```sql
select
    min(pickup_datetime)::date as start_date,
    max(pickup_datetime)::date as end_date
from {{ ref('stg_yellow_taxi_trips') }}
```

Then `generate_series(start_date, end_date, '1 day')` produces one row per calendar day.

**Columns:**

| Column | Type | Notes |
|--------|------|-------|
| `date_day` | date | Primary key — the calendar date |
| `year` | integer | `EXTRACT(year FROM date_day)` |
| `month` | integer | `EXTRACT(month FROM date_day)` |
| `day` | integer | `EXTRACT(day FROM date_day)` |
| `quarter` | integer | `EXTRACT(quarter FROM date_day)` |
| `day_of_week` | integer | 0=Sunday … 6=Saturday (Postgres `EXTRACT(dow ...)`) |
| `day_name` | text | `TO_CHAR(date_day, 'Day')` — e.g. `'Monday   '` trimmed |
| `month_name` | text | `TO_CHAR(date_day, 'Month')` trimmed |
| `is_weekend` | boolean | `day_of_week IN (0, 6)` |
| `is_weekday` | boolean | `NOT is_weekend` |

**Tests:** `not_null` and `unique` on `date_day`.

---

## Step 3 — dim_location

**File:** `dbt/models/marts/dim_location.sql`
**Materialization:** table
**Source:** `raw.taxi_zone_lookup` (populated in Step 0)

Simple select with column renames to match the schema:

| Raw column            | Mart column   |
|-----------------------|---------------|
| `location_id`         | `location_id` |
| `borough`             | `borough`     |
| `zone`                | `zone_name`   |
| `service_zone`        | `service_zone'|

No transformations needed beyond renaming. The TLC CSV is already clean.

**Tests:** `not_null` and `unique` on `location_id`. `not_null` on `borough`.

---

## Step 4 — fact_trips

**File:** `dbt/models/marts/fact_trips.sql`
**Materialization:** table
**Grain:** one row per clean trip (anomalous rows excluded)

**Filter:** `WHERE NOT is_anomaly` applied at the top of the CTE chain. This removes:
- 37,447 negative-fare rows
- 140,162 null-passenger / payment_type=0 / null-rate_code rows
- Any rows with negative or extreme durations

Expected output row count for Jan 2024: ~2,786,997 (2,964,606 minus ~177,609 anomalies).
Exact count confirmed after build.

**Columns:**

| Column                   | Source  | Notes                                            |
|--------------------------|---------|--------------------------------------------------|
| `pickup_datetime`        | staging | Timestamp of pickup                              |
| `dropoff_datetime`       | staging | Timestamp of dropoff                             |
| `pickup_date`            | derived | `pickup_datetime::date` — join key to `dim_date` |
| `pickup_location_id`     | staging | FK → `dim_location.location_id`                  |
| `dropoff_location_id`    | staging | FK → `dim_location.location_id`                  |
| `payment_type`           | staging | FK → `dim_payment_type.payment_type_id`          |
| `rate_code_id`           | staging | FK → `dim_rate_code.rate_code_id`                |
| `vendor_id`              | staging | 1 or 2                                           |
| `passenger_count`        | staging | integer, non-null (anomalies filtered)           |
| `trip_distance`          | staging | miles                                            |
| `trip_duration_minutes`  | staging | pre-computed in staging                          |
| `fare_amount`            | staging | NUMERIC(10,2)                                    |
| `tip_amount`             | staging | NUMERIC(10,2)                                    |
| `tolls_amount`           | staging | NUMERIC(10,2)                                    |
| `total_amount`           | staging | NUMERIC(10,2)                                    |
| `congestion_surcharge`   | staging | NUMERIC(10,2)                                    |
| `airport_fee`            | staging | NUMERIC(10,2)                                    |

**No joins in this model** — `fact_trips` stores the natural foreign keys from staging directly.
Metabase (and any analyst) can join to dimensions on demand. This keeps the fact model simple
and avoids denormalizing data that's better left in the dimension tables.

**Tests:**
- `not_null` on `pickup_datetime`, `dropoff_datetime`, `pickup_location_id`, `dropoff_location_id`
- `not_null` on `fare_amount`, `total_amount`, `passenger_count`
- Custom test: `fare_amount >= 0` — no negative fares should exist post-filter
- Custom test: `trip_duration_minutes >= 0` — no negative durations post-filter

Custom tests use dbt's singular test pattern: a SQL file in `dbt/tests/` that returns 0 rows
when the assertion holds.

---

## Step 5 — fact_hourly_summary

**File:** `dbt/models/marts/fact_hourly_summary.sql`
**Materialization:** table
**Grain:** one row per (pickup_hour, pickup_location_id)
**Source:** `{{ ref('fact_trips') }}` — inherits the anomaly filter

**Aggregation columns:**

| Column                      | Expression                              |
|-----------------------------|-----------------------------------------|
| `pickup_hour`               | `DATE_TRUNC('hour', pickup_datetime)`   |
| `pickup_location_id`        | group by key                            |
| `trip_count`                | `COUNT(*)`                              |
| `avg_fare_amount`           | `ROUND(AVG(fare_amount), 2)`            |
| `avg_trip_distance`         | `ROUND(AVG(trip_distance), 2)`          |
| `avg_trip_duration_minutes` | `ROUND(AVG(trip_duration_minutes), 2)`  |
| `total_revenue`             | `ROUND(SUM(total_amount), 2)`           |
| `avg_tip_amount`            | `ROUND(AVG(tip_amount), 2)`             |

**Tests:** `not_null` on `pickup_hour` and `pickup_location_id`.

---

## Custom Tests

Two singular tests to verify fact_trips data quality post-filter:

**`dbt/tests/assert_no_negative_fares.sql`**
```sql
select * from {{ ref('fact_trips') }}
where fare_amount < 0
```
Must return 0 rows.

**`dbt/tests/assert_no_negative_durations.sql`**
```sql
select * from {{ ref('fact_trips') }}
where trip_duration_minutes < 0
```
Must return 0 rows.

---

## Build Order

dbt resolves dependencies automatically via `ref()`, but the logical order is:

```
1. make ingest-zones          (load raw.taxi_zone_lookup — prerequisite, not dbt)
2. cd dbt && dbt seed         (dim_payment_type, dim_rate_code)
3. dbt run --select dim_date  (no upstream dbt deps)
4. dbt run --select dim_location   (no upstream dbt deps after zones loaded)
5. dbt run --select fact_trips     (depends on stg_yellow_taxi_trips)
6. dbt run --select fact_hourly_summary  (depends on fact_trips)

Or all at once after seeds:
   dbt run --select marts
```

---

## Schema.yml Tests Summary

| Model | Tested columns |
|-------|---------------|
| `dim_date` | `date_day` not_null + unique |
| `dim_location` | `location_id` not_null + unique; `borough` not_null |
| `fact_trips` | `pickup_datetime`, `dropoff_datetime`, `pickup_location_id`, `dropoff_location_id`, `fare_amount`, `total_amount`, `passenger_count` — all not_null |
| `fact_hourly_summary` | `pickup_hour`, `pickup_location_id` — not_null |
| Seeds | dbt validates seed column types automatically |

---

## Definition of Done

- [ ] `make ingest-zones` loads 265 rows into `raw.taxi_zone_lookup`
- [ ] `dbt seed` runs clean (dim_payment_type: 7 rows, dim_rate_code: 8 rows)
- [ ] `dbt run --select marts` completes with 0 errors across all 4 models
- [ ] `fact_trips` row count < 2,964,606 (anomalies filtered out)
- [ ] `dbt test --select marts` passes all schema tests
- [ ] Both singular tests pass (no negative fares, no negative durations in fact_trips)
- [ ] `staging.stg_yellow_taxi_trips` → `marts.fact_trips` lineage visible in `dbt docs`
