# Plan: dbt Project Setup + Staging Layer

**Status:** Approved — ready to implement
**Last updated:** 2026-04-11
**Scope:** dbt scaffolding + `stg_yellow_taxi_trips` only. Marts are a separate plan.

---

## Goal

Initialize the dbt project and build `staging.stg_yellow_taxi_trips` — a cleaned, typed, enriched
view of `raw.yellow_taxi_trips` that every mart model will read from. No mart models in this plan.

---

## Materialization Decision

**Table** (not view, not incremental).

Staging reads 3M raw rows and applies several casts. Downstream marts would re-execute that scan
on every query if staging were a view. A table materializes the result once per `dbt run`, making
Metabase queries fast without added complexity. Incremental is deferred until Airflow orchestration
is in place and multiple months of data exist.

---

## Files to Create

| File | Purpose |
|------|---------|
| `dbt/dbt_project.yml` | Project config — name, model paths, schema defaults |
| `dbt/profiles.yml` | Connection to local Postgres on port 5433 |
| `dbt/models/staging/sources.yml` | Declares `raw.yellow_taxi_trips` as a dbt source |
| `dbt/models/staging/stg_yellow_taxi_trips.sql` | The staging model |
| `dbt/models/staging/schema.yml` | Column descriptions + dbt tests for the staging model |

No other files. `dbt init` generates boilerplate we don't need (example models, seeds dirs) —
those will be deleted or never created.

---

## dbt Project Configuration

**Project name:** `nyc_taxi`

**`dbt_project.yml` schema routing:**
```yaml
models:
  nyc_taxi:
    staging:
      +schema: staging        # writes to staging schema in Postgres
      +materialized: table
    marts:
      +schema: marts
      +materialized: table
```

This means models under `models/staging/` write to the `staging` schema automatically — no
per-model config needed.

**`profiles.yml` target:**
```yaml
nyc_taxi:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5433          # Docker postgres on host port 5433 (local postgres occupies 5432)
      dbname: nyctaxi
      user: nyctaxi
      password: nyctaxi
      schema: staging     # default schema; overridden per layer in dbt_project.yml
```

`profiles.yml` lives at `dbt/profiles.yml` (not `~/.dbt/`). The Makefile passes
`--profiles-dir .` so dbt finds it locally — no global config needed, portable for any machine.

---

## Source Declaration (`sources.yml`)

Declares `raw.yellow_taxi_trips` as the upstream source. This lets dbt:
- Generate lineage from raw → staging → marts
- Run `dbt source freshness` in future
- Use `{{ source('raw', 'yellow_taxi_trips') }}` in SQL instead of a hardcoded table name

---

## Staging Model: `stg_yellow_taxi_trips.sql`

### Structure

A single CTE chain:

```
source      → select all columns from {{ source('raw', 'yellow_taxi_trips') }}
renamed     → apply any final column renames (none needed — ingestion already renamed)
cast        → apply type casts
enriched    → add derived columns (trip_duration_minutes, is_anomaly, store_and_fwd_flag bool)
final       → select in logical column order
```

### Five Transformations

**1. Money columns: `REAL` → `NUMERIC(10,2)`**

`fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`, `extra`, `mta_tax`,
`improvement_surcharge`, `congestion_surcharge`, `airport_fee` are all stored as `REAL` (float32)
in raw. Float32 accumulates rounding error in aggregations (e.g. SUM of fares drifts by cents).
Cast to `NUMERIC(10,2)` in staging so all downstream math is exact.

```sql
ROUND(fare_amount::numeric, 2) AS fare_amount
```

**2. `passenger_count`: `REAL` → `INTEGER`**

TLC stores passenger count as float (e.g. `1.0`). Cast to `INTEGER`. Null-safe: null stays null
(140,162 null rows in the January 2024 data — don't drop them, flag them in `is_anomaly`).

```sql
passenger_count::integer AS passenger_count
```

**3. `store_and_fwd_flag`: `TEXT ('Y'/'N'/NULL)` → `BOOLEAN`**

The raw column has three values: `'Y'`, `'N'`, and `NULL`. Map to boolean:

```sql
CASE store_and_fwd_flag
    WHEN 'Y' THEN true
    WHEN 'N' THEN false
    ELSE null
END AS store_and_fwd_flag
```

**4. `trip_duration_minutes`: derived column**

Computed once here so no mart has to repeat it. Cast to `NUMERIC(8,2)` for clean decimal minutes.
Guard against negative durations (dropoff before pickup exists in TLC data) — those are anomalies,
not errors to drop.

```sql
ROUND(
    EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60.0,
    2
) AS trip_duration_minutes
```

**5. `is_anomaly`: boolean flag**

A row is flagged as anomalous if ANY of these are true:
- `fare_amount < 0` (37,447 rows in Jan 2024)
- `trip_distance < 0` (0 rows in Jan 2024, but possible in other months)
- `trip_duration_minutes < 0` (dropoff before pickup)
- `trip_duration_minutes > 1440` (trip longer than 24 hours)
- `passenger_count IS NULL` (140,162 rows in Jan 2024)

Rows are **not dropped** — they're flagged. The marts layer will filter or segment on
`is_anomaly = false`. This preserves auditability and lets the dashboard show data quality metrics.

```sql
(
    fare_amount < 0
    OR trip_distance < 0
    OR trip_duration_minutes < 0
    OR trip_duration_minutes > 1440
    OR passenger_count IS NULL
) AS is_anomaly
```

---

## Tests (`schema.yml`)

| Column | Tests |
|--------|-------|
| `pickup_datetime` | `not_null` |
| `dropoff_datetime` | `not_null` |
| `pickup_location_id` | `not_null` |
| `dropoff_location_id` | `not_null` |
| `fare_amount` | `not_null` |
| `payment_type` | `not_null`, `accepted_values: [1, 2, 3, 4, 5, 6]` |
| `is_anomaly` | `not_null` |

**Not tested:**
- `passenger_count` — intentionally null for 140k rows; a `not_null` test would fail by design
- Uniqueness — there's no row-level key in the TLC data; uniqueness tests would always fail

---

## What Could Go Wrong

| Risk | Mitigation |
|------|-----------|
| `profiles.yml` not found | Pass `--profiles-dir .` in Makefile dbt targets |
| Schema mismatch if a second month is loaded with different columns | `stg_yellow_taxi_trips` will fail to build — caught by `dbt run` before marts are touched |
| `NUMERIC` cast fails on a value that's `Inf` or `NaN` (possible in Parquet) | Cast via `ROUND(...::numeric, 2)` — Postgres will raise an error; fix in ingestion if it surfaces |
| `dbt_project.yml` schema prefix — dbt appends the target schema to the model schema by default | Need to use a custom `generate_schema_name` macro OR set `+schema: staging` with the full name approach. Verified approach: use `custom_schema_name` macro to prevent dbt from generating `nyctaxi_staging` instead of `staging`. |

The last risk is the most common dbt gotcha on Postgres. The fix is a one-line macro:

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) %}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{% endmacro %}
```

This macro is required — without it, dbt writes to `nyctaxi_staging` instead of `staging`.
Add it to the files list.

---

## Files to Create (revised with macro)

| File | Purpose |
|------|---------|
| `dbt/dbt_project.yml` | Project config |
| `dbt/profiles.yml` | Local Postgres connection |
| `dbt/models/staging/sources.yml` | Raw source declaration |
| `dbt/models/staging/stg_yellow_taxi_trips.sql` | Staging model |
| `dbt/models/staging/schema.yml` | Tests + column descriptions |
| `dbt/macros/generate_schema_name.sql` | Prevents dbt from prefixing schema names |

---

## Implementation Steps

1. Create `dbt/dbt_project.yml` — project name, model path, schema routing per layer
2. Create `dbt/profiles.yml` — Postgres connection on port 5433
3. Create `dbt/macros/generate_schema_name.sql` — schema name fix
4. Create `dbt/models/staging/sources.yml` — declare `raw.yellow_taxi_trips`
5. Create `dbt/models/staging/stg_yellow_taxi_trips.sql` — CTE chain with all 5 transformations
6. Create `dbt/models/staging/schema.yml` — descriptions + tests
7. Update `Makefile` — ensure `dbt-run` and `dbt-test` pass `--profiles-dir .`
8. Run `make dbt-run` — verify model builds and row count matches expectations
9. Run `make dbt-test` — verify all tests pass
10. Verify schema: confirm `staging.stg_yellow_taxi_trips` exists in Postgres with correct types

---

## Definition of Done

- [ ] `make dbt-run` completes with 0 errors
- [ ] `staging.stg_yellow_taxi_trips` exists in Postgres with ~2,964,606 rows
- [ ] `fare_amount` column type is `numeric`, not `real`
- [ ] `is_anomaly` is populated (non-null) on every row
- [ ] `make dbt-test` passes all schema tests
- [ ] `ruff` is not relevant here (SQL only), but SQL follows lowercase keywords + CTE style
