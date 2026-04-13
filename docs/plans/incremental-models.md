# Implementation Plan: Incremental dbt Models

**Status:** Approved — ready to implement
**Approved:** 2026-04-12
**Approach:** Option 1 — `delete+insert` with `trip_id` surrogate key + dbt vars
**dbt versions:** core 1.11.8, dbt-postgres 1.10.0

---

## Goal

Convert `stg_yellow_taxi_trips` and `fact_trips` from `materialized='table'` (full
41M-row rebuild per run) to `materialized='incremental'` (process only the target
month's ~3M rows per DAG run). Each monthly DAG run passes `year`/`month` as dbt
`--vars`, scoping the incremental run to exactly one month.

`fact_hourly_summary` stays as `materialized='table'` — it's an aggregation that must
reflect all historical data and is fast enough to rebuild from `fact_trips`.

---

## Critical pre-implementation check

Before writing any model code, verify `incremental_predicates` with `dbt_internal_dest`
works on dbt-postgres 1.10.0. `incremental_predicates` was introduced in dbt 1.3 and
the `dbt_internal_dest` alias is the internal name dbt uses for the target table in the
DELETE step of `delete+insert`. Run a minimal smoke test during implementation:

```bash
cd dbt && .venv/bin/dbt run --select stg_yellow_taxi_trips --vars '{"year": 2024, "month": 1}'
```

Check the compiled SQL in `dbt/target/compiled/` to confirm the DELETE statement includes
the month predicate. If `dbt_internal_dest` is rejected, fall back to omitting
`incremental_predicates` — the `trip_id`-based DELETE is still correct, just slower
(scans all trip_ids rather than filtering by month first).

---

## Surrogate key: `trip_id`

Generated once in `stg_yellow_taxi_trips`, carried through unchanged to `fact_trips`.

```sql
md5(
    coalesce(pickup_datetime::text,      '') ||
    coalesce(dropoff_datetime::text,     '') ||
    coalesce(pickup_location_id::text,   '') ||
    coalesce(dropoff_location_id::text,  '') ||
    coalesce(vendor_id::text,            '') ||
    coalesce(fare_amount::text,          '')
) as trip_id
```

- Not globally unique — true duplicate trips (identical on all 6 fields) get the same
  hash and will be merged on re-run. This is the correct behaviour.
- `coalesce(..., '')` prevents null-poisoning the hash.

---

## Files to Change

| File | Action |
|------|--------|
| `dbt/models/staging/stg_yellow_taxi_trips.sql` | Change config, add `trip_id`, add incremental filter |
| `dbt/models/marts/fact_trips.sql` | Change config, carry `trip_id`, add incremental filter |
| `dbt/models/staging/schema.yml` | Add `trip_id` column entry |
| `dbt/models/marts/schema.yml` | Add `trip_id` column entry to `fact_trips` |
| `dags/nyc_taxi_monthly.py` | Add `--vars` to `dbt_run` BashOperator command |

No new files. No new tests beyond schema.yml entries.

---

## Model Designs

### `stg_yellow_taxi_trips.sql`

```sql
{{ config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='delete+insert',
    incremental_predicates=[
        "dbt_internal_dest.pickup_datetime >= '{{ var(\"start_date\") }}'",
        "dbt_internal_dest.pickup_datetime <  '{{ var(\"end_date\") }}'"
    ]
) }}
```

`start_date` and `end_date` are computed at the top of the model from `var('year')` and
`var('month')` using the `make_date()` Postgres function (available since Postgres 10;
we run 16).

Incremental filter added to the `source` CTE:

```sql
with source as (
    select * from {{ source('raw', 'yellow_taxi_trips') }}
    {% if is_incremental() %}
    where date_trunc('month', pickup_datetime)
          = make_date({{ var('year') }}::int, {{ var('month') }}::int, 1)
    {% endif %}
),
```

`trip_id` added in the `typed` CTE (before `enriched`, so it's available throughout).

**`var()` defaults / fallback behaviour:**

```sql
{% set yr    = var('year',  none) %}
{% set mo    = var('month', none) %}
```

- `is_incremental() = false` (full refresh or first run): no filter, all rows processed.
  vars are irrelevant.
- `is_incremental() = true`, vars provided (normal DAG run): month filter applied.
- `is_incremental() = true`, vars absent (manual `dbt run` in local dev): fall back to
  `pickup_datetime > (SELECT MAX(pickup_datetime) FROM {{ this }})`.
  This prevents errors during ad-hoc local runs.

### `fact_trips.sql`

Same config block as `stg_yellow_taxi_trips` (same `unique_key`, strategy, predicates).

`trip_id` is selected from `stg_yellow_taxi_trips` (no re-computation).

Incremental filter on the `clean_trips` CTE:

```sql
with clean_trips as (
    select * from {{ ref('stg_yellow_taxi_trips') }}
    where not is_anomaly
    {% if is_incremental() %}
    and date_trunc('month', pickup_datetime)
        = make_date({{ var('year') }}::int, {{ var('month') }}::int, 1)
    {% endif %}
),
```

Because `stg_yellow_taxi_trips` is already filtered to the target month during an
incremental run, the filter here is a safety guard — it ensures `fact_trips` never
accidentally ingests rows outside the intended month even if the upstream model's filter
changes.

### DAG — `dbt_run` command

Current:
```python
bash_command=f"cd {DBT_DIR} && {DBT} run --profiles-dir ."
```

Updated:
```python
bash_command=(
    f"cd {DBT_DIR} && {DBT} run --profiles-dir . "
    "--vars '{\"year\": {{ params.year }}, \"month\": {{ params.month }}}'"
)
```

`dbt_test` and `dbt_seed` commands are unchanged.

---

## Schema.yml additions

Both `schema.yml` files get a `trip_id` entry:

```yaml
- name: trip_id
  description: >
    MD5 surrogate key derived from pickup_datetime, dropoff_datetime,
    pickup_location_id, dropoff_location_id, vendor_id, and fare_amount.
    Used as the unique_key for incremental delete+insert. Not guaranteed
    globally unique — true duplicate trips hash identically (correct behaviour).
  tests:
    - not_null
```

No `unique` test on `trip_id` — true duplicates in the raw data would cause it to fail,
and deduplication is intentional behaviour, not a bug.

---

## Run order during implementation

1. Modify `stg_yellow_taxi_trips.sql` + its `schema.yml`
2. Modify `fact_trips.sql` + its `schema.yml`
3. Modify `dags/nyc_taxi_monthly.py`
4. Verify `incremental_predicates` syntax:
   ```bash
   cd dbt && .venv/bin/dbt run \
     --select stg_yellow_taxi_trips \
     --vars '{"year": 2024, "month": 1}' \
     --profiles-dir .
   ```
   Inspect `dbt/target/run/nyc_taxi/models/staging/stg_yellow_taxi_trips.sql` for the
   compiled DELETE statement.
5. If `dbt_internal_dest` is accepted: keep `incremental_predicates`.
   If rejected: remove `incremental_predicates` from both models (the `trip_id`-based
   DELETE still works, just without the month-scoped optimisation).
6. Full run:
   ```bash
   cd dbt && .venv/bin/dbt run \
     --vars '{"year": 2024, "month": 1}' \
     --profiles-dir .
   ```
7. Verify row counts: `stg` and `fact_trips` totals should be unchanged (incremental
   run for Jan 2024 on top of existing data = no net change since Jan is already present).
8. Run `dbt test --profiles-dir .` — all existing tests must still pass.
9. Update `docs/architecture.md`, `docs/changelog.md`, `docs/plan.md`.

---

## Definition of Done

- [ ] `stg_yellow_taxi_trips` config shows `materialized='incremental'`
- [ ] `fact_trips` config shows `materialized='incremental'`
- [ ] `trip_id` column present in both tables (verify in Postgres)
- [ ] `dbt run --vars '{"year": 2024, "month": 2}'` only processes Feb rows (verify via row count delta)
- [ ] `dbt run --full-refresh` rebuilds both tables completely without error
- [ ] All dbt tests pass after the change
- [ ] DAG `dbt_run` task passes `--vars` correctly (inspect Airflow task log)
- [ ] `incremental_predicates` behaviour documented (kept or dropped with reason)
