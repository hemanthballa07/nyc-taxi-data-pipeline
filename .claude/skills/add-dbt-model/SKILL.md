---
name: add-dbt-model
description: Create or modify dbt models for the NYC taxi pipeline. Use when building staging models, dimension tables, fact tables, or any SQL transformation.
---

# dbt Model Skill

## Project Location
`dbt/` directory. Run all dbt commands from inside this directory.

## Model Layers (follow this strictly)
1. **staging** (`dbt/models/staging/`) — Light cleaning of raw data. One model per source table. Prefix: `stg_`
2. **intermediate** (`dbt/models/intermediate/`) — Business logic, joins, computed columns. Prefix: `int_`
3. **marts** (`dbt/models/marts/`) — Final dimension and fact tables consumed by dashboards. Prefix: `dim_` or `fact_`

## SQL Style Rules
- Lowercase SQL keywords: `select`, `from`, `where`, `join`
- Use CTEs, never subqueries in SELECT or WHERE
- One column per line in SELECT
- Always alias tables in joins
- Use `{{ ref('model_name') }}` to reference other models, never raw table names
- Use `{{ source('source_name', 'table_name') }}` for raw tables

## Every Model Must Have
1. A corresponding entry in `schema.yml` with:
   - `description` for the model
   - `description` for every column
   - At least `unique` and `not_null` tests on primary keys
2. A `config` block specifying materialization:
   - staging: `view`
   - intermediate: `ephemeral` or `view`
   - marts/dims: `table`
   - marts/facts: `table` or `incremental`

## Example Model Pattern
```sql
-- models/staging/stg_yellow_taxi_trips.sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'yellow_taxi_trips') }}
),

renamed as (
    select
        "VendorID" as vendor_id,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        passenger_count,
        trip_distance,
        "RatecodeID" as rate_code_id,
        "PULocationID" as pickup_location_id,
        "DOLocationID" as dropoff_location_id,
        payment_type as payment_type_id,
        fare_amount,
        tip_amount,
        total_amount
    from source
)

select * from renamed
```

## Testing Pattern
```yaml
# schema.yml
models:
  - name: stg_yellow_taxi_trips
    description: Cleaned and renamed yellow taxi trip records
    columns:
      - name: pickup_datetime
        description: When the meter was engaged
        tests:
          - not_null
      - name: fare_amount
        description: The time-and-distance fare calculated by the meter
```

## Key dbt Commands
- `dbt run` — build all models
- `dbt run --select staging` — build only staging models
- `dbt test` — run all tests
- `dbt docs generate && dbt docs serve` — generate and view documentation
