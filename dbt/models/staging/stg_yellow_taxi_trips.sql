-- year/month vars are passed by the Airflow DAG (--vars '{"year": 2024, "month": 1}').
-- Defaults of 1900/1 are safe fallbacks for manual dev runs without vars:
-- the DELETE targets 1900-01, which has no rows, so it becomes a no-op.
--
-- NOTE: set-vars must appear before config() so values are available
-- for Jinja ~ concatenation inside incremental_predicates (those strings are
-- not re-rendered by dbt — they are passed as literal SQL to the DELETE step).
{% set yr = var('year', 1900) %}
{% set mo = var('month', 1) %}
{% set have_vars = var('year', none) is not none %}
{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='delete+insert',
        incremental_predicates=[
            "dbt_internal_dest.pickup_datetime >= make_date(" ~ yr ~ "::int, " ~ mo ~ "::int, 1)::timestamp",
            "dbt_internal_dest.pickup_datetime <  make_date(" ~ yr ~ "::int, " ~ mo ~ "::int, 1)::timestamp + interval '1 month'"
        ]
    )
}}

with source as (

    select * from {{ source('raw', 'yellow_taxi_trips') }}

    {% if is_incremental() %}
        {% if have_vars %}
            -- normal DAG run: scope to the target month only (~3M rows vs 41M)
            where date_trunc('month', pickup_datetime)
                  = make_date({{ yr }}::int, {{ mo }}::int, 1)
        {% else %}
            -- fallback for manual `dbt run` without vars
            where pickup_datetime > (select max(pickup_datetime) from {{ this }})
        {% endif %}
    {% endif %}

),

typed as (

    select
        -- surrogate key: md5 of trip signature for delete+insert idempotency
        md5(
            coalesce(pickup_datetime::text,     '') ||
            coalesce(dropoff_datetime::text,    '') ||
            coalesce(pickup_location_id::text,  '') ||
            coalesce(dropoff_location_id::text, '') ||
            coalesce(vendor_id::text,           '') ||
            coalesce(fare_amount::text,         '')
        )                                                   as trip_id,

        -- identifiers
        vendor_id,
        pickup_location_id,
        dropoff_location_id,
        rate_code_id,
        payment_type,

        -- timestamps (already correct type in raw)
        pickup_datetime,
        dropoff_datetime,

        -- passenger count: raw is REAL, cast to integer (nulls preserved)
        passenger_count::integer                            as passenger_count,

        -- distance
        trip_distance,

        -- flag: store and forward (Y/N/null → true/false/null)
        case store_and_fwd_flag
            when 'Y' then true
            when 'N' then false
            else null
        end                                                 as store_and_fwd_flag,

        -- money columns: REAL → NUMERIC(10,2) to eliminate float32 rounding drift
        round(fare_amount::numeric, 2)                      as fare_amount,
        round(extra::numeric, 2)                            as extra,
        round(mta_tax::numeric, 2)                          as mta_tax,
        round(tip_amount::numeric, 2)                       as tip_amount,
        round(tolls_amount::numeric, 2)                     as tolls_amount,
        round(improvement_surcharge::numeric, 2)            as improvement_surcharge,
        round(total_amount::numeric, 2)                     as total_amount,
        round(congestion_surcharge::numeric, 2)             as congestion_surcharge,
        round(airport_fee::numeric, 2)                      as airport_fee

    from source

),

enriched as (

    select
        *,

        -- derived: trip duration in decimal minutes
        round(
            extract(epoch from (dropoff_datetime - pickup_datetime)) / 60.0,
            2
        )                                                   as trip_duration_minutes,

        -- anomaly flag: row is kept but flagged for downstream filtering
        (
            fare_amount < 0
            or trip_distance < 0
            or extract(epoch from (dropoff_datetime - pickup_datetime)) < 0
            or extract(epoch from (dropoff_datetime - pickup_datetime)) > 86400  -- > 24 hours
            or passenger_count is null
        )                                                   as is_anomaly

    from typed

)

select * from enriched
