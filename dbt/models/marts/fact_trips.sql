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

with clean_trips as (

    select * from {{ ref('stg_yellow_taxi_trips') }}
    where not is_anomaly

    {% if is_incremental() %}
        {% if have_vars %}
            and date_trunc('month', pickup_datetime)
                = make_date({{ yr }}::int, {{ mo }}::int, 1)
        {% else %}
            and pickup_datetime > (select max(pickup_datetime) from {{ this }})
        {% endif %}
    {% endif %}

),

final as (

    select
        -- surrogate key (carried from staging)
        trip_id,

        -- timestamps
        pickup_datetime,
        dropoff_datetime,
        pickup_datetime::date       as pickup_date,

        -- foreign keys (natural IDs — join to dim tables on demand)
        pickup_location_id,
        dropoff_location_id,
        payment_type,
        rate_code_id,

        -- attributes
        vendor_id,
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        store_and_fwd_flag,

        -- financials
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        extra,
        mta_tax,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee

    from clean_trips

)

select * from final
