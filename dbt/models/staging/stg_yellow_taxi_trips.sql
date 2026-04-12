{{ config(materialized='table') }}

with source as (

    select * from {{ source('raw', 'yellow_taxi_trips') }}

),

typed as (

    select
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
