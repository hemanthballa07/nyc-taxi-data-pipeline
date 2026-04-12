{{ config(materialized='table') }}

with clean_trips as (

    select * from {{ ref('stg_yellow_taxi_trips') }}
    where not is_anomaly

),

final as (

    select
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
