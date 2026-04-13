{#
  Emits the 19 shared trip field SELECT expressions used in both
  stg_yellow_taxi_trips and stg_live_trips.

  Covers:
    - identifiers: vendor_id, pickup/dropoff_location_id, rate_code_id, payment_type
    - timestamps:  pickup_datetime, dropoff_datetime
    - passenger_count (REAL → INTEGER, nulls preserved)
    - trip_distance
    - store_and_fwd_flag (Y/N/null → boolean true/false/null)
    - 9 money columns (REAL → NUMERIC(10,2) to eliminate float32 rounding drift)

  Does NOT include:
    - Surrogate/identity keys (trip_id, event_id)
    - Streaming metadata (kafka_offset, kafka_partition, ingested_at)
    - Derived columns (trip_duration_minutes, is_anomaly)
  Those remain in each calling model's own CTEs.

  Usage:
    typed as (
        select
            <model-specific key columns>,
            {{ clean_trip_fields() }}
        from source
    )
#}
{% macro clean_trip_fields() %}
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
{% endmacro %}
