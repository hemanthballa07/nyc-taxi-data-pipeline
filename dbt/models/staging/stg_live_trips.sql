{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='append'
    )
}}

with source as (

    select * from {{ source('raw', 'live_trips') }}

    {% if is_incremental() %}
        -- on incremental runs, only process rows not yet in staging
        where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

),

typed as (

    select
        -- streaming metadata (outside macro — specific to this model)
        event_id,
        kafka_offset,
        kafka_partition,
        ingested_at,

        -- shared trip field casts (identifiers, timestamps, money columns)
        {{ clean_trip_fields() }}

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
