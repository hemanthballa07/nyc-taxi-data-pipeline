{{ config(materialized='table') }}

with trips as (

    select * from {{ ref('fact_trips') }}

),

final as (

    select
        date_trunc('hour', pickup_datetime)         as pickup_hour,
        pickup_location_id,
        count(*)                                    as trip_count,
        round(avg(fare_amount), 2)                          as avg_fare_amount,
        round(avg(trip_distance)::numeric, 2)               as avg_trip_distance,
        round(avg(trip_duration_minutes), 2)                as avg_trip_duration_minutes,
        round(sum(total_amount), 2)                         as total_revenue,
        round(avg(tip_amount), 2)                           as avg_tip_amount

    from trips
    group by 1, 2

)

select * from final
