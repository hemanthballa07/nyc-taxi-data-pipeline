{{ config(materialized='table') }}

with date_bounds as (

    select
        min(pickup_datetime)::date as start_date,
        max(pickup_datetime)::date as end_date
    from {{ ref('stg_yellow_taxi_trips') }}

),

date_spine as (

    select generate_series(start_date, end_date, '1 day'::interval)::date as date_day
    from date_bounds

),

final as (

    select
        date_day,
        extract(year    from date_day)::integer                 as year,
        extract(month   from date_day)::integer                 as month,
        extract(day     from date_day)::integer                 as day,
        extract(quarter from date_day)::integer                 as quarter,
        extract(dow     from date_day)::integer                 as day_of_week,
        trim(to_char(date_day, 'Day'))                          as day_name,
        trim(to_char(date_day, 'Month'))                        as month_name,
        extract(dow from date_day) in (0, 6)                    as is_weekend,
        extract(dow from date_day) not in (0, 6)                as is_weekday

    from date_spine

)

select * from final
