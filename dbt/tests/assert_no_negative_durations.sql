-- Asserts that no clean trips in fact_trips have a negative trip_duration_minutes.
-- Returns 0 rows when the assertion holds (test passes).
select *
from {{ ref('fact_trips') }}
where trip_duration_minutes < 0
