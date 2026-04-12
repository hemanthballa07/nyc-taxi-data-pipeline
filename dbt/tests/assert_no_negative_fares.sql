-- Asserts that no clean trips in fact_trips have a negative fare_amount.
-- Returns 0 rows when the assertion holds (test passes).
select *
from {{ ref('fact_trips') }}
where fare_amount < 0
