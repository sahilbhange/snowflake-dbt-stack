{{ config(
    materialized='table',
    schema='MART',
    alias='metricflow_time_spine',
    tags=['layer:mart', 'semantic:time_spine']
) }}

with spine as (
    select
        dateadd(day, seq4(), to_date('2019-01-01')) as date_day
    from table(generator(rowcount => 3650))
)

select
    date_day
from spine

