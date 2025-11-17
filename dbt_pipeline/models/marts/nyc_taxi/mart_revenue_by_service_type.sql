{{ config(
    materialized='table',
    schema='MART',
    alias='mart_revenue_by_service_type',
    tags=['layer:mart', 'domain:nyc_taxi']
) }}

with fact as (
    select * from {{ ref('fact_nyc_taxi_trips') }}
),

aggregated as (
    select
        pickup_date,
        service_type,
        sum(total_amount) as gross_revenue,
        sum(fare_amount) as fare_revenue,
        sum(tip_amount) as tip_revenue,
        sum(tolls_amount) as tolls_revenue
    from fact
    group by
        pickup_date,
        service_type
)

select * from aggregated

