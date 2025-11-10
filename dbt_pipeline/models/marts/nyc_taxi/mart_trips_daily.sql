{{ config(
    materialized='table',
    schema='MART',
    alias='mart_trips_daily',
    tags=['layer:mart', 'domain:nyc_taxi']
) }}

with fact as (
    select * from {{ ref('fact_nyc_taxi_trips') }}
),

aggregated as (
    select
        pickup_date,
        service_type,
        count(distinct trip_sk) as trip_count,
        sum(passenger_count) as passenger_count,
        sum(total_amount) as gross_revenue,
        sum(fare_amount) as fare_revenue,
        sum(tip_amount) as tip_revenue,
        sum(tolls_amount) as tolls_revenue,
        avg(trip_distance_miles) as avg_trip_distance_miles,
        avg(trip_duration_minutes) as avg_trip_duration_minutes,
        avg(average_speed_mph) as avg_speed_mph,
        sum(case when pickup_hour between 7 and 9 then 1 else 0 end) as am_peak_trip_count,
        sum(case when pickup_hour between 16 and 19 then 1 else 0 end) as pm_peak_trip_count
    from fact
    group by 1,2
)

select * from aggregated
