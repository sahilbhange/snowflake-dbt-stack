

with fact as (
    select * from ANALYTICS.RAW_CORE.fact_nyc_taxi_trips
),

pickup_zone as (
    select taxi_zone_sk, zone_name as pickup_zone_name, borough as pickup_borough
    from ANALYTICS.RAW_CORE.dim_taxi_zone
),

dropoff_zone as (
    select taxi_zone_sk, zone_name as dropoff_zone_name, borough as dropoff_borough
    from ANALYTICS.RAW_CORE.dim_taxi_zone
),

joined as (
    select
        f.pickup_date,
        f.service_type,
        p.pickup_borough,
        p.pickup_zone_name,
        d.dropoff_borough,
        d.dropoff_zone_name,
        count(distinct f.trip_sk) as trip_count,
        sum(f.passenger_count) as passenger_count,
        sum(f.total_amount) as revenue,
        avg(f.trip_distance_miles) as avg_trip_distance_miles,
        avg(f.trip_duration_minutes) as avg_trip_duration_minutes
    from fact f
    left join pickup_zone p on f.pickup_zone_sk = p.taxi_zone_sk
    left join dropoff_zone d on f.dropoff_zone_sk = d.taxi_zone_sk
    group by 1,2,3,4,5,6
)

select * from joined