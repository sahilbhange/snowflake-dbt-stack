
    
    

select
    trip_sk as unique_field,
    count(*) as n_records

from ANALYTICS.RAW_CORE.fact_nyc_taxi_trips
where trip_sk is not null
group by trip_sk
having count(*) > 1


