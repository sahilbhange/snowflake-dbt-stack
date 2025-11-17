
    
    

select
    taxi_zone_sk as unique_field,
    count(*) as n_records

from ANALYTICS.RAW_CORE.dim_taxi_zone
where taxi_zone_sk is not null
group by taxi_zone_sk
having count(*) > 1


