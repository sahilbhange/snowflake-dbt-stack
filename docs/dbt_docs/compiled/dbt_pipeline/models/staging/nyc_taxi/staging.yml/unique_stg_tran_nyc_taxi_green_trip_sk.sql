
    
    

select
    trip_sk as unique_field,
    count(*) as n_records

from ANALYTICS.RAW_STAGE.stg_tran_nyc_taxi_green
where trip_sk is not null
group by trip_sk
having count(*) > 1


