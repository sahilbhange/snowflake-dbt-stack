
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    trip_sk as unique_field,
    count(*) as n_records

from ANALYTICS_DEV.RAW_CORE.fact_nyc_taxi_trips
where trip_sk is not null
group by trip_sk
having count(*) > 1



  
  
      
    ) dbt_internal_test