
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_timestamp
from ANALYTICS_DEV.RAW_CORE.fact_nyc_taxi_trips
where pickup_timestamp is null



  
  
      
    ) dbt_internal_test