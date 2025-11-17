
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select taxi_zone_sk
from ANALYTICS_DEV.RAW_CORE.dim_taxi_zone
where taxi_zone_sk is null



  
  
      
    ) dbt_internal_test