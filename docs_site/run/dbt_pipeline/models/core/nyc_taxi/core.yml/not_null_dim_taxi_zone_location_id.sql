
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_id
from ANALYTICS_DEV.RAW_CORE.dim_taxi_zone
where location_id is null



  
  
      
    ) dbt_internal_test