
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trip_count
from ANALYTICS_DEV.RAW_MART.mart_trips_daily
where trip_count is null



  
  
      
    ) dbt_internal_test