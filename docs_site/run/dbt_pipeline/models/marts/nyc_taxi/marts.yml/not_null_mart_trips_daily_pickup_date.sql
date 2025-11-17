
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_date
from ANALYTICS_DEV.RAW_MART.mart_trips_daily
where pickup_date is null



  
  
      
    ) dbt_internal_test