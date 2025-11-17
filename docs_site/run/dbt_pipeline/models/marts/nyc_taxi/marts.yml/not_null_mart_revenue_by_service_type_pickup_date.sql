
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_date
from ANALYTICS_DEV.RAW_MART.mart_revenue_by_service_type
where pickup_date is null



  
  
      
    ) dbt_internal_test