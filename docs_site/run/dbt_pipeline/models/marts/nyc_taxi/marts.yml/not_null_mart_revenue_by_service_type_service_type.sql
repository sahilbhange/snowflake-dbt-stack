
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select service_type
from ANALYTICS_DEV.RAW_MART.mart_revenue_by_service_type
where service_type is null



  
  
      
    ) dbt_internal_test