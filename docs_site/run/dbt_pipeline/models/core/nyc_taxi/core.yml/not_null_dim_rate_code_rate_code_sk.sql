
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rate_code_sk
from ANALYTICS_DEV.RAW_CORE.dim_rate_code
where rate_code_sk is null



  
  
      
    ) dbt_internal_test