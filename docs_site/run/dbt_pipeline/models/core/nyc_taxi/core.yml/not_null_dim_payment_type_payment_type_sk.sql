
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_type_sk
from ANALYTICS_DEV.RAW_CORE.dim_payment_type
where payment_type_sk is null



  
  
      
    ) dbt_internal_test