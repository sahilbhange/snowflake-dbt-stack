
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trip_sk
from ANALYTICS_DEV.RAW_STAGE.stg_tran_nyc_taxi_fhvhv
where trip_sk is null



  
  
      
    ) dbt_internal_test