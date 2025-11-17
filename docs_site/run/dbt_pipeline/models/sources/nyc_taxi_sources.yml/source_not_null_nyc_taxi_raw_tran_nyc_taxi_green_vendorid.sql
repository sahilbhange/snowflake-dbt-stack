
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select vendorid
from ANALYTICS.RAW.raw_tran_nyc_taxi_green
where vendorid is null



  
  
      
    ) dbt_internal_test