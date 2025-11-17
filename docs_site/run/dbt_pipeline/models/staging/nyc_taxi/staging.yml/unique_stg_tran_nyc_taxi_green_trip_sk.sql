
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    trip_sk as unique_field,
    count(*) as n_records

from ANALYTICS_DEV.RAW_STAGE.stg_tran_nyc_taxi_green
where trip_sk is not null
group by trip_sk
having count(*) > 1



  
  
      
    ) dbt_internal_test