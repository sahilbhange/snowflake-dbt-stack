
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    taxi_zone_sk as unique_field,
    count(*) as n_records

from ANALYTICS_DEV.RAW_CORE.dim_taxi_zone
where taxi_zone_sk is not null
group by taxi_zone_sk
having count(*) > 1



  
  
      
    ) dbt_internal_test