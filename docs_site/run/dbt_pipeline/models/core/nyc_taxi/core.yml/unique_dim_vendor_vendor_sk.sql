
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    vendor_sk as unique_field,
    count(*) as n_records

from ANALYTICS_DEV.RAW_CORE.dim_vendor
where vendor_sk is not null
group by vendor_sk
having count(*) > 1



  
  
      
    ) dbt_internal_test