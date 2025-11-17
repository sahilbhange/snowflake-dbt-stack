
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select pickup_zone_sk as from_field
    from ANALYTICS_DEV.RAW_CORE.fact_nyc_taxi_trips
    where pickup_zone_sk is not null
),

parent as (
    select taxi_zone_sk as to_field
    from ANALYTICS_DEV.RAW_CORE.dim_taxi_zone
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test