
    
    

with child as (
    select vendor_sk as from_field
    from ANALYTICS.RAW_CORE.fact_nyc_taxi_trips
    where vendor_sk is not null
),

parent as (
    select vendor_sk as to_field
    from ANALYTICS.RAW_CORE.dim_vendor
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


