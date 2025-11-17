
    
    

with child as (
    select payment_type_sk as from_field
    from ANALYTICS_DEV.RAW_CORE.fact_nyc_taxi_trips
    where payment_type_sk is not null
),

parent as (
    select payment_type_sk as to_field
    from ANALYTICS_DEV.RAW_CORE.dim_payment_type
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


