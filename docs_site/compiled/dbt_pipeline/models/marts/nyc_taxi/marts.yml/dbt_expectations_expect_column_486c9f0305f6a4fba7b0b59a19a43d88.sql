






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and trip_count >= 0
)
 as expression


    from ANALYTICS_DEV.RAW_MART.mart_trips_daily
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







