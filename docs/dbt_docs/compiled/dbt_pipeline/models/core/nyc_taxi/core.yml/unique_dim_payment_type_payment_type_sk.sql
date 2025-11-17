
    
    

select
    payment_type_sk as unique_field,
    count(*) as n_records

from ANALYTICS.RAW_CORE.dim_payment_type
where payment_type_sk is not null
group by payment_type_sk
having count(*) > 1


