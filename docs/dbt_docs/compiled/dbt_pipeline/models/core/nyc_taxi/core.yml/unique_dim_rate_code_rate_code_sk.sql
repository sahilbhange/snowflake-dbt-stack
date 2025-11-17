
    
    

select
    rate_code_sk as unique_field,
    count(*) as n_records

from ANALYTICS.RAW_CORE.dim_rate_code
where rate_code_sk is not null
group by rate_code_sk
having count(*) > 1


