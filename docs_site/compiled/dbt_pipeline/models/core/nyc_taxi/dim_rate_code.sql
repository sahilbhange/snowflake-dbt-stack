

with distinct_codes as (
    select distinct rate_code_id
    from ANALYTICS_DEV.RAW_STAGE.int_tran_nyc_taxi_all
    where rate_code_id is not null
),

mapped as (
    select
        rate_code_id,
        case rate_code_id
            when 1 then 'Standard rate'
            when 2 then 'JFK'
            when 3 then 'Newark'
            when 4 then 'Nassau or Westchester'
            when 5 then 'Negotiated fare'
            when 6 then 'Group ride'
            else 'Other / Unmapped'
        end as rate_code_name,
        current_timestamp() as record_loaded_at
    from distinct_codes
)

select
    md5(cast(coalesce(cast(rate_code_id as TEXT), '') as TEXT)) as rate_code_sk,
    rate_code_id,
    rate_code_name,
    record_loaded_at
from mapped