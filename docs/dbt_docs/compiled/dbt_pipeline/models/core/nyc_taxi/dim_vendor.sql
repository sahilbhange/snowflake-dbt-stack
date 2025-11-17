

with distinct_vendors as (
    select distinct vendor_id
    from ANALYTICS.RAW_STAGE.int_tran_nyc_taxi_all
    where vendor_id is not null
),

mapped as (
    select
        vendor_id,
        case vendor_id
            when 1 then 'Creative Mobile Technologies'
            when 2 then 'VeriFone Inc.'
            when 4 then 'CMT Shared Rides'
            when 5 then 'Dial 7'
            when 6 then 'MTA'
            else 'Other / Unmapped'
        end as vendor_name,
        current_timestamp() as record_loaded_at
    from distinct_vendors
)

select
    md5(cast(coalesce(cast(vendor_id as TEXT), '') as TEXT)) as vendor_sk,
    vendor_id,
    vendor_name,
    record_loaded_at
from mapped