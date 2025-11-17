

with distinct_codes as (
    select distinct payment_type_code
    from ANALYTICS.RAW_STAGE.int_tran_nyc_taxi_all
    where payment_type_code is not null
),

mapped as (
    select
        payment_type_code,
        case payment_type_code
            when 1 then 'Credit Card'
            when 2 then 'Cash'
            when 3 then 'No Charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided Trip'
            else 'Other / Unmapped'
        end as payment_type_name,
        current_timestamp() as record_loaded_at
    from distinct_codes
)

select
    md5(cast(coalesce(cast(payment_type_code as TEXT), '') as TEXT)) as payment_type_sk,
    payment_type_code,
    payment_type_name,
    record_loaded_at
from mapped