{{ config(
    materialized='table',
    schema='CORE',
    alias='dim_vendor',
    tags=['layer:core', 'domain:nyc_taxi']
) }}

with distinct_vendors as (
    select distinct vendor_id
    from {{ ref('int_tran_nyc_taxi_all') }}
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
    {{ dbt_utils.generate_surrogate_key(['vendor_id']) }} as vendor_sk,
    vendor_id,
    vendor_name,
    record_loaded_at
from mapped
