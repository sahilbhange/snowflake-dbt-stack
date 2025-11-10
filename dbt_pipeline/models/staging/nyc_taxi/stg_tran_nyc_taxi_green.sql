{{ config(
    materialized='view',
    schema='STAGE',
    alias='stg_tran_nyc_taxi_green',
    tags=['layer:stage', 'domain:nyc_taxi']
) }}

with source as (
    select * from {{ source('nyc_taxi', 'raw_tran_nyc_taxi_green') }}
),

typed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "'green'",
            '"VENDORID"',
            '"LPEP_PICKUP_DATETIME"',
            '"LPEP_DROPOFF_DATETIME"',
            "coalesce(\"PULOCATIONID\"::string, '')",
            "coalesce(\"DOLOCATIONID\"::string, '')",
            "coalesce(\"TRIP_DISTANCE\"::string, '')",
            "coalesce(\"TOTAL_AMOUNT\"::string, '')"
        ]) }} as trip_sk,
        'green' as service_type,
        cast("VENDORID" as number(10,0)) as vendor_id,
        dateadd('microsecond', "LPEP_PICKUP_DATETIME", to_timestamp_ntz('1970-01-01 00:00:00')) as pickup_timestamp,
        dateadd('microsecond', "LPEP_DROPOFF_DATETIME", to_timestamp_ntz('1970-01-01 00:00:00')) as dropoff_timestamp,
        cast("RATECODEID" as number(10,0)) as rate_code_id,
        cast("PULOCATIONID" as number(10,0)) as pickup_location_id,
        cast("DOLOCATIONID" as number(10,0)) as dropoff_location_id,
        cast("PASSENGER_COUNT" as number(10,0)) as passenger_count,
        cast("TRIP_DISTANCE" as number(18,6)) as trip_distance_miles,
        cast("FARE_AMOUNT" as number(18,2)) as fare_amount,
        cast("EXTRA" as number(18,2)) as extra_amount,
        cast("IMPROVEMENT_SURCHARGE" as number(18,2)) as improvement_surcharge,
        cast("MTA_TAX" as number(18,2)) as mta_tax,
        cast("TIP_AMOUNT" as number(18,2)) as tip_amount,
        cast("TOLLS_AMOUNT" as number(18,2)) as tolls_amount,
        cast("TOTAL_AMOUNT" as number(18,2)) as total_amount,
        cast("EHAIL_FEE" as number(18,2)) as ehail_fee,
        cast("CONGESTION_SURCHARGE" as number(18,2)) as congestion_surcharge,
        null as airport_fee,
        cast("PAYMENT_TYPE" as number(10,0)) as payment_type_code,
        cast("TRIP_TYPE" as number(10,0)) as trip_type_code,
        nullif("STORE_AND_FWD_FLAG", '') as store_and_fwd_flag_raw,
        datediff(
            'minute',
            dateadd('microsecond', "LPEP_PICKUP_DATETIME", to_timestamp_ntz('1970-01-01 00:00:00')),
            dateadd('microsecond', "LPEP_DROPOFF_DATETIME", to_timestamp_ntz('1970-01-01 00:00:00'))
        ) as trip_duration_minutes_raw,
        case
            when datediff(
                'minute',
                dateadd('microsecond', "LPEP_PICKUP_DATETIME", to_timestamp_ntz('1970-01-01 00:00:00')),
                dateadd('microsecond', "LPEP_DROPOFF_DATETIME", to_timestamp_ntz('1970-01-01 00:00:00'))
            ) > 0
            then cast("TRIP_DISTANCE" as number(18,6)) /
                 nullif(datediff(
                    'minute',
                    dateadd('microsecond', "LPEP_PICKUP_DATETIME", to_timestamp_ntz('1970-01-01 00:00:00')),
                    dateadd('microsecond', "LPEP_DROPOFF_DATETIME", to_timestamp_ntz('1970-01-01 00:00:00'))
                 ), 0) * 60
            else null
        end as average_speed_mph_raw
    from source
),

enhanced as (
    select
        trip_sk,
        service_type,
        vendor_id,
        pickup_timestamp,
        dropoff_timestamp,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance_miles,
        fare_amount,
        extra_amount,
        improvement_surcharge,
        mta_tax,
        tip_amount,
        tolls_amount,
        total_amount,
        ehail_fee,
        congestion_surcharge,
        airport_fee,
        payment_type_code,
        trip_type_code,
        case when upper(store_and_fwd_flag_raw) = 'Y' then true
             when upper(store_and_fwd_flag_raw) = 'N' then false
             else null end as was_store_and_forward,
        trip_duration_minutes_raw as trip_duration_minutes,
        cast(date_trunc('day', pickup_timestamp) as date) as pickup_date,
        extract(hour from pickup_timestamp) as pickup_hour,
        case when dayofweekiso(pickup_timestamp) in (6,7) then true else false end as is_weekend,
        average_speed_mph_raw as average_speed_mph,
        current_timestamp() as record_loaded_at
    from typed
)

select * from enhanced
