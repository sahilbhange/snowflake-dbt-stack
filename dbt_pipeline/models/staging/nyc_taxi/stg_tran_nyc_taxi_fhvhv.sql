{{ config(
    materialized='view',
    schema='STAGE',
    alias='stg_tran_nyc_taxi_fhvhv',
    tags=['layer:stage', 'domain:nyc_taxi']
) }}

with source as (
    select * from {{ source('nyc_taxi', 'raw_tran_nyc_taxi_fhvhv') }}
),

typed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "'fhvhv'",
            '"HVFHS_LICENSE_NUM"',
            '"DISPATCHING_BASE_NUM"',
            '"PICKUP_DATETIME"',
            '"DROPOFF_DATETIME"',
            "coalesce(\"PULOCATIONID\"::string, '')",
            "coalesce(\"DOLOCATIONID\"::string, '')",
            "coalesce(\"TRIP_MILES\"::string, '')",
            "coalesce(\"BASE_PASSENGER_FARE\"::string, '')"
        ]) }} as trip_sk,
        'fhvhv' as service_type,
        null as vendor_id,
        dateadd('microsecond', "PICKUP_DATETIME", to_timestamp_ntz('1970-01-01 00:00:00')) as pickup_timestamp,
        dateadd('microsecond', "DROPOFF_DATETIME", to_timestamp_ntz('1970-01-01 00:00:00')) as dropoff_timestamp,
        null as rate_code_id,
        cast("PULOCATIONID" as number(10,0)) as pickup_location_id,
        cast("DOLOCATIONID" as number(10,0)) as dropoff_location_id,
        null as passenger_count,
        cast("TRIP_MILES" as number(18,6)) as trip_distance_miles,
        cast("BASE_PASSENGER_FARE" as number(18,2)) as fare_amount,
        cast("BCF" as number(18,2)) as improvement_surcharge,
        cast("SALES_TAX" as number(18,2)) as mta_tax,
        null as extra_amount,
        cast("TIPS" as number(18,2)) as tip_amount,
        cast("TOLLS" as number(18,2)) as tolls_amount,
        coalesce(cast("BASE_PASSENGER_FARE" as number(18,2)), 0) +
        coalesce(cast("BCF" as number(18,2)), 0) +
        coalesce(cast("SALES_TAX" as number(18,2)), 0) +
        coalesce(cast("TIPS" as number(18,2)), 0) +
        coalesce(cast("TOLLS" as number(18,2)), 0) +
        coalesce(cast("CONGESTION_SURCHARGE" as number(18,2)), 0) +
        coalesce(cast("AIRPORT_FEE" as number(18,2)), 0) as total_amount,
        cast("CONGESTION_SURCHARGE" as number(18,2)) as congestion_surcharge,
        cast("AIRPORT_FEE" as number(18,2)) as airport_fee,
        null as payment_type_code,
        null as trip_type_code,
        null as store_and_fwd_flag_raw,
        cast("TRIP_TIME" as number(18,0)) as trip_time_seconds,
        case
            when cast("TRIP_TIME" as number(18,0)) > 0
            then cast("TRIP_MILES" as number(18,6)) /
                 nullif(cast("TRIP_TIME" as number(18,0)) / 3600, 0)
            else null
        end as average_speed_mph_raw,
        hvfhs_license_num,
        dispatching_base_num,
        originating_base_num,
        cast("DRIVER_PAY" as number(18,2)) as driver_pay,
        nullif("SHARED_REQUEST_FLAG", '') as shared_request_flag,
        nullif("SHARED_MATCH_FLAG", '') as shared_match_flag,
        nullif("ACCESS_A_RIDE_FLAG", '') as access_a_ride_flag,
        nullif("WAV_REQUEST_FLAG", '') as wav_request_flag,
        nullif("WAV_MATCH_FLAG", '') as wav_match_flag
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
        congestion_surcharge,
        airport_fee,
        payment_type_code,
        trip_type_code,
        null as was_store_and_forward,
        coalesce(trip_time_seconds, datediff('second', pickup_timestamp, dropoff_timestamp)) / 60.0 as trip_duration_minutes,
        cast(date_trunc('day', pickup_timestamp) as date) as pickup_date,
        extract(hour from pickup_timestamp) as pickup_hour,
        case when dayofweekiso(pickup_timestamp) in (6,7) then true else false end as is_weekend,
        average_speed_mph_raw as average_speed_mph,
        hvfhs_license_num,
        dispatching_base_num,
        originating_base_num,
        driver_pay,
        shared_request_flag,
        shared_match_flag,
        access_a_ride_flag,
        wav_request_flag,
        wav_match_flag,
        current_timestamp() as record_loaded_at
    from typed
)

select * from enhanced
