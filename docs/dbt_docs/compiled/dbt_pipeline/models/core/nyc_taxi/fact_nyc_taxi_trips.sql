

with source_data as (
    select * from ANALYTICS.RAW_STAGE.int_tran_nyc_taxi_all
    
    where pickup_date >= dateadd('day', -7, current_date)
    
),

dim_vendor as (
    select vendor_id, vendor_sk from ANALYTICS.RAW_CORE.dim_vendor
),

dim_payment_type as (
    select payment_type_code, payment_type_sk from ANALYTICS.RAW_CORE.dim_payment_type
),

dim_rate_code as (
    select rate_code_id, rate_code_sk from ANALYTICS.RAW_CORE.dim_rate_code
),

dim_pickup_zone as (
    select location_id, taxi_zone_sk as pickup_zone_sk from ANALYTICS.RAW_CORE.dim_taxi_zone
),

dim_dropoff_zone as (
    select location_id, taxi_zone_sk as dropoff_zone_sk from ANALYTICS.RAW_CORE.dim_taxi_zone
),

final as (
    select
        sd.trip_sk,
        sd.service_type,
        sd.pickup_timestamp,
        sd.dropoff_timestamp,
        sd.pickup_date,
        sd.pickup_hour,
        sd.pickup_location_id,
        pz.pickup_zone_sk,
        sd.dropoff_location_id,
        dz.dropoff_zone_sk,
        sd.passenger_count,
        sd.trip_distance_miles,
        sd.trip_duration_minutes,
        sd.fare_amount,
        sd.tip_amount,
        sd.tolls_amount,
        sd.extra_amount,
        sd.improvement_surcharge,
        sd.mta_tax,
        sd.congestion_surcharge,
        sd.airport_fee,
        sd.total_amount,
        sd.payment_type_code,
        pt.payment_type_sk,
        sd.rate_code_id,
        rc.rate_code_sk,
        sd.vendor_id,
        v.vendor_sk,
        sd.trip_type_code,
        sd.average_speed_mph,
        sd.was_store_and_forward,
        sd.ehail_fee,
        sd.driver_pay,
        sd.hvfhs_license_num,
        sd.dispatching_base_num,
        sd.originating_base_num,
        sd.shared_request_flag,
        sd.shared_match_flag,
        sd.access_a_ride_flag,
        sd.wav_request_flag,
        sd.wav_match_flag,
        sd.record_loaded_at,
        current_timestamp() as dw_load_ts
    from source_data sd
    left join dim_vendor v on sd.vendor_id = v.vendor_id
    left join dim_payment_type pt on sd.payment_type_code = pt.payment_type_code
    left join dim_rate_code rc on sd.rate_code_id = rc.rate_code_id
    left join dim_pickup_zone pz on sd.pickup_location_id = pz.location_id
    left join dim_dropoff_zone dz on sd.dropoff_location_id = dz.location_id
)

select * from final