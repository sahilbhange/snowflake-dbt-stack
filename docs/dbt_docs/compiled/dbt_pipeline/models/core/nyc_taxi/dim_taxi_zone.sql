

with distinct_ids as (
    select pickup_location_id as location_id from ANALYTICS.RAW_STAGE.int_tran_nyc_taxi_all where pickup_location_id is not null
    union
    select dropoff_location_id as location_id from ANALYTICS.RAW_STAGE.int_tran_nyc_taxi_all where dropoff_location_id is not null
),

zone_lookup as (
    select
        location_id::number as location_id,
        borough,
        zone,
        service_zone
    from ANALYTICS.RAW.taxi_zone_lookup
),

enriched as (
    select
        d.location_id,
        coalesce(z.borough, 'Unknown') as borough,
        coalesce(z.zone, 'Unknown') as zone_name,
        coalesce(z.service_zone, 'Unknown') as service_zone,
        current_timestamp() as record_loaded_at
    from distinct_ids d
    left join zone_lookup z on d.location_id = z.location_id
)

select
    md5(cast(coalesce(cast(location_id as TEXT), '') as TEXT)) as taxi_zone_sk,
    location_id,
    borough,
    zone_name,
    service_zone,
    record_loaded_at
from enriched