
    
    

select
    location_id as unique_field,
    count(*) as n_records

from ANALYTICS_DEV.RAW.taxi_zone_lookup
where location_id is not null
group by location_id
having count(*) > 1


