{% snapshot snap_dim_taxi_zone %}

{%- set snapshot_config = {
    'target_database': target.database,
    'target_schema': 'CORE',
    'unique_key': 'taxi_zone_sk',
    'strategy': 'check',
    'check_cols': ['borough', 'zone_name', 'service_zone']
} -%}

{{ config(**snapshot_config) }}

select *
from {{ ref('dim_taxi_zone') }}

{% endsnapshot %}
