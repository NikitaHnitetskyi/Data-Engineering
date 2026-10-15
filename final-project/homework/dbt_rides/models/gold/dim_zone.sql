-- gold.dim_zone — ЕТАП 2. Grain: зона. SPEC.md, розділ 4.4.
--   * table; джерело — ref('seed_taxi_zone'); порожні значення -> 'Unknown'; плюс член zone_key = -1

{{ config(materialized='table') }}

select
    location_id as zone_key,
    coalesce(nullif(borough, ''), 'Unknown') as borough,
    coalesce(nullif(zone_name, ''), 'Unknown') as zone_name,
    coalesce(nullif(service_zone, ''), 'Unknown') as service_zone
from {{ ref('seed_taxi_zone') }}

union all

select -1, 'Unknown', 'Unknown', 'Unknown'
