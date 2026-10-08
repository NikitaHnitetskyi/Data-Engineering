-- gold.dim_driver — ЕТАП 2. Grain: водій. SPEC.md, розділ 4.4.
--   * incremental, unique_key='driver_key', delete+insert; за подіями ride_accepted у ref('events')
--   * «останній» рейтинг — за occurred_at; перераховуйте водія з УСІЄЇ його історії, не з батча
--   * плюс член driver_key = 'unknown' (поїздки, скасовані до прийняття)

{{ config(materialized='incremental', unique_key='driver_key', incremental_strategy='delete+insert') }}

with accepted as (
    select
        payload #>> '{driver,id}' as driver_key,
        payload #>> '{driver,vehicle,type}' as vehicle_type,
        payload #>> '{driver,vehicle,medallion}' as medallion,
        (payload #>> '{driver,rating}')::numeric as rating,
        occurred_at,
        _ingested_at
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
),

touched_drivers as (
    {% if is_incremental() %}
    select distinct driver_key
    from accepted
    where _ingested_at > {{ high_watermark() }}
    {% else %}
    select distinct driver_key
    from accepted
    {% endif %}
),

ranked as (
    select
        a.*,
        row_number() over (partition by a.driver_key order by a.occurred_at desc) as rn,
        min(a.occurred_at) over (partition by a.driver_key) as first_seen_at,
        max(a.occurred_at) over (partition by a.driver_key) as last_seen_at,
        max(a._ingested_at) over (partition by a.driver_key) as max_ingested_at
    from accepted a
    join touched_drivers t using (driver_key)
)

select
    driver_key,
    vehicle_type,
    medallion,
    rating as latest_rating,
    first_seen_at,
    last_seen_at,
    max_ingested_at as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from ranked
where rn = 1

union all

select
    'unknown' as driver_key,
    'unknown' as vehicle_type,
    null as medallion,
    null as latest_rating,
    null as first_seen_at,
    null as last_seen_at,
    '-infinity'::timestamptz as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
