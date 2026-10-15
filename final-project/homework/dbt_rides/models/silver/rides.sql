-- silver.rides — ЕТАП 2. Grain: одна поїздка (ride_id). SPEC.md, розділ 4.3.
--   * incremental, unique_key='ride_id', delete+insert
--   * перебудовуйте поїздки, яких торкнулися НОВІ події, з УСІЄЇ їхньої історії в ref('events')
--   * поїздка існує, коли прийшла її ride_requested; порядок прибуття подій байдужий
--   * status, фактична зона (перекриває заявлену), wait_seconds, trip_seconds, суми з ride_completed
--   * _ingested_at = максимум _ingested_at усіх подій поїздки

{{ config(materialized='incremental', unique_key='ride_id', incremental_strategy='delete+insert') }}

with touched_rides as (
    {% if is_incremental() %}
    select distinct ride_id
    from {{ ref('events') }}
    where _ingested_at > {{ high_watermark() }}
    {% else %}
    select distinct ride_id
    from {{ ref('events') }}
    {% endif %}
),

ride_events as (
    select e.*
    from {{ ref('events') }} e
    join touched_rides t using (ride_id)
),

agg as (
    select
        ride_id,

        max(payload #>> '{rider,id}') filter (where event_type = 'ride_requested') as rider_id,
        max(payload #>> '{rider,platform}') filter (where event_type = 'ride_requested') as rider_platform,
        max(payload #>> '{rider,app_version}') filter (where event_type = 'ride_requested') as app_version,
        max(payload ->> 'requested_vehicle') filter (where event_type = 'ride_requested') as requested_vehicle,
        max((payload ->> 'surge_estimate')::numeric) filter (where event_type = 'ride_requested') as surge_estimate,
        max((payload #>> '{pickup,zone_id}')::int) filter (where event_type = 'ride_requested') as req_pu,
        max((payload #>> '{dropoff,zone_id}')::int) filter (where event_type = 'ride_requested') as req_do,
        max((payload #>> '{pickup,zone_id}')::int) filter (where event_type = 'ride_started') as act_pu,
        max((payload #>> '{dropoff,zone_id}')::int) filter (where event_type = 'ride_completed') as act_do,

        coalesce(
            max(payload #>> '{driver,id}') filter (where event_type = 'ride_completed'),
            max(payload #>> '{driver,id}') filter (where event_type = 'ride_started'),
            max(payload #>> '{driver,id}') filter (where event_type = 'ride_accepted')
        ) as driver_id,

        max(occurred_at) filter (where event_type = 'ride_requested') as requested_at,
        max(occurred_at) filter (where event_type = 'ride_accepted') as accepted_at,
        max(occurred_at) filter (where event_type = 'ride_started') as started_at,
        max(occurred_at) filter (where event_type = 'ride_completed') as completed_at,
        max(occurred_at) filter (where event_type = 'ride_cancelled') as cancelled_at,
        max(occurred_at) filter (where event_type = 'payment_captured') as paid_at,

        max((payload ->> 'distance_km')::numeric) filter (where event_type = 'ride_completed') as distance_km,
        max((payload #>> '{fare,surge_multiplier}')::numeric) filter (where event_type = 'ride_completed') as surge_multiplier,
        max((payload #>> '{fare,amount}')::numeric) filter (where event_type = 'ride_completed') as fare_amount,
        max((payload #>> '{fare,tolls}')::numeric) filter (where event_type = 'ride_completed') as tolls_amount,
        max((payload #>> '{fare,tip}')::numeric) filter (where event_type = 'ride_completed') as tip_amount,
        max((payload #>> '{fare,total}')::numeric) filter (where event_type = 'ride_completed') as total_amount,
        max(payload #>> '{fare,currency}') filter (where event_type = 'ride_completed') as currency,

        max(payload ->> 'cancelled_by') filter (where event_type = 'ride_cancelled') as cancelled_by,
        max(payload ->> 'reason') filter (where event_type = 'ride_cancelled') as cancel_reason,
        max(payload ->> 'stage') filter (where event_type = 'ride_cancelled') as cancel_stage,

        max(payload #>> '{payment,method}') filter (where event_type = 'payment_captured') as payment_method,
        max(payload #>> '{payment,psp_reference}') filter (where event_type = 'payment_captured') as psp_reference,
        max((payload #>> '{payment,amount}')::numeric) filter (where event_type = 'payment_captured') as payment_amount,

        max(_ingested_at) as _ingested_at
    from ride_events
    group by ride_id
)

select
    ride_id,
    rider_id,
    rider_platform,
    app_version,
    requested_vehicle,
    driver_id,
    case
        when completed_at is not null then 'completed'
        when cancelled_at is not null then 'cancelled'
        when started_at is not null then 'in_progress'
        when accepted_at is not null then 'accepted'
        else 'requested'
    end as status,
    requested_at,
    accepted_at,
    started_at,
    completed_at,
    cancelled_at,
    paid_at,
    coalesce(act_pu, req_pu) as pickup_zone_id,
    coalesce(act_do, req_do) as dropoff_zone_id,
    extract(epoch from (accepted_at - requested_at))::int as wait_seconds,
    extract(epoch from (completed_at - started_at))::int as trip_seconds,
    surge_estimate,
    distance_km,
    surge_multiplier,
    fare_amount::numeric(10, 2) as fare_amount,
    tolls_amount::numeric(10, 2) as tolls_amount,
    tip_amount::numeric(10, 2) as tip_amount,
    total_amount::numeric(10, 2) as total_amount,
    currency,
    cancelled_by,
    cancel_reason,
    cancel_stage,
    payment_method,
    psp_reference,
    payment_amount::numeric(10, 2) as payment_amount,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from agg
where requested_at is not null
