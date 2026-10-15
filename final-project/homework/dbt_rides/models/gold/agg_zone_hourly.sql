-- gold.agg_zone_hourly — ЕТАП 2. Grain: (requested_hour, pickup_zone_key). SPEC.md, розділ 4.4.
--   * incremental; після будь-якого інкременту = перерахунок із fact_ride рядок у рядок
--   * подумайте: що перераховувати, коли поїздка змінила зону? Який ключ стабільний?

-- unique_key навмисно = тільки requested_hour (не композитний ключ): delete+insert видаляє з
-- таргета ВСІ рядки, чий requested_hour зустрічається у новій вибірці, і вставляє нову вибірку
-- цілком. pickup_zone_key може змінитись для поїздки (зона посадки), а requested_hour — ніколи
-- (береться з незмінного ride_requested). Тому при зміні зони треба перерахувати ВЕСЬ час по
-- всіх зонах одразу — інакше стара зона лишиться "привидом" зі застарілим лічильником, бо нова
-- вибірка просто не міститиме для неї рядка і delete+insert з композитним ключем її не зачепить.
{{ config(materialized='incremental', unique_key='requested_hour', incremental_strategy='delete+insert') }}

with touched_hours as (
    select distinct requested_hour
    from {{ ref('fact_ride') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
)

select
    f.requested_hour,
    f.pickup_zone_key,
    count(*) as rides_requested,
    count(*) filter (where f.status = 'completed') as rides_completed,
    count(*) filter (where f.status = 'cancelled') as rides_cancelled,
    coalesce(sum(f.total_amount) filter (where f.status = 'completed'), 0)::numeric(12, 2) as gross_revenue,
    coalesce(sum(f.tip_amount) filter (where f.status = 'completed'), 0)::numeric(12, 2) as tips,
    avg(f.wait_seconds) as avg_wait_seconds,
    max(f._ingested_at) as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from {{ ref('fact_ride') }} f
join touched_hours h using (requested_hour)
group by f.requested_hour, f.pickup_zone_key
