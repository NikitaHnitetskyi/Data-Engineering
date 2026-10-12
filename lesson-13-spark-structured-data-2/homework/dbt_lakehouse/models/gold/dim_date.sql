-- Крок 7: gold.dim_date. Специфікація: ../../SPEC.md → «Крок 7».

with dates as (
    select to_date(pushed_at) as d from {{ ref('commits') }}
    union all
    select to_date(opened_at) from {{ ref('pull_requests') }}
    union all
    select to_date(merged_at) from {{ ref('pull_requests') }} where merged_at is not null
    union all
    select to_date(opened_at) from {{ ref('issues') }}
    union all
    select to_date(closed_at) from {{ ref('issues') }} where closed_at is not null
),

bounds as (
    select min(d) as min_date, max(d) as max_date from dates
)

select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_id,
    date_day,
    dayofweek(date_day) as day_of_week,
    dayofweek(date_day) in (1, 7) as is_weekend,
    weekofyear(date_day) as iso_week,
    year(date_day) as year
from bounds
lateral view explode(sequence(min_date, max_date, interval 1 day)) t as date_day
