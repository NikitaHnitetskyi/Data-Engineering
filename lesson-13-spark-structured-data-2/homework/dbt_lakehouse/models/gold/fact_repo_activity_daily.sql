-- Крок 10: gold.fact_repo_activity_daily. Специфікація: ../../SPEC.md → «Крок 10».

with commits_agg as (
    select
        repo_name,
        to_date(pushed_at) as day,
        count(*) as commits,
        count(distinct author_email) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('commits') }}
    group by repo_name, to_date(pushed_at)
),

prs_opened_agg as (
    select
        repo_name,
        to_date(opened_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        count(*) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('pull_requests') }}
    group by repo_name, to_date(opened_at)
),

prs_merged_agg as (
    select
        repo_name,
        to_date(merged_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        count(*) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('pull_requests') }}
    where merged_at is not null
    group by repo_name, to_date(merged_at)
),

issues_opened_agg as (
    select
        repo_name,
        to_date(opened_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        count(*) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('issues') }}
    group by repo_name, to_date(opened_at)
),

issues_closed_agg as (
    select
        repo_name,
        to_date(closed_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        count(*) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('issues') }}
    where closed_at is not null
    group by repo_name, to_date(closed_at)
),

watch_agg as (
    select
        repo_name,
        to_date(created_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        count(*) as stars,
        cast(0 as bigint) as forks
    from {{ ref('events') }}
    where event_type = 'WatchEvent'
    group by repo_name, to_date(created_at)
),

fork_agg as (
    select
        repo_name,
        to_date(created_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        count(*) as forks
    from {{ ref('events') }}
    where event_type = 'ForkEvent'
    group by repo_name, to_date(created_at)
),

unioned as (
    select * from commits_agg
    union all
    select * from prs_opened_agg
    union all
    select * from prs_merged_agg
    union all
    select * from issues_opened_agg
    union all
    select * from issues_closed_agg
    union all
    select * from watch_agg
    union all
    select * from fork_agg
),

daily as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(day, 'yyyyMMdd') as int) as date_id,
        sum(commits) as commits,
        sum(distinct_committers) as distinct_committers,
        sum(prs_opened) as prs_opened,
        sum(prs_merged) as prs_merged,
        sum(issues_opened) as issues_opened,
        sum(issues_closed) as issues_closed,
        sum(stars) as stars,
        sum(forks) as forks
    from unioned
    group by repo_name, day
)

select
    md5(concat_ws('|', repo_id, cast(date_id as string))) as activity_id,
    repo_id,
    date_id,
    commits,
    distinct_committers,
    prs_opened,
    prs_merged,
    issues_opened,
    issues_closed,
    stars,
    forks
from daily
