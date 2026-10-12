{{ config(materialized='incremental', incremental_strategy='append') }}

-- Крок 1: silver.events. Специфікація: ../../SPEC.md → «Крок 1».

with source as (
    select
        id as event_id,
        type as event_type,
        actor.login as actor_login,
        repo.name as repo_name,
        to_timestamp(created_at) as created_at,
        payload,
        _ingested_at,
        _source_file
    from {{ source('bronze', 'raw_events') }}
    where type in ('PushEvent', 'PullRequestEvent', 'IssuesEvent', 'IssueCommentEvent', 'WatchEvent', 'ForkEvent')
      and public = true
      and id is not null
      and repo.name is not null
      and created_at is not null
    {% if is_incremental() %}
      and _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}
),

dedup as (
    select
        *,
        row_number() over (partition by event_id order by _ingested_at asc, _source_file asc) as rn
    from source
)

select
    event_id,
    event_type,
    actor_login,
    repo_name,
    split(repo_name, '/')[0] as repo_owner,
    created_at,
    payload,
    _ingested_at,
    _source_file
from dedup
where rn = 1
