-- Крок 5: gold.dim_repo. Специфікація: ../../SPEC.md → «Крок 5».

select
    md5(repo_name) as repo_id,
    repo_name,
    split(repo_name, '/')[0] as repo_owner,
    min(created_at) as first_seen_at,
    max(created_at) as last_seen_at,
    count(*) as event_count,
    max(event_type = 'ForkEvent') as is_forked
from {{ ref('events') }}
group by repo_name
