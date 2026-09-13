-- Крок 4: silver.issues. Специфікація: ../../SPEC.md → «Крок 4».

with parsed as (
    select
        event_id,
        event_type,
        repo_name,
        created_at as event_at,
        from_json(payload, '{{ var("issue_schema") }}') as iss
    from {{ ref('events') }}
    where event_type in ('IssuesEvent', 'IssueCommentEvent')
),

comment_counts as (
    select
        repo_name,
        iss.issue.number as issue_number,
        count(*) as comment_events_seen
    from parsed
    where event_type = 'IssueCommentEvent'
    group by repo_name, iss.issue.number
),

ranked as (
    select
        repo_name,
        iss.issue.number as issue_number,
        iss.issue.title as title,
        iss.issue.user.login as author_login,
        iss.issue.state as state,
        to_timestamp(iss.issue.created_at) as opened_at,
        to_timestamp(iss.issue.closed_at) as closed_at,
        iss.issue.comments as comments,
        iss.issue.labels.name as label_names,
        event_at as last_event_at,
        row_number() over (
            partition by repo_name, iss.issue.number
            order by event_at desc, event_id desc
        ) as rn
    from parsed
)

select
    r.repo_name,
    r.issue_number,
    r.title,
    r.author_login,
    r.state,
    r.opened_at,
    r.closed_at,
    r.comments,
    r.label_names,
    coalesce(c.comment_events_seen, 0) as comment_events_seen,
    r.last_event_at,
    case
        when r.closed_at is not null
            then (unix_timestamp(r.closed_at) - unix_timestamp(r.opened_at)) / 3600.0
    end as hours_to_close
from ranked r
left join comment_counts c
    on r.repo_name = c.repo_name and r.issue_number = c.issue_number
where r.rn = 1
