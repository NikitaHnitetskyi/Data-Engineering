-- Крок 2: silver.commits. Специфікація: ../../SPEC.md → «Крок 2».

with parsed as (
    select
        event_id,
        repo_name,
        actor_login as pushed_by,
        created_at as pushed_at,
        from_json(payload, '{{ var("push_schema") }}') as push
    from {{ ref('events') }}
    where event_type = 'PushEvent'
),

exploded as (
    select
        event_id,
        repo_name,
        pushed_by,
        pushed_at,
        regexp_replace(push.ref, '^refs/heads/', '') as branch,
        commit.sha as commit_sha,
        commit.message as message,
        commit.`distinct` as is_distinct,
        commit.author.name as author_name,
        commit.author.email as author_email
    from parsed
    lateral view explode(push.commits) t as commit
),

flagged as (
    select
        *,
        message like 'Merge %' as is_merge_commit,
        split(message, '\n')[0] as message_subject,
        length(message) as message_length,
        row_number() over (partition by commit_sha order by pushed_at asc, event_id asc) as rn
    from exploded
)

select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    is_merge_commit,
    message_subject,
    message_length
from flagged
where rn = 1
