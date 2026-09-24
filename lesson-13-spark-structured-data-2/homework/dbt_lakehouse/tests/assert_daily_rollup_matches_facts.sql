-- Тест: sum(fact_repo_activity_daily.commits) = count(*) з fact_commit.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
select a.total_commits, b.total_fact_commits
from (select sum(commits) as total_commits from {{ ref('fact_repo_activity_daily') }}) a
cross join (select count(*) as total_fact_commits from {{ ref('fact_commit') }}) b
where a.total_commits != b.total_fact_commits
