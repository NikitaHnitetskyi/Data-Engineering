-- SPEC 4.4: член driver_key = 'unknown' має існувати РІВНО ОДИН раз, незалежно від кількості
-- прогонів dbt (ідемпотентність синтетичного UNION-рядка при incremental delete+insert).
-- Жоден із 11 даних тестів цього не перевіряє явно.
select count(*) as unknown_rows
from {{ ref('dim_driver') }}
where driver_key = 'unknown'
having count(*) <> 1
