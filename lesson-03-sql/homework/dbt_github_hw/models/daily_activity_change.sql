-- =====================================================================
-- TASK 4 — daily_activity_change (12 балів). Специфікація: ../../MODELS.md → «daily_activity_change».
-- Зміна кількості подій день-до-дня: LAG(...) OVER (ORDER BY ...).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
WITH agg AS (
    SELECT event_date, count(*) AS events
    FROM {{ ref('stg_events') }}
    GROUP BY event_date
)
SELECT
    event_date,
    events,
    lag(events) over (order by event_date) as prev_day_events,
    events - lag(events) over (order by event_date) as delta_events
FROM agg
ORDER BY event_date
