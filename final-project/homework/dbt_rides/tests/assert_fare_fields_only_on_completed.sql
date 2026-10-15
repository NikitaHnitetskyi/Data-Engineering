-- Тарифні поля (fare_amount/total_amount) заповнені РІВНО тоді, коли поїздка завершена.
-- Ловить витік/загублення даних через неправильний FILTER (where event_type = 'ride_completed')
-- у pivot-агрегації silver.rides.
select ride_id, status, fare_amount, total_amount
from {{ ref('rides') }}
where (status = 'completed') is distinct from (fare_amount is not null and total_amount is not null)
