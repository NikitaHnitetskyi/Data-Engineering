"""github_archive_daily — ВАШ DAG. Специфікація: ../SPEC.md → «DAG».

Готові ETL-цеглинки вже є — імпортуйте і викликайте їх у задачах (не переписуйте):

    from include.gh_etl import download, validate, load_to_duckdb, summarize
    from gh_sensor import GHArchiveSensor   # ваш custom sensor із plugins/

Що треба зібрати (деталі й бали — у SPEC.md):
  * DAG `github_archive_daily`, розклад «щодня о 06:00 UTC», catchup=False;
  * усі задачі працюють із logical date {{ ds }}, а не datetime.now() — це дає
    ідемпотентність і коректний backfill;
  * граф:
        check_availability -> download_archive -> validate_file
            -> load_to_duckdb -> notify_completion
  * download_archive кладе шлях у XCom; validate_file і load_to_duckdb беруть його з XCom;
  * шляхи (дано):
        DB_PATH     = "/opt/airflow/data/github_analytics.duckdb"
        LANDING_DIR = "/opt/airflow/data/landing"

Перевірка: `airflow dags test github_archive_daily 2024-01-14` має пройти всі задачі;
наскрізно — `./verify.sh` із кореня homework/.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from include.gh_etl import download, load_to_duckdb, summarize, validate
from gh_sensor import GHArchiveSensor  

DB_PATH = "/opt/airflow/data/github_analytics.duckdb"
LANDING_DIR = "/opt/airflow/data/landing"


def download_archive(ds, **_):
    path = download(ds, LANDING_DIR)
    print(f"download_archive: завантажено {path}")
    return path  


def validate_file(ti, **_):
    path = ti.xcom_pull(task_ids="download_archive")
    validate(path)
    print(f"validate_file: OK {path}")


def load_to_duckdb_task(ti, ds, **_):
    path = ti.xcom_pull(task_ids="download_archive")
    n = load_to_duckdb(path, ds, DB_PATH)
    print(f"load_to_duckdb: завантажено {n} рядків за {ds}")


def notify_completion(ds, **_):
    stats = summarize(ds, DB_PATH)
    print(f"notify_completion: {stats['rows']} подій, {stats['event_types']} типів за {ds}")


with DAG(
    dag_id="github_archive_daily",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["github", "archive", "duckdb"],
) as dag:
    check_availability = GHArchiveSensor(
        task_id="check_availability",
        hour=14,
        timeout=600,
        poke_interval=60,
        mode="reschedule",
    )

    t_download = PythonOperator(task_id="download_archive", python_callable=download_archive)
    t_validate = PythonOperator(task_id="validate_file", python_callable=validate_file)
    t_load = PythonOperator(task_id="load_to_duckdb", python_callable=load_to_duckdb_task)
    t_notify = PythonOperator(task_id="notify_completion", python_callable=notify_completion)

    check_availability >> t_download >> t_validate >> t_load >> t_notify
