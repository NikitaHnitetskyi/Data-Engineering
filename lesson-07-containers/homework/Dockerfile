# syntax=docker/dockerfile:1
#
# Завдання 1 — Dockerfile для ingestor. Специфікація: ../SPEC.md → «Dockerfile».
#
# Що має бути (див. SPEC.md для деталей і балів):
#   * multi-stage build: окремий builder-стейдж ставить залежності, runtime-стейдж
#     копіює тільки готовий venv + код (без pip/uv-кешу у фінальному образі);
#   * залежності з app/requirements.txt (psycopg);
#   * НЕ запускати від root — створіть і використайте non-root користувача;
#   * точка монтування кешу (/cache) має належати цьому користувачу;
#   * ENTRYPOINT запускає app/gh_ingest.py.
#
# Перевірте збірку: docker build -t gh-ingest:latest .
# Скрипт app/gh_ingest.py — ДАНО, не редагуйте його.

# TODO: реалізуйте multi-stage Dockerfile тут.
