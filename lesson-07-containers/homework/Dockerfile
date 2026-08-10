# Завдання 1 — Dockerfile для ingestor. Специфікація: SPEC.md → «Завдання 1».
#
# Один образ, одна стадія. Що має бути (деталі й бали — у SPEC.md):
#
#   * базовий образ python:3.12-slim з фіксованим тегом (не latest);
#   * PYTHONUNBUFFERED=1 — інакше логів завантаження не видно;
#   * non-root користувач (напр. appuser, uid 1000), USER — перед ENTRYPOINT;
#   * каталог /cache має належати цьому користувачу — туди монтується volume;
#   * залежності з app/requirements.txt ставляться ОКРЕМИМ шаром ПЕРЕД копіюванням
#     коду, з --no-cache-dir (правка коду не має перебудовувати pip install);
#   * ENTRYPOINT в exec-form запускає app/gh_ingest.py.
#
# Перевірте збірку:  docker build -t gh-ingest:latest .
# Скрипт app/gh_ingest.py — ДАНО, не редагуйте його.

FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1

RUN useradd --uid 1000 --create-home appuser \
    && mkdir -p /cache \
    && chown appuser:appuser /cache

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import psycopg" || exit 1

USER appuser
ENTRYPOINT ["python", "gh_ingest.py"]

