"""GHArchiveSensor — ВАШ custom sensor. Специфікація: ../../SPEC.md → «Sensor».

Сенсор чекає, поки годинний файл GitHub Archive за logical date стане доступним,
і лише тоді пропускає DAG далі.
"""

from __future__ import annotations

import logging
import urllib.error
import urllib.request

from airflow.sensors.base import BaseSensorOperator

log = logging.getLogger("gh_sensor")


class GHArchiveSensor(BaseSensorOperator):
    def __init__(self, hour: int = 14, **kwargs) -> None:
        super().__init__(**kwargs)
        self.hour = hour

    def poke(self, context) -> bool:
        ds = context["ds"]
        url = f"https://data.gharchive.org/{ds}-{self.hour}.json.gz"
        req = urllib.request.Request(
              url,
              method="HEAD",
              headers={"User-Agent": "Mozilla/5.0 (compatible; airflow-sensor/1.0)"},
)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                available = resp.status == 200
        except Exception as exc:
            log.info("HEAD %s failed: %s", url, exc)
            available = False

        log.info("%s is %s", url, "available" if available else "not available yet")
        return available