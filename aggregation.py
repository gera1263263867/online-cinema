import json
import logging
import os
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Thread

import psycopg
from fastapi import FastAPI, HTTPException

from common import PG, clickhouse, retry

app = FastAPI()
QUERY = Path("sql/metrics.sql").read_text()
INTERVAL = max(1, int(os.getenv("AGGREGATION_INTERVAL", "120")))


def calculate(day):
    started = time.monotonic()
    logging.info("Aggregation started day=%s", day)
    with psycopg.connect(PG, connect_timeout=5) as connection:
        # Also serializes ClickHouse staging across HTTP calls and scheduled work.
        connection.execute("SELECT pg_advisory_xact_lock(641025)")
        rows = [json.loads(line) for line in clickhouse(QUERY, day=day).splitlines()]
        processed = int(clickhouse(
            "SELECT count() FROM raw_events FINAL WHERE toDate(timestamp) = {day:Date}",
            day=day,
        ))
        stamp = datetime.now(UTC)
        payload = "\n".join(json.dumps({
            **row, "day": day.isoformat(),
            "computed_at": stamp.strftime("%Y-%m-%d %H:%M:%S.%f"),
        }) for row in rows)
        clickhouse("TRUNCATE TABLE metrics_stage")
        clickhouse("INSERT INTO metrics_stage FORMAT JSONEachRow", data=payload)
        clickhouse("ALTER TABLE daily_metrics REPLACE PARTITION {day:Date} FROM metrics_stage",
                   day=day)
        connection.execute("DELETE FROM daily_metrics WHERE day = %s", (day,))
        with connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO daily_metrics (day,metric,dimension,value,computed_at)"
                " VALUES (%s,%s,%s,%s,%s)",
                [(day, row["metric"], row["dimension"], row["value"], stamp)
                 for row in rows],
            )
    logging.info("Aggregation complete day=%s events=%d metrics=%d seconds=%.2f",
                 day, processed, len(rows), time.monotonic() - started)
    return {"day": day, "events": processed, "metrics": len(rows)}


def scheduled():
    while True:
        today = datetime.now(UTC).date()
        # Recalculate recent cohorts as their return days become observable.
        for offset in range(13):
            day = today - timedelta(days=offset)
            try:
                retry(lambda: calculate(day), attempts=3)
            except Exception:
                logging.exception("Scheduled aggregation failed day=%s", day)
                break
        time.sleep(INTERVAL)


@app.on_event("startup")
def start():
    Thread(target=scheduled, daemon=True).start()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/aggregate/{day}")
def aggregate(day: date):
    try:
        return retry(lambda: calculate(day), attempts=3)
    except Exception as error:
        logging.exception("Manual aggregation failed day=%s", day)
        raise HTTPException(503, "Aggregation failed") from error
