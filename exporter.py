import json
import logging
import os
import time
from datetime import UTC, date, datetime, timedelta
from threading import Thread

import boto3
import psycopg
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from psycopg.rows import dict_row

from common import PG

app = FastAPI()
BUCKET = "movie-analytics"
INTERVAL = max(1, int(os.getenv("EXPORT_INTERVAL", "300")))


def export(day):
    with psycopg.connect(PG, row_factory=dict_row, connect_timeout=5) as connection:
        rows = connection.execute(
            "SELECT * FROM daily_metrics WHERE day = %s ORDER BY metric, dimension",
            (day,),
        ).fetchall()
    if not rows:
        raise LookupError(f"No metrics for {day}")

    s3 = boto3.client(
        "s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=15,
                      s3={"addressing_style": "path"}),
    )
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
        try:
            s3.create_bucket(Bucket=BUCKET)
        except s3.exceptions.BucketAlreadyOwnedByYou:
            pass

    key = f"daily/{day}/aggregates.json"
    for row in rows:
        row["day"] = row["day"].isoformat()
        row["computed_at"] = row["computed_at"].isoformat()
    s3.put_object(Bucket=BUCKET, Key=key, ContentType="application/json",
                  Body=json.dumps(rows, ensure_ascii=False).encode())
    logging.info("Exported day=%s rows=%d key=%s", day, len(rows), key)
    return {"uri": f"s3://{BUCKET}/{key}", "metrics": len(rows)}


def scheduled():
    while True:
        today = datetime.now(UTC).date()
        # Repeat recent exports to persist backfill and finalized D7 retention.
        for offset in range(1, 9):
            day = today - timedelta(days=offset)
            try:
                export(day)
            except Exception:
                logging.exception("Export failed day=%s. Will retry next cycle", day)
        time.sleep(INTERVAL)


@app.on_event("startup")
def start():
    Thread(target=scheduled, daemon=True).start()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/export/{day}")
def export_date(day: date):
    try:
        return export(day)
    except LookupError as error:
        raise HTTPException(404, str(error)) from error
    except Exception as error:
        logging.exception("Manual export failed day=%s", day)
        raise HTTPException(503, "Export failed") from error
