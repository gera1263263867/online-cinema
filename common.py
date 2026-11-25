import logging
import os
import time

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
CH = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://registry:8081")
BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092,kafka2:9092,kafka3:9092")
TOPIC = "movie-events"
PG = os.getenv("POSTGRES_DSN", "postgresql://cinema:cinema_local@postgres:5432/cinema")


def retry(operation, attempts=5):
    for attempt in range(attempts):
        try:
            return operation()
        except Exception:
            if attempt + 1 == attempts:
                raise
            logging.warning("Attempt %s/%s failed", attempt + 1, attempts, exc_info=True)
            time.sleep(min(2 ** attempt, 15))


def clickhouse(sql, *, data=None, **parameters):
    response = requests.post(
        CH,
        params={"database": "cinema", "query": sql, **{
            f"param_{key}": value for key, value in parameters.items()
        }},
        data=data,
        auth=("cinema", "cinema_local"),
        timeout=60,
    )
    if not response.ok:
        raise RuntimeError(f"ClickHouse {response.status_code}: {response.text}")
    return response.text
