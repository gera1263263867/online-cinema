import json
import logging
from pathlib import Path

import psycopg
import requests
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError, KafkaException

from common import BROKERS, PG, REGISTRY, TOPIC, clickhouse, retry


def initialize():
    client = AdminClient({"bootstrap.servers": BROKERS})
    topic = NewTopic(TOPIC, num_partitions=3, replication_factor=3,
                     config={"min.insync.replicas": "2"})
    try:
        client.create_topics([topic], operation_timeout=15)[TOPIC].result(20)
    except KafkaException as error:
        if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise

    subject = f"{REGISTRY}/subjects/{TOPIC}-value"
    schema = Path("schemas/movie-event.avsc").read_text()
    response = requests.post(f"{subject}/versions", json={
        "schemaType": "AVRO", "schema": json.dumps(json.loads(schema))
    }, timeout=10)
    response.raise_for_status()
    version = requests.get(f"{subject}/versions/latest", timeout=10)
    version.raise_for_status()
    logging.info("Schema %s", version.json())
    for statement in Path("sql/clickhouse.sql").read_text().split(";"):
        if statement.strip():
            clickhouse(statement)
    with psycopg.connect(PG, connect_timeout=5) as connection:
        connection.execute(Path("sql/postgres.sql").read_text())


if __name__ == "__main__":
    retry(initialize, attempts=10)
