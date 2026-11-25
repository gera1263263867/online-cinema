import io
import json
import logging
import random
import struct
from threading import Lock
import time
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from uuid import UUID, uuid4

import fastavro
import requests
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from common import BROKERS, REGISTRY, TOPIC, retry

SCHEMA = fastavro.parse_schema(json.loads(Path("schemas/movie-event.avsc").read_text()))
app = FastAPI()


class EventType(StrEnum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class Device(StrEnum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID = Field(default_factory=uuid4)
    user_id: str = Field(min_length=1)
    movie_id: str = Field(min_length=1)
    event_type: EventType
    timestamp: AwareDatetime
    device_type: Device
    session_id: str = Field(min_length=1)
    progress_seconds: int = Field(ge=0, le=2147483647, strict=True)


def publisher():
    subject = f"{REGISTRY}/subjects/{TOPIC}-value"
    schema = Path("schemas/movie-event.avsc").read_text()
    response = requests.post(subject, json={"schemaType": "AVRO", "schema": schema}, timeout=5)
    response.raise_for_status()
    schema_id = response.json()["id"]
    publication_lock = Lock()
    producer = Producer({
        "bootstrap.servers": BROKERS,
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": 5000,
        "partitioner": "murmur2_random",
    })

    def publish(event):
        record = event.model_dump()
        record["event_id"] = str(event.event_id)
        fastavro.validation.validate(record, SCHEMA)
        wire = io.BytesIO()
        wire.write(struct.pack(">BI", 0, schema_id))
        fastavro.schemaless_writer(wire, SCHEMA, record)
        payload = wire.getvalue()

        def send():
            from threading import Event as Signal

            delivered = Signal()
            errors = []

            def confirm(error, _message):
                errors.append(error)
                delivered.set()

            producer.produce(TOPIC, key=event.user_id.encode(), value=payload,
                             on_delivery=confirm)
            deadline = time.monotonic() + 7
            while not delivered.is_set() and time.monotonic() < deadline:
                producer.poll(0.15)
            if not delivered.is_set() or errors[0] is not None:
                raise RuntimeError(f"Kafka delivery failed: {errors}")

        with publication_lock:
            retry(send, attempts=4)
        logging.info("Published event_id=%s event_type=%s timestamp=%s",
                     event.event_id, event.event_type, event.timestamp.isoformat())
        return str(event.event_id)

    return publish


@app.on_event("startup")
def startup():
    app.state.publish = retry(publisher)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events", status_code=201)
def create_event(event: Event):
    try:
        return {"event_id": app.state.publish(event)}
    except Exception as error:
        logging.exception("Publication failed for %s", event.event_id)
        raise HTTPException(503, "Kafka publication failed") from error


def generate():
    publish = retry(publisher)
    rng = random.Random(83)
    prefix = uuid4().hex[:8]

    def session(user, start):
        Path("/tmp/generator-heartbeat").touch()
        movie = f"movie-{rng.randint(1, 12):02}"
        device = rng.choice(list(Device))
        session_id = uuid4().hex
        steps = [(0, "SEARCHED", 0), (2, "VIEW_STARTED", 0)]
        if rng.random() < 0.7:
            steps += [(122, "VIEW_PAUSED", 120), (158, "VIEW_RESUMED", 120)]
            if rng.random() < 0.8:
                steps += [(938, "VIEW_FINISHED", 900), (942, "LIKED", 0)]
        for offset, kind, progress in steps:
            publish(Event(user_id=user, movie_id=movie, device_type=device,
                          session_id=session_id, timestamp=start + timedelta(seconds=offset),
                          event_type=kind, progress_seconds=progress))

    midnight = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    for i in range(72):
        age = 1 + i % 12
        user = f"demo-{prefix}-{i}"
        first = midnight - timedelta(days=age) + timedelta(hours=17)
        session(user, first)
        for lag in range(1, min(8, age)):
            if rng.random() < 0.65 / lag ** 0.4:
                session(user, first + timedelta(days=lag))
    while True:
        session(f"demo-{prefix}-{rng.randrange(72)}", datetime.now(UTC) - timedelta(minutes=20))
        time.sleep(10)


if __name__ == "__main__":
    generate()
