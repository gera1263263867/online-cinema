import json
import time
from datetime import UTC, datetime
from uuid import uuid4

import requests

from common import clickhouse

base = "http://producer:8000"
identifier = str(uuid4())
timestamp = datetime.now(UTC)
event = {
    "event_id": identifier,
    "user_id": f"integration-{identifier}",
    "movie_id": "integration-movie",
    "event_type": "VIEW_FINISHED",
    "timestamp": timestamp.isoformat(timespec="milliseconds"),
    "device_type": "TABLET",
    "session_id": identifier,
    "progress_seconds": 742,
}
rejection = requests.post(f"{base}/events", json={**event, "event_type": "INVALID"},
                          timeout=10)
assert rejection.status_code == 422, rejection.text
response = requests.post(f"{base}/events", json=event, timeout=50)
assert response.status_code == 201, response.text
assert response.json() == {"event_id": identifier}, response.text
deadline = time.monotonic() + 60
while time.monotonic() < deadline:
    data = clickhouse(
        "SELECT * FROM raw_events FINAL WHERE event_id = {id:UUID} FORMAT JSONEachRow",
        id=identifier,
    )
    if data:
        row = json.loads(data)
        row["timestamp"] = datetime.fromisoformat(row["timestamp"]).replace(
            tzinfo=UTC).isoformat(timespec="milliseconds")
        assert row == event, (row, event)
        print(f"Kafka → ClickHouse: verified all fields for {identifier}")
        break
    time.sleep(0.5)
else:
    raise AssertionError(f"Event {identifier} was not ingested within 60 seconds")
