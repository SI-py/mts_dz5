import json
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
DB = os.getenv("DATABASE_URL", "postgresql://fraud:fraud@postgres:5432/fraud")
FEATURES = ["amount", "us_state", "merch", "cat_id"]
BUCKETS = [i / 20 for i in range(21)]


def now():
    return datetime.now(timezone.utc).isoformat()


def producer():
    return Producer({"bootstrap.servers": BROKERS, "enable.idempotence": True})


def consumer(group, topic):
    client = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    client.subscribe([topic])
    return client


def send(client, topic, value):
    errors = []
    client.produce(
        topic, key=value["transaction_id"], value=json.dumps(value),
        on_delivery=lambda error, message: errors.append(error) if error else None,
    )
    if client.flush(10) or errors:
        raise RuntimeError(f"Kafka delivery failed: {errors}")


def messages(client):
    while True:
        message = client.poll(1)
        if message is None:
            yield None
        elif message.error():
            raise RuntimeError(message.error())
        else:
            yield message
