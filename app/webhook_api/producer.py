import json
from kafka import KafkaProducer

from app.common.config import REDPANDA_BOOTSTRAP_SERVERS


producer = KafkaProducer(
    bootstrap_servers=REDPANDA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)


def publish_event(topic: str, key: str, value: dict) -> None:
    future = producer.send(topic, key=key, value=value)
    future.get(timeout=10)