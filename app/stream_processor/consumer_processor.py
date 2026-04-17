import json
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from app.common.config import (
    REDPANDA_BOOTSTRAP_SERVERS,
    STRIPE_RAW_TOPIC,
    STRIPE_PROCESSED_TOPIC,
    STRIPE_DLQ_TOPIC,
)


consumer = KafkaConsumer(
    STRIPE_RAW_TOPIC,
    bootstrap_servers=REDPANDA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="stripe-processor-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
)

producer = KafkaProducer(
    bootstrap_servers=REDPANDA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
)


def publish(topic: str, key: str, value: dict) -> None:
    future = producer.send(topic, key=key, value=value)
    future.get(timeout=10)


def normalize_event(message: dict) -> dict:
    metadata = message.get("metadata", {})
    event = message.get("event", {})

    event_id = event.get("id")  
    event_type = event.get("type")
    event_created = metadata.get("stripe_event_created")
    ingested_at = metadata.get("ingested_at")
    source_system = metadata.get("source_system", "stripe")

    event_object = event.get("data", {}).get("object", {})

    if event_type == "payment_intent.succeeded":
        return {
            "event_id": event_id,
            "event_type": event_type,
            "source_system": source_system,
            "ingested_at": ingested_at,
            "stripe_event_created": event_created,
            "payment_intent_id": event_object.get("id"),
            "charge_id": None,
            "customer_id": event_object.get("customer"),
            "amount": event_object.get("amount"),
            "currency": event_object.get("currency"),
            "status": event_object.get("status"),
        }

    if event_type == "charge.succeeded":
        return {
            "event_id": event_id,
            "event_type": event_type,
            "source_system": source_system,
            "ingested_at": ingested_at,
            "stripe_event_created": event_created,
            "payment_intent_id": event_object.get("payment_intent"),
            "charge_id": event_object.get("id"),
            "customer_id": event_object.get("customer"),
            "amount": event_object.get("amount"),
            "currency": event_object.get("currency"),
            "status": event_object.get("status"),
        }
    
    if event_type == "payment_intent.created":
        
        return {
            "event_id": event_id,
            "event_type": event_type,
            "source_system": source_system,
            "ingested_at": ingested_at,
            "stripe_event_created": event_created,

            "payment_intent_id": event_object.get("id"),
            "charge_id": None,
            "customer_id": event_object.get("customer"),

            "amount": event_object.get("amount"),
            "currency": event_object.get("currency"),
            "status": event_object.get("status"),

            "capture_method": event_object.get("capture_method"),
            "payment_method_types": event_object.get("payment_method_types"),
        }
    


    raise ValueError(f"Unsupported event type: {event_type}")


def build_dlq_record(message: dict, reason: str) -> dict:
    event = message.get("event", {})
    return {
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "event_id": event.get("id"),
        "event_type": event.get("type"),
        "raw_message": message,
    }


def main():
    print(f"Listening to topic: {STRIPE_RAW_TOPIC}")

    for record in consumer:
        key = record.key
        message = record.value

        try:
            normalized = normalize_event(message)

            publish(
                topic=STRIPE_PROCESSED_TOPIC,
                key=normalized["event_id"],
                value=normalized,
            )

            print(
                json.dumps(
                    {
                        "message": "Published normalized event",
                        "event_id": normalized["event_id"],
                        "event_type": normalized["event_type"],
                        "topic": STRIPE_PROCESSED_TOPIC,
                    }
                )
            )

        except Exception as e:
            dlq_record = build_dlq_record(message, str(e))

            publish(
                topic=STRIPE_DLQ_TOPIC,
                key=dlq_record.get("event_id") or "unknown",
                value=dlq_record,
            )

            print(
                json.dumps(
                    {
                        "message": "Published event to DLQ",
                        "event_id": dlq_record.get("event_id"),
                        "event_type": dlq_record.get("event_type"),
                        "reason": str(e),
                        "topic": STRIPE_DLQ_TOPIC,
                    }
                )
            )


if __name__ == "__main__":
    main()