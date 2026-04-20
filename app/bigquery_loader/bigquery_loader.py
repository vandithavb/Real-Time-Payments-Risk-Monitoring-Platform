import json
import os
import hashlib
import time
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer
from google.cloud import bigquery


# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================
if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
    logger.warning("⚠️ GOOGLE_APPLICATION_CREDENTIALS not set!")
    logger.warning(
        "Set via: export GOOGLE_APPLICATION_CREDENTIALS=/full/path/to/service-account.json"
    )

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
PROCESSED_TOPIC = os.getenv("PROCESSED_TOPIC", "stripe.processed")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "stripe.dlq")

BATCH_SIZE = 100
BATCH_TIMEOUT = 10
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "v1")

client = bigquery.Client()
project_id = client.project

PROCESSED_TABLE = f"{project_id}.stripe_dw.stripe_processed"
DLQ_TABLE = f"{project_id}.stripe_dw.stripe_dlq"

logger.info("BigQuery Loader starting...")
logger.info("Project: %s", project_id)
logger.info("Processed table: %s", PROCESSED_TABLE)
logger.info("DLQ table: %s", DLQ_TABLE)


# ============================================================================
# ALLOWED FIELDS (MATCH BIGQUERY TABLE SCHEMA)
# ============================================================================
PROCESSED_FIELDS = {
    "event_id", "event_type", "trace_id", "source_system", "source_topic",
    "api_version", "ingested_at", "event_created_ts", "processed_at",
    "pipeline_version", "object_id", "object_type", "payment_intent_id",
    "charge_id", "refund_id", "dispute_id", "customer_id", "amount",
    "amount_refunded", "amount_captured", "currency", "status",
    "payment_method", "payment_method_type", "payment_method_types",
    "card_brand", "card_country", "card_funding", "card_last4",
    "customer_email", "billing_country", "shipping_country", "risk_level",
    "risk_score", "outcome_network_status", "outcome_type",
    "outcome_seller_message", "failure_code", "failure_message",
    "decline_code", "disputed", "refunded", "description", "reason",
    "receipt_url", "receipt_number", "capture_method",
    "is_charge_refundable", "raw_event"
}

DLQ_FIELDS = {
    "failed_at", "reason", "event_id", "event_type", "trace_id",
    "source_system", "source_topic", "pipeline_version", "raw_message"
}


# ============================================================================
# HELPERS
# ============================================================================
def utc_now():
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(ts_str):
    """Convert ISO timestamp string to ISO string BigQuery can ingest."""
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, TypeError) as e:
        logger.warning("Failed to parse timestamp '%s': %s", ts_str, e)
        return None


def generate_row_id(event_id=None, trace_id=None, raw_json=None, prefix="row"):
    """
    Generate deterministic row ID for BigQuery insert_id.
    Helps reduce duplicates on retry.
    """
    if event_id:
        return event_id

    if trace_id:
        return f"trace_{trace_id}"

    if raw_json:
        raw_str = raw_json if isinstance(raw_json, str) else json.dumps(raw_json, sort_keys=True)
        hash_obj = hashlib.sha256(raw_str.encode("utf-8"))
        return f"{prefix}_{hash_obj.hexdigest()[:24]}"

    return f"{prefix}_{int(time.time() * 1000000)}"


def normalize_json_for_bigquery(value, field_name):
    """
    BigQuery insert_rows_json expects JSON-serializable values.
    For JSON columns, sending a JSON string is the most reliable approach here.
    """
    if value is None:
        return "{}"

    if isinstance(value, (dict, list)):
        return json.dumps(value)

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return json.dumps(parsed)
        except json.JSONDecodeError:
            return json.dumps({
                "error": f"failed to parse {field_name}",
                "raw": value[:5000]
            })

    return json.dumps({
        "error": f"unsupported type for {field_name}",
        "raw": str(value)[:5000]
    })


def normalize_repeated_string_field(value):
    """
    Normalize repeated STRING fields for BigQuery.
    """
    if value is None:
        return []

    if isinstance(value, list):
        return [str(v) for v in value if str(v).strip()]

    if isinstance(value, str):
        return [value] if value.strip() else []

    return [str(value)]


def build_fallback_dlq(raw_message, reason, source_topic):
    """
    Build a safe DLQ row that matches stripe_dw.stripe_dlq schema.
    """
    raw_payload = raw_message if isinstance(raw_message, str) else str(raw_message)

    row = {
        "failed_at": utc_now(),
        "reason": reason,
        "event_id": None,
        "event_type": None,
        "trace_id": None,
        "source_system": "bigquery_loader",
        "source_topic": source_topic,
        "pipeline_version": PIPELINE_VERSION,
        "raw_message": json.dumps({
            "raw": raw_payload[:5000]
        }),
    }

    insert_id = generate_row_id(
        event_id=None,
        trace_id=None,
        raw_json=raw_payload,
        prefix="dlq",
    )
    return row, insert_id


def transform_processed_event(event_json):
    """
    Transform processed Kafka event into a row that matches stripe_processed schema exactly.
    Reject rows that do not satisfy REQUIRED BigQuery fields.
    """
    event = json.loads(event_json)

    row = {field: None for field in PROCESSED_FIELDS}

    for field in PROCESSED_FIELDS:
        if field in event:
            row[field] = event[field]

    row["ingested_at"] = parse_timestamp(event.get("ingested_at"))
    row["event_created_ts"] = parse_timestamp(event.get("event_created_ts"))
    row["processed_at"] = parse_timestamp(event.get("processed_at"))

    row["payment_method_types"] = normalize_repeated_string_field(
        event.get("payment_method_types")
    )

    row["raw_event"] = normalize_json_for_bigquery(
        event.get("raw_event"), "raw_event"
    )

    # REQUIRED fields for stripe_processed table
    if not row["event_id"] or not str(row["event_id"]).strip():
        raise ValueError("missing required field: event_id")

    if not row["event_type"] or not str(row["event_type"]).strip():
        raise ValueError("missing required field: event_type")

    insert_id = generate_row_id(
        row.get("event_id"),
        row.get("trace_id"),
        event_json,
        prefix="processed",
    )

    return row, insert_id


def transform_dlq_event(event_json):
    """
    Transform DLQ Kafka event into a row that matches stripe_dlq schema exactly.
    """
    event = json.loads(event_json)

    row = {field: None for field in DLQ_FIELDS}

    for field in DLQ_FIELDS:
        if field in event:
            row[field] = event[field]

    row["failed_at"] = parse_timestamp(event.get("failed_at")) or utc_now()
    row["raw_message"] = normalize_json_for_bigquery(
        event.get("raw_message"), "raw_message"
    )

    if not row["reason"] or not str(row["reason"]).strip():
        row["reason"] = "unknown_dlq_reason"

    insert_id = generate_row_id(
        event.get("event_id"),
        event.get("trace_id"),
        event_json,
        prefix="dlq",
    )

    return row, insert_id


def write_batch_to_bigquery(table_id, rows_with_ids, event_type):
    """
    Write batch to BigQuery with insert IDs for idempotency.
    """
    if not rows_with_ids:
        return True

    try:
        rows = [row for row, _ in rows_with_ids]
        row_ids = [insert_id for _, insert_id in rows_with_ids]

        errors = client.insert_rows_json(
            table_id,
            rows,
            row_ids=row_ids,
        )

        if errors:
            logger.error("❌ Errors writing %s %s events:", len(rows), event_type)
            for error in errors:
                logger.error("  %s", error)
            return False

        logger.info("✅ Wrote %s %s events to BigQuery", len(rows), event_type)
        return True

    except Exception as e:
        logger.error("❌ Exception writing %s %s events: %s", len(rows_with_ids), event_type, e)
        return False


# ============================================================================
# MAIN CONSUMER
# ============================================================================
def main():
    logger.info("Starting Kafka consumers...")

    processed_consumer = KafkaConsumer(
        PROCESSED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="bigquery-loader-processed",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    dlq_consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="bigquery-loader-dlq",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    logger.info("✅ Consumers started")
    logger.info("Topics: %s, %s", PROCESSED_TOPIC, DLQ_TOPIC)
    logger.info("Batch size: %s, Timeout: %ss", BATCH_SIZE, BATCH_TIMEOUT)
    logger.info("Mode: Manual commit with idempotent inserts and fallback DLQ")

    processed_batch = []
    dlq_batch = []

    last_processed_write_time = time.time()
    last_dlq_write_time = time.time()

    try:
        while True:
            # ------------------------------------------------------------
            # POLL PROCESSED TOPIC
            # ------------------------------------------------------------
            processed_msgs = processed_consumer.poll(timeout_ms=1000, max_records=50)
            for _, messages in processed_msgs.items():
                for message in messages:
                    try:
                        result = transform_processed_event(message.value)
                        processed_batch.append(result)
                    except Exception as e:
                        logger.error("❌ Transform error (processed): %s", e)
                        logger.error("   Raw message: %s...", message.value[:500])

                        fallback_row = build_fallback_dlq(
                            raw_message=message.value,
                            reason=f"processed_transform_failed: {str(e)}",
                            source_topic=PROCESSED_TOPIC,
                        )
                        dlq_batch.append(fallback_row)
                        logger.warning("↪ Routed bad processed message to BigQuery DLQ batch")

            # ------------------------------------------------------------
            # POLL DLQ TOPIC
            # ------------------------------------------------------------
            dlq_msgs = dlq_consumer.poll(timeout_ms=1000, max_records=50)
            for _, messages in dlq_msgs.items():
                for message in messages:
                    try:
                        result = transform_dlq_event(message.value)
                        dlq_batch.append(result)
                    except Exception as e:
                        logger.error("❌ Transform error (DLQ): %s", e)
                        logger.error("   Raw message: %s...", message.value[:500])

                        fallback_row = build_fallback_dlq(
                            raw_message=message.value,
                            reason=f"dlq_transform_failed: {str(e)}",
                            source_topic=DLQ_TOPIC,
                        )
                        dlq_batch.append(fallback_row)
                        logger.warning("↪ Routed malformed DLQ message to fallback BigQuery DLQ row")

            # ------------------------------------------------------------
            # WRITE PROCESSED BATCH
            # ------------------------------------------------------------
            current_time = time.time()
            processed_time_elapsed = current_time - last_processed_write_time

            should_write_processed = (
                len(processed_batch) >= BATCH_SIZE
                or (processed_batch and processed_time_elapsed >= BATCH_TIMEOUT)
            )

            if should_write_processed:
                logger.info("Writing processed batch: %s events", len(processed_batch))
                success = write_batch_to_bigquery(PROCESSED_TABLE, processed_batch, "processed")

                if success:
                    processed_batch = []
                    processed_consumer.commit()
                    last_processed_write_time = current_time
                    logger.info("✅ Committed processed offsets")
                else:
                    logger.warning("⚠️ Processed batch write failed - will retry")

            # ------------------------------------------------------------
            # WRITE DLQ BATCH
            # ------------------------------------------------------------
            dlq_time_elapsed = current_time - last_dlq_write_time

            should_write_dlq = (
                len(dlq_batch) >= BATCH_SIZE
                or (dlq_batch and dlq_time_elapsed >= BATCH_TIMEOUT)
            )

            if should_write_dlq:
                logger.info("Writing DLQ batch: %s events", len(dlq_batch))
                success = write_batch_to_bigquery(DLQ_TABLE, dlq_batch, "DLQ")

                if success:
                    dlq_batch = []
                    dlq_consumer.commit()
                    last_dlq_write_time = current_time
                    logger.info("✅ Committed DLQ offsets")
                else:
                    logger.warning("⚠️ DLQ batch write failed - will retry")

    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")

        if processed_batch:
            logger.info("Writing final processed batch: %s events", len(processed_batch))
            success = write_batch_to_bigquery(PROCESSED_TABLE, processed_batch, "processed")
            if success:
                processed_consumer.commit()
                logger.info("✅ Committed final processed offsets")

        if dlq_batch:
            logger.info("Writing final DLQ batch: %s events", len(dlq_batch))
            success = write_batch_to_bigquery(DLQ_TABLE, dlq_batch, "DLQ")
            if success:
                dlq_consumer.commit()
                logger.info("✅ Committed final DLQ offsets")

        processed_consumer.close()
        dlq_consumer.close()
        logger.info("✅ Consumers closed")


if __name__ == "__main__":
    main()