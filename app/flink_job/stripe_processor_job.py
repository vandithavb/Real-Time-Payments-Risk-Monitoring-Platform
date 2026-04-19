"""
PyFlink Job: Stripe Event Normalizer (Production-Grade Risk Monitoring)
=========================================================================
Reads from stripe.raw, normalizes events with risk/fraud fields, 
writes to stripe.processed or stripe.dlq

Schema designed for:
- Fraud detection (card country mismatches, repeat cards)
- Risk monitoring (risk scores, decline codes)
- Analytics (customer behavior, payment patterns)
- Debugging (raw_event, trace_id, object_id)
"""
import json
import os
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)

# ============================================================================
# CONFIGURATION
# ============================================================================
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "stripe.raw")
PROCESSED_TOPIC = os.getenv("PROCESSED_TOPIC", "stripe.processed")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "stripe.dlq")
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "v2")

# Supported Stripe event types
SUPPORTED_EVENTS = {
    "payment_intent.created",
    "payment_intent.succeeded",
    "payment_intent.payment_failed",
    "charge.succeeded",
    "charge.dispute.created",
    "refund.created",
}


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================
def now_iso():
    """Return current timestamp in ISO 8601 format"""
    return datetime.now(timezone.utc).isoformat()


def to_iso_timestamp(unix_timestamp):
    """
    Convert Unix timestamp to ISO 8601 string
    
    Returns None if timestamp is missing or malformed.
    BigQuery schema should allow NULL for timestamp fields.
    """
    if unix_timestamp is None:
        return None
    try:
        return datetime.fromtimestamp(int(unix_timestamp), tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return None


# ============================================================================
# NORMALIZATION LOGIC (PRODUCTION-GRADE)
# ============================================================================
def normalize_event(message: dict) -> dict:
    """
    Extract risk-monitoring and downstream analytics fields from Stripe events
    using a stable schema across all supported event types.
    
    Captures:
    - Risk signals (risk_level, risk_score, decline_code)
    - Fraud indicators (disputed, refunded, card country mismatches)
    - Payment method details (card_brand, card_country, card_funding)
    - Customer context (email, billing_country, shipping_country)
    - Traceability (raw_event, object_id, trace_id)
    """
    metadata = message.get("metadata", {}) or {}
    event = message.get("event", {}) or {}

    # Extract envelope fields
    event_id = event.get("id")
    event_type = event.get("type")
    event_created_unix = event.get("created")

    # Extract metadata
    ingested_at = metadata.get("ingested_at")
    trace_id = metadata.get("trace_id")
    source_system = metadata.get("source_system", "stripe")
    api_version = metadata.get("api_version")

    # Standardize timestamp
    event_created_ts = to_iso_timestamp(event_created_unix)

    # Get data object
    event_object = event.get("data", {}).get("object", {}) or {}

    # ========================================================================
    # STANDARDIZED BASE SCHEMA (ALL EVENTS)
    # ========================================================================
    base = {
        # Envelope & Lineage
        "event_id": event_id,
        "event_type": event_type,
        "trace_id": trace_id,
        "source_system": source_system,
        "source_topic": RAW_TOPIC,
        "api_version": api_version,
        "ingested_at": ingested_at,
        "event_created_ts": event_created_ts,
        "processed_at": now_iso(),
        "pipeline_version": PIPELINE_VERSION,

        # Object Traceability
        "object_id": event_object.get("id"),
        "object_type": event_object.get("object"),

        # Core Identifiers
        "payment_intent_id": None,
        "charge_id": None,
        "refund_id": None,
        "dispute_id": None,
        "customer_id": None,

        # Monetary
        "amount": None,
        "amount_refunded": None,
        "amount_captured": None,
        "currency": None,

        # Status
        "status": None,

        # Payment Method
        "payment_method": None,
        "payment_method_type": None,
        "payment_method_types": None,
        "card_brand": None,
        "card_country": None,
        "card_funding": None,
        "card_last4": None,

        # Customer Context
        "customer_email": None,
        "billing_country": None,
        "shipping_country": None,

        # Risk Signals
        "risk_level": None,
        "risk_score": None,
        "outcome_network_status": None,
        "outcome_type": None,
        "outcome_seller_message": None,

        # Failure Details
        "failure_code": None,
        "failure_message": None,
        "decline_code": None,

        # Fraud Indicators
        "disputed": None,
        "refunded": None,

        # Misc
        "description": None,
        "reason": None,
        "receipt_url": None,
        "receipt_number": None,
        "capture_method": None,
        "is_charge_refundable": None,

        # Raw Event (for replay/debugging)
        "raw_event": message,
    }

    # ========================================================================
    # EVENT-SPECIFIC FIELD EXTRACTION
    # ========================================================================
    
    if event_type == "payment_intent.succeeded":
        charges = event_object.get("charges", {}).get("data", [])
        first_charge = charges[0] if charges else {}
        payment_method_details = first_charge.get("payment_method_details", {}) or {}
        card_details = payment_method_details.get("card", {}) or {}

        base.update({
            "payment_intent_id": event_object.get("id"),
            "customer_id": event_object.get("customer"),
            "amount": event_object.get("amount"),
            "amount_captured": first_charge.get("amount_captured"),
            "currency": event_object.get("currency"),
            "status": event_object.get("status"),
            "payment_method": event_object.get("payment_method"),
            "description": event_object.get("description"),

            "payment_method_type": payment_method_details.get("type"),
            "card_brand": card_details.get("brand"),
            "card_country": card_details.get("country"),
            "card_funding": card_details.get("funding"),
            "card_last4": card_details.get("last4"),
        })
        return base

    if event_type == "payment_intent.payment_failed":
        last_payment_error = event_object.get("last_payment_error", {}) or {}
        payment_method_obj = last_payment_error.get("payment_method", {}) or {}
        card_details = payment_method_obj.get("card", {}) or {}

        base.update({
            "payment_intent_id": event_object.get("id"),
            "customer_id": event_object.get("customer"),
            "amount": event_object.get("amount"),
            "currency": event_object.get("currency"),
            "status": event_object.get("status"),
            "payment_method": event_object.get("payment_method"),
            "description": event_object.get("description"),

            "failure_code": last_payment_error.get("code"),
            "failure_message": last_payment_error.get("message"),
            "decline_code": last_payment_error.get("decline_code"),

            "card_brand": card_details.get("brand"),
            "card_country": card_details.get("country"),
            "card_funding": card_details.get("funding"),
            "card_last4": card_details.get("last4"),
        })
        return base

    if event_type == "payment_intent.created":
        payment_method_types = event_object.get("payment_method_types", []) or []

        base.update({
            "payment_intent_id": event_object.get("id"),
            "customer_id": event_object.get("customer"),
            "amount": event_object.get("amount"),
            "currency": event_object.get("currency"),
            "status": event_object.get("status"),
            "capture_method": event_object.get("capture_method"),
            "payment_method_types": payment_method_types,
            "payment_method_type": payment_method_types[0] if payment_method_types else None,
            "description": event_object.get("description"),
        })
        return base

    if event_type == "charge.succeeded":
        payment_method_details = event_object.get("payment_method_details", {}) or {}
        card_details = payment_method_details.get("card", {}) or {}
        outcome = event_object.get("outcome", {}) or {}
        billing_details = event_object.get("billing_details", {}) or {}
        shipping = event_object.get("shipping", {}) or {}

        base.update({
            "payment_intent_id": event_object.get("payment_intent"),
            "charge_id": event_object.get("id"),
            "customer_id": event_object.get("customer"),
            "amount": event_object.get("amount"),
            "amount_captured": event_object.get("amount_captured"),
            "amount_refunded": event_object.get("amount_refunded"),
            "currency": event_object.get("currency"),
            "status": event_object.get("status"),
            "payment_method": event_object.get("payment_method"),
            "receipt_url": event_object.get("receipt_url"),
            "description": event_object.get("description"),

            "payment_method_type": payment_method_details.get("type"),
            "card_brand": card_details.get("brand"),
            "card_country": card_details.get("country"),
            "card_funding": card_details.get("funding"),
            "card_last4": card_details.get("last4"),

            "customer_email": billing_details.get("email"),
            "billing_country": (billing_details.get("address") or {}).get("country"),
            "shipping_country": (shipping.get("address") or {}).get("country"),

            "risk_level": outcome.get("risk_level"),
            "risk_score": outcome.get("risk_score"),
            "outcome_network_status": outcome.get("network_status"),
            "outcome_type": outcome.get("type"),
            "outcome_seller_message": outcome.get("seller_message"),

            "disputed": event_object.get("disputed"),
            "refunded": event_object.get("refunded"),
        })
        return base

    if event_type == "charge.dispute.created":
        base.update({
            "dispute_id": event_object.get("id"),
            "charge_id": event_object.get("charge"),
            "amount": event_object.get("amount"),
            "currency": event_object.get("currency"),
            "status": event_object.get("status"),
            "reason": event_object.get("reason"),
            "is_charge_refundable": event_object.get("is_charge_refundable"),
            "disputed": True,
        })
        return base

    if event_type == "refund.created":
        base.update({
            "refund_id": event_object.get("id"),
            "payment_intent_id": event_object.get("payment_intent"),
            "charge_id": event_object.get("charge"),
            "amount": event_object.get("amount"),
            "amount_refunded": event_object.get("amount"),
            "currency": event_object.get("currency"),
            "status": event_object.get("status"),
            "reason": event_object.get("reason"),
            "receipt_number": event_object.get("receipt_number"),
            "refunded": True,
        })
        return base

    raise ValueError(f"Unsupported event type: {event_type}")


# ============================================================================
# ERROR HANDLING (DLQ)
# ============================================================================
def build_dlq_record(message: dict, reason: str) -> dict:
    """Build structured DLQ record with error context"""
    event = message.get("event", {})
    metadata = message.get("metadata", {})
    
    return {
        "failed_at": now_iso(),
        "reason": reason,
        "event_id": event.get("id"),
        "event_type": event.get("type"),
        "trace_id": metadata.get("trace_id"),
        "source_system": metadata.get("source_system", "stripe"),
        "source_topic": RAW_TOPIC,
        "pipeline_version": PIPELINE_VERSION,
        "raw_message": message,
    }


def process_raw_event(raw: str):
    """Main processing function - routes to normalization or DLQ"""
    try:
        message = json.loads(raw)
        event_type = message.get("event", {}).get("type")

        if event_type not in SUPPORTED_EVENTS:
            dlq_record = build_dlq_record(
                message, 
                f"Unsupported event type: {event_type}"
            )
            return ("dlq", json.dumps(dlq_record))

        normalized = normalize_event(message)
        return ("processed", json.dumps(normalized))

    except json.JSONDecodeError as e:
        dlq_record = {
            "failed_at": now_iso(),
            "reason": f"JSON parsing error: {str(e)}",
            "event_id": None,
            "event_type": None,
            "trace_id": None,
            "source_system": "stripe",
            "source_topic": RAW_TOPIC,
            "pipeline_version": PIPELINE_VERSION,
            "raw_message": raw,
        }
        return ("dlq", json.dumps(dlq_record))
        
    except Exception as e:
        try:
            message = json.loads(raw)
            dlq_record = build_dlq_record(message, f"Processing error: {str(e)}")
        except Exception:
            dlq_record = {
                "failed_at": now_iso(),
                "reason": f"Critical error: {str(e)}",
                "event_id": None,
                "event_type": None,
                "trace_id": None,
                "source_system": "stripe",
                "source_topic": RAW_TOPIC,
                "pipeline_version": PIPELINE_VERSION,
                "raw_message": raw,
            }
        
        return ("dlq", json.dumps(dlq_record))


# ============================================================================
# STREAM HELPER FUNCTIONS
# ============================================================================
def is_tag(tag: str):
    """Filter function to check tag"""
    return lambda record: record[0] == tag


def get_payload(tagged_record):
    """Extract payload from (tag, payload) tuple"""
    return tagged_record[1]


# ============================================================================
# MAIN PIPELINE
# ============================================================================
def main():
    """Main PyFlink pipeline execution"""
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Stripe Event Normalizer (Risk Monitoring)")
    logger.info(f"Bootstrap servers: {BOOTSTRAP}")
    logger.info(f"Input topic: {RAW_TOPIC}")
    logger.info(f"Output topics: {PROCESSED_TOPIC}, {DLQ_TOPIC}")
    logger.info(f"Pipeline version: {PIPELINE_VERSION}")
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # SOURCE
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(RAW_TOPIC)
        .set_group_id("stripe-normalizer")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "stripe-raw-source",
    )

    # TRANSFORM
    routed_stream = raw_stream.map(
        process_raw_event,
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )

    processed_stream = routed_stream.filter(
        is_tag("processed")
    ).map(
        get_payload,
        output_type=Types.STRING()
    )

    dlq_stream = routed_stream.filter(
        is_tag("dlq")
    ).map(
        get_payload,
        output_type=Types.STRING()
    )

    # SINKS
    processed_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(PROCESSED_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    dlq_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(DLQ_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    processed_stream.sink_to(processed_sink)
    dlq_stream.sink_to(dlq_sink)

    logger.info("Pipeline configured successfully")
    logger.info("Starting execution...")
    env.execute("stripe-normalizer")


if __name__ == "__main__":
    main()