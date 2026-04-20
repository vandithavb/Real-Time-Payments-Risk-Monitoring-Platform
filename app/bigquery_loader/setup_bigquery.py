"""
BigQuery Setup: Create tables in stripe_dw dataset
Creates:
  - stripe_dw.stripe_processed (normalized events)
  - stripe_dw.stripe_dlq (failed events)
"""
from google.cloud import bigquery

client = bigquery.Client()
project_id = client.project

print(f"Setting up BigQuery tables in: {project_id}.stripe_dw\n")

# Dataset should already exist 
dataset_id = f"{project_id}.stripe_dw"

# ============================================================================
# SCHEMA: PROCESSED EVENTS
# ============================================================================
processed_schema = [
    # Envelope & Lineage
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("trace_id", "STRING"),
    bigquery.SchemaField("source_system", "STRING"),
    bigquery.SchemaField("source_topic", "STRING"),
    bigquery.SchemaField("api_version", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("event_created_ts", "TIMESTAMP"),
    bigquery.SchemaField("processed_at", "TIMESTAMP"),
    bigquery.SchemaField("pipeline_version", "STRING"),
    
    # Object Traceability
    bigquery.SchemaField("object_id", "STRING"),
    bigquery.SchemaField("object_type", "STRING"),
    
    # Core Identifiers
    bigquery.SchemaField("payment_intent_id", "STRING"),
    bigquery.SchemaField("charge_id", "STRING"),
    bigquery.SchemaField("refund_id", "STRING"),
    bigquery.SchemaField("dispute_id", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    
    # Monetary
    bigquery.SchemaField("amount", "INTEGER"),
    bigquery.SchemaField("amount_refunded", "INTEGER"),
    bigquery.SchemaField("amount_captured", "INTEGER"),
    bigquery.SchemaField("currency", "STRING"),
    
    # Status
    bigquery.SchemaField("status", "STRING"),
    
    # Payment Method
    bigquery.SchemaField("payment_method", "STRING"),
    bigquery.SchemaField("payment_method_type", "STRING"),
    bigquery.SchemaField("payment_method_types", "STRING", mode="REPEATED"),
    bigquery.SchemaField("card_brand", "STRING"),
    bigquery.SchemaField("card_country", "STRING"),
    bigquery.SchemaField("card_funding", "STRING"),
    bigquery.SchemaField("card_last4", "STRING"),
    
    # Customer Context
    bigquery.SchemaField("customer_email", "STRING"),
    bigquery.SchemaField("billing_country", "STRING"),
    bigquery.SchemaField("shipping_country", "STRING"),
    
    # Risk Signals
    bigquery.SchemaField("risk_level", "STRING"),
    bigquery.SchemaField("risk_score", "INTEGER"),
    bigquery.SchemaField("outcome_network_status", "STRING"),
    bigquery.SchemaField("outcome_type", "STRING"),
    bigquery.SchemaField("outcome_seller_message", "STRING"),
    
    # Failure Details
    bigquery.SchemaField("failure_code", "STRING"),
    bigquery.SchemaField("failure_message", "STRING"),
    bigquery.SchemaField("decline_code", "STRING"),
    
    # Fraud Indicators
    bigquery.SchemaField("disputed", "BOOLEAN"),
    bigquery.SchemaField("refunded", "BOOLEAN"),
    
    # Misc
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("reason", "STRING"),
    bigquery.SchemaField("receipt_url", "STRING"),
    bigquery.SchemaField("receipt_number", "STRING"),
    bigquery.SchemaField("capture_method", "STRING"),
    bigquery.SchemaField("is_charge_refundable", "BOOLEAN"),
    
    # Raw Event (JSON for debugging/replay)
    bigquery.SchemaField("raw_event", "JSON"),
]

# ============================================================================
# SCHEMA: DLQ (Dead Letter Queue)
# ============================================================================
dlq_schema = [
    # Error Context
    bigquery.SchemaField("failed_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("reason", "STRING", mode="REQUIRED"),
    
    # Event Context (nullable - may not be parseable)
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("trace_id", "STRING"),
    
    # Pipeline Context
    bigquery.SchemaField("source_system", "STRING"),
    bigquery.SchemaField("source_topic", "STRING"),
    bigquery.SchemaField("pipeline_version", "STRING"),
    
    # Raw Message (JSON for debugging)
    bigquery.SchemaField("raw_message", "JSON", mode="REQUIRED"),
]

# ============================================================================
# TABLE 1: PROCESSED EVENTS
# ============================================================================
processed_table_id = f"{dataset_id}.stripe_processed"

processed_table = bigquery.Table(processed_table_id, schema=processed_schema)
processed_table.description = "Normalized Stripe events with risk signals"

# Partition by processed_at (PyFlink always populates this field)
processed_table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="processed_at"
)

# Cluster by common query fields
processed_table.clustering_fields = ["customer_id", "event_type", "card_country"]

try:
    processed_table = client.create_table(processed_table, exists_ok=True)
    print(f"✅ Created table: {processed_table_id}")
    print(f"   - {len(processed_schema)} columns")
    print(f"   - Partitioned by: processed_at (daily)")
    print(f"   - Clustered by: customer_id, event_type, card_country")
except Exception as e:
    print(f"❌ Error creating processed table: {e}")
    exit(1)

# ============================================================================
# TABLE 2: DLQ (Dead Letter Queue)
# ============================================================================
dlq_table_id = f"{dataset_id}.stripe_dlq"

dlq_table = bigquery.Table(dlq_table_id, schema=dlq_schema)
dlq_table.description = "Failed/unsupported Stripe events for debugging"

# Partition by failed_at
dlq_table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="failed_at"
)

# Cluster by debugging fields
dlq_table.clustering_fields = ["event_type", "pipeline_version"]

try:
    dlq_table = client.create_table(dlq_table, exists_ok=True)
    print(f"✅ Created table: {dlq_table_id}")
    print(f"   - {len(dlq_schema)} columns")
    print(f"   - Partitioned by: failed_at (daily)")
    print(f"   - Clustered by: event_type, pipeline_version")
except Exception as e:
    print(f"❌ Error creating DLQ table: {e}")
    exit(1)

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*70)
print("✅ BigQuery tables created in stripe_dw!")
print("="*70)
print(f"\nDataset: {dataset_id}")
print(f"\nTables:")
print(f"  1. {processed_table_id}")
print(f"     → Receives from Kafka: stripe.processed")
print(f"  2. {dlq_table_id}")
print(f"     → Receives from Kafka: stripe.dlq")
print(f"\nArchitecture:")
print(f"  Stripe → FastAPI → stripe.raw (Kafka buffer)")
print(f"                         ↓")
print(f"                    PyFlink normalizer")
print(f"                         ↓")
print(f"          stripe.processed / stripe.dlq (Kafka)")
print(f"                         ↓")
print(f"              BigQuery Loader (Python)")
print(f"                         ↓")
print(f"    stripe_processed / stripe_dlq (BigQuery)")
print(f"\nView in console:")
print(f"https://console.cloud.google.com/bigquery?project={project_id}&d=stripe_dw")