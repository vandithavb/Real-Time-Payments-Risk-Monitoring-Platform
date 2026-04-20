select
    cast(failed_at as timestamp) as failed_at,
    reason,
    event_id,
    event_type,
    trace_id,
    source_system,
    source_topic,
    pipeline_version,
    raw_message
from {{ source('stripe_raw', 'stripe_dlq') }}