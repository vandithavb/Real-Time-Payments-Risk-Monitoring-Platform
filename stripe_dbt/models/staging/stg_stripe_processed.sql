select
    event_id,
    event_type,
    trace_id,
    source_system,
    source_topic,
    api_version,
    cast(ingested_at as timestamp) as ingested_at,
    cast(event_created_ts as timestamp) as event_created_ts,
    cast(processed_at as timestamp) as processed_at,
    pipeline_version,

    object_id,
    object_type,

    payment_intent_id,
    charge_id,
    refund_id,
    dispute_id,
    customer_id,

    amount,
    amount_refunded,
    amount_captured,
    upper(currency) as currency,

    status,

    payment_method,
    payment_method_type,
    payment_method_types,
    card_brand,
    card_country,
    card_funding,
    card_last4,

    customer_email,
    billing_country,
    shipping_country,

    risk_level,
    risk_score,
    outcome_network_status,
    outcome_type,
    outcome_seller_message,

    failure_code,
    failure_message,
    decline_code,

    disputed,
    refunded,

    description,
    reason,
    receipt_url,
    receipt_number,
    capture_method,
    is_charge_refundable,

    raw_event

from {{ source('stripe_raw', 'stripe_processed') }}
where event_id is not null
  and event_type is not null