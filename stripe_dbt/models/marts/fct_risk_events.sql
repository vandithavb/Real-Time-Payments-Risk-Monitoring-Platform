select
    event_id,
    event_type,
    trace_id,
    processed_at,
    event_created_ts,

    payment_intent_id,
    charge_id,
    customer_id,

    amount,
    currency,

    status,
    failure_code,
    failure_message,
    decline_code,

    risk_level,
    risk_score,

    disputed,
    refunded,

    outcome_network_status,
    outcome_type,
    outcome_seller_message

from {{ ref('stg_stripe_processed') }}
where event_type in (
    'payment_intent.payment_failed',
    'charge.dispute.created',
    'refund.created'
)