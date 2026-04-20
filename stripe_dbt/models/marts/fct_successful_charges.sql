select
    charge_id,
    payment_intent_id,
    customer_id,
    event_id,
    trace_id,
    processed_at,
    event_created_ts,

    amount,
    amount_captured,
    amount_refunded,
    currency,
    status,

    payment_method,
    payment_method_type,
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

    disputed,
    refunded,
    description,
    receipt_url

from {{ ref('stg_stripe_processed') }}
where event_type = 'charge.succeeded'
  and charge_id is not null