import json
import stripe
from fastapi import APIRouter, HTTPException, Request
from app.common.config import (
    STRIPE_RAW_TOPIC,
    STRIPE_WEBHOOK_SECRET,
)
from datetime import datetime, timezone  # For timestamps
import uuid  # For generating unique trace IDs
from app.webhook_api.producer import publish_event

router = APIRouter()


@router.post("/webhook/stripe")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    if not sig_header:
        raise HTTPException(status_code=400, detail="Missing Stripe signature header")

    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="Webhook secret not configured")

    # Verify if the event is from Stripe , security check
    try:
        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=STRIPE_WEBHOOK_SECRET,
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    event_type = event["type"]
    event_id = event["id"]
    event_created = event["created"]      # Stripe's timestamp
    api_version = event["api_version"] 

    # Capture metadata 
    metadata = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),  # When WE received it
        "trace_id": str(uuid.uuid4()),  # Unique ID to track this event through in pipeline
        "source_system": "stripe",  # Where the event came from
        "api_version": api_version,  # Stripe API version, Future proof in case Stripe changes their event structure
        "stripe_event_created": event_created,
    }

    event_payload = json.loads(payload.decode("utf-8"))


    message = {
    "metadata": metadata,
    "event": event_payload,
    }

    # Publish the above message (json with metadata) to the redpanda topic

    try:
        print(f"About to publish event {event_id} to topic {STRIPE_RAW_TOPIC}")
        publish_event(
            topic=STRIPE_RAW_TOPIC,
            key=event_id,
            value=message,
        )
    except Exception as e:
        print(f"ERROR publishing event to Redpanda: {repr(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to publish event: {str(e)}")

    print(
        json.dumps(
            {
                "message": "Received Stripe webhook and published to Redpanda",
                "event_id": event_id,
                "event_type": event_type,
                "event_created": event_created,
                "trace_id": metadata["trace_id"],
                "ingested_at": metadata["ingested_at"],
                "api_version":api_version,
            }
        )
    )

    return {
        "received": True,
        "event_id": event_id,
        "event_type": event_type,
    }