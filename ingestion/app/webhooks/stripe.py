"""
Stripe webhook handlers.

Receives webhooks from Stripe for:
- customer.created, customer.updated
- charge.succeeded, charge.failed, charge.refunded
- payment_intent.succeeded, payment_intent.failed

See: https://docs.stripe.com/webhooks
"""

import logging
from typing import Any, Dict

from fastapi import APIRouter, Header, HTTPException, Request, status

from ..config import get_settings, get_topic_name
from ..producers import get_producer
from ..validators import validate_stripe_signature

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/stripe", tags=["stripe"])


# Mapping of Stripe event types to topic suffixes
EVENT_TYPE_TO_TOPIC = {
    "customer.created": "customers",
    "customer.updated": "customers",
    "customer.deleted": "customers",
    "charge.succeeded": "charges",
    "charge.failed": "charges",
    "charge.refunded": "charges",
    "charge.dispute.created": "disputes",
    "charge.dispute.updated": "disputes",
    "charge.dispute.closed": "disputes",
    "payment_intent.created": "payment_intents",
    "payment_intent.succeeded": "payment_intents",
    "payment_intent.payment_failed": "payment_intents",
    "payment_intent.canceled": "payment_intents",
    "invoice.created": "invoices",
    "invoice.finalized": "invoices",
    "invoice.paid": "invoices",
    "invoice.payment_failed": "invoices",
    "subscription.created": "subscriptions",
    "customer.subscription.created": "subscriptions",
    "customer.subscription.updated": "subscriptions",
    "customer.subscription.deleted": "subscriptions",
    "refund.created": "refunds",
    "refund.updated": "refunds",
}


@router.post("/webhook")
async def stripe_webhook(
    request: Request,
    stripe_signature: str | None = Header(None),
) -> Dict[str, Any]:
    """
    Handle all Stripe webhooks.

    Stripe sends all events to a single endpoint. The event type is in
    the payload's "type" field. We route to appropriate topics based on
    the event type.

    Header: Stripe-Signature (format: t=timestamp,v1=signature)
    """
    settings = get_settings()
    body = await request.body()

    # Validate signature
    if not settings.skip_signature_validation:
        if not settings.stripe_webhook_secret:
            logger.warning("Stripe webhook secret not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Webhook secret not configured",
            )

        if not stripe_signature:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing Stripe-Signature header",
            )

        if not validate_stripe_signature(body, stripe_signature, settings.stripe_webhook_secret):
            logger.warning("Invalid Stripe webhook signature")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid signature",
            )

    # Parse payload
    try:
        import json
        payload = json.loads(body)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON payload: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON payload",
        )

    # Extract event info from Stripe envelope
    event_type = payload.get("type", "unknown")
    event_id = payload.get("id", "")
    data_object = payload.get("data", {}).get("object", {})

    if not data_object:
        logger.warning(f"Stripe event {event_id} has no data.object")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Event has no data.object",
        )

    # Add webhook metadata to the data object
    data_object["_webhook_event_id"] = event_id

    # Determine topic based on event type
    topic_suffix = EVENT_TYPE_TO_TOPIC.get(event_type)
    if not topic_suffix:
        # Use a generic topic for unknown event types
        logger.warning(f"Unknown Stripe event type: {event_type}")
        topic_suffix = "events"

    # Get record key for partitioning
    record_key = str(data_object.get("id", event_id))

    # Publish to Redpanda
    producer = await get_producer()
    topic = get_topic_name("stripe", topic_suffix)

    success = await producer.send_webhook_event(
        topic=topic,
        event_data=data_object,
        source="stripe",
        event_type=event_type,
        event_id=event_id,
        key=record_key,
    )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to publish event",
        )

    logger.info(
        f"Stripe {event_type} event published to {topic}, "
        f"event_id={event_id}, object_id={record_key}"
    )

    return {"status": "ok", "topic": topic, "event_id": event_id}
