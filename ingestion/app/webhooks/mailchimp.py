"""
Mailchimp webhook handlers.

Receives webhooks from Mailchimp for:
- subscribe, unsubscribe, profile, upemail, cleaned → subscribers
- campaign → campaigns
- send, open, click, bounce, unsub, abuse → events

Note: In production, Mailchimp sends form-encoded webhooks. This handler
expects JSON payloads for consistency with the rest of the pipeline
(Shopify, Stripe, HubSpot all use JSON). If migrating to real Mailchimp
webhooks, add form-decoding middleware before this handler.

Mailchimp authenticates webhooks via a shared secret in the URL query
parameter (?secret=...), NOT via HMAC body signatures like other sources.

See: https://mailchimp.com/developer/transactional/docs/webhooks/
"""

import json
import logging
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Query, Request, status

from ..config import get_settings, get_topic_name
from ..producers import get_producer
from ..validators import validate_mailchimp_signature

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/mailchimp", tags=["mailchimp"])


# Mapping of Mailchimp webhook type to Redpanda topic suffix
EVENT_TYPE_TO_TOPIC = {
    # Subscriber events → mailchimp.subscribers
    "subscribe": "subscribers",
    "unsubscribe": "subscribers",
    "profile": "subscribers",
    "upemail": "subscribers",
    "cleaned": "subscribers",
    # Campaign events → mailchimp.campaigns
    "campaign": "campaigns",
    # Engagement events → mailchimp.events
    "sent": "events",
    "open": "events",
    "click": "events",
    "bounce": "events",
    "unsub": "events",
    "abuse": "events",
    "sms_sent": "events",
    "sms_click": "events",
}


@router.get("/webhook")
async def mailchimp_validation_ping() -> Dict[str, Any]:
    """
    Handle Mailchimp webhook URL validation.

    Mailchimp sends a GET request to validate the webhook URL
    during setup. Must return 200.
    """
    return {"status": "ok"}


@router.post("/webhook")
async def mailchimp_webhook(
    request: Request,
    secret: str | None = Query(None, description="Mailchimp webhook shared secret"),
) -> Dict[str, Any]:
    """
    Handle Mailchimp webhooks.

    Mailchimp sends a single event per request with a `type` field
    that determines routing. The shared secret is passed as a URL
    query parameter (?secret=...).
    """
    settings = get_settings()
    body = await request.body()

    # Validate secret
    if not settings.skip_signature_validation:
        if not settings.mailchimp_webhook_secret:
            logger.warning("Mailchimp webhook secret not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Webhook secret not configured",
            )

        if not secret:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing secret query parameter",
            )

        if not validate_mailchimp_signature(secret, settings.mailchimp_webhook_secret):
            logger.warning("Invalid Mailchimp webhook secret")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid secret",
            )

    # Parse payload
    try:
        payload: Dict[str, Any] = json.loads(body)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON payload: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON payload",
        )

    # Determine event type and route to correct topic
    event_type = payload.get("_webhook_event_type") or payload.get("type", "unknown")

    topic_suffix = EVENT_TYPE_TO_TOPIC.get(event_type)
    if topic_suffix is None:
        logger.warning(f"Unknown Mailchimp event type: {event_type}")
        return {"status": "ignored", "reason": "unknown_event_type"}

    topic = get_topic_name("mailchimp", topic_suffix)

    # Enrich with webhook metadata (immutable — create new dict)
    enriched_payload = {**payload, "_webhook_event_type": event_type}

    # Determine message key
    data = payload.get("data", {})
    key = data.get("email") or data.get("id") or payload.get("campaign_id", "")

    producer = await get_producer()
    success = await producer.send_webhook_event(
        topic=topic,
        event_data=enriched_payload,
        source="mailchimp",
        event_type=event_type,
        event_id=payload.get("event_id", ""),
        key=str(key),
    )

    if not success:
        logger.error(f"Failed to publish Mailchimp {event_type} event to {topic}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to publish event",
        )

    logger.info(
        f"Mailchimp {event_type} event published to {topic}, key={key}"
    )

    return {
        "status": "ok",
        "published": 1,
        "topic": topic,
    }
