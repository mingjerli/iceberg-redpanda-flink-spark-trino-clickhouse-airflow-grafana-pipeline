"""
HubSpot webhook handlers.

Receives webhooks from HubSpot for:
- contact.creation, contact.propertyChange
- company.creation, company.propertyChange
- deal.creation, deal.propertyChange

Note: HubSpot webhooks send minimal data. For full object data,
you must fetch from the API using the objectId. This handler
publishes the webhook event as-is, and the Flink job enriches
by fetching the full object.

See: https://developers.hubspot.com/docs/api/webhooks
"""

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Header, HTTPException, Request, status

from ..config import get_settings, get_topic_name
from ..producers import get_producer
from ..validators import validate_hubspot_signature

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/hubspot", tags=["hubspot"])


# Mapping of subscription types to topic suffixes
SUBSCRIPTION_TYPE_TO_TOPIC = {
    "contact.creation": "contacts",
    "contact.deletion": "contacts",
    "contact.propertyChange": "contacts",
    "contact.merge": "contacts",
    "company.creation": "companies",
    "company.deletion": "companies",
    "company.propertyChange": "companies",
    "deal.creation": "deals",
    "deal.deletion": "deals",
    "deal.propertyChange": "deals",
}


@router.post("/webhook")
async def hubspot_webhook(
    request: Request,
    x_hubspot_signature_v3: str | None = Header(None),
    x_hubspot_signature: str | None = Header(None),
    x_hubspot_request_timestamp: str | None = Header(None),
) -> Dict[str, Any]:
    """
    Handle HubSpot webhooks.

    HubSpot sends an array of events in each webhook request.
    Each event contains minimal data (objectId, propertyName, propertyValue).

    Signature validation uses v3 (preferred) or v2 (fallback).
    """
    settings = get_settings()
    body = await request.body()

    # Validate signature
    if not settings.skip_signature_validation:
        if not settings.hubspot_client_secret:
            logger.warning("HubSpot client secret not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Webhook secret not configured",
            )

        # Try v3 signature first, then v2
        signature = x_hubspot_signature_v3 or x_hubspot_signature
        version = "v3" if x_hubspot_signature_v3 else "v2"

        if not signature:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing HubSpot signature header",
            )

        # Build full request URI for v3 validation
        request_uri = str(request.url)

        if not validate_hubspot_signature(
            body,
            signature,
            settings.hubspot_client_secret,
            request_uri=request_uri,
            http_method=request.method,
            timestamp=x_hubspot_request_timestamp,
            version=version,
        ):
            logger.warning("Invalid HubSpot webhook signature")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid signature",
            )

    # Parse payload (HubSpot sends an array of events)
    try:
        import json
        events: List[Dict[str, Any]] = json.loads(body)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON payload: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON payload",
        )

    if not isinstance(events, list):
        events = [events]

    # Process each event in the batch
    producer = await get_producer()
    published_count = 0
    errors = []

    for event in events:
        subscription_type = event.get("subscriptionType", "unknown")
        object_id = str(event.get("objectId", ""))
        event_id = str(event.get("eventId", ""))

        # Add webhook metadata
        event["_webhook_subscription_type"] = subscription_type

        # Determine topic based on subscription type
        topic_suffix = SUBSCRIPTION_TYPE_TO_TOPIC.get(subscription_type, "events")
        topic = get_topic_name("hubspot", topic_suffix)

        success = await producer.send_webhook_event(
            topic=topic,
            event_data=event,
            source="hubspot",
            event_type=subscription_type,
            event_id=event_id,
            key=object_id,
        )

        if success:
            published_count += 1
            logger.info(
                f"HubSpot {subscription_type} event published to {topic}, "
                f"object_id={object_id}, event_id={event_id}"
            )
        else:
            errors.append(event_id)

    if errors:
        logger.error(f"Failed to publish {len(errors)} HubSpot events")
        if published_count == 0:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to publish all {len(errors)} events",
            )

    return {
        "status": "ok" if not errors else "partial",
        "published": published_count,
        "failed": len(errors),
    }
