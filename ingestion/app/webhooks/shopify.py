"""
Shopify webhook handlers.

Receives webhooks from Shopify for:
- orders/create, orders/updated
- customers/create, customers/update
- products/create, products/update

See: https://shopify.dev/docs/api/admin-rest/2024-10/resources/webhook
"""

import logging
from typing import Any, Dict

from fastapi import APIRouter, Header, HTTPException, Request, status

from ..config import get_settings, get_topic_name
from ..producers import get_producer
from ..validators import validate_shopify_signature

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/shopify", tags=["shopify"])


async def _process_shopify_webhook(
    request: Request,
    topic_suffix: str,
    hmac_header: str | None,
) -> Dict[str, Any]:
    """
    Common processing for all Shopify webhooks.

    Args:
        request: FastAPI request object
        topic_suffix: Topic suffix (orders, customers, products)
        hmac_header: X-Shopify-Hmac-Sha256 header value

    Returns:
        Response dict with status

    Raises:
        HTTPException: On validation or processing errors
    """
    settings = get_settings()
    body = await request.body()

    # Validate HMAC signature
    if not settings.skip_signature_validation:
        if not settings.shopify_webhook_secret:
            logger.warning("Shopify webhook secret not configured")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Webhook secret not configured",
            )

        if not hmac_header:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing X-Shopify-Hmac-Sha256 header",
            )

        if not validate_shopify_signature(body, hmac_header, settings.shopify_webhook_secret):
            logger.warning("Invalid Shopify webhook signature")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid signature",
            )

    # Parse webhook topic from header
    webhook_topic = request.headers.get("X-Shopify-Topic", "unknown")

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

    # Add webhook metadata
    payload["_webhook_topic"] = webhook_topic

    # Get record key for partitioning
    record_key = str(payload.get("id", ""))

    # Publish to Redpanda
    producer = await get_producer()
    topic = get_topic_name("shopify", topic_suffix)

    success = await producer.send_webhook_event(
        topic=topic,
        event_data=payload,
        source="shopify",
        event_type=webhook_topic,
        event_id=request.headers.get("X-Shopify-Webhook-Id"),
        key=record_key,
    )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to publish event",
        )

    logger.info(f"Shopify {webhook_topic} event published to {topic}, id={record_key}")

    return {"status": "ok", "topic": topic, "id": record_key}


@router.post("/orders")
async def shopify_orders_webhook(
    request: Request,
    x_shopify_hmac_sha256: str | None = Header(None),
) -> Dict[str, Any]:
    """
    Handle Shopify order webhooks.

    Webhook topics:
    - orders/create: New order created
    - orders/updated: Order updated
    - orders/cancelled: Order cancelled
    - orders/fulfilled: Order fulfilled
    - orders/paid: Order paid
    """
    return await _process_shopify_webhook(request, "orders", x_shopify_hmac_sha256)


@router.post("/customers")
async def shopify_customers_webhook(
    request: Request,
    x_shopify_hmac_sha256: str | None = Header(None),
) -> Dict[str, Any]:
    """
    Handle Shopify customer webhooks.

    Webhook topics:
    - customers/create: New customer created
    - customers/update: Customer updated
    - customers/delete: Customer deleted
    """
    return await _process_shopify_webhook(request, "customers", x_shopify_hmac_sha256)


@router.post("/products")
async def shopify_products_webhook(
    request: Request,
    x_shopify_hmac_sha256: str | None = Header(None),
) -> Dict[str, Any]:
    """
    Handle Shopify product webhooks.

    Webhook topics:
    - products/create: New product created
    - products/update: Product updated
    - products/delete: Product deleted
    """
    return await _process_shopify_webhook(request, "products", x_shopify_hmac_sha256)
