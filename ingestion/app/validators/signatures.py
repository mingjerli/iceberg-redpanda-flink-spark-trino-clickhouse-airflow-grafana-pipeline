"""
HMAC signature validation for webhook payloads.

Each vendor has a slightly different signature scheme:
- Shopify: HMAC-SHA256, base64 encoded, in X-Shopify-Hmac-Sha256 header
- Stripe: HMAC-SHA256 with timestamp, in Stripe-Signature header
- HubSpot: HMAC-SHA256 v3 or v2, in X-HubSpot-Signature-v3 header
"""

import hashlib
import hmac
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


def validate_shopify_signature(
    payload: bytes,
    signature: str,
    secret: str,
) -> bool:
    """
    Validate Shopify webhook HMAC signature.

    Shopify sends webhooks with an HMAC-SHA256 signature in the
    X-Shopify-Hmac-Sha256 header, base64 encoded.

    Args:
        payload: Raw request body bytes
        signature: Base64-encoded HMAC from header
        secret: Shopify webhook secret

    Returns:
        True if signature is valid, False otherwise
    """
    import base64

    try:
        # Compute expected signature
        computed = hmac.new(
            secret.encode("utf-8"),
            payload,
            hashlib.sha256
        ).digest()
        computed_b64 = base64.b64encode(computed).decode("utf-8")

        # Compare signatures (constant time to prevent timing attacks)
        return hmac.compare_digest(computed_b64, signature)
    except Exception as e:
        logger.warning(f"Shopify signature validation failed: {e}")
        return False


def validate_stripe_signature(
    payload: bytes,
    signature_header: str,
    secret: str,
    tolerance: int = 300,
) -> bool:
    """
    Validate Stripe webhook signature.

    Stripe uses a more complex scheme with timestamp to prevent replay attacks:
    - Header format: t=timestamp,v1=signature,v0=legacy_signature
    - Signature is HMAC-SHA256 of "{timestamp}.{payload}"

    Args:
        payload: Raw request body bytes
        signature_header: Full Stripe-Signature header value
        secret: Stripe webhook signing secret (whsec_xxx)
        tolerance: Max age in seconds for timestamp (default 5 minutes)

    Returns:
        True if signature is valid, False otherwise
    """
    try:
        # Parse the signature header
        elements = {}
        for item in signature_header.split(","):
            key, value = item.split("=", 1)
            elements[key] = value

        timestamp = int(elements.get("t", 0))
        signature = elements.get("v1", "")

        if not timestamp or not signature:
            logger.warning("Stripe signature missing timestamp or v1 signature")
            return False

        # Check timestamp tolerance (prevent replay attacks)
        current_time = int(time.time())
        if abs(current_time - timestamp) > tolerance:
            logger.warning(f"Stripe signature timestamp too old: {timestamp}")
            return False

        # Compute expected signature
        signed_payload = f"{timestamp}.{payload.decode('utf-8')}"
        computed = hmac.new(
            secret.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(computed, signature)
    except Exception as e:
        logger.warning(f"Stripe signature validation failed: {e}")
        return False


def validate_hubspot_signature(
    payload: bytes,
    signature: str,
    secret: str,
    request_uri: str,
    http_method: str = "POST",
    timestamp: Optional[str] = None,
    version: str = "v3",
) -> bool:
    """
    Validate HubSpot webhook signature.

    HubSpot v3 signature (recommended):
    - HMAC-SHA256 of "{http_method}{request_uri}{payload}{timestamp}"

    HubSpot v2 signature (legacy):
    - HMAC-SHA256 of "{client_secret}{payload}"

    Args:
        payload: Raw request body bytes
        signature: Signature from X-HubSpot-Signature-v3 or v2 header
        secret: HubSpot client secret
        request_uri: Full request URI including query string
        http_method: HTTP method (usually POST)
        timestamp: Timestamp from X-HubSpot-Request-Timestamp header (v3 only)
        version: Signature version ("v3" or "v2")

    Returns:
        True if signature is valid, False otherwise
    """
    try:
        if version == "v3":
            if not timestamp:
                logger.warning("HubSpot v3 signature requires timestamp")
                return False

            # v3: HMAC-SHA256(method + uri + body + timestamp)
            source_string = (
                f"{http_method}{request_uri}"
                f"{payload.decode('utf-8')}{timestamp}"
            )
            computed = hmac.new(
                secret.encode("utf-8"),
                source_string.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()
        else:
            # v2: HMAC-SHA256(secret + body)
            source_string = secret + payload.decode("utf-8")
            computed = hashlib.sha256(source_string.encode("utf-8")).hexdigest()

        return hmac.compare_digest(computed, signature)
    except Exception as e:
        logger.warning(f"HubSpot signature validation failed: {e}")
        return False


def compute_shopify_signature(payload: bytes, secret: str) -> str:
    """
    Compute Shopify signature for testing purposes.

    Args:
        payload: Request body bytes
        secret: Webhook secret

    Returns:
        Base64-encoded HMAC-SHA256 signature
    """
    import base64

    computed = hmac.new(
        secret.encode("utf-8"),
        payload,
        hashlib.sha256
    ).digest()
    return base64.b64encode(computed).decode("utf-8")


def compute_stripe_signature(payload: bytes, secret: str, timestamp: int) -> str:
    """
    Compute Stripe signature for testing purposes.

    Args:
        payload: Request body bytes
        secret: Webhook signing secret
        timestamp: Unix timestamp

    Returns:
        Full Stripe-Signature header value
    """
    signed_payload = f"{timestamp}.{payload.decode('utf-8')}"
    signature = hmac.new(
        secret.encode("utf-8"),
        signed_payload.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return f"t={timestamp},v1={signature}"
