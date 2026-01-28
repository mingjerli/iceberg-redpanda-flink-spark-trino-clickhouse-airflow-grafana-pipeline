"""
Iceberg Incremental Demo - Ingestion API

FastAPI application for receiving webhooks from Shopify, Stripe, and HubSpot,
validating signatures, and publishing events to Redpanda.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator

from .config import get_settings
from .producers.redpanda import get_producer, shutdown_producer
from .webhooks import shopify_router, stripe_router, hubspot_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle.

    - Startup: Initialize Redpanda producer
    - Shutdown: Close producer connections
    """
    settings = get_settings()

    # Startup
    logger.info(f"Starting {settings.service_name}...")
    logger.info(f"Kafka brokers: {settings.kafka_bootstrap_servers}")
    logger.info(f"Signature validation: {'disabled' if settings.skip_signature_validation else 'enabled'}")

    # Initialize producer
    try:
        await get_producer()
        logger.info("Redpanda producer initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Redpanda producer: {e}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down...")
    await shutdown_producer()
    logger.info("Shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Iceberg Incremental Demo - Ingestion API",
    description="""
    Webhook ingestion service for receiving events from:
    - Shopify (orders, customers, products)
    - Stripe (customers, charges, payments)
    - HubSpot (contacts, companies, deals)

    Events are validated (HMAC signatures) and published to Redpanda topics.
    """,
    version="0.1.0",
    lifespan=lifespan,
)

# Add CORS middleware (for testing from browser)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Prometheus metrics instrumentation
# Exposes /metrics endpoint for Prometheus scraping
Instrumentator().instrument(app).expose(app)


# Register webhook routers
settings = get_settings()
if settings.shopify_enabled:
    app.include_router(shopify_router, prefix="/webhooks")
if settings.stripe_enabled:
    app.include_router(stripe_router, prefix="/webhooks")
if settings.hubspot_enabled:
    app.include_router(hubspot_router, prefix="/webhooks")


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint for container orchestration."""
    return {
        "status": "healthy",
        "service": get_settings().service_name,
    }


@app.get("/ready")
async def readiness_check() -> Dict[str, Any]:
    """
    Readiness check endpoint.

    Verifies that the Redpanda producer is connected.
    """
    try:
        producer = await get_producer()
        if producer._started:
            return {"status": "ready"}
        else:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "not_ready", "reason": "producer not started"},
            )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "not_ready", "reason": str(e)},
        )


@app.get("/")
async def root() -> Dict[str, Any]:
    """Root endpoint with API information."""
    settings = get_settings()
    return {
        "service": settings.service_name,
        "version": "0.1.0",
        "endpoints": {
            "shopify_orders": "/webhooks/shopify/orders" if settings.shopify_enabled else None,
            "shopify_customers": "/webhooks/shopify/customers" if settings.shopify_enabled else None,
            "shopify_products": "/webhooks/shopify/products" if settings.shopify_enabled else None,
            "stripe_webhook": "/webhooks/stripe/webhook" if settings.stripe_enabled else None,
            "hubspot_webhook": "/webhooks/hubspot/webhook" if settings.hubspot_enabled else None,
            "health": "/health",
            "ready": "/ready",
        },
    }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"},
    )
