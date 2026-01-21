"""
Configuration management for the ingestion service.

All configuration is loaded from environment variables with sensible defaults
for local development.
"""

import os
from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Service settings
    service_name: str = Field(default="ingestion-api", description="Service name for logging")
    debug: bool = Field(default=False, description="Enable debug mode")
    log_level: str = Field(default="INFO", description="Logging level")

    # Redpanda/Kafka settings
    kafka_bootstrap_servers: str = Field(
        default="redpanda:9092",
        description="Kafka/Redpanda broker addresses"
    )
    kafka_producer_acks: str = Field(
        default="all",
        description="Producer acknowledgment level"
    )
    kafka_producer_retries: int = Field(
        default=3,
        description="Number of retries for failed sends"
    )

    # Topic configuration (prefixed by source)
    topic_prefix: str = Field(
        default="",
        description="Optional prefix for all topics"
    )

    # Shopify webhook settings
    shopify_webhook_secret: Optional[str] = Field(
        default=None,
        description="Shopify webhook HMAC secret for signature validation"
    )
    shopify_enabled: bool = Field(
        default=True,
        description="Enable Shopify webhook endpoints"
    )

    # Stripe webhook settings
    stripe_webhook_secret: Optional[str] = Field(
        default=None,
        description="Stripe webhook signing secret (whsec_xxx)"
    )
    stripe_enabled: bool = Field(
        default=True,
        description="Enable Stripe webhook endpoints"
    )

    # HubSpot webhook settings
    hubspot_client_secret: Optional[str] = Field(
        default=None,
        description="HubSpot client secret for signature validation"
    )
    hubspot_enabled: bool = Field(
        default=True,
        description="Enable HubSpot webhook endpoints"
    )

    # Signature validation
    skip_signature_validation: bool = Field(
        default=False,
        description="Skip HMAC signature validation (for testing only)"
    )

    # Rate limiting
    rate_limit_enabled: bool = Field(
        default=False,
        description="Enable rate limiting"
    )
    rate_limit_requests_per_minute: int = Field(
        default=1000,
        description="Max requests per minute when rate limiting is enabled"
    )

    class Config:
        env_prefix = "INGESTION_"
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


def get_topic_name(source: str, entity: str) -> str:
    """
    Build topic name from source and entity.

    Args:
        source: Data source (shopify, stripe, hubspot)
        entity: Entity type (orders, customers, etc.)

    Returns:
        Full topic name (e.g., shopify.orders or prefix.shopify.orders)
    """
    settings = get_settings()
    base_name = f"{source}.{entity}"
    if settings.topic_prefix:
        return f"{settings.topic_prefix}.{base_name}"
    return base_name
