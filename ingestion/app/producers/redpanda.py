"""
Redpanda/Kafka producer for publishing webhook events.

Uses aiokafka for async production with proper error handling,
batching, and delivery guarantees.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from ..config import Settings, get_settings

logger = logging.getLogger(__name__)


class RedpandaProducer:
    """
    Async Kafka producer for publishing webhook events to Redpanda.

    Provides:
    - Async message production
    - Automatic serialization to JSON
    - Metadata enrichment
    - Error handling with retries
    """

    def __init__(self, settings: Optional[Settings] = None):
        """
        Initialize the producer.

        Args:
            settings: Optional settings override (uses env vars by default)
        """
        self.settings = settings or get_settings()
        self._producer: Optional[AIOKafkaProducer] = None
        self._started = False

    async def start(self) -> None:
        """Start the producer connection."""
        if self._started:
            return

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            acks=self.settings.kafka_producer_acks,
            enable_idempotence=True,  # Exactly-once semantics
            max_batch_size=16384,
            linger_ms=5,  # Small batch window for low latency
            retry_backoff_ms=100,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

        await self._producer.start()
        self._started = True
        logger.info(
            f"Redpanda producer started, "
            f"brokers={self.settings.kafka_bootstrap_servers}"
        )

    async def stop(self) -> None:
        """Stop the producer connection."""
        if self._producer and self._started:
            await self._producer.stop()
            self._started = False
            logger.info("Redpanda producer stopped")

    async def send(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Send a message to a topic.

        Args:
            topic: Target topic name
            value: Message payload (will be JSON serialized)
            key: Optional message key for partitioning
            headers: Optional message headers

        Returns:
            True if send was successful, False otherwise
        """
        if not self._started or not self._producer:
            logger.error("Producer not started, cannot send message")
            return False

        # Convert headers dict to list of tuples with bytes values
        kafka_headers = None
        if headers:
            kafka_headers = [
                (k, v.encode("utf-8")) for k, v in headers.items()
            ]

        try:
            # Send with retries handled by aiokafka
            result = await self._producer.send_and_wait(
                topic=topic,
                value=value,
                key=key,
                headers=kafka_headers,
            )
            logger.debug(
                f"Message sent to {topic}, "
                f"partition={result.partition}, offset={result.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            return False

    async def send_webhook_event(
        self,
        topic: str,
        event_data: Dict[str, Any],
        source: str,
        event_type: str,
        event_id: Optional[str] = None,
        key: Optional[str] = None,
    ) -> bool:
        """
        Send a webhook event with standard metadata.

        Enriches the event with:
        - _webhook_received_at: Current UTC timestamp
        - _source: Data source name
        - _event_type: Webhook event type

        Args:
            topic: Target topic name
            event_data: Raw webhook payload
            source: Source system (shopify, stripe, hubspot)
            event_type: Webhook event type (e.g., orders/create)
            event_id: Optional unique event ID for deduplication
            key: Optional message key (e.g., order ID)

        Returns:
            True if send was successful, False otherwise
        """
        # Enrich with metadata
        enriched_event = {
            **event_data,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat(),
            "_source": source,
            "_event_type": event_type,
        }

        headers = {
            "source": source,
            "event_type": event_type,
        }
        if event_id:
            headers["event_id"] = event_id

        return await self.send(
            topic=topic,
            value=enriched_event,
            key=key,
            headers=headers,
        )


# Global producer instance (initialized on startup)
_producer: Optional[RedpandaProducer] = None


async def get_producer() -> RedpandaProducer:
    """
    Get the global producer instance.

    Creates and starts the producer if not already initialized.

    Returns:
        Initialized RedpandaProducer instance
    """
    global _producer
    if _producer is None:
        _producer = RedpandaProducer()
        await _producer.start()
    return _producer


async def shutdown_producer() -> None:
    """Shutdown the global producer instance."""
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None
