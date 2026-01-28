#!/usr/bin/env python3
"""
Webhook Simulator for Iceberg Incremental Demo

Sends generated mock data as webhook events to the ingestion API.
This simulates real webhook traffic from Shopify, Stripe, and HubSpot.

Usage:
    # Send 10 Shopify orders (default)
    python simulate_webhooks.py

    # Send specific events
    python simulate_webhooks.py --source shopify --entity orders --count 100

    # Send all entity types
    python simulate_webhooks.py --source all --count 50

    # Continuous mode (sends events at intervals)
    python simulate_webhooks.py --continuous --interval 5
"""

import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import click
import requests
from tqdm import tqdm

# Add parent directory to path for provider imports
import sys
sys.path.insert(0, str(Path(__file__).parent))

from providers.shopify_provider import ShopifyProvider
from providers.stripe_provider import StripeProvider
from providers.hubspot_provider import HubSpotProvider


class WebhookSimulator:
    """Simulates webhook traffic to the ingestion API."""

    def __init__(
        self,
        api_base_url: str = "http://localhost:8090",
        seed: Optional[int] = None,
    ):
        """
        Initialize the simulator.

        Args:
            api_base_url: Base URL of the ingestion API
            seed: Random seed for reproducibility
        """
        self.api_base_url = api_base_url.rstrip("/")
        self.shopify = ShopifyProvider(seed=seed)
        self.stripe = StripeProvider(seed=seed)
        self.hubspot = HubSpotProvider(seed=seed)

        # Shared customer pool for cross-source consistency
        self._customers = []

    def _generate_shared_customers(self, count: int = 50) -> None:
        """Generate a pool of shared customers."""
        for _ in range(count):
            customer = {
                "email": self.shopify.fake.email(),
                "first_name": self.shopify.fake.first_name(),
                "last_name": self.shopify.fake.last_name(),
                "phone": self.shopify.fake.phone_number(),
                "company": self.shopify.fake.company() if random.random() > 0.3 else None,
                "created_at": self.shopify.fake.date_time_between(start_date="-2y", end_date="-1d"),
            }
            self._customers.append(customer)

    def _get_shared_customer(self) -> Optional[Dict]:
        """Get a random shared customer."""
        if self._customers:
            return random.choice(self._customers)
        return None

    def send_shopify_order(self) -> bool:
        """Generate and send a Shopify order webhook."""
        order = self.shopify.generate_order(
            customer=self.shopify.generate_customer(shared=self._get_shared_customer()),
            products=[self.shopify.generate_product() for _ in range(3)],
        )

        response = requests.post(
            f"{self.api_base_url}/webhooks/shopify/orders",
            json=order,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Topic": "orders/create",
                "X-Shopify-Webhook-Id": f"wh_{random.randint(10000000, 99999999)}",
            },
        )

        return response.status_code == 200

    def send_shopify_customer(self) -> bool:
        """Generate and send a Shopify customer webhook."""
        customer = self.shopify.generate_customer(shared=self._get_shared_customer())

        response = requests.post(
            f"{self.api_base_url}/webhooks/shopify/customers",
            json=customer,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Topic": "customers/create",
                "X-Shopify-Webhook-Id": f"wh_{random.randint(10000000, 99999999)}",
            },
        )

        return response.status_code == 200

    def send_stripe_customer(self) -> bool:
        """Generate and send a Stripe customer.created event."""
        customer = self.stripe.generate_customer(shared=self._get_shared_customer())

        # Wrap in Stripe event envelope
        event = {
            "id": self.stripe._stripe_id("evt", "event"),
            "object": "event",
            "api_version": "2024-12-18.acacia",
            "created": int(datetime.now(timezone.utc).timestamp()),
            "data": {"object": customer},
            "livemode": True,
            "pending_webhooks": 1,
            "type": "customer.created",
        }

        response = requests.post(
            f"{self.api_base_url}/webhooks/stripe/webhook",
            json=event,
            headers={
                "Content-Type": "application/json",
            },
        )

        return response.status_code == 200

    def send_stripe_charge(self) -> bool:
        """Generate and send a Stripe charge.succeeded event."""
        customer = self.stripe.generate_customer(shared=self._get_shared_customer())
        charge = self.stripe.generate_charge(customer=customer)

        # Wrap in Stripe event envelope
        event = {
            "id": self.stripe._stripe_id("evt", "event"),
            "object": "event",
            "api_version": "2024-12-18.acacia",
            "created": int(datetime.now(timezone.utc).timestamp()),
            "data": {"object": charge},
            "livemode": True,
            "pending_webhooks": 1,
            "type": "charge.succeeded",
        }

        response = requests.post(
            f"{self.api_base_url}/webhooks/stripe/webhook",
            json=event,
            headers={
                "Content-Type": "application/json",
            },
        )

        return response.status_code == 200

    def send_hubspot_contact(self) -> bool:
        """Generate and send a HubSpot contact.creation event."""
        contact = self.hubspot.generate_contact(shared=self._get_shared_customer())

        # HubSpot sends an array of events
        events = [{
            "subscriptionId": random.randint(100000, 999999),
            "portalId": random.randint(10000000, 99999999),
            "appId": random.randint(100000, 999999),
            "occurredAt": int(datetime.now(timezone.utc).timestamp() * 1000),
            "subscriptionType": "contact.creation",
            "attemptNumber": 0,
            "objectId": contact["id"],
            "eventId": random.randint(1000000000, 9999999999),
            # Include contact properties for richer data
            **contact,
        }]

        response = requests.post(
            f"{self.api_base_url}/webhooks/hubspot/webhook",
            json=events,
            headers={
                "Content-Type": "application/json",
            },
        )

        return response.status_code == 200

    def simulate(
        self,
        source: str = "all",
        entity: Optional[str] = None,
        count: int = 10,
        show_progress: bool = True,
    ) -> Dict[str, int]:
        """
        Simulate webhook events.

        Args:
            source: Data source (shopify, stripe, hubspot, all)
            entity: Specific entity type (orders, customers, etc.)
            count: Number of events to generate
            show_progress: Show progress bar

        Returns:
            Dict with success/failure counts
        """
        # Generate shared customer pool
        self._generate_shared_customers(min(count, 50))

        results = {"success": 0, "failed": 0}

        # Build list of send functions to call
        senders = []
        if source in ("all", "shopify"):
            if entity is None or entity == "orders":
                senders.append(("shopify_orders", self.send_shopify_order))
            if entity is None or entity == "customers":
                senders.append(("shopify_customers", self.send_shopify_customer))
        if source in ("all", "stripe"):
            if entity is None or entity == "customers":
                senders.append(("stripe_customers", self.send_stripe_customer))
            if entity is None or entity == "charges":
                senders.append(("stripe_charges", self.send_stripe_charge))
        if source in ("all", "hubspot"):
            if entity is None or entity == "contacts":
                senders.append(("hubspot_contacts", self.send_hubspot_contact))

        if not senders:
            print(f"No matching senders for source={source}, entity={entity}")
            return results

        # Send events
        iterator = range(count)
        if show_progress:
            iterator = tqdm(iterator, desc="Sending webhooks")

        for _ in iterator:
            # Randomly select a sender
            name, sender = random.choice(senders)
            try:
                if sender():
                    results["success"] += 1
                else:
                    results["failed"] += 1
            except requests.RequestException as e:
                results["failed"] += 1
                if not show_progress:
                    print(f"Failed to send {name}: {e}")

        return results


@click.command()
@click.option(
    "--api-url",
    default=os.getenv("INGESTION_API_URL", "http://localhost:8090"),
    help="Ingestion API base URL",
)
@click.option(
    "--source",
    type=click.Choice(["shopify", "stripe", "hubspot", "all"]),
    default="all",
    help="Data source to simulate",
)
@click.option(
    "--entity",
    default=None,
    help="Specific entity type (orders, customers, charges, contacts)",
)
@click.option(
    "--count",
    type=int,
    default=10,
    help="Number of events to send",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Random seed for reproducibility",
)
@click.option(
    "--continuous",
    is_flag=True,
    help="Run continuously",
)
@click.option(
    "--interval",
    type=float,
    default=1.0,
    help="Seconds between events in continuous mode",
)
def main(
    api_url: str,
    source: str,
    entity: Optional[str],
    count: int,
    seed: Optional[int],
    continuous: bool,
    interval: float,
):
    """Simulate webhook traffic to the ingestion API."""
    print("=" * 60)
    print("Webhook Simulator - Iceberg Incremental Demo")
    print("=" * 60)
    print(f"  API URL: {api_url}")
    print(f"  Source: {source}")
    print(f"  Entity: {entity or 'all'}")
    print(f"  Count: {count}")
    print(f"  Continuous: {continuous}")
    print("=" * 60)

    simulator = WebhookSimulator(api_base_url=api_url, seed=seed)

    # Test connectivity
    try:
        response = requests.get(f"{api_url}/health", timeout=5)
        if response.status_code != 200:
            print(f"Warning: Health check returned {response.status_code}")
    except requests.RequestException as e:
        print(f"Error: Cannot connect to {api_url}: {e}")
        print("Make sure the ingestion API is running.")
        return

    if continuous:
        print(f"\nRunning continuously (interval: {interval}s). Press Ctrl+C to stop.\n")
        total = {"success": 0, "failed": 0}
        try:
            while True:
                result = simulator.simulate(
                    source=source,
                    entity=entity,
                    count=1,
                    show_progress=False,
                )
                total["success"] += result["success"]
                total["failed"] += result["failed"]
                print(
                    f"\rSent: {total['success']} success, {total['failed']} failed",
                    end="",
                    flush=True,
                )
                time.sleep(interval)
        except KeyboardInterrupt:
            print(f"\n\nStopped. Total: {total['success']} success, {total['failed']} failed")
    else:
        result = simulator.simulate(
            source=source,
            entity=entity,
            count=count,
            show_progress=True,
        )
        print(f"\nResults: {result['success']} success, {result['failed']} failed")


if __name__ == "__main__":
    main()
