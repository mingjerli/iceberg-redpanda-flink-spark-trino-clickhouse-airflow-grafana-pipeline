#!/usr/bin/env python3
"""
Post Mock Data to Webhook Endpoints

Generates mock data using the datagen providers and posts them to the
ingestion API webhook endpoints for end-to-end testing.

Usage:
    # Post all data types (default: 10 each)
    python post_mock_data.py --url http://localhost:8000

    # Post specific counts
    python post_mock_data.py --url http://localhost:8000 \
        --shopify-customers 50 --shopify-orders 100 --hubspot-contacts 30

    # Post only Shopify data
    python post_mock_data.py --url http://localhost:8000 --source shopify

    # Dry run (generate but don't post)
    python post_mock_data.py --dry-run
"""

import asyncio
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
import httpx

# Add datagen to path
sys.path.insert(0, str(Path(__file__).parent.parent / "datagen"))

from providers.shopify_provider import ShopifyProvider
from providers.stripe_provider import StripeProvider
from providers.hubspot_provider import HubSpotProvider


class MockDataPoster:
    """Post generated mock data to webhook endpoints."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        skip_signature: bool = True,
        seed: Optional[int] = None,
    ):
        """
        Initialize the poster.

        Args:
            base_url: Base URL of the ingestion API (e.g., http://localhost:8000)
            timeout: HTTP request timeout in seconds
            skip_signature: If True, don't send HMAC signatures (requires server to skip validation)
            seed: Random seed for reproducibility
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.skip_signature = skip_signature

        # Initialize providers
        self.shopify = ShopifyProvider(seed=seed)
        self.stripe = StripeProvider(seed=seed)
        self.hubspot = HubSpotProvider(seed=seed)

        # Shared customer pool for entity resolution testing
        self._shared_customers: List[Dict] = []

        # Stats
        self.stats = {
            "posted": 0,
            "failed": 0,
            "by_endpoint": {},
        }

    def generate_shared_customer_pool(self, count: int = 30) -> List[Dict]:
        """Generate shared customers that will appear across sources."""
        self._shared_customers = []
        for _ in range(count):
            customer = {
                "email": self.shopify.fake.email(),
                "first_name": self.shopify.fake.first_name(),
                "last_name": self.shopify.fake.last_name(),
                "phone": self.shopify.fake.phone_number(),
                "company": self.shopify.fake.company() if self.shopify.fake.random.random() > 0.3 else None,
                "address": {
                    "address1": self.shopify.fake.street_address(),
                    "city": self.shopify.fake.city(),
                    "province": self.shopify.fake.state(),
                    "country": "United States",
                    "country_code": "US",
                    "zip": self.shopify.fake.zipcode(),
                },
                "created_at": self.shopify.fake.date_time_between(
                    start_date="-2y", end_date="-6M"
                ),
            }
            self._shared_customers.append(customer)
        return self._shared_customers

    def get_shared_customer(self) -> Optional[Dict]:
        """Get a random customer from the shared pool (30% chance)."""
        if self._shared_customers and self.shopify.fake.random.random() < 0.3:
            return self.shopify.fake.random.choice(self._shared_customers)
        return None

    async def post_webhook(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        payload: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Post a single webhook payload.

        Args:
            client: HTTP client
            endpoint: Webhook endpoint path (e.g., /webhooks/shopify/customers)
            payload: Webhook payload
            headers: Optional additional headers

        Returns:
            True if successful, False otherwise
        """
        url = f"{self.base_url}{endpoint}"
        request_headers = {"Content-Type": "application/json"}
        if headers:
            request_headers.update(headers)

        try:
            response = await client.post(
                url,
                json=payload,
                headers=request_headers,
                timeout=self.timeout,
            )

            if response.status_code == 200:
                self.stats["posted"] += 1
                self.stats["by_endpoint"][endpoint] = self.stats["by_endpoint"].get(endpoint, 0) + 1
                return True
            else:
                self.stats["failed"] += 1
                print(f"  Failed: {endpoint} - {response.status_code}: {response.text[:100]}")
                return False

        except Exception as e:
            self.stats["failed"] += 1
            print(f"  Error: {endpoint} - {type(e).__name__}: {e}")
            return False

    async def post_shopify_customers(
        self,
        client: httpx.AsyncClient,
        count: int,
        dry_run: bool = False,
    ) -> List[Dict]:
        """Generate and post Shopify customers."""
        print(f"\n  Shopify Customers: {count}")
        customers = []

        for i in range(count):
            customer = self.shopify.generate_customer(shared=self.get_shared_customer())
            customers.append(customer)

            if not dry_run:
                headers = {"X-Shopify-Topic": "customers/create"}
                success = await self.post_webhook(
                    client,
                    "/webhooks/shopify/customers",
                    customer,
                    headers,
                )
                if (i + 1) % 10 == 0:
                    print(f"    Posted {i + 1}/{count}...")

        return customers

    async def post_shopify_orders(
        self,
        client: httpx.AsyncClient,
        count: int,
        customers: List[Dict],
        dry_run: bool = False,
    ) -> List[Dict]:
        """Generate and post Shopify orders."""
        print(f"\n  Shopify Orders: {count}")
        orders = []

        # Generate some products first
        products = [self.shopify.generate_product() for _ in range(20)]

        for i in range(count):
            customer = self.shopify.fake.random.choice(customers) if customers else None
            order = self.shopify.generate_order(customer=customer, products=products)
            orders.append(order)

            if not dry_run:
                headers = {"X-Shopify-Topic": "orders/create"}
                success = await self.post_webhook(
                    client,
                    "/webhooks/shopify/orders",
                    order,
                    headers,
                )
                if (i + 1) % 10 == 0:
                    print(f"    Posted {i + 1}/{count}...")

        return orders

    async def post_stripe_customers(
        self,
        client: httpx.AsyncClient,
        count: int,
        dry_run: bool = False,
    ) -> List[Dict]:
        """Generate and post Stripe customers (via webhook events)."""
        print(f"\n  Stripe Customers: {count}")
        customers = []

        for i in range(count):
            customer = self.stripe.generate_customer(shared=self.get_shared_customer())
            customers.append(customer)

            if not dry_run:
                # Stripe webhooks wrap the object in an event structure
                event = {
                    "id": f"evt_{uuid.uuid4().hex[:24]}",
                    "object": "event",
                    "type": "customer.created",
                    "data": {"object": customer},
                    "created": int(datetime.now(timezone.utc).timestamp()),
                }
                success = await self.post_webhook(
                    client,
                    "/webhooks/stripe/webhook",
                    event,
                )
                if (i + 1) % 10 == 0:
                    print(f"    Posted {i + 1}/{count}...")

        return customers

    async def post_stripe_charges(
        self,
        client: httpx.AsyncClient,
        count: int,
        customers: List[Dict],
        dry_run: bool = False,
    ) -> List[Dict]:
        """Generate and post Stripe charges."""
        print(f"\n  Stripe Charges: {count}")
        charges = []

        for i in range(count):
            customer = self.stripe.fake.random.choice(customers) if customers else None
            charge = self.stripe.generate_charge(customer=customer)
            charges.append(charge)

            if not dry_run:
                event = {
                    "id": f"evt_{uuid.uuid4().hex[:24]}",
                    "object": "event",
                    "type": "charge.succeeded",
                    "data": {"object": charge},
                    "created": int(datetime.now(timezone.utc).timestamp()),
                }
                success = await self.post_webhook(
                    client,
                    "/webhooks/stripe/webhook",
                    event,
                )
                if (i + 1) % 10 == 0:
                    print(f"    Posted {i + 1}/{count}...")

        return charges

    async def post_hubspot_contacts(
        self,
        client: httpx.AsyncClient,
        count: int,
        dry_run: bool = False,
    ) -> List[Dict]:
        """Generate and post HubSpot contacts."""
        print(f"\n  HubSpot Contacts: {count}")
        contacts = []

        for i in range(count):
            contact = self.hubspot.generate_contact(shared=self.get_shared_customer())
            contacts.append(contact)

            if not dry_run:
                # HubSpot webhook format
                webhook_payload = {
                    "subscriptionType": "contact.creation",
                    "objectId": contact["id"],
                    "properties": contact.get("properties", {}),
                    "occurredAt": int(datetime.now(timezone.utc).timestamp() * 1000),
                }
                success = await self.post_webhook(
                    client,
                    "/webhooks/hubspot/webhook",
                    [webhook_payload],  # HubSpot sends array of events
                )
                if (i + 1) % 10 == 0:
                    print(f"    Posted {i + 1}/{count}...")

        return contacts

    async def run(
        self,
        source: str = "all",
        shopify_customers: int = 10,
        shopify_orders: int = 20,
        stripe_customers: int = 10,
        stripe_charges: int = 20,
        hubspot_contacts: int = 10,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Run the mock data posting.

        Args:
            source: Source to post (shopify, stripe, hubspot, or all)
            shopify_customers: Number of Shopify customers
            shopify_orders: Number of Shopify orders
            stripe_customers: Number of Stripe customers
            stripe_charges: Number of Stripe charges
            hubspot_contacts: Number of HubSpot contacts
            dry_run: If True, generate data but don't post

        Returns:
            Stats dictionary
        """
        print("=" * 60)
        print("Mock Data Poster - Iceberg Incremental Demo")
        print("=" * 60)
        print(f"Target: {self.base_url}")
        print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
        print(f"Source: {source}")
        print("=" * 60)

        # Generate shared customer pool for entity resolution
        shared_count = max(shopify_customers, stripe_customers, hubspot_contacts) // 3
        if shared_count > 0:
            print(f"\nGenerating shared customer pool ({shared_count} customers)...")
            self.generate_shared_customer_pool(shared_count)

        async with httpx.AsyncClient() as client:
            # Check health first
            if not dry_run:
                try:
                    health = await client.get(f"{self.base_url}/health", timeout=5.0)
                    if health.status_code != 200:
                        print(f"ERROR: Health check failed: {health.status_code}")
                        return self.stats
                    print("Health check: OK")
                except Exception as e:
                    print(f"ERROR: Cannot connect to {self.base_url}: {e}")
                    return self.stats

            # Post data by source
            shopify_customer_list = []
            stripe_customer_list = []

            if source in ("all", "shopify"):
                print("\n[SHOPIFY]")
                shopify_customer_list = await self.post_shopify_customers(
                    client, shopify_customers, dry_run
                )
                await self.post_shopify_orders(
                    client, shopify_orders, shopify_customer_list, dry_run
                )

            if source in ("all", "stripe"):
                print("\n[STRIPE]")
                stripe_customer_list = await self.post_stripe_customers(
                    client, stripe_customers, dry_run
                )
                await self.post_stripe_charges(
                    client, stripe_charges, stripe_customer_list, dry_run
                )

            if source in ("all", "hubspot"):
                print("\n[HUBSPOT]")
                await self.post_hubspot_contacts(client, hubspot_contacts, dry_run)

        # Print summary
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print(f"Total Posted: {self.stats['posted']}")
        print(f"Total Failed: {self.stats['failed']}")
        if self.stats["by_endpoint"]:
            print("\nBy Endpoint:")
            for endpoint, count in sorted(self.stats["by_endpoint"].items()):
                print(f"  {endpoint}: {count}")
        print("=" * 60)

        return self.stats


@click.command()
@click.option(
    "--url",
    default="http://localhost:8000",
    help="Ingestion API base URL",
)
@click.option(
    "--source",
    type=click.Choice(["shopify", "stripe", "hubspot", "all"]),
    default="all",
    help="Data source to post",
)
@click.option(
    "--shopify-customers",
    default=10,
    type=int,
    help="Number of Shopify customers",
)
@click.option(
    "--shopify-orders",
    default=20,
    type=int,
    help="Number of Shopify orders",
)
@click.option(
    "--stripe-customers",
    default=10,
    type=int,
    help="Number of Stripe customers",
)
@click.option(
    "--stripe-charges",
    default=20,
    type=int,
    help="Number of Stripe charges",
)
@click.option(
    "--hubspot-contacts",
    default=10,
    type=int,
    help="Number of HubSpot contacts",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Random seed for reproducibility",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Generate data but don't post",
)
def main(
    url: str,
    source: str,
    shopify_customers: int,
    shopify_orders: int,
    stripe_customers: int,
    stripe_charges: int,
    hubspot_contacts: int,
    seed: Optional[int],
    dry_run: bool,
):
    """Post mock data to webhook endpoints for testing."""
    poster = MockDataPoster(base_url=url, seed=seed)

    asyncio.run(
        poster.run(
            source=source,
            shopify_customers=shopify_customers,
            shopify_orders=shopify_orders,
            stripe_customers=stripe_customers,
            stripe_charges=stripe_charges,
            hubspot_contacts=hubspot_contacts,
            dry_run=dry_run,
        )
    )


if __name__ == "__main__":
    main()
