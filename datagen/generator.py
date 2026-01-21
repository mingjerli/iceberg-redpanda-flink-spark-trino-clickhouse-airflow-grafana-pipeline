#!/usr/bin/env python3
"""
Mock Data Generator for Iceberg Incremental Demo

Generates realistic mock data matching Shopify, Stripe, and HubSpot API schemas.
Data can be output as JSON files (webhook format) or streamed to Redpanda/Kafka.

Usage:
    python generator.py --source shopify --entity orders --count 1000
    python generator.py --source all --count 500 --output-dir ./output
    python generator.py --stream --kafka-broker localhost:9092
"""

import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
import orjson
from tqdm import tqdm

from providers.shopify_provider import ShopifyProvider
from providers.stripe_provider import StripeProvider
from providers.hubspot_provider import HubSpotProvider


class DataGenerator:
    """Main orchestrator for generating mock data across all sources."""

    def __init__(self, seed: Optional[int] = None):
        """Initialize generators with optional seed for reproducibility."""
        self.seed = seed
        self.shopify = ShopifyProvider(seed=seed)
        self.stripe = StripeProvider(seed=seed)
        self.hubspot = HubSpotProvider(seed=seed)

        # Shared customer pool for cross-source entity resolution demo
        self._shared_customers: List[Dict] = []

    def generate_shared_customer_pool(self, count: int = 100) -> List[Dict]:
        """
        Generate a pool of shared customers that will appear across sources.
        This simulates real-world scenarios where the same person exists in
        Shopify, Stripe, and HubSpot with slightly different data.
        """
        self._shared_customers = []
        for _ in range(count):
            customer = {
                "email": self.shopify.fake.email(),
                "first_name": self.shopify.fake.first_name(),
                "last_name": self.shopify.fake.last_name(),
                "phone": self.shopify.fake.phone_number(),
                "company": self.shopify.fake.company() if random.random() > 0.3 else None,
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
        """Get a random customer from the shared pool."""
        if self._shared_customers:
            return random.choice(self._shared_customers)
        return None

    def generate_shopify_data(
        self,
        customers: int = 100,
        products: int = 50,
        orders: int = 500,
    ) -> Dict[str, List[Dict]]:
        """Generate complete Shopify dataset."""
        # Generate base entities
        customer_list = [
            self.shopify.generate_customer(shared=self.get_shared_customer())
            for _ in tqdm(range(customers), desc="Shopify Customers")
        ]

        product_list = [
            self.shopify.generate_product()
            for _ in tqdm(range(products), desc="Shopify Products")
        ]

        # Generate orders referencing customers and products
        order_list = [
            self.shopify.generate_order(
                customer=random.choice(customer_list) if customer_list else None,
                products=product_list,
            )
            for _ in tqdm(range(orders), desc="Shopify Orders")
        ]

        # Extract line items from orders
        line_items = []
        for order in order_list:
            for item in order.get("line_items", []):
                item["order_id"] = order["id"]
                item["_extracted_at"] = datetime.now(timezone.utc).isoformat()
                line_items.append(item)

        return {
            "customers": customer_list,
            "products": product_list,
            "orders": order_list,
            "line_items": line_items,
        }

    def generate_stripe_data(
        self,
        customers: int = 100,
        charges: int = 300,
        subscriptions: int = 50,
    ) -> Dict[str, List[Dict]]:
        """Generate complete Stripe dataset."""
        customer_list = [
            self.stripe.generate_customer(shared=self.get_shared_customer())
            for _ in tqdm(range(customers), desc="Stripe Customers")
        ]

        # Generate charges linked to customers
        charge_list = [
            self.stripe.generate_charge(
                customer=random.choice(customer_list) if customer_list else None
            )
            for _ in tqdm(range(charges), desc="Stripe Charges")
        ]

        # Generate subscriptions for a subset of customers
        subscription_customers = random.sample(
            customer_list, min(subscriptions, len(customer_list))
        )
        subscription_list = [
            self.stripe.generate_subscription(customer=cust)
            for cust in tqdm(subscription_customers, desc="Stripe Subscriptions")
        ]

        # Generate some refunds (10% of charges)
        refund_charges = random.sample(charge_list, len(charge_list) // 10)
        refund_list = [
            self.stripe.generate_refund(charge=charge)
            for charge in tqdm(refund_charges, desc="Stripe Refunds")
        ]

        # Generate payment intents
        payment_intent_list = [
            self.stripe.generate_payment_intent(
                customer=random.choice(customer_list) if customer_list else None
            )
            for _ in tqdm(range(charges), desc="Stripe Payment Intents")
        ]

        return {
            "customers": customer_list,
            "charges": charge_list,
            "subscriptions": subscription_list,
            "refunds": refund_list,
            "payment_intents": payment_intent_list,
        }

    def generate_hubspot_data(
        self,
        contacts: int = 100,
        companies: int = 30,
        deals: int = 80,
    ) -> Dict[str, List[Dict]]:
        """Generate complete HubSpot dataset."""
        # Generate companies first
        company_list = [
            self.hubspot.generate_company()
            for _ in tqdm(range(companies), desc="HubSpot Companies")
        ]

        # Generate contacts, some linked to companies
        contact_list = [
            self.hubspot.generate_contact(
                shared=self.get_shared_customer(),
                company=random.choice(company_list) if random.random() > 0.3 else None,
            )
            for _ in tqdm(range(contacts), desc="HubSpot Contacts")
        ]

        # Generate deals linked to contacts and companies
        deal_list = [
            self.hubspot.generate_deal(
                contact=random.choice(contact_list) if contact_list else None,
                company=random.choice(company_list) if random.random() > 0.5 else None,
            )
            for _ in tqdm(range(deals), desc="HubSpot Deals")
        ]

        return {
            "contacts": contact_list,
            "companies": company_list,
            "deals": deal_list,
        }

    def generate_all(
        self,
        scale: float = 1.0,
        shared_customer_ratio: float = 0.3,
    ) -> Dict[str, Dict[str, List[Dict]]]:
        """
        Generate data for all sources.

        Args:
            scale: Multiplier for default counts (1.0 = default, 2.0 = double)
            shared_customer_ratio: Ratio of customers that appear across sources
        """
        # Calculate shared customer pool size
        base_customers = int(100 * scale)
        shared_count = int(base_customers * shared_customer_ratio)

        print(f"\n📊 Generating shared customer pool ({shared_count} customers)...")
        self.generate_shared_customer_pool(shared_count)

        print("\n🛒 Generating Shopify data...")
        shopify_data = self.generate_shopify_data(
            customers=int(100 * scale),
            products=int(50 * scale),
            orders=int(500 * scale),
        )

        print("\n💳 Generating Stripe data...")
        stripe_data = self.generate_stripe_data(
            customers=int(100 * scale),
            charges=int(300 * scale),
            subscriptions=int(50 * scale),
        )

        print("\n📧 Generating HubSpot data...")
        hubspot_data = self.generate_hubspot_data(
            contacts=int(100 * scale),
            companies=int(30 * scale),
            deals=int(80 * scale),
        )

        return {
            "shopify": shopify_data,
            "stripe": stripe_data,
            "hubspot": hubspot_data,
        }

    def save_to_files(
        self,
        data: Dict[str, Dict[str, List[Dict]]],
        output_dir: Path,
        format: str = "jsonl",
    ) -> None:
        """
        Save generated data to files.

        Args:
            data: Generated data by source and entity
            output_dir: Output directory
            format: Output format ('jsonl' for line-delimited JSON, 'json' for arrays)
        """
        output_dir = Path(output_dir)

        for source, entities in data.items():
            source_dir = output_dir / source
            source_dir.mkdir(parents=True, exist_ok=True)

            for entity, records in entities.items():
                if format == "jsonl":
                    file_path = source_dir / f"{entity}.jsonl"
                    with open(file_path, "wb") as f:
                        for record in records:
                            f.write(orjson.dumps(record) + b"\n")
                else:
                    file_path = source_dir / f"{entity}.json"
                    with open(file_path, "wb") as f:
                        f.write(orjson.dumps(records, option=orjson.OPT_INDENT_2))

                print(f"  ✅ Saved {len(records)} {entity} to {file_path}")


@click.command()
@click.option(
    "--source",
    type=click.Choice(["shopify", "stripe", "hubspot", "all"]),
    default="all",
    help="Data source to generate",
)
@click.option(
    "--scale",
    type=float,
    default=1.0,
    help="Scale factor for data volume (1.0 = default)",
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default="./output",
    help="Output directory for generated files",
)
@click.option(
    "--format",
    type=click.Choice(["jsonl", "json"]),
    default="jsonl",
    help="Output format (jsonl = line-delimited, json = arrays)",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Random seed for reproducibility",
)
@click.option(
    "--shared-ratio",
    type=float,
    default=0.3,
    help="Ratio of customers shared across sources (for entity resolution demo)",
)
def main(
    source: str,
    scale: float,
    output_dir: str,
    format: str,
    seed: Optional[int],
    shared_ratio: float,
):
    """Generate mock data for Iceberg Incremental Demo."""
    print("=" * 60)
    print("🚀 Iceberg Incremental Demo - Data Generator")
    print("=" * 60)
    print(f"  Source: {source}")
    print(f"  Scale: {scale}x")
    print(f"  Output: {output_dir}")
    print(f"  Format: {format}")
    print(f"  Seed: {seed or 'random'}")
    print(f"  Shared customer ratio: {shared_ratio:.0%}")
    print("=" * 60)

    generator = DataGenerator(seed=seed)

    if source == "all":
        data = generator.generate_all(scale=scale, shared_customer_ratio=shared_ratio)
    else:
        # Generate shared customer pool
        generator.generate_shared_customer_pool(int(100 * scale * shared_ratio))

        if source == "shopify":
            data = {"shopify": generator.generate_shopify_data(
                customers=int(100 * scale),
                products=int(50 * scale),
                orders=int(500 * scale),
            )}
        elif source == "stripe":
            data = {"stripe": generator.generate_stripe_data(
                customers=int(100 * scale),
                charges=int(300 * scale),
                subscriptions=int(50 * scale),
            )}
        elif source == "hubspot":
            data = {"hubspot": generator.generate_hubspot_data(
                contacts=int(100 * scale),
                companies=int(30 * scale),
                deals=int(80 * scale),
            )}

    print("\n💾 Saving generated data...")
    generator.save_to_files(data, Path(output_dir), format=format)

    # Print summary
    print("\n" + "=" * 60)
    print("📈 Generation Summary")
    print("=" * 60)
    total_records = 0
    for source_name, entities in data.items():
        print(f"\n{source_name.upper()}:")
        for entity, records in entities.items():
            print(f"  - {entity}: {len(records):,} records")
            total_records += len(records)
    print(f"\n✅ Total: {total_records:,} records generated")
    print("=" * 60)


if __name__ == "__main__":
    main()
