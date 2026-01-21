"""
Shopify Mock Data Provider

Generates realistic Shopify webhook payloads matching the REST Admin API format.
Based on: https://shopify.dev/docs/api/admin-rest/2024-10/resources/order
"""

import random
import string
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from faker import Faker


class ShopifyProvider:
    """Generate mock Shopify data matching webhook payload format."""

    # Product types and vendors for realistic data
    PRODUCT_TYPES = [
        "T-Shirts", "Hoodies", "Pants", "Shoes", "Accessories",
        "Electronics", "Home & Garden", "Beauty", "Sports", "Books"
    ]

    VENDORS = [
        "Nike", "Adidas", "Apple", "Samsung", "Sony", "Levi's",
        "Patagonia", "North Face", "Under Armour", "New Balance"
    ]

    COLORS = ["Black", "White", "Blue", "Red", "Green", "Gray", "Navy", "Beige"]
    SIZES = ["XS", "S", "M", "L", "XL", "XXL"]

    FINANCIAL_STATUSES = ["pending", "authorized", "partially_paid", "paid", "partially_refunded", "refunded", "voided"]
    FULFILLMENT_STATUSES = [None, "partial", "fulfilled", "restocked"]
    SOURCE_NAMES = ["web", "pos", "mobile", "api", "shopify_draft_order"]

    def __init__(self, seed: Optional[int] = None):
        """Initialize with optional seed for reproducibility."""
        self.fake = Faker()
        if seed:
            Faker.seed(seed)
            random.seed(seed)

        # ID counters for sequential IDs
        self._customer_id = 1000000000
        self._product_id = 2000000000
        self._variant_id = 3000000000
        self._order_id = 4000000000
        self._line_item_id = 5000000000
        self._address_id = 6000000000

    def _next_id(self, id_type: str) -> int:
        """Generate next sequential ID."""
        attr = f"_{id_type}_id"
        current = getattr(self, attr)
        setattr(self, attr, current + 1)
        return current

    def _generate_address(self, customer_data: Optional[Dict] = None) -> Dict:
        """Generate a Shopify address object."""
        if customer_data and "address" in customer_data:
            addr = customer_data["address"]
            first_name = customer_data.get("first_name", self.fake.first_name())
            last_name = customer_data.get("last_name", self.fake.last_name())
        else:
            addr = None
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()

        return {
            "id": self._next_id("address"),
            "first_name": first_name,
            "last_name": last_name,
            "name": f"{first_name} {last_name}",
            "company": self.fake.company() if random.random() > 0.7 else None,
            "address1": addr["address1"] if addr else self.fake.street_address(),
            "address2": self.fake.secondary_address() if random.random() > 0.7 else None,
            "city": addr["city"] if addr else self.fake.city(),
            "province": addr["province"] if addr else self.fake.state(),
            "province_code": self.fake.state_abbr(),
            "country": addr["country"] if addr else "United States",
            "country_code": addr["country_code"] if addr else "US",
            "zip": addr["zip"] if addr else self.fake.zipcode(),
            "phone": self.fake.phone_number() if random.random() > 0.5 else None,
            "latitude": float(self.fake.latitude()),
            "longitude": float(self.fake.longitude()),
            "default": True,
        }

    def generate_customer(self, shared: Optional[Dict] = None) -> Dict:
        """
        Generate a Shopify customer object.

        Args:
            shared: Optional shared customer data for cross-source consistency
        """
        customer_id = self._next_id("customer")
        created_at = shared["created_at"] if shared else self.fake.date_time_between(
            start_date="-2y", end_date="-1d"
        )

        email = shared["email"] if shared else self.fake.email()
        first_name = shared["first_name"] if shared else self.fake.first_name()
        last_name = shared["last_name"] if shared else self.fake.last_name()
        phone = shared["phone"] if shared else (self.fake.phone_number() if random.random() > 0.3 else None)

        address = self._generate_address(shared)

        orders_count = random.randint(0, 20)
        total_spent = round(random.uniform(0, 5000), 2) if orders_count > 0 else 0

        return {
            "id": customer_id,
            "admin_graphql_api_id": f"gid://shopify/Customer/{customer_id}",
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "phone": phone,
            "created_at": created_at.isoformat() + "Z",
            "updated_at": self.fake.date_time_between(
                start_date=created_at, end_date="now"
            ).isoformat() + "Z",
            "accepts_marketing": random.choice([True, False]),
            "accepts_marketing_updated_at": self.fake.date_time_between(
                start_date=created_at, end_date="now"
            ).isoformat() + "Z",
            "currency": "USD",
            "default_address": address,
            "addresses": [address],
            "marketing_opt_in_level": random.choice(["single_opt_in", "confirmed_opt_in", None]),
            "note": self.fake.sentence() if random.random() > 0.8 else None,
            "orders_count": orders_count,
            "state": random.choice(["enabled", "disabled", "invited"]),
            "tags": ", ".join(random.sample(["VIP", "wholesale", "returning", "new", "loyalty"], k=random.randint(0, 3))),
            "tax_exempt": random.random() > 0.95,
            "tax_exemptions": [],
            "total_spent": str(total_spent),
            "verified_email": random.random() > 0.1,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_topic": "customers/create",
        }

    def generate_product(self) -> Dict:
        """Generate a Shopify product object with variants."""
        product_id = self._next_id("product")
        product_type = random.choice(self.PRODUCT_TYPES)
        vendor = random.choice(self.VENDORS)

        title = f"{vendor} {self.fake.word().title()} {product_type}"
        handle = title.lower().replace(" ", "-")

        created_at = self.fake.date_time_between(start_date="-1y", end_date="-1d")

        # Generate variants
        variants = []
        base_price = round(random.uniform(19.99, 299.99), 2)

        # Determine variant options
        has_sizes = random.random() > 0.3
        has_colors = random.random() > 0.4

        if has_sizes and has_colors:
            sizes = random.sample(self.SIZES, k=random.randint(2, 4))
            colors = random.sample(self.COLORS, k=random.randint(2, 3))
            option_combos = [(s, c) for s in sizes for c in colors]
        elif has_sizes:
            sizes = random.sample(self.SIZES, k=random.randint(2, 5))
            option_combos = [(s, None) for s in sizes]
        elif has_colors:
            colors = random.sample(self.COLORS, k=random.randint(2, 4))
            option_combos = [(None, c) for c in colors]
        else:
            option_combos = [(None, None)]

        for i, (size, color) in enumerate(option_combos):
            variant_id = self._next_id("variant")
            variant_title = " / ".join(filter(None, [size, color])) or "Default"

            variants.append({
                "id": variant_id,
                "admin_graphql_api_id": f"gid://shopify/ProductVariant/{variant_id}",
                "product_id": product_id,
                "title": variant_title,
                "price": str(base_price),
                "compare_at_price": str(round(base_price * 1.2, 2)) if random.random() > 0.7 else None,
                "sku": f"{vendor[:3].upper()}-{product_id}-{i+1:03d}",
                "barcode": self.fake.ean13() if random.random() > 0.5 else None,
                "position": i + 1,
                "inventory_item_id": variant_id + 1000000,
                "inventory_quantity": random.randint(0, 100),
                "weight": round(random.uniform(0.1, 5.0), 2),
                "weight_unit": "kg",
                "option1": size,
                "option2": color,
                "option3": None,
                "created_at": created_at.isoformat() + "Z",
                "updated_at": self.fake.date_time_between(
                    start_date=created_at, end_date="now"
                ).isoformat() + "Z",
            })

        # Build options array
        options = []
        if has_sizes:
            options.append({"id": product_id * 10 + 1, "product_id": product_id, "name": "Size", "position": 1})
        if has_colors:
            options.append({"id": product_id * 10 + 2, "product_id": product_id, "name": "Color", "position": 2})
        if not options:
            options.append({"id": product_id * 10, "product_id": product_id, "name": "Title", "position": 1})

        return {
            "id": product_id,
            "admin_graphql_api_id": f"gid://shopify/Product/{product_id}",
            "title": title,
            "body_html": f"<p>{self.fake.paragraph()}</p>",
            "vendor": vendor,
            "product_type": product_type,
            "handle": handle,
            "created_at": created_at.isoformat() + "Z",
            "updated_at": self.fake.date_time_between(
                start_date=created_at, end_date="now"
            ).isoformat() + "Z",
            "published_at": created_at.isoformat() + "Z" if random.random() > 0.1 else None,
            "published_scope": "global",
            "status": random.choice(["active", "active", "active", "draft", "archived"]),
            "tags": ", ".join(self.fake.words(nb=random.randint(0, 5))),
            "template_suffix": None,
            "variants": variants,
            "options": options,
            "images": [],
            "image": None,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_topic": "products/create",
        }

    def generate_order(
        self,
        customer: Optional[Dict] = None,
        products: Optional[List[Dict]] = None,
    ) -> Dict:
        """
        Generate a Shopify order object.

        Args:
            customer: Optional customer to associate with order
            products: Optional list of products to choose line items from
        """
        order_id = self._next_id("order")
        order_number = 1000 + order_id - 4000000000

        created_at = self.fake.date_time_between(start_date="-6M", end_date="now")

        # Generate line items
        line_items = []
        num_items = random.randint(1, 5)

        if products:
            selected_products = random.sample(products, k=min(num_items, len(products)))
        else:
            selected_products = [self.generate_product() for _ in range(num_items)]

        subtotal = 0
        for product in selected_products:
            variant = random.choice(product["variants"])
            quantity = random.randint(1, 3)
            price = float(variant["price"])
            item_total = price * quantity

            line_item = {
                "id": self._next_id("line_item"),
                "admin_graphql_api_id": f"gid://shopify/LineItem/{self._line_item_id - 1}",
                "fulfillable_quantity": quantity,
                "fulfillment_service": "manual",
                "fulfillment_status": None,
                "gift_card": False,
                "grams": int(variant.get("weight", 0.5) * 1000),
                "name": f"{product['title']} - {variant['title']}",
                "price": str(price),
                "product_exists": True,
                "product_id": product["id"],
                "properties": [],
                "quantity": quantity,
                "requires_shipping": True,
                "sku": variant.get("sku"),
                "taxable": True,
                "title": product["title"],
                "total_discount": "0.00",
                "variant_id": variant["id"],
                "variant_inventory_management": "shopify",
                "variant_title": variant["title"],
                "vendor": product["vendor"],
                "tax_lines": [{
                    "title": "State Tax",
                    "price": str(round(item_total * 0.08, 2)),
                    "rate": 0.08,
                    "channel_liable": False,
                }],
                "discount_allocations": [],
            }
            line_items.append(line_item)
            subtotal += item_total

        # Calculate totals
        discount_amount = round(subtotal * random.uniform(0, 0.2), 2) if random.random() > 0.7 else 0
        shipping = round(random.uniform(0, 15), 2)
        tax = round((subtotal - discount_amount) * 0.08, 2)
        total = round(subtotal - discount_amount + shipping + tax, 2)

        # Financial status based on realistic distribution
        financial_status = random.choices(
            ["paid", "pending", "authorized", "refunded", "partially_refunded", "voided"],
            weights=[70, 10, 5, 5, 5, 5]
        )[0]

        # Fulfillment status
        if financial_status in ["paid", "authorized"]:
            fulfillment_status = random.choices(
                [None, "partial", "fulfilled"],
                weights=[30, 10, 60]
            )[0]
        else:
            fulfillment_status = None

        # Customer data
        if customer:
            billing_address = customer.get("default_address", self._generate_address())
            shipping_address = billing_address.copy()
            email = customer.get("email")
            customer_obj = {
                "id": customer["id"],
                "email": email,
                "first_name": customer.get("first_name"),
                "last_name": customer.get("last_name"),
                "default_address": billing_address,
            }
        else:
            billing_address = self._generate_address()
            shipping_address = billing_address.copy()
            email = self.fake.email()
            customer_obj = None

        # Generate discount codes if discount applied
        discount_codes = []
        if discount_amount > 0:
            discount_codes.append({
                "code": "".join(random.choices(string.ascii_uppercase + string.digits, k=8)),
                "amount": str(discount_amount),
                "type": random.choice(["percentage", "fixed_amount"]),
            })

        return {
            "id": order_id,
            "admin_graphql_api_id": f"gid://shopify/Order/{order_id}",
            "app_id": 580111,
            "browser_ip": self.fake.ipv4() if random.random() > 0.3 else None,
            "buyer_accepts_marketing": random.choice([True, False]),
            "cancel_reason": "customer" if financial_status == "voided" else None,
            "cancelled_at": self.fake.date_time_between(
                start_date=created_at, end_date="now"
            ).isoformat() + "Z" if financial_status == "voided" else None,
            "cart_token": self.fake.uuid4(),
            "checkout_id": order_id + 100000000,
            "checkout_token": self.fake.uuid4(),
            "closed_at": self.fake.date_time_between(
                start_date=created_at, end_date="now"
            ).isoformat() + "Z" if fulfillment_status == "fulfilled" else None,
            "confirmation_number": "".join(random.choices(string.ascii_uppercase + string.digits, k=10)),
            "confirmed": True,
            "created_at": created_at.isoformat() + "Z",
            "currency": "USD",
            "current_subtotal_price": str(subtotal - discount_amount),
            "current_total_discounts": str(discount_amount),
            "current_total_price": str(total),
            "current_total_tax": str(tax),
            "customer_locale": "en",
            "discount_codes": discount_codes,
            "email": email,
            "estimated_taxes": False,
            "financial_status": financial_status,
            "fulfillment_status": fulfillment_status,
            "landing_site": f"/{self.fake.uri_path()}" if random.random() > 0.5 else None,
            "landing_site_ref": None,
            "name": f"#{order_number}",
            "note": self.fake.sentence() if random.random() > 0.9 else None,
            "note_attributes": [],
            "number": order_number,
            "order_number": order_number,
            "order_status_url": f"https://example.myshopify.com/orders/{self.fake.uuid4()}",
            "payment_gateway_names": ["shopify_payments"],
            "phone": self.fake.phone_number() if random.random() > 0.8 else None,
            "presentment_currency": "USD",
            "processed_at": created_at.isoformat() + "Z",
            "referring_site": self.fake.url() if random.random() > 0.6 else None,
            "source_identifier": None,
            "source_name": random.choice(self.SOURCE_NAMES),
            "source_url": None,
            "subtotal_price": str(subtotal),
            "tags": "",
            "tax_lines": [{
                "title": "State Tax",
                "price": str(tax),
                "rate": 0.08,
                "channel_liable": False,
            }],
            "taxes_included": False,
            "test": False,
            "token": self.fake.uuid4(),
            "total_discounts": str(discount_amount),
            "total_line_items_price": str(subtotal),
            "total_outstanding": "0.00" if financial_status == "paid" else str(total),
            "total_price": str(total),
            "total_shipping_price_set": {
                "shop_money": {"amount": str(shipping), "currency_code": "USD"},
                "presentment_money": {"amount": str(shipping), "currency_code": "USD"},
            },
            "total_tax": str(tax),
            "total_tip_received": "0.00",
            "total_weight": sum(item["grams"] * item["quantity"] for item in line_items),
            "updated_at": self.fake.date_time_between(
                start_date=created_at, end_date="now"
            ).isoformat() + "Z",
            "user_id": None,
            "customer": customer_obj,
            "billing_address": billing_address,
            "shipping_address": shipping_address,
            "line_items": line_items,
            "fulfillments": [],
            "refunds": [],
            "shipping_lines": [{
                "id": order_id + 200000000,
                "title": "Standard Shipping",
                "price": str(shipping),
                "code": "Standard",
                "source": "shopify",
                "discounted_price": str(shipping),
                "tax_lines": [],
            }],
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_topic": "orders/create",
        }
