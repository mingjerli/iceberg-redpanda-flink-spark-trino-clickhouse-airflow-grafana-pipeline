"""
Stripe Mock Data Provider

Generates realistic Stripe API objects matching the REST API format.
Based on: https://docs.stripe.com/api
"""

import random
import string
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from faker import Faker


class StripeProvider:
    """Generate mock Stripe data matching API format."""

    CARD_BRANDS = ["visa", "mastercard", "amex", "discover", "diners", "jcb"]
    CARD_FUNDING = ["credit", "debit", "prepaid"]
    CURRENCIES = ["usd", "eur", "gbp", "cad", "aud"]
    CHARGE_STATUSES = ["succeeded", "pending", "failed"]
    SUBSCRIPTION_STATUSES = ["active", "past_due", "canceled", "unpaid", "trialing", "incomplete"]
    REFUND_REASONS = ["duplicate", "fraudulent", "requested_by_customer", None]
    DISPUTE_REASONS = [
        "credit_not_processed", "duplicate", "fraudulent", "general",
        "product_not_received", "product_unacceptable", "subscription_canceled", "unrecognized"
    ]

    def __init__(self, seed: Optional[int] = None):
        """Initialize with optional seed for reproducibility."""
        self.fake = Faker()
        if seed:
            Faker.seed(seed)
            random.seed(seed)

        # Counters for unique IDs
        self._customer_counter = 0
        self._charge_counter = 0
        self._payment_intent_counter = 0
        self._subscription_counter = 0
        self._refund_counter = 0
        self._dispute_counter = 0
        self._event_counter = 0

    def _stripe_id(self, prefix: str, counter_name: str) -> str:
        """Generate a Stripe-style ID (e.g., cus_NffrFeUfNV2Hib)."""
        counter = getattr(self, f"_{counter_name}_counter")
        setattr(self, f"_{counter_name}_counter", counter + 1)

        # Generate random suffix like Stripe does
        suffix = "".join(random.choices(string.ascii_letters + string.digits, k=14))
        return f"{prefix}_{suffix}"

    def _unix_timestamp(self, dt: datetime) -> int:
        """Convert datetime to Unix timestamp."""
        return int(dt.timestamp())

    def _generate_address(self, shared_data: Optional[Dict] = None) -> Dict:
        """Generate a Stripe address object."""
        if shared_data and "address" in shared_data:
            addr = shared_data["address"]
            return {
                "city": addr.get("city", self.fake.city()),
                "country": "US",
                "line1": addr.get("address1", self.fake.street_address()),
                "line2": None,
                "postal_code": addr.get("zip", self.fake.zipcode()),
                "state": addr.get("province", self.fake.state_abbr()),
            }

        return {
            "city": self.fake.city(),
            "country": "US",
            "line1": self.fake.street_address(),
            "line2": self.fake.secondary_address() if random.random() > 0.7 else None,
            "postal_code": self.fake.zipcode(),
            "state": self.fake.state_abbr(),
        }

    def generate_customer(self, shared: Optional[Dict] = None) -> Dict:
        """
        Generate a Stripe customer object.

        Args:
            shared: Optional shared customer data for cross-source consistency
        """
        customer_id = self._stripe_id("cus", "customer")
        created = shared.get("created_at") if shared else None
        if not created:
            created = self.fake.date_time_between(start_date="-2y", end_date="-1d")

        email = shared["email"] if shared else self.fake.email()
        name = f"{shared['first_name']} {shared['last_name']}" if shared else self.fake.name()
        phone = shared["phone"] if shared else (self.fake.phone_number() if random.random() > 0.4 else None)

        address = self._generate_address(shared)

        return {
            "id": customer_id,
            "object": "customer",
            "address": address,
            "balance": random.randint(-10000, 10000) if random.random() > 0.9 else 0,
            "created": self._unix_timestamp(created),
            "currency": random.choice(self.CURRENCIES) if random.random() > 0.7 else None,
            "default_source": None,
            "delinquent": random.random() > 0.95,
            "description": self.fake.sentence() if random.random() > 0.7 else None,
            "discount": None,
            "email": email,
            "invoice_prefix": "".join(random.choices(string.ascii_uppercase, k=8)),
            "invoice_settings": {
                "custom_fields": None,
                "default_payment_method": None,
                "footer": None,
                "rendering_options": None,
            },
            "livemode": True,
            "metadata": {"source": "demo_generator"} if random.random() > 0.5 else {},
            "name": name,
            "next_invoice_sequence": random.randint(1, 100),
            "phone": phone,
            "preferred_locales": ["en"],
            "shipping": {
                "address": address,
                "name": name,
                "phone": phone,
            } if random.random() > 0.5 else None,
            "tax_exempt": random.choice(["none", "exempt", "reverse"]) if random.random() > 0.9 else "none",
            "test_clock": None,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_event_id": self._stripe_id("evt", "event"),
        }

    def generate_charge(self, customer: Optional[Dict] = None) -> Dict:
        """
        Generate a Stripe charge object.

        Args:
            customer: Optional customer to associate with charge
        """
        charge_id = self._stripe_id("ch", "charge")
        created = self.fake.date_time_between(start_date="-6M", end_date="now")

        # Amount in cents (between $1 and $500)
        amount = random.randint(100, 50000)

        # Determine status with realistic distribution
        status = random.choices(
            ["succeeded", "pending", "failed"],
            weights=[90, 5, 5]
        )[0]

        paid = status == "succeeded"
        captured = paid and random.random() > 0.05

        # Card details
        card_brand = random.choice(self.CARD_BRANDS)
        card_last4 = "".join(random.choices(string.digits, k=4))
        card_country = random.choice(["US", "US", "US", "CA", "GB", "DE", "FR"])

        # Failure details if failed
        failure_code = None
        failure_message = None
        if status == "failed":
            failure_code = random.choice(["card_declined", "insufficient_funds", "expired_card", "incorrect_cvc"])
            failure_message = {
                "card_declined": "Your card was declined.",
                "insufficient_funds": "Your card has insufficient funds.",
                "expired_card": "Your card has expired.",
                "incorrect_cvc": "Your card's security code is incorrect.",
            }[failure_code]

        billing_details = {
            "address": self._generate_address(),
            "email": customer["email"] if customer else self.fake.email(),
            "name": customer["name"] if customer else self.fake.name(),
            "phone": customer.get("phone") if customer else None,
        }

        # Risk assessment
        risk_level = random.choices(
            ["normal", "elevated", "highest"],
            weights=[85, 10, 5]
        )[0]
        risk_score = {
            "normal": random.randint(0, 30),
            "elevated": random.randint(31, 65),
            "highest": random.randint(66, 100),
        }[risk_level]

        return {
            "id": charge_id,
            "object": "charge",
            "amount": amount,
            "amount_captured": amount if captured else 0,
            "amount_refunded": 0,
            "application": None,
            "application_fee": None,
            "application_fee_amount": None,
            "balance_transaction": self._stripe_id("txn", "charge") if paid else None,
            "billing_details": billing_details,
            "calculated_statement_descriptor": "DEMO STORE",
            "captured": captured,
            "created": self._unix_timestamp(created),
            "currency": "usd",
            "customer": customer["id"] if customer else None,
            "description": f"Payment for order #{random.randint(1000, 9999)}",
            "disputed": False,
            "failure_balance_transaction": None,
            "failure_code": failure_code,
            "failure_message": failure_message,
            "fraud_details": {},
            "invoice": None,
            "livemode": True,
            "metadata": {},
            "on_behalf_of": None,
            "outcome": {
                "network_status": "approved_by_network" if paid else "declined_by_network",
                "reason": None if paid else failure_code,
                "risk_level": risk_level,
                "risk_score": risk_score,
                "seller_message": "Payment complete." if paid else failure_message,
                "type": "authorized" if paid else "issuer_declined",
            },
            "paid": paid,
            "payment_intent": self._stripe_id("pi", "payment_intent"),
            "payment_method": self._stripe_id("pm", "charge"),
            "payment_method_details": {
                "card": {
                    "brand": card_brand,
                    "checks": {
                        "address_line1_check": "pass" if paid else "fail",
                        "address_postal_code_check": "pass" if paid else "fail",
                        "cvc_check": "pass" if paid else "fail",
                    },
                    "country": card_country,
                    "exp_month": random.randint(1, 12),
                    "exp_year": random.randint(2025, 2030),
                    "funding": random.choice(self.CARD_FUNDING),
                    "last4": card_last4,
                    "network": card_brand,
                    "three_d_secure": None,
                },
                "type": "card",
            },
            "receipt_email": billing_details["email"],
            "receipt_number": None,
            "receipt_url": f"https://pay.stripe.com/receipts/{self.fake.uuid4()}",
            "refunded": False,
            "refunds": {"object": "list", "data": [], "has_more": False, "url": f"/v1/charges/{charge_id}/refunds"},
            "review": None,
            "shipping": None,
            "source": None,
            "source_transfer": None,
            "statement_descriptor": "DEMO STORE",
            "statement_descriptor_suffix": None,
            "status": status,
            "transfer_data": None,
            "transfer_group": None,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_event_id": self._stripe_id("evt", "event"),
        }

    def generate_payment_intent(self, customer: Optional[Dict] = None) -> Dict:
        """Generate a Stripe PaymentIntent object."""
        pi_id = self._stripe_id("pi", "payment_intent")
        created = self.fake.date_time_between(start_date="-6M", end_date="now")

        amount = random.randint(100, 50000)

        status = random.choices(
            ["succeeded", "requires_payment_method", "requires_confirmation", "processing", "canceled"],
            weights=[80, 5, 5, 5, 5]
        )[0]

        amount_received = amount if status == "succeeded" else 0

        return {
            "id": pi_id,
            "object": "payment_intent",
            "amount": amount,
            "amount_capturable": 0,
            "amount_details": {"tip": {}},
            "amount_received": amount_received,
            "application": None,
            "application_fee_amount": None,
            "automatic_payment_methods": {"allow_redirects": "always", "enabled": True},
            "canceled_at": self._unix_timestamp(created) if status == "canceled" else None,
            "cancellation_reason": "abandoned" if status == "canceled" else None,
            "capture_method": "automatic",
            "client_secret": f"{pi_id}_secret_{''.join(random.choices(string.ascii_letters, k=24))}",
            "confirmation_method": "automatic",
            "created": self._unix_timestamp(created),
            "currency": "usd",
            "customer": customer["id"] if customer else None,
            "description": f"Payment for order #{random.randint(1000, 9999)}",
            "invoice": None,
            "last_payment_error": None,
            "latest_charge": self._stripe_id("ch", "charge") if status == "succeeded" else None,
            "livemode": True,
            "metadata": {},
            "next_action": None,
            "on_behalf_of": None,
            "payment_method": self._stripe_id("pm", "payment_intent") if status == "succeeded" else None,
            "payment_method_configuration_details": None,
            "payment_method_options": {"card": {"request_three_d_secure": "automatic"}},
            "payment_method_types": ["card"],
            "processing": None,
            "receipt_email": customer["email"] if customer else self.fake.email(),
            "review": None,
            "setup_future_usage": None,
            "shipping": None,
            "statement_descriptor": "DEMO STORE",
            "statement_descriptor_suffix": None,
            "status": status,
            "transfer_data": None,
            "transfer_group": None,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_event_id": self._stripe_id("evt", "event"),
        }

    def generate_subscription(self, customer: Dict) -> Dict:
        """Generate a Stripe subscription object."""
        sub_id = self._stripe_id("sub", "subscription")
        created = self.fake.date_time_between(start_date="-1y", end_date="-1M")

        status = random.choices(
            ["active", "past_due", "canceled", "trialing"],
            weights=[70, 10, 10, 10]
        )[0]

        # Calculate billing periods
        period_start = datetime.now() - timedelta(days=random.randint(0, 30))
        period_end = period_start + timedelta(days=30)

        trial_end = None
        if status == "trialing":
            trial_end = datetime.now() + timedelta(days=random.randint(1, 14))

        canceled_at = None
        ended_at = None
        if status == "canceled":
            canceled_at = self.fake.date_time_between(start_date=created, end_date="now")
            ended_at = canceled_at

        return {
            "id": sub_id,
            "object": "subscription",
            "application": None,
            "application_fee_percent": None,
            "automatic_tax": {"enabled": False, "liability": None},
            "billing_cycle_anchor": self._unix_timestamp(created),
            "billing_cycle_anchor_config": None,
            "billing_thresholds": None,
            "cancel_at": None,
            "cancel_at_period_end": random.random() > 0.9,
            "canceled_at": self._unix_timestamp(canceled_at) if canceled_at else None,
            "cancellation_details": {"comment": None, "feedback": None, "reason": None},
            "collection_method": "charge_automatically",
            "created": self._unix_timestamp(created),
            "currency": "usd",
            "current_period_end": self._unix_timestamp(period_end),
            "current_period_start": self._unix_timestamp(period_start),
            "customer": customer["id"],
            "days_until_due": None,
            "default_payment_method": None,
            "default_source": None,
            "default_tax_rates": [],
            "description": None,
            "discount": None,
            "ended_at": self._unix_timestamp(ended_at) if ended_at else None,
            "items": {
                "object": "list",
                "data": [{
                    "id": self._stripe_id("si", "subscription"),
                    "object": "subscription_item",
                    "billing_thresholds": None,
                    "created": self._unix_timestamp(created),
                    "metadata": {},
                    "price": {
                        "id": self._stripe_id("price", "subscription"),
                        "object": "price",
                        "active": True,
                        "currency": "usd",
                        "product": self._stripe_id("prod", "subscription"),
                        "recurring": {"interval": "month", "interval_count": 1},
                        "unit_amount": random.choice([999, 1999, 2999, 4999, 9999]),
                    },
                    "quantity": 1,
                    "subscription": sub_id,
                }],
                "has_more": False,
            },
            "latest_invoice": self._stripe_id("in", "subscription"),
            "livemode": True,
            "metadata": {},
            "next_pending_invoice_item_invoice": None,
            "on_behalf_of": None,
            "pause_collection": None,
            "payment_settings": {
                "payment_method_options": None,
                "payment_method_types": None,
                "save_default_payment_method": "off",
            },
            "pending_invoice_item_interval": None,
            "pending_setup_intent": None,
            "pending_update": None,
            "schedule": None,
            "start_date": self._unix_timestamp(created),
            "status": status,
            "test_clock": None,
            "transfer_data": None,
            "trial_end": self._unix_timestamp(trial_end) if trial_end else None,
            "trial_settings": {"end_behavior": {"missing_payment_method": "create_invoice"}},
            "trial_start": self._unix_timestamp(created) if status == "trialing" else None,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_event_id": self._stripe_id("evt", "event"),
        }

    def generate_refund(self, charge: Dict) -> Dict:
        """Generate a Stripe refund object."""
        refund_id = self._stripe_id("re", "refund")
        created = self.fake.date_time_between(start_date="-3M", end_date="now")

        # Refund partial or full amount
        original_amount = charge["amount"]
        refund_amount = random.choice([
            original_amount,  # Full refund
            original_amount,
            int(original_amount * 0.5),  # Half refund
            int(original_amount * random.uniform(0.1, 0.9)),  # Partial refund
        ])

        return {
            "id": refund_id,
            "object": "refund",
            "amount": refund_amount,
            "balance_transaction": self._stripe_id("txn", "refund"),
            "charge": charge["id"],
            "created": self._unix_timestamp(created),
            "currency": "usd",
            "destination_details": {
                "card": {
                    "reference": "".join(random.choices(string.digits, k=24)),
                    "reference_status": "available",
                    "reference_type": "acquirer_reference_number",
                    "type": "refund",
                },
                "type": "card",
            },
            "failure_balance_transaction": None,
            "failure_reason": None,
            "instructions_email": None,
            "metadata": {},
            "next_action": None,
            "payment_intent": charge.get("payment_intent"),
            "reason": random.choice(self.REFUND_REASONS),
            "receipt_number": None,
            "source_transfer_reversal": None,
            "status": "succeeded",
            "transfer_reversal": None,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_event_id": self._stripe_id("evt", "event"),
        }

    def generate_dispute(self, charge: Dict) -> Dict:
        """Generate a Stripe dispute object."""
        dispute_id = self._stripe_id("dp", "dispute")
        created = self.fake.date_time_between(start_date="-2M", end_date="now")

        reason = random.choice(self.DISPUTE_REASONS)
        status = random.choices(
            ["needs_response", "under_review", "won", "lost"],
            weights=[30, 20, 25, 25]
        )[0]

        return {
            "id": dispute_id,
            "object": "dispute",
            "amount": charge["amount"],
            "balance_transactions": [],
            "charge": charge["id"],
            "created": self._unix_timestamp(created),
            "currency": "usd",
            "evidence": {
                "access_activity_log": None,
                "billing_address": None,
                "cancellation_policy": None,
                "cancellation_policy_disclosure": None,
                "cancellation_rebuttal": None,
                "customer_communication": None,
                "customer_email_address": charge.get("receipt_email"),
                "customer_name": charge.get("billing_details", {}).get("name"),
                "product_description": None,
                "receipt": None,
                "refund_policy": None,
                "refund_policy_disclosure": None,
                "refund_refusal_explanation": None,
                "service_date": None,
                "service_documentation": None,
                "shipping_address": None,
                "shipping_carrier": None,
                "shipping_date": None,
                "shipping_documentation": None,
                "shipping_tracking_number": None,
                "uncategorized_file": None,
                "uncategorized_text": None,
            },
            "evidence_details": {
                "due_by": self._unix_timestamp(datetime.now() + timedelta(days=7)),
                "has_evidence": False,
                "past_due": False,
                "submission_count": 0,
            },
            "is_charge_refundable": status in ["needs_response", "under_review"],
            "livemode": True,
            "metadata": {},
            "network_reason_code": None,
            "payment_intent": charge.get("payment_intent"),
            "payment_method_details": charge.get("payment_method_details"),
            "reason": reason,
            "status": status,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_event_id": self._stripe_id("evt", "event"),
        }
