"""
PII Field Registry
==================

Single source of truth for which staging columns hold direct identifiers and
what semantic class each belongs to.

Class matters more than column name. Shopify `email`, HubSpot `email`, Stripe
`billing_email` and GA4 `user_id` all carry class `email`, so the same address
produces the same token in all four. Keying tokens by column name instead would
give one person a different token per source, and cross-source entity resolution
would silently stop matching -- silently, because an unmatched record is not an
error, it just becomes a separate customer.

This registry is hand-curated rather than derived from schemas/*.json. Those
annotations are internally inconsistent (HubSpot contact `phone` is pii:true
while company `phone` is pii:false) and no schemas/ga4.json exists at all.

The __future__ import is load-bearing: the Spark image runs Python 3.8, where an
evaluated `dict[str, str]` annotation raises TypeError at import time.
"""
from __future__ import annotations

EMAIL = "email"
PHONE = "phone"
NAME = "name"
ADDRESS = "address"
NAME_PREFIX = "name_prefix"
MAILCHIMP_ID = "mailchimp_id"

PII_CLASSES = (EMAIL, PHONE, NAME, ADDRESS, NAME_PREFIX, MAILCHIMP_ID)

# Staging table -> {column: pii_class}. Column lists verified against the
# CREATE TABLE statements in jobs/spark/staging_batch.py.
PII_FIELDS = {
    "stg_shopify_orders": {
        "customer_email": EMAIL,
        "customer_phone": PHONE,
    },
    "stg_shopify_customers": {
        "email": EMAIL,
        "first_name": NAME,
        "last_name": NAME,
        "full_name": NAME,
        "phone": PHONE,
        "address_line1": ADDRESS,
        "address_line2": ADDRESS,
    },
    "stg_stripe_customers": {
        "email": EMAIL,
        "name": NAME,
        "first_name": NAME,
        "last_name": NAME,
        "full_name": NAME,
        "phone": PHONE,
        "address_line1": ADDRESS,
        "address_line2": ADDRESS,
        "shipping_name": NAME,
        "shipping_address_line1": ADDRESS,
    },
    "stg_stripe_charges": {
        "billing_name": NAME,
        "billing_email": EMAIL,
        "billing_phone": PHONE,
    },
    "stg_hubspot_contacts": {
        "email": EMAIL,
        "first_name": NAME,
        "last_name": NAME,
        "full_name": NAME,
        "phone": PHONE,
        "mobile_phone": PHONE,
        "address": ADDRESS,
    },
    "stg_mailchimp_subscribers": {
        # subscriber_id is MD5(lower(email)) -- an unsalted, publicly
        # reproducible hash, so it is re-identifiable by dictionary attack with
        # no secret at all. It is weaker than the tokens replacing it, and it
        # reaches marts.customer_360.mailchimp_subscriber_id.
        "subscriber_id": MAILCHIMP_ID,
        "email_address": EMAIL,
        "email_normalized": EMAIL,
        "first_name": NAME,
        "last_name": NAME,
        "full_name": NAME,
        "phone": PHONE,
        "phone_normalized": PHONE,
    },
    "stg_mailchimp_events": {
        "email_id": MAILCHIMP_ID,
        "email_address": EMAIL,
        "email_normalized": EMAIL,
    },
    # GA4 user_id is set to the customer's email for the demo's entity
    # resolution (entity_backfill.py:251), so it carries class email. It is
    # tokenized here, at the events layer, because compute_ga4_sessions derives
    # stg_ga4_sessions from stg_ga4_events by reading user_id straight through
    # (staging_batch.py::compute_ga4_sessions). Registering it a second time on
    # stg_ga4_sessions would hash an already-tokenized value --
    # token(token(email)) -- silently breaking every GA4 cross-source match.
    # stg_ga4_sessions carries the result through unchanged as user_id_token,
    # so it deliberately has no entry of its own here.
    "stg_ga4_events": {
        "user_id": EMAIL,
    },
}

# Columns computed from another PII column before the source is dropped.
# rebuild_blocking_index needs a surname prefix, and a hash has no meaningful
# prefix -- so the prefix is tokenized as its own value.
PII_DERIVED = {
    table: {"last_name_prefix": ("last_name", NAME_PREFIX)}
    for table in (
        "stg_shopify_customers",
        "stg_stripe_customers",
        "stg_hubspot_contacts",
        "stg_mailchimp_subscribers",
    )
}


def pii_columns(table):
    """Return {column: pii_class} for a staging table, empty if it holds no PII."""
    return dict(PII_FIELDS.get(table, {}))


def derived_columns(table):
    """Return {new_column: (source_column, pii_class)} for a staging table."""
    return dict(PII_DERIVED.get(table, {}))


def token_column(column):
    """Return the tokenized column name for a plaintext column."""
    return f"{column}_token"
