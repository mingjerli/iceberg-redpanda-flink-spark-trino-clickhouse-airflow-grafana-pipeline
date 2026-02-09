"""
Mailchimp Mock Data Provider

Generates realistic Mailchimp webhook payload objects for campaigns,
events, and subscribers. Follows the same pattern as ShopifyProvider,
StripeProvider, and HubSpotProvider.

Note: In production, Mailchimp sends form-encoded webhooks. This
provider generates JSON payloads for consistency with the rest of the
pipeline. The ingestion handler documents this difference.
"""

import hashlib
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from faker import Faker


class MailchimpProvider:
    """Generate mock Mailchimp data matching webhook payload format."""

    # Campaign types
    CAMPAIGN_TYPES = ["regular", "plaintext", "absplit", "rss", "variate", "automation", "sms"]

    # Campaign statuses
    CAMPAIGN_STATUSES = ["save", "paused", "schedule", "sending", "sent"]

    # Content types
    CONTENT_TYPES = ["template", "html", "url", "multichannel"]

    # Event action types with distribution weights
    ACTION_TYPES = [
        ("sent", 40),
        ("open", 25),
        ("click", 15),
        ("bounce", 8),
        ("unsub", 5),
        ("sms_sent", 5),
        ("sms_click", 2),
    ]

    # Subscriber statuses
    SUBSCRIBER_STATUSES = ["subscribed", "unsubscribed", "cleaned", "pending", "transactional"]

    # Mapping of subscriber status to webhook event type
    STATUS_TO_EVENT_TYPE = {
        "subscribed": "subscribe",
        "unsubscribed": "unsubscribe",
        "cleaned": "cleaned",
        "pending": "subscribe",
        "transactional": "profile",
    }

    # Subscriber sources
    SUBSCRIBER_SOURCES = ["API", "import", "popup", "landing_page"]

    # SMS statuses
    SMS_STATUSES = ["subscribed", "unsubscribed", "non_subscribed"]

    # Bounce types
    BOUNCE_TYPES = ["hard", "soft"]

    # Email types
    EMAIL_TYPES = ["html", "text"]

    # Languages
    LANGUAGES = ["en", "es", "fr", "de", "pt", "ja", "zh", "ko", "it", "nl"]

    def __init__(self, seed: Optional[int] = None):
        """Initialize with optional seed for reproducibility."""
        self.fake = Faker()
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

        # ID counters
        self._campaign_id = 0
        self._event_id = 0
        self._subscriber_id = 0

    def _next_campaign_id(self) -> str:
        """Generate next campaign ID as 10-char hex string (Mailchimp format)."""
        self._campaign_id += 1
        return format(self._campaign_id, "010x")

    def _next_event_id(self) -> str:
        """Generate next event ID."""
        self._event_id += 1
        return f"evt_{self._event_id:012d}"

    def _subscriber_id_from_email(self, email: str) -> str:
        """
        Generate subscriber ID as MD5 hash of lowercased email.

        This intentionally uses MD5 to match the real Mailchimp convention
        where subscriber hashes are MD5(lowercase(email)). This is NOT
        used for security — only for deterministic ID generation.
        """
        return hashlib.md5(email.lower().encode()).hexdigest()  # noqa: S324

    def _random_list_id(self) -> str:
        """Generate a random Mailchimp list/audience ID (10-char alphanumeric)."""
        return "".join(random.choices(string.ascii_lowercase + string.digits, k=10))

    def _weighted_choice(self, choices: List[tuple]) -> str:
        """Pick from weighted list of (value, weight) tuples."""
        values, weights = zip(*choices)
        return random.choices(values, weights=weights, k=1)[0]

    def generate_campaign(self, list_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate a Mailchimp campaign object.

        Args:
            list_id: Optional audience/list ID to associate with campaign
        """
        campaign_id = self._next_campaign_id()
        campaign_type = random.choice(self.CAMPAIGN_TYPES)
        status = random.choice(self.CAMPAIGN_STATUSES)
        _list_id = list_id or self._random_list_id()

        send_time = self.fake.date_time_between(
            start_date="-6M", end_date="-1d"
        ).replace(tzinfo=timezone.utc)

        emails_sent = random.randint(500, 50000) if status == "sent" else 0
        opens = int(emails_sent * random.uniform(0.10, 0.45)) if emails_sent > 0 else 0
        unique_opens = int(opens * random.uniform(0.6, 0.9))
        clicks = int(emails_sent * random.uniform(0.02, 0.15)) if emails_sent > 0 else 0
        unique_clicks = int(clicks * random.uniform(0.5, 0.85))
        unsubscribes = int(emails_sent * random.uniform(0.001, 0.02)) if emails_sent > 0 else 0
        bounces = int(emails_sent * random.uniform(0.01, 0.08)) if emails_sent > 0 else 0

        open_rate = unique_opens / emails_sent if emails_sent > 0 else 0.0
        click_rate = unique_clicks / emails_sent if emails_sent > 0 else 0.0

        return {
            "campaign_id": campaign_id,
            "campaign_type": campaign_type,
            "status": status,
            "list_id": _list_id,
            "subject_line": self.fake.sentence(nb_words=random.randint(4, 10)).rstrip("."),
            "preview_text": self.fake.sentence(nb_words=random.randint(8, 15)),
            "from_name": self.fake.company(),
            "from_email": self.fake.company_email(),
            "reply_to": self.fake.company_email(),
            "send_time": send_time.isoformat(),
            "content_type": random.choice(self.CONTENT_TYPES),
            "emails_sent": emails_sent,
            "opens": opens,
            "unique_opens": unique_opens,
            "clicks": clicks,
            "unique_clicks": unique_clicks,
            "unsubscribes": unsubscribes,
            "bounces": bounces,
            "open_rate": round(open_rate, 4),
            "click_rate": round(click_rate, 4),
            "settings": {
                "auto_footer": True,
                "use_conversation": False,
                "authenticate": True,
            },
            "tracking": {
                "opens": True,
                "html_clicks": True,
                "text_clicks": False,
                "google_analytics": campaign_id,
            },
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_event_type": "campaign",
        }

    def generate_event(
        self,
        campaign: Optional[Dict] = None,
        subscriber: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Generate a Mailchimp event object.

        Args:
            campaign: Optional campaign to associate with event
            subscriber: Optional subscriber to associate with event
        """
        event_id = self._next_event_id()
        action = self._weighted_choice(self.ACTION_TYPES)

        campaign_id = campaign["campaign_id"] if campaign else self._next_campaign_id()
        email_address = subscriber["email_address"] if subscriber else self.fake.email()
        list_id = campaign["list_id"] if campaign else (
            subscriber["list_id"] if subscriber else self._random_list_id()
        )

        # MD5 for email_id matches Mailchimp convention (not for security)
        email_id = hashlib.md5(  # noqa: S324
            f"{email_address}:{campaign_id}".encode()
        ).hexdigest()[:12]

        timestamp = self.fake.date_time_between(
            start_date="-30d", end_date="now"
        ).replace(tzinfo=timezone.utc)

        location = None
        if action in ("open", "click"):
            location = {
                "latitude": float(self.fake.latitude()),
                "longitude": float(self.fake.longitude()),
                "country_code": self.fake.country_code(),
                "region": self.fake.state_abbr(),
            }

        return {
            "event_id": event_id,
            "campaign_id": campaign_id,
            "email_id": email_id,
            "email_address": email_address,
            "action": action,
            "timestamp": timestamp.isoformat(),
            "url": self.fake.url() if action in ("click", "sms_click") else None,
            "ip": self.fake.ipv4() if action in ("open", "click") else None,
            "user_agent": self.fake.user_agent() if action in ("open", "click") else None,
            "location": location,
            "bounce_type": random.choice(self.BOUNCE_TYPES) if action == "bounce" else None,
            "list_id": list_id,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_event_type": action,
        }

    def generate_subscriber(self, shared: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Generate a Mailchimp subscriber object.

        Args:
            shared: Optional shared customer data for cross-source consistency
        """
        email = shared["email"] if shared else self.fake.email()
        first_name = shared["first_name"] if shared else self.fake.first_name()
        last_name = shared["last_name"] if shared else self.fake.last_name()
        phone = shared.get("phone") if shared else (
            self.fake.phone_number() if random.random() > 0.4 else None
        )

        subscriber_id = self._subscriber_id_from_email(email)
        status = random.choices(
            self.SUBSCRIBER_STATUSES,
            weights=[60, 15, 5, 10, 10],
            k=1,
        )[0]

        # Derive webhook event type from subscriber status
        event_type = self.STATUS_TO_EVENT_TYPE.get(status, "subscribe")

        signup_time = (
            shared["created_at"] if shared and "created_at" in shared
            else self.fake.date_time_between(start_date="-2y", end_date="-1d")
        )
        if isinstance(signup_time, str):
            signup_time = datetime.fromisoformat(signup_time)
        signup_time = signup_time.replace(tzinfo=timezone.utc)

        last_changed = self.fake.date_time_between(
            start_date=signup_time, end_date="now"
        ).replace(tzinfo=timezone.utc)

        avg_open_rate = round(random.uniform(0.05, 0.55), 4)
        avg_click_rate = round(random.uniform(0.01, 0.15), 4)

        # Format phone as E.164 if present
        phone_e164 = None
        if phone:
            digits = "".join(c for c in phone if c.isdigit())
            if digits:
                phone_e164 = f"+1{digits[-10:]}" if len(digits) >= 10 else f"+1{digits}"

        sms_status = random.choice(self.SMS_STATUSES) if phone_e164 else "non_subscribed"

        tag_name = random.choice(["newsletter", "vip", "promo", "inactive", "lead"])

        return {
            "subscriber_id": subscriber_id,
            "email_address": email,
            "email_type": random.choice(self.EMAIL_TYPES),
            "status": status,
            "merge_fields": {
                "FNAME": first_name,
                "LNAME": last_name,
                "PHONE": phone_e164 or "",
            },
            "stats": {
                "avg_open_rate": avg_open_rate,
                "avg_click_rate": avg_click_rate,
            },
            "list_id": self._random_list_id(),
            "tags": [{"id": random.randint(1, 999), "name": tag_name}],
            "ip_signup": self.fake.ipv4() if random.random() > 0.3 else None,
            "timestamp_signup": signup_time.isoformat(),
            "ip_opt": self.fake.ipv4() if random.random() > 0.5 else None,
            "timestamp_opt": (signup_time + timedelta(minutes=random.randint(1, 60))).isoformat(),
            "last_changed": last_changed.isoformat(),
            "language": random.choice(self.LANGUAGES),
            "vip": random.random() < 0.05,
            "source": random.choice(self.SUBSCRIBER_SOURCES),
            "phone": phone_e164,
            "sms_status": sms_status,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_event_type": event_type,
        }
