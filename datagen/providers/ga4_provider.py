"""
GA4Provider: Generate mock Google Analytics 4 BigQuery Export data.

Generates realistic GA4 event data matching the BigQuery Export schema.
Events are organized into coherent sessions with realistic timing and attribution.
"""
import random
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from faker import Faker


class GA4Provider:
    """Generate mock GA4 BigQuery Export data."""

    EVENT_WEIGHTS = {
        "page_view": 50,
        "scroll": 15,
        "click": 10,
        "view_item": 5,
        "add_to_cart": 8,
        "begin_checkout": 4,
        "purchase": 5,
        "sign_up": 3,
    }

    TRAFFIC_SOURCES = ["google", "facebook", "twitter", "email", "direct", "(none)"]
    TRAFFIC_MEDIUMS = ["organic", "cpc", "social", "email", "referral", "(none)"]
    DEVICE_CATEGORIES = ["desktop", "mobile", "tablet"]
    BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
    OS_LIST = ["Windows", "macOS", "iOS", "Android", "Linux"]

    def __init__(self, seed=None):
        """Initialize with optional seed for reproducibility."""
        if seed is not None:  # Must handle seed=0
            random.seed(seed)
            self.fake = Faker()
            Faker.seed(seed)
        else:
            self.fake = Faker()

        self._session_counter = 1000000

    def _next_session_id(self) -> int:
        """Get next session ID."""
        self._session_counter += 1
        return self._session_counter

    def _generate_client_id(self) -> str:
        """Generate GA4 client ID in format: {10_digit}.{unix_ts}."""
        random_digits = random.randint(1000000000, 9999999999)
        timestamp = int(time.time())
        return f"{random_digits}.{timestamp}"

    def _weighted_event(self) -> str:
        """Select event name using weighted distribution."""
        events = list(self.EVENT_WEIGHTS.keys())
        weights = list(self.EVENT_WEIGHTS.values())
        return random.choices(events, weights=weights)[0]

    def _generate_traffic_source(self) -> Dict:
        """Generate traffic source JSON."""
        source = random.choice(self.TRAFFIC_SOURCES)
        medium = random.choice(self.TRAFFIC_MEDIUMS)

        # Adjust medium based on source
        if source == "google" and medium not in ["organic", "cpc"]:
            medium = random.choice(["organic", "cpc"])
        elif source == "direct":
            medium = "(none)"

        campaign = ""
        if medium in ["cpc", "email"]:
            campaigns = ["spring_sale", "summer_promo", "black_friday", "newsletter"]
            campaign = random.choice(campaigns)

        return {
            "source": source,
            "medium": medium,
            "name": campaign
        }

    def _generate_device(self) -> Dict:
        """Generate device JSON."""
        category = random.choice(self.DEVICE_CATEGORIES)
        os = random.choice(self.OS_LIST)
        browser = random.choice(self.BROWSERS)

        # Adjust OS based on device category
        if category == "mobile":
            os = random.choice(["iOS", "Android"])
        elif category == "tablet":
            os = random.choice(["iOS", "Android"])
        elif category == "desktop":
            os = random.choice(["Windows", "macOS", "Linux"])

        screen_resolution = "1920x1080" if category == "desktop" else "375x667"

        return {
            "category": category,
            "operating_system": os,
            "web_info": {
                "browser": browser
            },
            "screen_resolution": screen_resolution
        }

    def _generate_geo(self) -> Dict:
        """Generate geo JSON."""
        countries = ["United States", "United Kingdom", "Canada", "Germany", "France"]
        country = random.choice(countries)

        region_map = {
            "United States": ["California", "New York", "Texas", "Florida"],
            "United Kingdom": ["England", "Scotland", "Wales"],
            "Canada": ["Ontario", "Quebec", "British Columbia"],
            "Germany": ["Bavaria", "Berlin", "Hamburg"],
            "France": ["Île-de-France", "Provence", "Brittany"]
        }

        region = random.choice(region_map.get(country, ["Unknown"]))
        city = self.fake.city()

        return {
            "country": country,
            "region": region,
            "city": city
        }

    def _generate_event_params(self, event_name: str) -> List[Dict]:
        """Generate event_params JSON array matching BigQuery Export nested structure."""
        params = []

        # Common params
        params.append({
            "key": "engagement_time_msec",
            "value": {"int_value": random.randint(1000, 60000)}
        })

        # Event-specific params
        if event_name in ["page_view", "view_item", "add_to_cart"]:
            params.append({
                "key": "page_location",
                "value": {"string_value": f"https://example.com/{self.fake.uri_path()}"}
            })

        if event_name in ["purchase", "add_to_cart", "begin_checkout"]:
            params.append({
                "key": "value",
                "value": {"double_value": round(random.uniform(10.0, 500.0), 2)}
            })
            params.append({
                "key": "currency",
                "value": {"string_value": "USD"}
            })

        return params

    def generate_event(
        self,
        client_id: Optional[str] = None,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        event_name: Optional[str] = None,
        event_timestamp: Optional[int] = None
    ) -> Dict:
        """Generate a single GA4 event."""
        if event_timestamp is None:
            event_timestamp = int(time.time() * 1_000_000)

        # Convert timestamp to date in YYYYMMDD format
        event_date = datetime.fromtimestamp(event_timestamp / 1_000_000).strftime("%Y%m%d")

        if client_id is None:
            client_id = self._generate_client_id()

        if session_id is None:
            session_id = str(self._next_session_id())

        if event_name is None:
            event_name = self._weighted_event()

        traffic_source = self._generate_traffic_source()
        device = self._generate_device()
        geo = self._generate_geo()
        event_params = self._generate_event_params(event_name)

        # Generate page data
        page_paths = ["/", "/products", "/about", "/contact", "/checkout", "/account"]
        page_location = f"https://example.com{random.choice(page_paths)}"
        page_title = random.choice(["Home", "Products", "About Us", "Contact", "Checkout", "Account"])
        page_referrer = ""
        if random.random() < 0.3:
            page_referrer = f"https://{random.choice(['google.com', 'facebook.com', 'twitter.com'])}"

        engagement_time_ms = random.randint(0, 120000)
        is_conversion = (event_name in ["purchase", "sign_up"])
        currency = "USD"
        value = round(random.uniform(10.0, 500.0), 2) if event_name == "purchase" else 0.0
        ga_session_number = random.randint(1, 10)

        return {
            "client_id": client_id,
            "user_id": user_id,
            "event_name": event_name,
            "event_timestamp": event_timestamp,
            "event_date": event_date,
            "event_params": json.dumps(event_params),
            "user_properties": json.dumps([]),
            "traffic_source": json.dumps(traffic_source),
            "device": json.dumps(device),
            "geo": json.dumps(geo),
            "page_location": page_location,
            "page_title": page_title,
            "page_referrer": page_referrer,
            "engagement_time_ms": engagement_time_ms,
            "is_conversion": is_conversion,
            "currency": currency,
            "value": value,
            "session_id": session_id,
            "ga_session_number": ga_session_number
        }

    def generate_session_events(
        self,
        client_id: Optional[str] = None,
        user_id: Optional[str] = None,
        event_count: Optional[int] = None,
        start_time: Optional[int] = None
    ) -> List[Dict]:
        """Generate a sequence of events forming a coherent session."""
        if client_id is None:
            client_id = self._generate_client_id()

        session_id = str(self._next_session_id())
        event_count = event_count or random.randint(2, 15)

        # Base time: use provided start_time or random time in last 30 days
        if start_time is not None:
            base_time = start_time
        else:
            base_time = int(time.time() * 1_000_000) - random.randint(0, 86400 * 30) * 1_000_000

        events = []

        # First event is always session_start
        events.append(self.generate_event(
            client_id=client_id,
            session_id=session_id,
            user_id=user_id,
            event_name="session_start",
            event_timestamp=base_time
        ))

        # Second event is always page_view (landing page)
        events.append(self.generate_event(
            client_id=client_id,
            session_id=session_id,
            user_id=user_id,
            event_name="page_view",
            event_timestamp=base_time + random.randint(100_000, 500_000)  # 0.1-0.5 sec
        ))

        # Remaining events with realistic timing (5s to 5min gaps)
        current_time = base_time + 500_000
        for i in range(event_count - 2):
            gap = random.randint(5_000_000, 300_000_000)  # 5 sec to 5 min
            current_time += gap

            event_name = self._weighted_event()
            events.append(self.generate_event(
                client_id=client_id,
                session_id=session_id,
                user_id=user_id,
                event_name=event_name,
                event_timestamp=current_time
            ))

        return events

    def generate_export_batch(
        self,
        num_users: int = 200,
        events_per_user_range: tuple = (3, 20),
        shared_customers: Optional[List[Dict]] = None
    ) -> List[Dict]:
        """Generate a batch of events simulating a BigQuery Export table."""
        all_events = []

        for i in range(num_users):
            client_id = self._generate_client_id()

            # 30% of users get a user_id from the shared customer pool
            user_id = None
            if shared_customers and random.random() < 0.3:
                shared = random.choice(shared_customers)
                user_id = shared["email"]  # user_id = email for entity resolution demo

            num_events = random.randint(*events_per_user_range)

            # Generate 1-3 sessions per user
            num_sessions = random.randint(1, min(3, max(1, num_events // 4)))

            events_per_session = num_events // num_sessions

            # CRITICAL FIX: Track last session end to enforce 31-min gap (Issue #1)
            last_session_end = None

            for session_idx in range(num_sessions):
                # Calculate session start time
                if last_session_end is None:
                    # First session: random time in last 30 days
                    start_time = int(time.time() * 1_000_000) - random.randint(0, 86400 * 30) * 1_000_000
                else:
                    # Subsequent sessions: 31-35 minutes after previous session ended
                    gap_minutes = random.randint(31, 35)
                    gap_microseconds = gap_minutes * 60 * 1_000_000
                    start_time = last_session_end + gap_microseconds

                session_events = self.generate_session_events(
                    client_id=client_id,
                    user_id=user_id,
                    event_count=events_per_session,
                    start_time=start_time
                )
                all_events.extend(session_events)

                # Update last session end time
                last_session_end = session_events[-1]["event_timestamp"]

        return all_events
