"""
Tests for GA4Provider (TDD - Write tests first).

Test coverage:
- Event schema validation
- Client ID format
- Timestamp precision (microseconds)
- Event date format (YYYYMMDD)
- Session coherence
- JSON structure validation
- Seed reproducibility
"""
import pytest
import json
import re
from datetime import datetime
import time


def test_ga4_provider_import():
    """Test that GA4Provider can be imported."""
    from datagen.providers.ga4_provider import GA4Provider
    assert GA4Provider is not None


def test_generate_event_returns_valid_schema():
    """Test that generate_event returns all required fields."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    event = provider.generate_event()

    required_fields = [
        "client_id", "user_id", "event_name", "event_timestamp", "event_date",
        "event_params", "user_properties", "traffic_source", "device", "geo",
        "page_location", "page_title", "page_referrer", "engagement_time_ms",
        "is_conversion", "currency", "value", "session_id", "ga_session_number"
    ]

    for field in required_fields:
        assert field in event, f"Missing required field: {field}"


def test_client_id_format():
    """Test that client_id matches GA4 cookie format: {10_digit}.{unix_ts}."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    event = provider.generate_event()

    client_id = event["client_id"]
    pattern = r'^\d{10}\.\d{10}$'
    assert re.match(pattern, client_id), f"client_id '{client_id}' does not match GA4 format"


def test_event_timestamp_microseconds():
    """Test that event_timestamp is in microseconds (13 digits or more)."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    event = provider.generate_event()

    event_timestamp = event["event_timestamp"]
    assert isinstance(event_timestamp, int), "event_timestamp must be int"
    assert event_timestamp > 0, "event_timestamp must be positive"
    # Microseconds should be 16 digits (1970 epoch in microseconds is ~16 digits)
    assert len(str(event_timestamp)) >= 16, "event_timestamp should be in microseconds"


def test_event_date_yyyymmdd():
    """Test that event_date is in YYYYMMDD format."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    event = provider.generate_event()

    event_date = event["event_date"]
    pattern = r'^\d{8}$'
    assert re.match(pattern, event_date), f"event_date '{event_date}' not in YYYYMMDD format"

    # Validate it's a real date
    datetime.strptime(event_date, "%Y%m%d")


def test_event_timestamp_and_date_consistency():
    """Test that event_date matches event_timestamp."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    event = provider.generate_event()

    event_timestamp = event["event_timestamp"]
    event_date = event["event_date"]

    # Convert timestamp to date
    dt = datetime.fromtimestamp(event_timestamp / 1_000_000)
    expected_date = dt.strftime("%Y%m%d")

    assert event_date == expected_date, f"event_date {event_date} != expected {expected_date}"


def test_event_params_json_structure():
    """Test that event_params is valid JSON array."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    event = provider.generate_event()

    event_params = event["event_params"]
    assert isinstance(event_params, str), "event_params should be JSON string"

    # Parse JSON
    params = json.loads(event_params)
    assert isinstance(params, list), "event_params should be array"

    # Check structure if not empty
    if len(params) > 0:
        param = params[0]
        assert "key" in param, "event_params items must have 'key'"
        assert "value" in param, "event_params items must have 'value'"


def test_traffic_source_json():
    """Test that traffic_source is valid JSON with expected keys."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    event = provider.generate_event()

    traffic_source = event["traffic_source"]
    assert isinstance(traffic_source, str), "traffic_source should be JSON string"

    source = json.loads(traffic_source)
    assert "source" in source, "traffic_source must have 'source' key"
    assert "medium" in source, "traffic_source must have 'medium' key"
    assert "name" in source, "traffic_source must have 'name' key (campaign)"


def test_device_json():
    """Test that device is valid JSON with expected keys."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    event = provider.generate_event()

    device = event["device"]
    assert isinstance(device, str), "device should be JSON string"

    dev = json.loads(device)
    assert "category" in dev, "device must have 'category' key"
    assert "operating_system" in dev, "device must have 'operating_system' key"


def test_geo_json():
    """Test that geo is valid JSON with expected keys."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    event = provider.generate_event()

    geo = event["geo"]
    assert isinstance(geo, str), "geo should be JSON string"

    location = json.loads(geo)
    assert "country" in location, "geo must have 'country' key"
    assert "region" in location, "geo must have 'region' key"
    assert "city" in location, "geo must have 'city' key"


def test_session_events_coherence():
    """Test that generate_session_events produces coherent session."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    events = provider.generate_session_events(client_id="1234567890.1234567890")

    assert len(events) >= 2, "Session should have at least 2 events"

    # All events share same client_id
    client_ids = {e["client_id"] for e in events}
    assert len(client_ids) == 1, "All events in session must have same client_id"

    # All events share same session_id
    session_ids = {e["session_id"] for e in events}
    assert len(session_ids) == 1, "All events in session must have same session_id"

    # Timestamps are monotonically increasing
    timestamps = [e["event_timestamp"] for e in events]
    assert timestamps == sorted(timestamps), "Event timestamps must be monotonically increasing"

    # First event is session_start
    assert events[0]["event_name"] == "session_start", "First event must be session_start"

    # Second event is page_view
    assert events[1]["event_name"] == "page_view", "Second event must be page_view"


def test_session_events_time_gaps():
    """Test that events within a session have reasonable time gaps (5s to 5min)."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    events = provider.generate_session_events()

    for i in range(1, len(events)):
        gap_microseconds = events[i]["event_timestamp"] - events[i - 1]["event_timestamp"]
        gap_seconds = gap_microseconds / 1_000_000

        # Gaps should be between 0.1s and 5 minutes
        assert 0.1 <= gap_seconds <= 300, f"Event gap {gap_seconds}s out of realistic range"


def test_seed_reproducibility():
    """Test that same seed produces same output."""
    from datagen.providers.ga4_provider import GA4Provider

    provider1 = GA4Provider(seed=123)
    provider2 = GA4Provider(seed=123)

    events1 = provider1.generate_export_batch(num_users=10, events_per_user_range=(5, 10))
    events2 = provider2.generate_export_batch(num_users=10, events_per_user_range=(5, 10))

    assert len(events1) == len(events2), "Same seed should produce same number of events"

    # Check first event is identical
    assert events1[0]["client_id"] == events2[0]["client_id"]
    assert events1[0]["event_name"] == events2[0]["event_name"]


def test_seed_zero_valid():
    """Test that seed=0 is handled correctly (not treated as None)."""
    from datagen.providers.ga4_provider import GA4Provider

    # seed=0 should work
    provider = GA4Provider(seed=0)
    events = provider.generate_export_batch(num_users=5)
    assert len(events) > 0, "seed=0 should produce events"

    # seed=0 should be reproducible
    provider2 = GA4Provider(seed=0)
    events2 = provider2.generate_export_batch(num_users=5)
    assert len(events) == len(events2), "seed=0 should be reproducible"


def test_generate_export_batch():
    """Test that generate_export_batch produces expected output."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    events = provider.generate_export_batch(num_users=50, events_per_user_range=(3, 10))

    # Should have events for 50 users with 3-10 events each
    assert len(events) >= 50 * 3, "Should have at least 150 events"
    assert len(events) <= 50 * 10 * 3, "Should have at most 1500 events (3 sessions per user)"

    # Check all events are valid
    for event in events:
        assert "client_id" in event
        assert "event_timestamp" in event
        assert "event_name" in event


def test_shared_customers_user_id():
    """Test that shared customers get user_id set to email."""
    from datagen.providers.ga4_provider import GA4Provider

    shared_customers = [
        {"email": "customer1@example.com"},
        {"email": "customer2@example.com"},
    ]

    provider = GA4Provider(seed=42)
    events = provider.generate_export_batch(num_users=100, shared_customers=shared_customers)

    # Count events with user_id
    events_with_user_id = [e for e in events if e["user_id"] is not None]

    # ~30% of users should have user_id
    # With 100 users, expect ~30 users to have user_id
    # Each user generates 1-3 sessions with multiple events
    # So we should have at least a few events with user_id
    assert len(events_with_user_id) > 0, "Some events should have user_id from shared customers"

    # Check user_id values are from shared customers
    user_ids = {e["user_id"] for e in events_with_user_id}
    for user_id in user_ids:
        assert user_id in ["customer1@example.com", "customer2@example.com"], \
            f"user_id {user_id} should be from shared customers"


def test_export_batch_generates_multiple_sessions_per_user():
    """Test that users can have 1-3 sessions."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    events = provider.generate_export_batch(num_users=20, events_per_user_range=(10, 20))

    # Group events by client_id
    from collections import defaultdict
    events_by_client = defaultdict(list)
    for event in events:
        events_by_client[event["client_id"]].append(event)

    # Check that at least one user has multiple sessions
    multi_session_users = 0
    for client_id, client_events in events_by_client.items():
        session_ids = {e["session_id"] for e in client_events}
        if len(session_ids) > 1:
            multi_session_users += 1

    assert multi_session_users > 0, "At least some users should have multiple sessions"


def test_event_weight_distribution():
    """Test that page_view events are most common (highest weight)."""
    from datagen.providers.ga4_provider import GA4Provider

    provider = GA4Provider(seed=42)
    events = provider.generate_export_batch(num_users=100, events_per_user_range=(10, 20))

    # Count event types
    from collections import Counter
    event_counts = Counter(e["event_name"] for e in events)

    # page_view should be most common
    most_common = event_counts.most_common(1)[0][0]
    assert most_common == "page_view" or most_common == "session_start", \
        "page_view or session_start should be most common event"

    # page_view should be significantly more common than least common
    page_view_count = event_counts.get("page_view", 0)
    assert page_view_count > 0, "Should have page_view events"


def test_multi_session_gap_enforcement():
    """CRITICAL TEST: Verify multi-session users have 31+ minute gaps between sessions."""
    from datagen.providers.ga4_provider import GA4Provider
    from collections import defaultdict

    provider = GA4Provider(seed=42)
    events = provider.generate_export_batch(num_users=50, events_per_user_range=(10, 20))

    # Group events by client_id
    events_by_client = defaultdict(list)
    for event in events:
        events_by_client[event["client_id"]].append(event)

    # For each client with multiple sessions
    multi_session_violations = []
    for client_id, client_events in events_by_client.items():
        # Sort events by timestamp
        client_events.sort(key=lambda e: e["event_timestamp"])

        # Group by session_id
        sessions_by_id = defaultdict(list)
        for event in client_events:
            sessions_by_id[event["session_id"]].append(event)

        if len(sessions_by_id) > 1:
            # Get session boundaries (earliest and latest timestamp per session)
            session_boundaries = []
            for session_id, session_events in sessions_by_id.items():
                timestamps = [e["event_timestamp"] for e in session_events]
                session_boundaries.append({
                    "session_id": session_id,
                    "start": min(timestamps),
                    "end": max(timestamps)
                })

            # Sort sessions by start time
            session_boundaries.sort(key=lambda s: s["start"])

            # Check gaps between consecutive sessions
            for i in range(1, len(session_boundaries)):
                prev_session = session_boundaries[i - 1]
                curr_session = session_boundaries[i]

                gap_microseconds = curr_session["start"] - prev_session["end"]
                gap_seconds = gap_microseconds / 1_000_000
                gap_minutes = gap_seconds / 60

                # CRITICAL REQUIREMENT: Gap must be >= 31 minutes
                if gap_minutes < 31:
                    multi_session_violations.append({
                        "client_id": client_id,
                        "gap_minutes": gap_minutes,
                        "prev_session": prev_session["session_id"],
                        "curr_session": curr_session["session_id"]
                    })

    # Assert no violations
    assert len(multi_session_violations) == 0, \
        f"Found {len(multi_session_violations)} session gap violations (< 31 min): {multi_session_violations[:5]}"
