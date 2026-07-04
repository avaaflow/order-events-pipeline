"""Tests for synthetic event generator."""

from generators.fake_events import EVENT_TYPES, CITIES, generate_event, generate_events


def test_generate_event_has_required_fields():
    event = generate_event()
    data = event.to_dict()
    assert data["event_id"]
    assert data["order_id"]
    assert data["user_id"] > 0
    assert data["restaurant_id"] > 0
    assert data["event_type"] in EVENT_TYPES
    assert data["city"] in CITIES
    assert data["amount"] > 0
    assert "timestamp" in data


def test_generate_events_count():
    events = list(generate_events(100))
    assert len(events) == 100


def test_duplicate_generation():
    events = list(generate_events(5000))
    event_ids = [e.event_id for e in events]
    # intentional duplicates reuse order data but new event_id
    assert len(event_ids) == len(set(event_ids))
