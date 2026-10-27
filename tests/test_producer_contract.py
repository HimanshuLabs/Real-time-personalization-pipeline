import importlib.util
from collections import defaultdict
from pathlib import Path

import pytest

# CI installs only ruff and pytest; skip instead of failing when the
# producer's runtime packages are missing.
pytest.importorskip("faker")
confluent_kafka = pytest.importorskip("confluent_kafka")


ROOT = Path(__file__).resolve().parents[1]

EXPECTED_KEYS = [
    "event_id", "session_id", "user_id", "user_name", "email", "gender", "age",
    "membership_tier", "loyalty_points", "preferred_language", "home_city", "home_state",
    "event_time", "event_type", "user_journey_stage", "user_segment", "is_prime_user",
    "product_id", "product_name", "category", "quantity", "original_price",
    "discount_percent", "discounted_price", "cart_value", "inventory_remaining",
    "search_query", "time_on_page_sec", "scroll_depth_percent", "hover_duration_ms",
    "session_duration_sec", "items_viewed_in_session", "repeat_product_view_count",
    "time_since_last_event_ms", "recommendation_rank", "recommendation_clicked",
    "recommendation_algorithm", "click_position", "engagement_score",
    "purchase_probability", "cart_abandonment_probability", "ab_test_group",
    "payment_method", "device_type", "operating_system", "browser", "network_type",
    "app_version", "traffic_source", "campaign_id", "api_latency_ms",
    "page_load_time_ms", "fraud_score", "country", "city", "ip_address",
    "schema_version", "source",
]

PROFILE_FIELDS = [
    "user_name", "email", "gender", "age", "membership_tier", "loyalty_points",
    "preferred_language", "home_city", "home_state", "user_segment", "is_prime_user",
]


@pytest.fixture(scope="module")
def producer():
    # Any Producer construction during import would mean a Kafka connection.
    original = confluent_kafka.Producer

    def refuse(*args, **kwargs):
        raise AssertionError("producer.py created a Kafka Producer on import")

    confluent_kafka.Producer = refuse
    try:
        spec = importlib.util.spec_from_file_location(
            "producer_under_test", ROOT / "data-generator" / "producer.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        confluent_kafka.Producer = original
    return module


@pytest.fixture(scope="module")
def events(producer):
    return [producer.generate_event() for _ in range(1000)]


def test_event_has_58_keys_in_order(events):
    assert len(EXPECTED_KEYS) == 58
    assert all(list(event) == EXPECTED_KEYS for event in events)


def test_schema_version(events):
    assert {event["schema_version"] for event in events} == {"4.0"}


def test_event_types_are_known(producer, events):
    unknown = {event["event_type"] for event in events} - set(producer.EVENT_TYPES)
    assert not unknown, f"event types not in EVENT_TYPES: {unknown}"


def test_event_ids_unique(events):
    assert len({event["event_id"] for event in events}) == len(events)


def test_every_category_has_products(producer, events):
    empty = [category for category in producer.PRODUCT_CATEGORIES if not producer.PRODUCTS.get(category)]
    assert not empty, f"categories without products: {empty}"
    assert not [event for event in events if event["product_name"] == "unknown"]


def test_product_ids_one_to_one(producer, events):
    catalog = [name for names in producer.PRODUCTS.values() for name in names]
    assert sorted(producer.PRODUCT_IDS) == sorted(catalog)
    assert len(set(producer.PRODUCT_IDS.values())) == len(producer.PRODUCT_IDS)

    ids_per_name = defaultdict(set)
    for event in events:
        ids_per_name[event["product_name"]].add(event["product_id"])
        assert event["product_id"] == producer.PRODUCT_IDS[event["product_name"]]
    assert all(len(ids) == 1 for ids in ids_per_name.values())


def test_same_user_same_profile(producer, events):
    profiles = defaultdict(set)
    for event in events:
        profiles[event["user_id"]].add(tuple(event[field] for field in PROFILE_FIELDS))
    assert all(len(seen) == 1 for seen in profiles.values())

    before = producer.get_user_profile(42)
    producer.get_user_profile.cache_clear()
    assert producer.get_user_profile(42) == before
