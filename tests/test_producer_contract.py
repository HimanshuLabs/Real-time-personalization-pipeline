import importlib.util
import statistics
from collections import defaultdict
from datetime import datetime, timedelta
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


@pytest.fixture(scope="module")
def commerce_events(producer, events):
    return [event for event in events if event["event_type"] in producer.COMMERCE_EVENT_TYPES]


@pytest.fixture(scope="module")
def stream(producer):
    # 5000 events from a fresh pool, in the order they were emitted.
    pool = producer.SessionPool()
    events = [pool.next_event() for _ in range(5000)]
    assert pool.sessions_completed > 500
    return events


@pytest.fixture(scope="module")
def sessions(stream):
    grouped = defaultdict(list)
    for event in stream:
        grouped[event["session_id"]].append(event)
    return grouped


def event_time(event):
    return datetime.fromisoformat(event["event_time"])


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


def test_product_ids_one_to_one(producer, commerce_events):
    catalog = [name for names in producer.PRODUCTS.values() for name in names]
    assert sorted(producer.PRODUCT_IDS) == sorted(catalog)
    assert len(set(producer.PRODUCT_IDS.values())) == len(producer.PRODUCT_IDS)

    ids_per_name = defaultdict(set)
    for event in commerce_events:
        ids_per_name[event["product_name"]].add(event["product_id"])
        assert event["product_id"] == producer.PRODUCT_IDS[event["product_name"]]
    assert all(len(ids) == 1 for ids in ids_per_name.values())


def test_reference_prices_inside_category_band(producer):
    for category, names in producer.PRODUCTS.items():
        low, high = producer.CATEGORY_PRICE_BANDS[category]
        for name in names:
            price = producer.PRODUCT_REFERENCE_PRICES[name]
            assert low <= price <= high, f"{name}: {price} outside {category} band {low}-{high}"


def test_reference_prices_cover_exactly_the_catalog(producer):
    catalog = {name for names in producer.PRODUCTS.values() for name in names}
    priced = set(producer.PRODUCT_REFERENCE_PRICES)
    assert len(catalog) == 29
    assert not catalog - priced, f"missing prices: {sorted(catalog - priced)}"
    assert not priced - catalog, f"prices for unknown products: {sorted(priced - catalog)}"


def test_original_price_within_10_percent_of_reference(producer, commerce_events):
    assert commerce_events
    for event in commerce_events:
        reference = producer.PRODUCT_REFERENCE_PRICES[event["product_name"]]
        # +0.005: original_price is rounded to 2 decimals after scaling.
        assert reference * 0.9 - 0.005 <= event["original_price"] <= reference * 1.1 + 0.005, event
        assert event["discounted_price"] <= event["original_price"]
        assert 0 <= event["discount_percent"] <= 50


def test_product_fields_are_event_keys(producer):
    assert set(producer.PRODUCT_FIELDS) <= set(EXPECTED_KEYS)


def test_non_commerce_events_have_no_product(producer, events):
    non_commerce = [e for e in events if e["event_type"] not in producer.COMMERCE_EVENT_TYPES]
    assert non_commerce
    for event in non_commerce:
        assert all(event[field] is None for field in producer.PRODUCT_FIELDS), event


def test_commerce_events_have_product_and_prices(producer, commerce_events):
    required = ["product_id", "product_name", "category", "quantity",
                "original_price", "discount_percent", "discounted_price"]
    for event in commerce_events:
        assert all(event[field] is not None for field in required), event

        if event["event_type"] in producer.CART_STAGE_EVENT_TYPES:
            assert 1 <= event["quantity"] <= 3
            assert event["cart_value"] == round(event["discounted_price"] * event["quantity"], 2)
        else:
            assert event["quantity"] == 1
            assert event["cart_value"] is None


def test_one_user_product_and_price_per_session(sessions):
    for session_events in sessions.values():
        fixed = {(e["user_id"], e["product_id"], e["original_price"], e["discount_percent"])
                 for e in session_events if e["product_id"] is not None}
        assert len(fixed) <= 1, session_events
        assert len({e["user_id"] for e in session_events}) == 1


def test_payment_success_follows_view_cart_and_checkout(sessions):
    payments = 0
    for session_events in sessions.values():
        seen = set()
        for event in session_events:
            if event["event_type"] == "payment_success":
                payments += 1
                assert {"product_view", "add_to_cart", "checkout_started"} <= seen, session_events
            seen.add(event["event_type"])
    assert payments > 0


def test_order_placed_only_after_payment_success(sessions):
    orders = 0
    for session_events in sessions.values():
        seen = set()
        for event in session_events:
            if event["event_type"] == "order_placed":
                orders += 1
                assert "payment_success" in seen, session_events
            seen.add(event["event_type"])
    assert orders > 0


RECOMMENDATION_FIELDS = [
    "recommendation_rank", "recommendation_clicked", "recommendation_algorithm", "click_position",
]


def test_recommendation_fields_only_on_recommendation_events(sessions):
    for session_events in sessions.values():
        impression = None
        for event in session_events:
            values = [event[field] for field in RECOMMENDATION_FIELDS]
            if event["event_type"] == "recommendation_impression":
                assert event["recommendation_clicked"] is False
                assert event["recommendation_rank"] is not None
                assert event["recommendation_algorithm"] is not None
                assert event["click_position"] is None
                impression = event
            elif event["event_type"] == "recommendation_click":
                assert event["recommendation_clicked"] is True
                assert event["click_position"] is not None
                # Same rank and algorithm as the impression it follows.
                assert impression is not None
                assert event["recommendation_rank"] == impression["recommendation_rank"]
                assert event["recommendation_algorithm"] == impression["recommendation_algorithm"]
            else:
                assert values == [None] * 4, event


def test_search_query_only_on_search_and_matches_category(producer, sessions):
    assert sorted(producer.CATEGORY_SEARCH_QUERIES) == sorted(producer.PRODUCT_CATEGORIES)
    searches = 0
    for session_events in sessions.values():
        categories = {e["category"] for e in session_events if e["category"] is not None}
        for event in session_events:
            if event["event_type"] != "search":
                assert event["search_query"] is None, event
                continue
            searches += 1
            assert event["search_query"]
            # Category is known once the session has a commerce event.
            for category in categories:
                assert event["search_query"] in producer.CATEGORY_SEARCH_QUERIES[category]
    assert searches > 0


def test_time_since_last_event_is_the_users_real_gap(stream):
    previous = {}
    for event in stream:
        user_id = event["user_id"]
        if user_id not in previous:
            assert event["time_since_last_event_ms"] is None, event
        else:
            gap = (event_time(event) - event_time(previous[user_id])).total_seconds() * 1000
            assert event["time_since_last_event_ms"] == round(gap), event
        previous[user_id] = event


def test_session_counters_follow_session_state(sessions):
    for session_events in sessions.values():
        started = event_time(session_events[0])
        views = 0
        for event in session_events:
            if event["event_type"] == "product_view":
                views += 1
            assert event["session_duration_sec"] == int((event_time(event) - started).total_seconds())
            assert event["items_viewed_in_session"] == views
            assert event["repeat_product_view_count"] == views


def test_purchase_probability_rises_along_the_funnel(stream):
    by_type = defaultdict(list)
    for event in stream:
        assert 0 < event["purchase_probability"] < 1, event
        by_type[event["event_type"]].append(event["purchase_probability"])

    high = sum(event["purchase_probability"] >= 0.95 for event in stream)
    assert high / len(stream) < 0.05

    medians = [
        statistics.median(by_type["home_page_view"] + by_type["login"] + by_type["search"]),
        statistics.median(by_type["product_view"]),
        statistics.median(by_type["add_to_cart"]),
        statistics.median(by_type["checkout_started"]),
        statistics.median(by_type["payment_success"]),
    ]
    assert medians == sorted(medians) and len(set(medians)) == len(medians), medians


def test_location_mostly_home(producer, stream):
    home_city = sum(e["city"] == e["home_city"] for e in stream) / len(stream)
    home_country = sum(e["country"] == producer.HOME_COUNTRY for e in stream) / len(stream)
    assert 0.80 <= home_city <= 0.90, home_city
    assert 0.92 <= home_country <= 0.98, home_country
    # The home city is only ever reported from the home country.
    assert all(e["country"] == producer.HOME_COUNTRY for e in stream if e["city"] == e["home_city"])


def test_inventory_drops_on_purchase_and_restocks(producer, stream):
    low, high = producer.INITIAL_STOCK_RANGE
    last = {}
    for event in stream:
        if event["event_type"] not in producer.COMMERCE_EVENT_TYPES:
            assert event["inventory_remaining"] is None, event
            continue

        product = event["product_name"]
        stock = event["inventory_remaining"]
        assert stock >= 0
        if product not in last:
            expected_before = None
        elif last[product] < producer.RESTOCK_BELOW:
            expected_before = None      # restocked after the previous event
        else:
            expected_before = last[product]

        before = stock + event["quantity"] if event["event_type"] == "payment_success" else stock
        if expected_before is None:
            assert low <= before <= high, event
        else:
            assert before == expected_before, event
        last[product] = stock

    # Restock path, rarely reached in a short stream: a sale leaving fewer
    # than RESTOCK_BELOW units shows that stock, then the product is restocked.
    pool = producer.SessionPool()
    session = producer.Session(user_id=1)
    session.cart_quantity = 3
    pool.stock[session.product_name] = producer.RESTOCK_BELOW + 1
    assert pool.inventory_remaining(session, "payment_success") == producer.RESTOCK_BELOW - 2
    assert low <= pool.inventory_remaining(session, "product_view") <= high


def test_no_events_from_inactive_accounts_or_products(producer, stream):
    assert any(producer.account_status(u) == "inactive" for u in range(1, producer.NUM_USERS + 1))
    assert any(not producer.is_product_active(pid) for pid in producer.PRODUCT_IDS.values())
    for event in stream:
        assert producer.account_status(event["user_id"]) == "active", event
        if event["product_id"] is not None:
            assert producer.is_product_active(event["product_id"]), event


def test_remove_from_cart_ends_without_checkout(sessions):
    removed = 0
    for session_events in sessions.values():
        types = [event["event_type"] for event in session_events]
        if "remove_from_cart" not in types:
            continue
        removed += 1
        assert types.index("add_to_cart") < types.index("remove_from_cart")
        after = set(types[types.index("remove_from_cart") + 1:])
        assert after <= {"logout"}, types
        assert not {"checkout_started", "payment_initiated", "payment_success"} & set(types)
    assert removed > 0


def test_coupon_only_between_checkout_and_payment(sessions):
    coupons = 0
    for session_events in sessions.values():
        types = [event["event_type"] for event in session_events]
        for index, event_type in enumerate(types):
            if event_type != "coupon_applied":
                continue
            coupons += 1
            assert index > 0 and types[index - 1] == "checkout_started", types
            # The session may still be running at the end of the stream.
            assert index + 1 == len(types) or types[index + 1] == "payment_initiated", types
    assert coupons > 0


def test_new_event_types_placement(sessions):
    after = {
        "ad_impression": {"home_page_view"},
        "ad_click": {"ad_impression"},
        "image_zoom": {"product_view"},
        "wishlist_add": {"product_view", "image_zoom"},
        "review_submitted": {"order_placed"},
        "rating_given": {"order_placed"},
    }
    seen = set()
    for session_events in sessions.values():
        types = [event["event_type"] for event in session_events]
        for index, event_type in enumerate(types):
            if event_type in after:
                seen.add(event_type)
                assert index > 0 and types[index - 1] in after[event_type], types
    assert seen >= {"ad_impression", "ad_click", "image_zoom", "wishlist_add"}


def test_time_gap_too_large_for_int32_is_none(producer):
    pool = producer.SessionPool()
    now = datetime.fromisoformat("2026-11-11T09:00:00+00:00")

    pool.last_event_at[1] = now - timedelta(milliseconds=producer.MAX_INT32)
    assert pool.time_since_last_event_ms(1, now) == 2_147_483_647

    pool.last_event_at[1] = now - timedelta(milliseconds=producer.MAX_INT32 + 1)
    assert pool.time_since_last_event_ms(1, now) is None


def test_journey_stage_follows_event_type(producer, sessions):
    assert set(producer.JOURNEY_STAGE_BY_EVENT_TYPE.values()) <= set(producer.USER_JOURNEY_STAGES)
    logouts = 0
    for session_events in sessions.values():
        previous = None
        for event in session_events:
            if event["event_type"] == "logout":
                logouts += 1
                assert previous is not None
                assert event["user_journey_stage"] == previous["user_journey_stage"], session_events
            else:
                expected = producer.JOURNEY_STAGE_BY_EVENT_TYPE[event["event_type"]]
                assert event["user_journey_stage"] == expected, event
            previous = event
    assert logouts > 0


DEVICE_CONTEXT = ["device_type", "operating_system", "browser", "network_type", "app_version"]


def test_device_context_fixed_per_session_and_consistent(producer, sessions):
    for session_events in sessions.values():
        contexts = {tuple(event[field] for field in DEVICE_CONTEXT) for event in session_events}
        assert len(contexts) == 1, session_events

        device, operating_system, browser, network, app_version = contexts.pop()
        assert device in producer.DEVICES
        assert operating_system in producer.DEVICE_OS_BROWSERS[device]
        assert browser in producer.DEVICE_OS_BROWSERS[device][operating_system]
        assert network in producer.DEVICE_NETWORKS[device]
        assert app_version

    # The combinations only use known values, and phones and tablets run mobile systems.
    for device, systems in producer.DEVICE_OS_BROWSERS.items():
        assert set(systems) <= set(producer.OPERATING_SYSTEMS)
        assert all(set(browsers) <= set(producer.BROWSERS) for browsers in systems.values())
        assert set(producer.DEVICE_NETWORKS[device]) <= set(producer.NETWORK_TYPES)
        if device in ("mobile", "tablet"):
            assert set(systems) <= {"android", "ios"}
    assert "safari" not in producer.DEVICE_OS_BROWSERS["mobile"]["android"]


def test_payment_method_only_on_payment_events(producer, sessions):
    payments = 0
    for session_events in sessions.values():
        methods = set()
        for event in session_events:
            if event["event_type"] in producer.PAYMENT_EVENT_TYPES:
                payments += 1
                assert event["payment_method"] in producer.PAYMENT_METHODS, event
                methods.add(event["payment_method"])
            else:
                assert event["payment_method"] is None, event
        assert len(methods) <= 1, session_events
    assert payments > 0


def test_same_user_same_profile(producer, events):
    profiles = defaultdict(set)
    for event in events:
        profiles[event["user_id"]].add(tuple(event[field] for field in PROFILE_FIELDS))
    assert all(len(seen) == 1 for seen in profiles.values())

    before = producer.get_user_profile(42)
    producer.get_user_profile.cache_clear()
    assert producer.get_user_profile(42) == before
