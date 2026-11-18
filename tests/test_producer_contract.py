import ast
import csv
import importlib.util
import os
import shutil
import signal
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import confluent_kafka
import pytest

ROOT = Path(__file__).resolve().parents[1]

SHOP_MASTER = ROOT / "reference-data" / "shop_master"


def read_shop_csv(file_name, key):
    # Read independently of producer.py, so the tests compare against the file itself.
    with open(SHOP_MASTER / file_name, newline="", encoding="utf-8") as file:
        return {int(row[key]): row for row in csv.DictReader(file)}


def shop_customers():
    return read_shop_csv("customers.csv", "customer_id")


def shop_products():
    return read_shop_csv("products.csv", "product_id")


EXPECTED_KEYS = [
    "event_id",
    "session_id",
    "user_id",
    "user_name",
    "email",
    "gender",
    "age",
    "membership_tier",
    "loyalty_points",
    "preferred_language",
    "home_city",
    "home_state",
    "event_time",
    "event_type",
    "user_journey_stage",
    "user_segment",
    "is_prime_user",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "original_price",
    "discount_percent",
    "discounted_price",
    "cart_value",
    "inventory_remaining",
    "search_query",
    "time_on_page_sec",
    "scroll_depth_percent",
    "hover_duration_ms",
    "session_duration_sec",
    "items_viewed_in_session",
    "repeat_product_view_count",
    "time_since_last_event_ms",
    "recommendation_rank",
    "recommendation_clicked",
    "recommendation_algorithm",
    "click_position",
    "engagement_score",
    "purchase_probability",
    "cart_abandonment_probability",
    "ab_test_group",
    "payment_method",
    "device_type",
    "operating_system",
    "browser",
    "network_type",
    "app_version",
    "traffic_source",
    "campaign_id",
    "api_latency_ms",
    "page_load_time_ms",
    "fraud_score",
    "country",
    "city",
    "ip_address",
    "schema_version",
    "source",
]

PROFILE_FIELDS = [
    "user_name",
    "email",
    "gender",
    "age",
    "membership_tier",
    "loyalty_points",
    "preferred_language",
    "home_city",
    "home_state",
    "user_segment",
    "is_prime_user",
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
    empty = [
        category for category in producer.PRODUCT_CATEGORIES if not producer.PRODUCTS.get(category)
    ]
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
    assert len(catalog) == 147
    assert not catalog - priced, f"missing prices: {sorted(catalog - priced)}"
    assert not priced - catalog, f"prices for unknown products: {sorted(priced - catalog)}"


def test_original_price_is_the_list_price(producer, commerce_events):
    assert commerce_events
    for event in commerce_events:
        assert event["original_price"] == producer.PRODUCT_REFERENCE_PRICES[event["product_name"]]
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
    required = [
        "product_id",
        "product_name",
        "category",
        "quantity",
        "original_price",
        "discount_percent",
        "discounted_price",
    ]
    for event in commerce_events:
        assert all(event[field] is not None for field in required), event

        if event["event_type"] in producer.CART_EVENT_TYPES:
            assert 1 <= event["quantity"] <= 3
            assert event["cart_value"] == round(event["discounted_price"] * event["quantity"], 2)
        else:
            assert event["quantity"] == 1
            assert event["cart_value"] is None


def test_one_user_product_and_price_per_session(sessions):
    for session_events in sessions.values():
        fixed = {
            (e["user_id"], e["product_id"], e["original_price"], e["discount_percent"])
            for e in session_events
            if e["product_id"] is not None
        }
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
    "recommendation_rank",
    "recommendation_clicked",
    "recommendation_algorithm",
    "click_position",
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
            assert event["session_duration_sec"] == int(
                (event_time(event) - started).total_seconds()
            )
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


def test_abroad_event_never_uses_home_country_or_city(producer, monkeypatch):
    # Force the abroad branch, for a profile whose home city shares a name with
    # an abroad city.
    abroad_city = producer.ABROAD_LOCATIONS[0][1]
    profile = dict(producer.get_user_profile(producer.USER_IDS[0]), home_city=abroad_city)
    monkeypatch.setattr(producer, "ABROAD_RATE", 1.0)

    draws = {producer.draw_location(profile) for _ in range(500)}
    assert draws <= set(producer.ABROAD_LOCATIONS)
    assert all(country != producer.HOME_COUNTRY for country, _ in draws)
    assert all(city != abroad_city for _, city in draws)


def test_home_events_use_home_city_or_another_shop_city(producer, monkeypatch, stream):
    # Every at-home city is a city the shop's customers live in.
    cities = {customer["home_city"] for customer in shop_customers().values()}
    assert set(producer.SHOP_CITIES) == cities
    home = [e for e in stream if e["country"] == producer.HOME_COUNTRY]
    assert home and all(e["city"] in cities for e in home)

    # The other-city branch never returns the home city.
    profile = producer.get_user_profile(producer.USER_IDS[0])
    monkeypatch.setattr(producer, "ABROAD_RATE", 0.0)
    monkeypatch.setattr(producer, "HOME_CITY_RATE", 0.0)
    draws = {producer.draw_location(profile) for _ in range(500)}
    assert all(country == producer.HOME_COUNTRY for country, _ in draws)
    assert {city for _, city in draws} <= cities - {profile["home_city"]}


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
            expected_before = None  # restocked after the previous event
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
    session = producer.Session(user_id=producer.USER_IDS[0])
    session.cart_quantity = 3
    pool.stock[session.product_name] = producer.RESTOCK_BELOW + 1
    assert pool.inventory_remaining(session, "payment_success") == producer.RESTOCK_BELOW - 2
    assert low <= pool.inventory_remaining(session, "product_view") <= high


def test_no_events_from_inactive_accounts_or_discontinued_products(producer, stream):
    # Both kinds exist in the shop, so the check below is not vacuous.
    assert any(producer.account_status(u) == "inactive" for u in producer.USER_IDS)
    discontinued = {
        int(row["product_id"])
        for row in shop_products().values()
        if row["product_status"] == "discontinued"
    }
    assert discontinued
    assert all(not producer.is_product_active(pid) for pid in discontinued)

    for event in stream:
        assert producer.account_status(event["user_id"]) == "active", event
        if event["product_id"] is not None:
            assert event["product_id"] not in discontinued, event
            assert producer.is_product_active(event["product_id"]), event

    # No pool can start a session for an inactive account or a discontinued product.
    inactive = {u for u in producer.USER_IDS if producer.account_status(u) == "inactive"}
    assert not inactive & set(producer.SessionPool().active_user_ids)
    picked = {name for names in producer.ACTIVE_PRODUCTS.values() for name in names}
    assert not {producer.PRODUCT_IDS[name] for name in picked} & discontinued


def test_remove_from_cart_ends_without_checkout(sessions):
    removed = 0
    for session_events in sessions.values():
        types = [event["event_type"] for event in session_events]
        if "remove_from_cart" not in types:
            continue
        removed += 1
        assert types.index("add_to_cart") < types.index("remove_from_cart")
        after = set(types[types.index("remove_from_cart") + 1 :])
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


def test_ad_zoom_wishlist_and_feedback_events_follow_their_trigger(sessions):
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

    # get_user_profile returns a copy: changing it never changes later events.
    user_id = producer.USER_IDS[0]
    before = producer.get_user_profile(user_id)
    producer.get_user_profile(user_id)["user_name"] = "Changed"
    assert producer.get_user_profile(user_id) == before


# --- running the producer: exit status, signals, NUM_USERS, broker address ----------

PRODUCER_SCRIPT = ROOT / "data-generator" / "producer.py"


class StubMessage:
    def topic(self):
        return "user_events"


class StubProducer:
    """Delivers queued events on poll/flush; can fail the first one or leave some queued."""

    def __init__(self, fail_first=False, undelivered=0):
        self.queued = []
        self.fail_first = fail_first
        self.undelivered = undelivered
        self.produced = 0
        self.flush_timeouts = []

    def produce(self, topic, key, value, on_delivery):
        self.produced += 1
        self.queued.append(on_delivery)

    def poll(self, timeout=0):
        while self.queued:
            callback = self.queued.pop(0)
            failed = self.fail_first and self.produced == 1
            callback("simulated broker error" if failed else None, StubMessage())

    def flush(self, timeout):
        self.flush_timeouts.append(timeout)
        self.poll()
        return self.undelivered


def stop_after(calls):
    count = 0

    def pause():
        nonlocal count
        count += 1
        if count == calls:
            raise KeyboardInterrupt

    return pause


@pytest.fixture
def fresh_stats(producer):
    producer.delivery_stats.update(delivered=0, failed=0)
    return producer.delivery_stats


@pytest.mark.parametrize(
    ("stub", "expected_status", "summary"),
    [
        (StubProducer(), 0, "delivered=3 failed=0 undelivered=0"),
        (StubProducer(fail_first=True), 1, "delivered=2 failed=1 undelivered=0"),
        (StubProducer(undelivered=2), 1, "delivered=3 failed=0 undelivered=2"),
    ],
    ids=["delivered_only", "a_delivery_failed", "undelivered_at_flush"],
)
def test_run_exit_status(producer, fresh_stats, capsys, stub, expected_status, summary):
    status = producer.run(stub, producer.SessionPool(), pause=stop_after(3))

    assert status == expected_status
    assert stub.produced == 3
    assert stub.flush_timeouts == [producer.FLUSH_TIMEOUT_SECONDS]
    assert f"[producer] summary: {summary}" in capsys.readouterr().err


def test_sigterm_takes_the_ctrl_c_stop_path(producer, fresh_stats, capsys):
    originals = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    stub = StubProducer()
    calls = 0

    def pause():
        nonlocal calls
        calls += 1
        if calls == 2:
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(5)  # the handler interrupts this sleep

    try:
        producer.install_signal_handlers()
        assert signal.getsignal(signal.SIGINT) is producer.stop_on_first_signal
        assert signal.getsignal(signal.SIGTERM) is producer.stop_on_first_signal
        started = time.monotonic()
        status = producer.run(stub, producer.SessionPool(), pause=pause)
        elapsed = time.monotonic() - started
        # After the first stop signal, both are ignored for the rest of the shutdown.
        assert signal.getsignal(signal.SIGINT) is signal.SIG_IGN
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_IGN
    finally:
        for s, handler in originals.items():
            signal.signal(s, handler)

    assert status == 0
    assert elapsed < 4
    assert stub.produced == 2
    assert stub.flush_timeouts == [producer.FLUSH_TIMEOUT_SECONDS]
    assert "[producer] summary: delivered=2 failed=0 undelivered=0" in capsys.readouterr().err


# The real producer in its own process with a stub Kafka client whose final flush
# takes 1.5 seconds, so a second stop signal can land during it.
SLOW_FLUSH_PRODUCER = """
import sys, time
sys.path.insert(0, {producer_dir!r})
import producer

class SlowFlushProducer:
    def __init__(self, config):
        self.queued = []

    def produce(self, topic, key, value, on_delivery):
        self.queued.append(on_delivery)
        print("[stub] produced", file=sys.stderr, flush=True)

    def poll(self, timeout=0):
        while self.queued:
            self.queued.pop(0)(None, None)

    def flush(self, timeout):
        print("[stub] flushing", file=sys.stderr, flush=True)
        time.sleep(1.5)
        self.poll()
        return 0

producer.Producer = SlowFlushProducer
sys.exit(producer.main())
"""


def read_stderr_until(proc, marker, lines):
    for line in proc.stderr:
        lines.append(line)
        if marker in line:
            return
    raise AssertionError(f"producer exited before {marker!r}: {''.join(lines)}")


@pytest.mark.parametrize(
    ("first", "second", "second_during_flush"),
    [
        (signal.SIGINT, signal.SIGINT, True),
        (signal.SIGINT, signal.SIGINT, False),
        (signal.SIGTERM, signal.SIGINT, True),
    ],
    ids=["second_ctrl_c_during_flush", "two_sigint_back_to_back", "sigterm_then_sigint"],
)
def test_second_stop_signal_still_flushes_and_prints_the_summary(
    first, second, second_during_flush
):
    # GNU timeout signals the child and then its process group; a user may press
    # Ctrl+C twice. Only the first signal stops the producer.
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            SLOW_FLUSH_PRODUCER.format(producer_dir=str(PRODUCER_SCRIPT.parent)),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    lines = []
    try:
        read_stderr_until(proc, "[stub] produced", lines)
        os.kill(proc.pid, first)
        if second_during_flush:
            read_stderr_until(proc, "[stub] flushing", lines)
        os.kill(proc.pid, second)
        _, rest = proc.communicate(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
    stderr = "".join(lines) + rest

    assert proc.returncode == 0, stderr
    assert "Traceback" not in stderr
    assert "KeyboardInterrupt" not in stderr
    assert "[producer] summary: delivered=" in stderr
    assert "failed=0 undelivered=0" in stderr


def test_small_num_users_produces_events_without_hanging(producer):
    # NUM_USERS=10 caps the users to the first 10 customers of customers.csv.
    code = (
        "import sys, json\n"
        f"sys.path.insert(0, {str(PRODUCER_SCRIPT.parent)!r})\n"
        "import producer\n"
        "pool = producer.SessionPool()\n"
        "events = [pool.next_event() for _ in range(500)]\n"
        "print(json.dumps([pool.max_active, len(events), sorted({e['user_id'] for e in events})]))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "NUM_USERS": "10"},
    )
    assert result.returncode == 0, result.stderr

    max_active, count, users = __import__("json").loads(result.stdout)
    active = [u for u in producer.USER_IDS[:10] if producer.account_status(u) == "active"]
    assert 0 < len(active) < 10
    assert max_active == len(active)
    assert count == 500
    assert set(users) <= set(active)


def test_num_users_without_active_accounts_is_a_clear_error(producer, monkeypatch):
    # Not reachable with the real data, so force it.
    monkeypatch.setattr(producer, "account_status", lambda user_id: "inactive")

    with pytest.raises(ValueError, match="NUM_USERS=3 has no active accounts"):
        producer.SessionPool(num_users=3)


def test_num_users_below_1_exits_2_with_one_line_error():
    result = subprocess.run(
        [sys.executable, str(PRODUCER_SCRIPT)],
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "NUM_USERS": "0", "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1"},
    )

    assert result.returncode == 2
    assert result.stderr.strip() == "[producer] error: NUM_USERS must be at least 1, got 0"
    assert result.stdout == ""


def load_fresh_producer(monkeypatch, **env):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    spec = importlib.util.spec_from_file_location("producer_env_under_test", PRODUCER_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_producer_config_reads_bootstrap_env_and_is_idempotent(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    default = load_fresh_producer(monkeypatch)
    assert default.producer_config() == {
        "bootstrap.servers": "localhost:9092",
        "enable.idempotence": True,
    }

    custom = load_fresh_producer(monkeypatch, KAFKA_BOOTSTRAP_SERVERS="broker.test:19092")
    assert custom.producer_config()["bootstrap.servers"] == "broker.test:19092"
    assert "localhost:9092" not in PRODUCER_SCRIPT.read_text().replace(
        'os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")', ""
    )


# --- the shop: customers.csv and products.csv from P2's warehouse --------------------

# Event field -> customers.csv column, and how the CSV text becomes the event value.
CUSTOMER_FIELD_SOURCES = {
    "user_name": ("customer_name", str),
    "email": ("email", str),
    "gender": ("gender", str),
    "age": ("age", int),
    "membership_tier": ("membership_tier", str),
    "loyalty_points": ("loyalty_points", int),
    "preferred_language": ("preferred_language", str),
    "home_city": ("home_city", str),
    "home_state": ("home_state", str),
    "user_segment": ("customer_segment", str),
    "is_prime_user": ("is_prime_user", lambda value: value == "t"),
}


def test_every_events_profile_equals_its_customers_csv_row(producer, stream):
    assert sorted(CUSTOMER_FIELD_SOURCES) == sorted(PROFILE_FIELDS)
    customers = shop_customers()
    for event in stream:
        row = customers[event["user_id"]]
        for field, (column, convert) in CUSTOMER_FIELD_SOURCES.items():
            assert event[field] == convert(row[column]), (field, event)
        # Same types as before: bool and int, not CSV text.
        assert type(event["is_prime_user"]) is bool and type(event["loyalty_points"]) is int

    # get_user_profile is the full row, country included, for every customer.
    for user_id, row in customers.items():
        profile = producer.get_user_profile(user_id)
        assert set(profile) == set(PROFILE_FIELDS) | {"country"}
        assert profile["country"] == row["country"]
        assert all(
            profile[field] == convert(row[column])
            for field, (column, convert) in CUSTOMER_FIELD_SOURCES.items()
        )
        assert (
            producer.user_reference_attributes(user_id)["signup_date"].isoformat()
            == (row["signup_date"])
        )


def test_every_events_product_matches_products_csv(producer, stream):
    products = shop_products()
    commerce = [event for event in stream if event["product_id"] is not None]
    assert commerce
    for event in commerce:
        row = products[event["product_id"]]
        assert event["product_name"] == row["product_name"], event
        assert event["category"] == row["category"], event
        assert event["original_price"] == float(row["original_price"]), event

    # The producer's catalogue is products.csv, row for row.
    assert sorted(producer.PRODUCT_IDS.values()) == sorted(products)
    for product_id, row in products.items():
        name = row["product_name"]
        assert producer.PRODUCT_IDS[name] == product_id
        assert name in producer.PRODUCTS[row["category"]]
        assert producer.PRODUCT_REFERENCE_PRICES[name] == float(row["original_price"])
        assert producer.PRODUCT_BRAND_BY_NAME[name] == row["brand"]
        assert producer.is_product_active(product_id) == (row["product_status"] == "active")
    assert sorted(producer.PRODUCT_CATEGORIES) == sorted(
        ["Electronics", "Fashion", "Home", "Beauty", "Fitness", "Books"]
    )


def test_only_customer_ids_from_the_csv_are_produced(producer, stream):
    customer_ids = set(shop_customers())
    # IDs 1-3 are quarantined in P2: the IDs are not contiguous.
    assert not {1, 2, 3} & customer_ids
    assert producer.USER_IDS == list(shop_customers())
    produced = {event["user_id"] for event in stream}
    assert produced <= customer_ids, sorted(produced - customer_ids)
    assert len(produced) > 100  # many customers, not a handful


def test_num_users_caps_to_the_first_customers(producer):
    pool = producer.SessionPool(num_users=5)
    assert set(pool.active_user_ids) <= set(producer.USER_IDS[:5])
    assert producer.SessionPool().active_user_ids == [
        u for u in producer.USER_IDS if producer.account_status(u) == "active"
    ]


def test_producer_never_imports_faker():
    tree = ast.parse(PRODUCER_SCRIPT.read_text())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert "faker" not in {name.lower() for name in imported}
    assert "faker" not in PRODUCER_SCRIPT.read_text().lower()


def run_producer_with_root(root):
    return subprocess.run(
        [sys.executable, str(PRODUCER_SCRIPT)],
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "PROJECT1_ROOT": str(root), "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1"},
    )


def test_missing_shop_csv_exits_2_with_a_clear_message(tmp_path):
    result = run_producer_with_root(tmp_path)

    assert result.returncode == 2
    expected = tmp_path / "reference-data" / "shop_master" / "customers.csv"
    assert result.stderr.strip() == (
        f"[producer] error: {expected} not found (set PROJECT1_ROOT to the repository root)"
    )
    assert result.stdout == ""


def test_shop_csv_without_a_required_column_exits_2_naming_it(tmp_path):
    shop = tmp_path / "reference-data" / "shop_master"
    shop.mkdir(parents=True)
    shutil.copy(SHOP_MASTER / "customers.csv", shop / "customers.csv")
    with open(SHOP_MASTER / "products.csv", newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))
    columns = [column for column in rows[0] if column != "product_status"]
    with open(shop / "products.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    result = run_producer_with_root(tmp_path)

    assert result.returncode == 2
    assert result.stderr.strip() == (
        f"[producer] error: {shop / 'products.csv'} lacks required column(s): product_status"
    )
    assert "Traceback" not in result.stderr
