import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]

VALID_EVENT = {
    "event_id": "valid",
    "session_id": "s-1",
    "user_id": 7,
    "user_name": "Test User",
    "email": "test@example.com",
    "gender": "other",
    "age": 30,
    "membership_tier": "plus",
    "loyalty_points": 100,
    "preferred_language": "english",
    "home_city": "Pune",
    "home_state": "Maharashtra",
    "event_time": "2026-10-25T15:24:46.105673+00:00",
    "event_type": "product_view",
    "user_journey_stage": "discovery",
    "user_segment": "new_user",
    "is_prime_user": True,
    "product_id": 10000,
    "product_name": "air_fryer",
    "category": "home",
    "quantity": 2,
    "original_price": 1000.0,
    "discount_percent": 10.0,
    "discounted_price": 900.0,
    "cart_value": 1800.0,
    "inventory_remaining": 50,
    "search_query": "iphone",
    "time_on_page_sec": 12.5,
    "scroll_depth_percent": 40,
    "hover_duration_ms": 300,
    "session_duration_sec": 600,
    "items_viewed_in_session": 3,
    "repeat_product_view_count": 1,
    "time_since_last_event_ms": 1000,
    "recommendation_rank": 4,
    "recommendation_clicked": False,
    "recommendation_algorithm": "hybrid_v2",
    "click_position": 2,
    "engagement_score": 5.5,
    "purchase_probability": 0.5,
    "cart_abandonment_probability": 0.2,
    "ab_test_group": "A",
    "payment_method": "upi",
    "device_type": "mobile",
    "operating_system": "android",
    "browser": "chrome",
    "network_type": "wifi",
    "app_version": "3.1",
    "traffic_source": "organic",
    "campaign_id": 1234,
    "api_latency_ms": 120,
    "page_load_time_ms": 800,
    "fraud_score": 0.1,
    "country": "India",
    "city": "Pune",
    "ip_address": "10.0.0.1",
    "schema_version": "4.0",
    "source": "real-time-personalization-generator",
}

INVALID_CHANGES = {
    "unknown_event_type": {"event_type": "teleport"},
    "bad_schema_version": {"schema_version": "3.0"},
    "discounted_gt_original": {"discounted_price": 1200.0},
    "probability_1_5": {"purchase_probability": 1.5},
    "quantity_0": {"quantity": 0},
    "null_fraud_score": {"fraud_score": None},
    "malformed_event_time": {"event_time": "not-a-timestamp"},
}


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_stream_validation")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def read_stream():
    # Importing must not start Spark or Kafka: the work lives in main().
    spec = importlib.util.spec_from_file_location(
        "read_stream_under_test", ROOT / "spark-streaming" / "read_stream.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def split(spark, read_stream):
    events = [VALID_EVENT]
    for name, change in INVALID_CHANGES.items():
        event = copy.deepcopy(VALID_EVENT)
        event.update(change)
        event["event_id"] = name
        events.append(event)

    raw_df = spark.createDataFrame(
        [(bytearray(json.dumps(event).encode("utf-8")),) for event in events],
        "value binary",
    )
    valid_df, invalid_df = read_stream.split_valid_invalid(
        read_stream.build_validated(read_stream.parse_events(raw_df))
    )
    valid_ids = sorted(row.event_id for row in valid_df.collect())
    invalid_ids = sorted(json.loads(row.value)["event_id"] for row in invalid_df.collect())
    return events, valid_df, invalid_df, valid_ids, invalid_ids


def test_import_has_no_side_effects():
    # Fresh interpreter, so this test's own SparkSession can't hide one.
    code = (
        "import importlib.util, sys\n"
        f"spec = importlib.util.spec_from_file_location('rs', {str(ROOT / 'spark-streaming' / 'read_stream.py')!r})\n"
        "module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)\n"
        "from pyspark import SparkContext\n"
        "assert SparkContext._active_spark_context is None, 'import started Spark'\n"
        "assert callable(module.main)\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr


def test_valid_row_passes(split):
    _, _, _, valid_ids, _ = split
    assert valid_ids == ["valid"]


@pytest.mark.parametrize("name", sorted(INVALID_CHANGES))
def test_invalid_rows_go_to_dlq(split, name):
    _, _, _, _, invalid_ids = split
    assert name in invalid_ids


def test_every_row_lands_in_exactly_one_branch(split):
    events, _, _, valid_ids, invalid_ids = split
    assert len(valid_ids) + len(invalid_ids) == len(events)
    assert not set(valid_ids) & set(invalid_ids)


def test_bronze_and_dlq_columns_unchanged(split, read_stream):
    _, valid_df, invalid_df, _, _ = split
    expected = [field.name for field in read_stream.event_schema.fields] + ["event_timestamp"]
    assert valid_df.columns == expected
    assert len(valid_df.columns) == 59
    assert invalid_df.columns == ["value"]
