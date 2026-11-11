import copy
import importlib.util
import json
import os
import subprocess
import sys
from datetime import timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
# create_reference_data.py imports producer.py, which needs these.
pytest.importorskip("faker")
pytest.importorskip("confluent_kafka")

from pyspark.sql import SparkSession  # noqa: E402

from test_stream_validation import PRODUCT_AND_PRICE_FIELDS, VALID_EVENT  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]

SCRIPTS = [
    "reference-data/create_reference_data.py",
    "spark-batch/bronze_to_silver.py",
    "spark-batch/silver_to_gold.py",
]

# Non-commerce events carry no product or price.
NO_PRODUCT = {field: None for field in PRODUCT_AND_PRICE_FIELDS}


def event(event_id, user_id, event_type, event_time, session_id, **overrides):
    row = copy.deepcopy(VALID_EVENT)
    row.update(
        event_id=event_id,
        user_id=user_id,
        event_type=event_type,
        event_time=event_time,
        session_id=session_id,
        product_id=10000,
        product_name="air_fryer",
        category="home",
        quantity=1,
        discounted_price=900.0,
        cart_value=None,
        user_segment="segment_from_event",
    )
    row.update(overrides)
    return row


# User 1's name and tier change between events; Gold must still give one row
# with the latest values. User 2's two prices average exactly 1126.035:
# DECIMAL rounds that to 1126.04, while AVG on DOUBLE gives 1126.03.
# User 3 has no commerce event.
BRONZE_EVENTS = [
    event("u1-view-1", 1, "home_page_view", "2026-10-25T10:00:00+00:00", "s-1a",
          user_name="Old Name", membership_tier="free", **NO_PRODUCT),
    event("u1-view-2", 1, "home_page_view", "2026-10-25T10:05:00+00:00", "s-1b",
          user_name="Old Name", membership_tier="free", **NO_PRODUCT),
    event("u1-view-2", 1, "home_page_view", "2026-10-25T10:05:00+00:00", "s-1b",
          user_name="Old Name", membership_tier="free", **NO_PRODUCT),
    event("u1-buy-1", 1, "payment_success", "2026-10-25T10:10:00+00:00", "s-1b",
          user_name="New Name", membership_tier="premium", quantity=2, cart_value=1800.0),
    event("u2-view-1", 2, "product_view", "2026-10-25T11:00:00+00:00", "s-2",
          original_price=1200.0, discounted_price=1085.49),
    event("u2-view-ghost", 2, "product_view", "2026-10-25T11:01:00+00:00", "s-2",
          product_id=99999, product_name="ghost_product", category="home",
          original_price=1200.0, discounted_price=1166.58),
    event("u3-login", 3, "login", "2026-10-25T12:00:00+00:00", "s-3", **NO_PRODUCT),
]

EXISTING_GOLD_SCHEMA = [
    ("user_id", "int"),
    ("user_name", "string"),
    ("membership_tier", "string"),
    ("total_events", "bigint"),
    ("page_view_count", "bigint"),
    ("product_view_count", "bigint"),
    ("add_to_cart_count", "bigint"),
    ("purchase_count", "bigint"),
    ("search_count", "bigint"),
    ("avg_event_price", "double"),
    ("max_event_price", "double"),
    ("avg_engagement_score", "double"),
    ("avg_purchase_probability", "double"),
    ("last_event_timestamp", "timestamp"),
    ("unique_products_interacted", "bigint"),
]

NEW_GOLD_SCHEMA = [
    ("total_spend", "double"),
    ("session_count", "bigint"),
    ("features_computed_at", "timestamp"),
    ("recency_hours", "double"),
]


@pytest.fixture(scope="module")
def pipeline(tmp_path_factory):
    root = tmp_path_factory.mktemp("pipeline")

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_batch_pipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Bronze written through the stream's own parse/validate code.
    spec = importlib.util.spec_from_file_location(
        "read_stream_for_batch", ROOT / "spark-streaming" / "read_stream.py"
    )
    read_stream = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(read_stream)
    raw_df = spark.createDataFrame(
        [(bytearray(json.dumps(e).encode("utf-8")),) for e in BRONZE_EVENTS],
        "value binary",
    )
    valid_df, invalid_df = read_stream.split_valid_invalid(
        read_stream.build_validated(read_stream.parse_events(raw_df))
    )
    assert invalid_df.count() == 0
    valid_df.write.mode("overwrite").parquet(str(root / "data" / "bronze" / "user_events"))

    for script in SCRIPTS:
        result = subprocess.run(
            [sys.executable, str(ROOT / script)],
            cwd=root,
            env={**_clean_env(), "PROJECT1_ROOT": str(root)},
            capture_output=True,
            text=True,
            timeout=600,
        )
        assert result.returncode == 0, f"{script} failed:\n{result.stderr[-3000:]}"

    silver = spark.read.parquet(str(root / "data" / "silver" / "user_events"))
    gold = spark.read.parquet(str(root / "data" / "gold" / "user_features"))
    users_dim = spark.read.parquet(str(root / "data" / "dim" / "users"))
    products_dim = spark.read.parquet(str(root / "data" / "dim" / "products"))
    yield {
        "silver": [row.asDict() for row in silver.collect()],
        "silver_columns": silver.columns,
        "gold": {row.user_id: row.asDict() for row in gold.collect()},
        "gold_rows": gold.count(),
        "gold_schema": [(field.name, field.dataType.simpleString()) for field in gold.schema],
        "account_status": {row.user_id: row.account_status for row in users_dim.collect()},
        "is_active": {row.product_id: row.is_active for row in products_dim.collect()},
    }
    spark.stop()


def _clean_env():
    env = dict(os.environ)
    # Use the default NUM_USERS so users 1 and 2 are in the users dim.
    env.pop("NUM_USERS", None)
    return env


def test_silver_drops_duplicate_event_id(pipeline):
    event_ids = [row["event_id"] for row in pipeline["silver"]]
    assert len(event_ids) == 6
    assert sorted(event_ids) == sorted(set(event_ids))


def test_silver_keeps_event_user_segment(pipeline):
    assert {row["user_segment"] for row in pipeline["silver"]} == {"segment_from_event"}


def test_silver_has_no_price_column(pipeline):
    assert "price" not in pipeline["silver_columns"]
    assert "reference_price" in pipeline["silver_columns"]


def test_silver_product_reference_missing_only_for_unknown_products(pipeline):
    assert not [row for row in pipeline["silver"] if row["is_user_reference_missing"]]
    missing = sorted(row["event_id"] for row in pipeline["silver"] if row["is_product_reference_missing"])
    # Events without a product are not missing a reference.
    assert missing == ["u2-view-ghost"]


def test_gold_one_row_per_user(pipeline):
    assert pipeline["gold_rows"] == 3
    assert sorted(pipeline["gold"]) == [1, 2, 3]


def test_gold_schema_keeps_existing_columns_and_appends_new(pipeline):
    assert pipeline["gold_schema"] == EXISTING_GOLD_SCHEMA + NEW_GOLD_SCHEMA


def test_gold_price_features_use_commerce_events_only(pipeline):
    user_1, user_2, user_3 = (pipeline["gold"][user_id] for user_id in (1, 2, 3))

    # User 1: only payment_success has a price; the page views have none.
    assert user_1["avg_event_price"] == 900.0
    assert user_1["max_event_price"] == 900.0
    # 1085.49 and 1166.58 average 1126.035, rounded half up on DECIMAL.
    assert user_2["avg_event_price"] == 1126.04
    assert user_2["max_event_price"] == 1166.58
    assert user_3["avg_event_price"] is None
    assert user_3["max_event_price"] is None
    assert user_3["unique_products_interacted"] == 0


def test_gold_new_columns(pipeline):
    gold = pipeline["gold"]

    assert [gold[u]["total_spend"] for u in (1, 2, 3)] == [1800.0, 0.0, 0.0]
    assert [gold[u]["session_count"] for u in (1, 2, 3)] == [2, 1, 1]

    computed_at = {row["features_computed_at"] for row in gold.values()}
    assert len(computed_at) == 1

    for row in gold.values():
        micros = (row["features_computed_at"] - row["last_event_timestamp"]) // timedelta(microseconds=1)
        expected = (Decimal(micros) / Decimal(3_600_000_000)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        assert row["recency_hours"] == float(expected)
        assert row["recency_hours"] > 0


def test_gold_counts_for_user_1(pipeline):
    user_1 = pipeline["gold"][1]
    assert user_1["page_view_count"] == 2
    assert user_1["purchase_count"] == 1
    assert user_1["total_events"] == 3
    assert user_1["user_name"] == "New Name"
    assert user_1["membership_tier"] == "premium"


def load_producer():
    sys.path.insert(0, str(ROOT / "data-generator"))
    try:
        spec = importlib.util.spec_from_file_location(
            "producer_for_dims", ROOT / "data-generator" / "producer.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(ROOT / "data-generator"))
    return module


def test_dim_status_matches_producer_rules(pipeline):
    producer = load_producer()
    account_status = pipeline["account_status"]
    is_active = pipeline["is_active"]

    assert len(account_status) == producer.NUM_USERS
    assert all(status == producer.account_status(user_id) for user_id, status in account_status.items())

    assert sorted(is_active) == sorted(producer.PRODUCT_IDS.values())
    assert all(active == producer.is_product_active(pid) for pid, active in is_active.items())
