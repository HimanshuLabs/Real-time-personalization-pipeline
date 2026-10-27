import copy
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
# create_reference_data.py imports producer.py, which needs these.
pytest.importorskip("faker")
pytest.importorskip("confluent_kafka")

from pyspark.sql import SparkSession  # noqa: E402

from test_stream_validation import VALID_EVENT  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]

SCRIPTS = [
    "reference-data/create_reference_data.py",
    "spark-batch/bronze_to_silver.py",
    "spark-batch/silver_to_gold.py",
]


def event(event_id, user_id, event_type, event_time, **overrides):
    row = copy.deepcopy(VALID_EVENT)
    row.update(
        event_id=event_id,
        user_id=user_id,
        event_type=event_type,
        event_time=event_time,
        product_id=10000,
        product_name="air_fryer",
        category="home",
        user_segment="segment_from_event",
    )
    row.update(overrides)
    return row


# User 1's name and tier change between events;
# Gold must still give one row with the latest values.
BRONZE_EVENTS = [
    event("u1-view-1", 1, "home_page_view", "2026-10-25T10:00:00+00:00",
          user_name="Old Name", membership_tier="free"),
    event("u1-view-2", 1, "home_page_view", "2026-10-25T10:05:00+00:00",
          user_name="Old Name", membership_tier="free"),
    event("u1-view-2", 1, "home_page_view", "2026-10-25T10:05:00+00:00",
          user_name="Old Name", membership_tier="free"),
    event("u1-buy-1", 1, "payment_success", "2026-10-25T10:10:00+00:00",
          user_name="New Name", membership_tier="premium"),
    event("u2-view-1", 2, "product_view", "2026-10-25T11:00:00+00:00"),
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
    yield {
        "silver": [row.asDict() for row in silver.collect()],
        "gold": {row.user_id: row.asDict() for row in gold.collect()},
        "gold_rows": gold.count(),
    }
    spark.stop()


def _clean_env():
    env = dict(os.environ)
    # Use the default NUM_USERS so users 1 and 2 are in the users dim.
    env.pop("NUM_USERS", None)
    return env


def test_silver_drops_duplicate_event_id(pipeline):
    event_ids = [row["event_id"] for row in pipeline["silver"]]
    assert len(event_ids) == 4
    assert sorted(event_ids) == sorted(set(event_ids))


def test_silver_keeps_event_user_segment(pipeline):
    assert {row["user_segment"] for row in pipeline["silver"]} == {"segment_from_event"}


def test_silver_has_no_missing_references(pipeline):
    assert not [row for row in pipeline["silver"] if row["is_user_reference_missing"]]
    assert not [row for row in pipeline["silver"] if row["is_product_reference_missing"]]


def test_gold_one_row_per_user(pipeline):
    assert pipeline["gold_rows"] == 2
    assert sorted(pipeline["gold"]) == [1, 2]


def test_gold_counts_for_user_1(pipeline):
    user_1 = pipeline["gold"][1]
    assert user_1["page_view_count"] == 2
    assert user_1["purchase_count"] == 1
    assert user_1["total_events"] == 3
    assert user_1["user_name"] == "New Name"
    assert user_1["membership_tier"] == "premium"
