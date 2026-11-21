"""Validation rules without a dedicated case in tests/test_stream_validation.py.

Each case changes one thing in the valid base event and must reach the dead-letter
branch only, with its original message text as the value, unchanged.
"""

import importlib.util
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from test_stream_validation import VALID_EVENT

ROOT = Path(__file__).resolve().parents[1]

BASE_ID = "gap-base"

# One change each; the case name is the event_id (session_id where event_id is the
# field under test).
JSON_CASES = {
    "null_event_id": {"event_id": None, "session_id": "null_event_id"},
    "null_user_id": {"user_id": None},
    "null_event_type": {"event_type": None},
    "null_event_time": {"event_time": None},
    "null_schema_version": {"schema_version": None},
    "cart_abandonment_probability_1_5": {"cart_abandonment_probability": 1.5},
    "null_purchase_probability": {"purchase_probability": None},
    "negative_discounted_price": {"discounted_price": -1.0},
    # VALID_EVENT is a product_view, a commerce event.
    "commerce_null_original_price": {"original_price": None},
    "commerce_null_discounted_price": {"discounted_price": None},
    "commerce_null_discount_percent": {"discount_percent": None},
    "user_id_not_int": {"user_id": "abc"},
}


def message(name, change=None):
    return json.dumps({**VALID_EVENT, "event_id": name, **(change or {})})


# The original message text of every case, by name.
MESSAGES = {name: message(name, change) for name, change in JSON_CASES.items()}
MESSAGES["truncated_json"] = message("truncated_json")[:-1]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_stream_validation_gaps")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def read_stream():
    spec = importlib.util.spec_from_file_location(
        "read_stream_for_gaps", ROOT / "spark-streaming" / "read_stream.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def split(spark, read_stream):
    values = [message(BASE_ID), *MESSAGES.values()]
    raw_df = spark.createDataFrame(
        [(bytearray(value.encode("utf-8")),) for value in values], "value binary"
    )
    valid_df, invalid_df = read_stream.split_valid_invalid(
        read_stream.build_validated(read_stream.parse_events(raw_df))
    )
    valid_ids = [row.event_id for row in valid_df.collect()]
    dlq_values = [row.value for row in invalid_df.collect()]
    return valid_ids, dlq_values


def test_the_unchanged_base_event_is_valid(split):
    valid_ids, dlq_values = split
    # Only the base passes, so no case below can pass because of a broken base.
    assert valid_ids == [BASE_ID]
    assert message(BASE_ID) not in dlq_values


@pytest.mark.parametrize("name", sorted(MESSAGES))
def test_case_goes_to_the_dlq_only_with_its_original_text(split, name):
    valid_ids, dlq_values = split
    assert name not in valid_ids
    assert dlq_values.count(MESSAGES[name]) == 1
