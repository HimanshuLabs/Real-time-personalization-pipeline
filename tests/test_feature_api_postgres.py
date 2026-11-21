"""The feature API against a real PostgreSQL, at the integer boundaries of user_id.

user_features.user_id is a PostgreSQL integer. psycopg sends a Python int typed by
its value (int2, int4, int8 or numeric) and PostgreSQL compares any of them with an
integer column without error, so an id no user can have simply matches no row (404)
and a cursor past the last id gives an empty last page. The fake database of
tests/test_feature_api.py cannot show this: these tests run the API's real queries.

P1_TEST_DATABASE_URL points at a maintenance database (e.g. .../postgres) of a server
where the role may create databases. The tests create TEST_DATABASE, work only there
and drop it at the end. Skipped when the variable is unset, except in CI.
"""

import importlib.util
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest
from prometheus_client import REGISTRY
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from test_feature_api import FEATURE_KEYS, http_get

ROOT = Path(__file__).resolve().parents[1]

TEST_DATABASE = "p1_api_test"
DATABASE_URL = os.environ.get("P1_TEST_DATABASE_URL")

INT4_MIN = -(2**31)
INT4_MAX = 2**31 - 1
INT8_MIN = -(2**63)
INT8_MAX = 2**63 - 1

# The columns and types of user_features (README, Feature table).
CREATE_TABLE = """
    CREATE TABLE user_features (
        user_id integer,
        user_name text,
        membership_tier text,
        total_events bigint,
        page_view_count bigint,
        product_view_count bigint,
        add_to_cart_count bigint,
        purchase_count bigint,
        search_count bigint,
        avg_event_price double precision,
        max_event_price double precision,
        avg_engagement_score double precision,
        avg_purchase_probability double precision,
        last_event_timestamp timestamp with time zone,
        unique_products_interacted bigint,
        total_spend double precision,
        session_count bigint,
        features_computed_at timestamp with time zone,
        recency_hours double precision,
        CONSTRAINT user_features_pkey PRIMARY KEY (user_id)
    )
"""

INSERT_ROW = """
    INSERT INTO user_features VALUES (
        %s, 'Name', 'premium', 7, 2, 3, 1, 1, 0, 1500.5, 2999.0, 15.99, 0.75,
        %s, 4, 2999.0, 2, %s, 1.0
    )
"""

# The two users: the smallest real id, and the largest id an integer column holds.
USER_IDS = (1, INT4_MAX)

EMPTY_PAGE = {"items": [], "count": 0, "next_after_user_id": None}

LEAKS = ("Traceback", "psycopg", "out of range", "Exception")


@pytest.fixture(scope="module")
def test_db():
    if not DATABASE_URL:
        if os.environ.get("CI"):
            pytest.fail("P1_TEST_DATABASE_URL must be set in CI")
        pytest.skip("P1_TEST_DATABASE_URL is not set")

    if conninfo_to_dict(DATABASE_URL).get("dbname") in (TEST_DATABASE, "personalization_db"):
        pytest.fail("P1_TEST_DATABASE_URL must name a maintenance database such as postgres")

    drop = sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TEST_DATABASE))
    with psycopg.connect(DATABASE_URL, autocommit=True) as admin:
        admin.execute(drop)
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEST_DATABASE)))

    conninfo = make_conninfo(DATABASE_URL, dbname=TEST_DATABASE)
    event_time = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)
    computed_at = datetime(2026, 11, 11, 13, 0, tzinfo=timezone.utc)
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(CREATE_TABLE)
        for user_id in USER_IDS:
            conn.execute(INSERT_ROW, (user_id, event_time, computed_at))

    yield conninfo

    with psycopg.connect(DATABASE_URL, autocommit=True) as admin:
        admin.execute(drop)


@pytest.fixture
def live_api(monkeypatch, test_db):
    # The API reads POSTGRES_* at import: point them at the test database.
    params = conninfo_to_dict(test_db)
    monkeypatch.setenv("POSTGRES_HOST", params.get("host") or "localhost")
    monkeypatch.setenv("POSTGRES_PORT", str(params.get("port") or 5432))
    monkeypatch.setenv("POSTGRES_DB", TEST_DATABASE)
    monkeypatch.setenv("POSTGRES_USER", params.get("user") or "")
    monkeypatch.setenv("POSTGRES_PASSWORD", params.get("password") or "")

    # As in tests/test_feature_api.py: drop the instrumentator's http_* collectors
    # so each fresh app can register its own metrics.
    for collector, names in list(REGISTRY._collector_to_names.items()):
        if any(name.startswith("http_") for name in names):
            REGISTRY.unregister(collector)

    spec = importlib.util.spec_from_file_location(
        "feature_api_live_under_test", ROOT / "api" / "main.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.password = params.get("password") or ""
    return module


def get(api, path, query=""):
    """One request through the app; the body never carries exception text or the password."""
    status, body = http_get(api.app, path, query)
    text = json.dumps(body)
    for leak in LEAKS:
        assert leak not in text
    if api.password:
        assert api.password not in text
    return status, body


# --- GET /features/{user_id} -----------------------------------------------------------


def test_get_int4_max_user_is_served(live_api):
    status, body = get(live_api, f"/features/{INT4_MAX}")

    assert status == 200
    assert body["user_id"] == INT4_MAX
    assert list(body["features"]) == FEATURE_KEYS
    assert body["features"]["user_id"] == INT4_MAX


@pytest.mark.parametrize(
    "user_id",
    [
        INT4_MAX + 1,
        INT8_MAX,
        INT8_MAX + 1,
        10**20,
        -1,
        INT4_MIN,
        INT4_MIN - 1,
        INT8_MIN,
        INT8_MIN - 1,
    ],
)
def test_get_ids_no_user_has_are_404(live_api, user_id):
    assert get(live_api, f"/features/{user_id}") == (
        404,
        {"detail": f"No features found for user_id={user_id}"},
    )


# --- GET /features ---------------------------------------------------------------------


def test_page_before_int4_max_holds_the_last_user(live_api):
    status, body = get(live_api, "/features", f"after_user_id={INT4_MAX - 1}&limit=1")

    assert status == 200
    assert body["count"] == 1
    assert body["next_after_user_id"] is None
    [item] = body["items"]
    assert item["user_id"] == INT4_MAX
    assert list(item) == FEATURE_KEYS + ["features_computed_at"]


@pytest.mark.parametrize("after_user_id", [INT4_MAX, INT4_MAX + 1, INT8_MAX, INT8_MAX + 1, 10**20])
def test_page_after_any_large_cursor_is_empty(live_api, after_user_id):
    assert get(live_api, "/features", f"after_user_id={after_user_id}&limit=1") == (
        200,
        EMPTY_PAGE,
    )


@pytest.mark.parametrize("after_user_id", [-1, INT4_MIN - 1])
def test_negative_cursor_is_still_422(live_api, after_user_id):
    status, body = get(live_api, "/features", f"after_user_id={after_user_id}")

    assert status == 422
    assert [error["loc"] for error in body["detail"]] == [["query", "after_user_id"]]


def test_user_id_too_long_to_parse_is_422(live_api):
    status, body = get(live_api, "/features/" + "9" * 5000)

    assert status == 422
    assert [error["loc"] for error in body["detail"]] == [["path", "user_id"]]


def test_cursor_too_long_to_parse_is_422(live_api):
    status, body = get(live_api, "/features", "after_user_id=" + "9" * 5000)

    assert status == 422
    assert [error["loc"] for error in body["detail"]] == [["query", "after_user_id"]]
