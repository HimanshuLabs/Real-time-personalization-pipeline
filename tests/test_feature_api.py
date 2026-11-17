import asyncio
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest
from prometheus_client import REGISTRY

ROOT = Path(__file__).resolve().parents[1]

DB_ENV = {
    "POSTGRES_HOST": "db.test.internal",
    "POSTGRES_PORT": "6543",
    "POSTGRES_DB": "features_test",
    "POSTGRES_USER": "api_test_user",
    "POSTGRES_PASSWORD": "s3cret-test-password",
}

SECRET_TEXT = "connection failed: password authentication failed for user api_test_user"

# What GET /features/{user_id} serves, in order: every user_features column
# except user_name, membership_tier and features_computed_at.
FEATURE_KEYS = [
    "user_id",
    "total_events",
    "page_view_count",
    "product_view_count",
    "add_to_cart_count",
    "purchase_count",
    "search_count",
    "avg_event_price",
    "max_event_price",
    "last_event_timestamp",
    "unique_products_interacted",
    "avg_engagement_score",
    "avg_purchase_probability",
    "total_spend",
    "session_count",
    "recency_hours",
]

FEATURE_ROW = {
    "user_id": 42,
    "total_events": 7,
    "page_view_count": 2,
    "product_view_count": 3,
    "add_to_cart_count": 1,
    "purchase_count": 1,
    "search_count": 0,
    "avg_event_price": 1500.5,
    "max_event_price": 2999.0,
    "last_event_timestamp": datetime(2026, 10, 25, 15, 24, 46, tzinfo=timezone.utc),
    "unique_products_interacted": 4,
    "avg_engagement_score": 15.99,
    "avg_purchase_probability": 0.75,
    "total_spend": 2999.0,
    "session_count": 3,
    "recency_hours": 12.5,
}

COMPUTED_AT = datetime(2026, 11, 11, 13, 0, tzinfo=timezone.utc)


def table_row(user_id):
    # A full user_features row, including the columns the API must never serve.
    return {
        **FEATURE_ROW,
        "user_id": user_id,
        "user_name": f"Name {user_id}",
        "membership_tier": "premium",
        "last_event_timestamp": datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc),
        "features_computed_at": COMPUTED_AT,
    }


def selected_columns(query):
    select_list = query.split("SELECT", 1)[1].split("FROM", 1)[0]
    return [column.strip() for column in select_list.split(",")]


class FakeCursor:
    """Returns scripted rows from fetchone, whatever the query (health checks)."""

    def __init__(self, results):
        self.results = list(results)
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        self.queries.append((query, params))

    def fetchone(self):
        return self.results.pop(0)


class TableCursor(FakeCursor):
    """An in-memory user_features table. Answers the API's two queries
    (WHERE user_id = %s, and WHERE user_id > %s ... LIMIT %s) with only the
    columns the SQL selects, so what the API serves is what its SQL asks for."""

    def __init__(self, user_ids):
        super().__init__([])
        self.table = sorted(
            (table_row(user_id) for user_id in user_ids), key=lambda r: r["user_id"]
        )

    def execute(self, query, params=None):
        super().execute(query, params)
        columns = selected_columns(query)
        if "LIMIT" in query:
            after_user_id, limit = params
            rows = [row for row in self.table if row["user_id"] > after_user_id][:limit]
        else:
            (user_id,) = params
            rows = [row for row in self.table if row["user_id"] == user_id]
        self.rows = [{column: row[column] for column in columns} for row in rows]

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self._cursor


@pytest.fixture
def api(monkeypatch):
    for key, value in DB_ENV.items():
        monkeypatch.setenv(key, value)

    # The instrumentator skips any metric whose name is already registered, so
    # without this only the first app imported in the process would record
    # requests. Dropping its http_* collectors gives each test fresh metrics.
    for collector, names in list(REGISTRY._collector_to_names.items()):
        if any(name.startswith("http_") for name in names):
            REGISTRY.unregister(collector)

    # Fresh module per test so DB_CONFIG is read from the patched env.
    spec = importlib.util.spec_from_file_location(
        "feature_api_under_test", ROOT / "api" / "main.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.connect_calls = []
    return module


def use_cursor(monkeypatch, api, cursor):
    def fake_connect(**kwargs):
        api.connect_calls.append(kwargs)
        return FakeConnection(cursor)

    monkeypatch.setattr(api.psycopg, "connect", fake_connect)
    return cursor


def use_db(monkeypatch, api, results):
    return use_cursor(monkeypatch, api, FakeCursor(results))


def use_table(monkeypatch, api, user_ids):
    return use_cursor(monkeypatch, api, TableCursor(user_ids))


def use_db_down(monkeypatch, api):
    def fake_connect(**kwargs):
        api.connect_calls.append(kwargs)
        raise psycopg.OperationalError(SECRET_TEXT)

    monkeypatch.setattr(api.psycopg, "connect", fake_connect)


def assert_no_leak(body):
    text = json.dumps(body) if not isinstance(body, str) else body
    assert "password" not in text.lower()
    assert DB_ENV["POSTGRES_USER"] not in text
    assert DB_ENV["POSTGRES_PASSWORD"] not in text
    assert SECRET_TEXT not in text


def http_request(app, path, query=""):
    # One GET through the ASGI app, so FastAPI's routing, validation and JSON
    # encoding all run (TestClient would need httpx, which isn't a dependency).
    messages = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": query.encode(),
        "headers": [],
        "client": ("127.0.0.1", 50000),
        "server": ("testserver", 80),
    }
    asyncio.run(app(scope, receive, send))

    start = next(m for m in messages if m["type"] == "http.response.start")
    headers = {k.decode().lower(): v.decode() for k, v in start["headers"]}
    body = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
    return start["status"], headers, body


def http_get(app, path, query=""):
    status, _, body = http_request(app, path, query)
    return status, json.loads(body)


# --- GET / and GET /metrics ------------------------------------------------------------


def test_root_describes_the_service(api):
    assert http_get(api.app, "/") == (
        200,
        {
            "message": "Feature serving API is running",
            "available_endpoint": "/features/{user_id}",
        },
    )


def test_metrics_serves_prometheus_text(api):
    http_get(api.app, "/")

    status, headers, body = http_request(api.app, "/metrics")

    assert status == 200
    assert headers["content-type"].startswith("text/plain")
    text = body.decode()
    assert "# TYPE http_requests_total counter" in text
    assert "# TYPE http_request_duration_seconds histogram" in text


def test_metrics_records_feature_requests_but_never_its_own_scrapes(monkeypatch, api):
    use_table(monkeypatch, api, [41, 42, 43])
    http_request(api.app, "/metrics")
    assert http_get(api.app, "/features/42")[0] == 200
    http_request(api.app, "/metrics")

    text = http_request(api.app, "/metrics")[2].decode()
    series = [line for line in text.splitlines() if line.startswith("http_")]

    feature_series = [line for line in series if 'handler="/features/{user_id}"' in line]
    assert any(line.startswith("http_requests_total{") for line in feature_series)
    assert any(line.startswith("http_request_duration_seconds_bucket{") for line in feature_series)
    assert any(line.startswith("http_request_duration_highr_seconds_count ") for line in series)
    assert [line for line in series if 'handler="/metrics"' in line] == []


# --- GET /health -----------------------------------------------------------------------


def test_health_200_when_table_has_rows(monkeypatch, api):
    use_db(monkeypatch, api, [{"table_exists": True}, {"has_rows": True}])

    assert http_get(api.app, "/health") == (200, {"status": "healthy", "postgres": "connected"})


@pytest.mark.parametrize(
    ("results", "reason"),
    [
        ([{"table_exists": False}], "user_features missing"),
        ([{"table_exists": True}, {"has_rows": False}], "user_features empty"),
    ],
    ids=["table_missing", "table_empty"],
)
def test_health_503_reasons(monkeypatch, api, results, reason):
    use_db(monkeypatch, api, results)

    assert http_get(api.app, "/health") == (503, {"detail": reason})


def test_health_503_when_database_down_without_leaks(monkeypatch, api):
    use_db_down(monkeypatch, api)

    status, body = http_get(api.app, "/health")

    assert (status, body) == (503, {"detail": "database unavailable"})
    assert_no_leak(body)


# --- GET /features/{user_id} -----------------------------------------------------------


def test_get_features_200_with_exact_keys(monkeypatch, api):
    cursor = use_table(monkeypatch, api, [41, 42, 43])

    status, body = http_get(api.app, "/features/42")

    assert status == 200
    assert list(body) == ["user_id", "features"]
    assert body["user_id"] == 42
    assert list(body["features"]) == FEATURE_KEYS
    assert "user_name" not in body["features"]
    assert "membership_tier" not in body["features"]
    assert body["features"]["last_event_timestamp"] == "2026-11-11T12:00:00+00:00"
    assert body["features"]["total_spend"] == 2999.0
    # The id goes to the query as a parameter.
    assert cursor.queries[0][1] == (42,)


def test_get_features_404_for_unknown_user(monkeypatch, api):
    use_table(monkeypatch, api, [41, 42, 43])

    assert http_get(api.app, "/features/999999") == (
        404,
        {"detail": "No features found for user_id=999999"},
    )


@pytest.mark.parametrize("user_id", ["abc", "1.5", "42x"])
def test_get_features_422_for_non_integer_id(monkeypatch, api, user_id):
    use_table(monkeypatch, api, [42])

    status, _ = http_get(api.app, f"/features/{user_id}")

    assert status == 422
    assert api.connect_calls == []


def test_get_features_500_when_database_down_without_leaks(monkeypatch, api):
    use_db_down(monkeypatch, api)

    status, body = http_get(api.app, "/features/42")

    assert (status, body) == (500, {"detail": "Feature lookup failed"})
    assert_no_leak(body)


# --- connection settings (not visible over HTTP) ---------------------------------------


def test_postgres_env_vars_are_honored(monkeypatch, api):
    assert api.DB_CONFIG == {
        "host": "db.test.internal",
        "port": 6543,
        "dbname": "features_test",
        "user": "api_test_user",
        "password": "s3cret-test-password",
    }

    use_table(monkeypatch, api, [42])
    http_get(api.app, "/features/42")
    assert api.connect_calls[0]["host"] == "db.test.internal"
    assert api.connect_calls[0]["port"] == 6543


def test_every_connection_uses_the_connect_timeout(monkeypatch, api):
    assert api.DB_CONNECT_TIMEOUT_SECONDS == 3

    use_table(monkeypatch, api, [42])
    http_get(api.app, "/features/42")
    http_get(api.app, "/features")
    use_db(monkeypatch, api, [{"table_exists": True}, {"has_rows": True}])
    http_get(api.app, "/health")

    assert len(api.connect_calls) == 3
    assert all(call["connect_timeout"] == 3 for call in api.connect_calls)


# --- GET /features: keyset pagination --------------------------------------------------


def page_ids(body):
    return [item["user_id"] for item in body["items"]]


def test_page_first_middle_and_last(monkeypatch, api):
    use_table(monkeypatch, api, [5, 1, 4, 2, 3])

    status, first = http_get(api.app, "/features", "limit=2")
    assert status == 200
    assert (page_ids(first), first["count"], first["next_after_user_id"]) == ([1, 2], 2, 2)

    _, middle = http_get(api.app, "/features", "after_user_id=2&limit=2")
    assert (page_ids(middle), middle["count"], middle["next_after_user_id"]) == ([3, 4], 2, 4)

    _, last = http_get(api.app, "/features", "after_user_id=4&limit=2")
    assert (page_ids(last), last["count"], last["next_after_user_id"]) == ([5], 1, None)


def test_page_exactly_full_last_page_has_null_next(monkeypatch, api):
    # The extra fetched row is absent, so the walk ends without an empty page.
    use_table(monkeypatch, api, [1, 2, 3, 4])

    _, page = http_get(api.app, "/features", "after_user_id=2&limit=2")
    assert (page_ids(page), page["count"], page["next_after_user_id"]) == ([3, 4], 2, None)


def test_page_returns_only_limit_rows_when_more_exist(monkeypatch, api):
    use_table(monkeypatch, api, [1, 2, 3])

    _, page = http_get(api.app, "/features", "limit=1")
    assert (page_ids(page), page["count"], page["next_after_user_id"]) == ([1], 1, 1)


def walk(api, limit):
    seen, after, requests = [], 0, 0
    while after is not None:
        status, body = http_get(api.app, "/features", f"after_user_id={after}&limit={limit}")
        assert status == 200
        requests += 1
        seen += page_ids(body)
        after = body["next_after_user_id"]
    return seen, requests


@pytest.mark.parametrize(("users", "expected_requests"), [(250, 3), (200, 2)])
def test_page_walk_returns_every_user_once_in_order(monkeypatch, api, users, expected_requests):
    user_ids = list(range(1, users + 1))
    use_table(monkeypatch, api, reversed(user_ids))

    seen, requests = walk(api, 100)

    assert seen == user_ids
    # 200 users fill two pages exactly: no third, empty request.
    assert requests == expected_requests


def test_page_empty_table(monkeypatch, api):
    use_table(monkeypatch, api, [])

    status, body = http_get(api.app, "/features")
    assert status == 200
    assert body == {"items": [], "count": 0, "next_after_user_id": None}


def test_page_defaults(monkeypatch, api):
    cursor = use_table(monkeypatch, api, [1])

    http_get(api.app, "/features")

    # limit + 1: the extra row only tells whether another page exists.
    assert cursor.queries[0][1] == (0, 101)


@pytest.mark.parametrize("limit", [1, 1000])
def test_page_limit_bounds_accepted(monkeypatch, api, limit):
    cursor = use_table(monkeypatch, api, [1, 2])

    status, _ = http_get(api.app, "/features", f"limit={limit}")

    assert status == 200
    assert cursor.queries[0][1] == (0, limit + 1)


@pytest.mark.parametrize(
    "query",
    ["limit=0", "limit=1001", "limit=-5", "limit=abc", "after_user_id=-1", "after_user_id=1.5"],
)
def test_page_invalid_params_return_422(monkeypatch, api, query):
    use_table(monkeypatch, api, [1])

    status, _ = http_get(api.app, "/features", query)

    assert status == 422
    assert api.connect_calls == []


def test_page_items_have_the_feature_keys_plus_computed_at(monkeypatch, api):
    use_table(monkeypatch, api, [1, 2])
    _, body = http_get(api.app, "/features")

    assert body["items"]
    for item in body["items"]:
        assert list(item) == FEATURE_KEYS + ["features_computed_at"]
        assert "user_name" not in item
        assert "membership_tier" not in item
    assert body["items"][0]["features_computed_at"] == "2026-11-11T13:00:00+00:00"


def test_page_db_down_returns_500_without_leaks(monkeypatch, api):
    use_db_down(monkeypatch, api)

    status, body = http_get(api.app, "/features")

    assert status == 500
    assert body == {"detail": "Feature lookup failed"}
    assert_no_leak(body)


def test_page_sql_is_parameterised(monkeypatch, api):
    cursor = use_table(monkeypatch, api, [1])

    http_get(api.app, "/features", "after_user_id=7777&limit=333")

    query, params = cursor.queries[0]
    assert params == (7777, 334)
    normalised = " ".join(query.split())
    assert "WHERE user_id > %s ORDER BY user_id LIMIT %s" in normalised
    assert "7777" not in query and "333" not in query and "334" not in query
    assert query == api.FEATURE_PAGE_QUERY
