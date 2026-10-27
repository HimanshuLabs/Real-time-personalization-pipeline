import importlib.util
import inspect
from pathlib import Path

import pytest

# CI installs only ruff and pytest; skip instead of failing when the API's
# runtime packages are missing.
pytest.importorskip("fastapi")
psycopg = pytest.importorskip("psycopg")
pytest.importorskip("prometheus_fastapi_instrumentator")

from fastapi import HTTPException  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]

DB_ENV = {
    "POSTGRES_HOST": "db.test.internal",
    "POSTGRES_PORT": "6543",
    "POSTGRES_DB": "features_test",
    "POSTGRES_USER": "api_test_user",
    "POSTGRES_PASSWORD": "s3cret-test-password",
}

SECRET_TEXT = "connection failed: password authentication failed for user api_test_user"

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
    "last_event_timestamp": "2026-10-25T15:24:46+00:00",
    "unique_products_interacted": 4,
    "avg_engagement_score": 15.99,
    "avg_purchase_probability": 0.75,
}


class FakeCursor:
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

    # Fresh module per test so DB_CONFIG is read from the patched env.
    spec = importlib.util.spec_from_file_location("feature_api_under_test", ROOT / "api" / "main.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.connect_calls = []
    return module


def use_db(monkeypatch, api, results):
    cursor = FakeCursor(results)

    def fake_connect(**kwargs):
        api.connect_calls.append(kwargs)
        return FakeConnection(cursor)

    monkeypatch.setattr(api.psycopg, "connect", fake_connect)
    return cursor


def use_db_down(monkeypatch, api):
    def fake_connect(**kwargs):
        api.connect_calls.append(kwargs)
        raise psycopg.OperationalError(SECRET_TEXT)

    monkeypatch.setattr(api.psycopg, "connect", fake_connect)


def assert_no_leak(detail):
    text = str(detail)
    assert "password" not in text.lower()
    assert DB_ENV["POSTGRES_USER"] not in text
    assert DB_ENV["POSTGRES_PASSWORD"] not in text
    assert SECRET_TEXT not in text


def test_postgres_env_vars_are_honored(monkeypatch, api):
    assert api.DB_CONFIG == {
        "host": "db.test.internal",
        "port": 6543,
        "dbname": "features_test",
        "user": "api_test_user",
        "password": "s3cret-test-password",
    }

    use_db(monkeypatch, api, [FEATURE_ROW])
    api.get_user_features(42)
    assert api.connect_calls[0]["host"] == "db.test.internal"
    assert api.connect_calls[0]["port"] == 6543


def test_features_found_includes_new_fields(monkeypatch, api):
    cursor = use_db(monkeypatch, api, [FEATURE_ROW])

    response = api.get_user_features(42)

    assert response == {"user_id": 42, "features": FEATURE_ROW}
    query, params = cursor.queries[0]
    assert params == (42,)
    for column in ("avg_engagement_score", "avg_purchase_probability"):
        assert column in query


def test_unknown_user_returns_404(monkeypatch, api):
    use_db(monkeypatch, api, [None])

    with pytest.raises(HTTPException) as exc:
        api.get_user_features(999999)

    assert exc.value.status_code == 404
    assert exc.value.detail == "No features found for user_id=999999"


def test_db_down_health_503_and_features_500_without_leaks(monkeypatch, api):
    use_db_down(monkeypatch, api)

    with pytest.raises(HTTPException) as health:
        api.health_check()
    assert health.value.status_code == 503
    assert health.value.detail == "database unavailable"
    assert_no_leak(health.value.detail)

    with pytest.raises(HTTPException) as features:
        api.get_user_features(42)
    assert features.value.status_code == 500
    assert features.value.detail == "Feature lookup failed"
    assert_no_leak(features.value.detail)


@pytest.mark.parametrize(
    ("results", "reason"),
    [
        ([{"table_exists": False}], "user_features missing"),
        ([{"table_exists": True}, {"has_rows": False}], "user_features empty"),
    ],
)
def test_health_503_reasons(monkeypatch, api, results, reason):
    use_db(monkeypatch, api, results)

    with pytest.raises(HTTPException) as exc:
        api.health_check()

    assert exc.value.status_code == 503
    assert exc.value.detail == reason


def test_health_ok(monkeypatch, api):
    use_db(monkeypatch, api, [{"table_exists": True}, {"has_rows": True}])

    assert api.health_check() == {"status": "healthy", "postgres": "connected"}


def test_user_id_is_typed_int(api):
    # FastAPI turns a non-integer path value into a 422 from this annotation.
    parameter = inspect.signature(api.get_user_features).parameters["user_id"]
    assert parameter.annotation is int

    route = next(r for r in api.app.routes if getattr(r, "path", None) == "/features/{user_id}")
    assert [p.name for p in route.dependant.path_params] == ["user_id"]
