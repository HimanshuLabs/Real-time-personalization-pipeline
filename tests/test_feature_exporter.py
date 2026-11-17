import importlib.util
import math
from pathlib import Path

import psycopg
import pytest
from prometheus_client import REGISTRY

ROOT = Path(__file__).resolve().parents[1]

NOW = 1_800_000_000.0
LAST_EVENT_EPOCH = NOW - 90.0
COMPUTED_EPOCH = NOW - 3_600.0

GAUGES = [
    "project1_user_features_rows",
    "project1_latest_feature_event_timestamp_unix",
    "project1_feature_freshness_seconds",
    "project1_features_computed_age_seconds",
]


@pytest.fixture(scope="module")
def exporter():
    # Imported once: its gauges register in the process-wide Prometheus registry.
    # Importing it must not start the HTTP server or poll; that lives in main().
    spec = importlib.util.spec_from_file_location(
        "feature_exporter_under_test",
        ROOT / "monitoring" / "exporters" / "postgres_feature_exporter.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def value(name):
    return REGISTRY.get_sample_value(name)


class FakeCursor:
    def __init__(self, row, queries):
        self.row = row
        self.queries = queries

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query):
        self.queries.append(query)

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, row, queries):
        self.row = row
        self.queries = queries

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return FakeCursor(self.row, self.queries)


def database_returning(monkeypatch, exporter, row):
    queries = []
    monkeypatch.setattr(exporter.psycopg, "connect", lambda **kwargs: FakeConnection(row, queries))
    monkeypatch.setattr(exporter.time, "time", lambda: NOW)
    return queries


def database_down(monkeypatch, exporter):
    def refuse(**kwargs):
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(exporter.psycopg, "connect", refuse)


def test_db_up_reports_features_computed_age_from_max_features_computed_at(monkeypatch, exporter):
    queries = database_returning(monkeypatch, exporter, (400, LAST_EVENT_EPOCH, COMPUTED_EPOCH))

    exporter.collect_metrics()

    assert "MAX(features_computed_at)" in queries[0]
    assert value("project1_feature_exporter_up") == 1
    assert value("project1_features_computed_age_seconds") == 3_600.0
    assert value("project1_feature_freshness_seconds") == 90.0
    assert value("project1_user_features_rows") == 400
    assert value("project1_latest_feature_event_timestamp_unix") == LAST_EVENT_EPOCH


def test_db_up_with_empty_table_reports_minus_one_ages(monkeypatch, exporter):
    database_returning(monkeypatch, exporter, (0, None, None))

    exporter.collect_metrics()

    assert value("project1_feature_exporter_up") == 1
    assert value("project1_user_features_rows") == 0
    assert value("project1_features_computed_age_seconds") == -1
    assert value("project1_feature_freshness_seconds") == -1


def test_db_down_after_a_good_scrape_reports_nan_for_every_gauge(monkeypatch, exporter):
    database_returning(monkeypatch, exporter, (400, LAST_EVENT_EPOCH, COMPUTED_EPOCH))
    exporter.collect_metrics()
    assert value("project1_features_computed_age_seconds") == 3_600.0

    database_down(monkeypatch, exporter)
    exporter.collect_metrics()

    assert value("project1_feature_exporter_up") == 0
    for name in GAUGES:
        assert math.isnan(value(name)), name


def test_db_coming_back_replaces_nan_with_the_computed_age(monkeypatch, exporter):
    database_down(monkeypatch, exporter)
    exporter.collect_metrics()
    assert math.isnan(value("project1_features_computed_age_seconds"))

    database_returning(monkeypatch, exporter, (400, LAST_EVENT_EPOCH, COMPUTED_EPOCH))
    exporter.collect_metrics()

    assert value("project1_feature_exporter_up") == 1
    assert value("project1_features_computed_age_seconds") == 3_600.0
