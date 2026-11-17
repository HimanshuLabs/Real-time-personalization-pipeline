import importlib.util
import os
import time
from pathlib import Path

import psycopg
import pytest
from psycopg import errors, sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def publish():
    # Importing the job must not start Spark: the work lives in main().
    spec = importlib.util.spec_from_file_location(
        "publish_under_test", ROOT / "spark-batch" / "publish_gold_to_postgres.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- unit tests: fake connection, no database ----------------------------------------


class FakeCursor:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        text = query.as_string(None) if isinstance(query, sql.Composable) else query
        self.db.log.append(text)
        if self.db.fail_on and text.startswith(self.db.fail_on[0]):
            raise self.db.fail_on[1]

    def fetchone(self):
        return self.db.staging_counts


class FakeTransaction:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db.log.append("BEGIN")
        return self

    def __exit__(self, exc_type, *exc):
        self.db.log.append("ROLLBACK" if exc_type else "COMMIT")
        return False


class FakeDatabase:
    """Records every statement and how the transaction ended."""

    def __init__(self, staging_counts=(3, 3), fail_on=None):
        self.staging_counts = staging_counts  # (COUNT(*), COUNT(DISTINCT user_id))
        self.fail_on = fail_on  # (statement prefix, exception to raise)
        self.log = []

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.log.append("CLOSE")
        return False

    def transaction(self):
        return FakeTransaction(self)

    def cursor(self):
        return FakeCursor(self)


SET_TIMEOUT = "SET LOCAL lock_timeout = '10s'"
CHECK_STAGING = 'SELECT COUNT(*), COUNT(DISTINCT user_id) FROM "user_features_staging"'
ADD_PK = (
    'ALTER TABLE "user_features_staging" ADD CONSTRAINT "user_features_staging_pkey" '
    "PRIMARY KEY (user_id)"
)
DROP_LIVE = 'DROP TABLE IF EXISTS "user_features"'
RENAME_TABLE = 'ALTER TABLE "user_features_staging" RENAME TO "user_features"'
RENAME_PK = (
    'ALTER TABLE "user_features" RENAME CONSTRAINT "user_features_staging_pkey" '
    'TO "user_features_pkey"'
)


def test_swap_statement_order(publish):
    db = FakeDatabase()

    publish.swap_staging_into_live(db.connect)

    assert db.log == [
        "BEGIN",
        SET_TIMEOUT,
        CHECK_STAGING,
        ADD_PK,
        DROP_LIVE,
        RENAME_TABLE,
        RENAME_PK,
        "COMMIT",
        "CLOSE",
    ]


def test_lock_timeout_parameter_default_is_10s(publish):
    assert publish.SWAP_LOCK_TIMEOUT == "10s"

    db = FakeDatabase()
    publish.swap_staging_into_live(db.connect, lock_timeout="1s")
    assert db.log[1] == "SET LOCAL lock_timeout = '1s'"


@pytest.mark.parametrize(
    ("staging_counts", "message"),
    [
        ((0, 0), "Staging table is empty"),
        ((3, 2), "duplicate or NULL user_id"),
    ],
)
def test_bad_staging_refused_before_any_change(publish, staging_counts, message):
    db = FakeDatabase(staging_counts=staging_counts)

    with pytest.raises(RuntimeError, match=message):
        publish.swap_staging_into_live(db.connect)

    assert db.log == ["BEGIN", SET_TIMEOUT, CHECK_STAGING, "ROLLBACK", "CLOSE"]


@pytest.mark.parametrize("failing", [ADD_PK, DROP_LIVE, RENAME_TABLE, RENAME_PK])
def test_any_failure_rolls_back(publish, failing):
    db = FakeDatabase(fail_on=(failing, psycopg.OperationalError("simulated failure")))

    with pytest.raises(psycopg.OperationalError):
        publish.swap_staging_into_live(db.connect)

    # Nothing after the failing statement runs, and nothing is committed.
    assert db.log[-3:] == [failing, "ROLLBACK", "CLOSE"]
    assert "COMMIT" not in db.log


def test_lock_timeout_exits_1_with_clean_message(publish, capsys):
    driver_text = "canceling statement due to lock timeout (user de_user, password s3cret)"
    db = FakeDatabase(fail_on=(DROP_LIVE, errors.LockNotAvailable(driver_text)))

    with pytest.raises(SystemExit) as exit_info:
        publish.swap_or_exit(db.connect)

    assert exit_info.value.code == 1
    assert db.log[-2:] == ["ROLLBACK", "CLOSE"]
    err = capsys.readouterr().err
    assert err.startswith("Publish aborted: could not lock user_features within 10s")
    assert "live user_features table is unchanged" in err
    for leaked in (
        driver_text,
        "canceling statement",
        "s3cret",
        "de_user",
        "Traceback",
        "LockNotAvailable",
    ):
        assert leaked not in err


def test_other_failures_are_not_turned_into_exit_1(publish):
    db = FakeDatabase(staging_counts=(0, 0))

    with pytest.raises(RuntimeError, match="Staging table is empty"):
        publish.swap_or_exit(db.connect)


# --- integration tests: real PostgreSQL ----------------------------------------------
#
# P1_TEST_DATABASE_URL points at a maintenance database (e.g. .../postgres) of a server
# where the role may create databases. The tests create TEST_DATABASE, work only there
# and drop it at the end. They are skipped when the variable is unset, except in CI.

TEST_DATABASE = "p1_publish_test"
DATABASE_URL = os.environ.get("P1_TEST_DATABASE_URL")


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

    yield make_conninfo(DATABASE_URL, dbname=TEST_DATABASE)

    with psycopg.connect(DATABASE_URL, autocommit=True) as admin:
        admin.execute(drop)


def execute(conninfo, query, params=None):
    with psycopg.connect(conninfo, autocommit=True) as conn:
        cur = conn.execute(query, params)
        return cur.fetchall() if cur.description else None


@pytest.fixture
def db(test_db):
    # A live table as a previous publish leaves it, and no staging table.
    execute(test_db, "DROP TABLE IF EXISTS user_features, user_features_staging")
    execute(test_db, "CREATE TABLE user_features (user_id integer, total_events bigint)")
    execute(
        test_db, "ALTER TABLE user_features ADD CONSTRAINT user_features_pkey PRIMARY KEY (user_id)"
    )
    execute(test_db, "INSERT INTO user_features VALUES (1, 10), (2, 20)")
    return test_db


def make_staging(conninfo, rows):
    # As the JDBC write leaves it: no primary key.
    execute(conninfo, "CREATE TABLE user_features_staging (user_id integer, total_events bigint)")
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.cursor().executemany("INSERT INTO user_features_staging VALUES (%s, %s)", rows)


def live_state(conninfo):
    [(oid,)] = execute(conninfo, "SELECT 'user_features'::regclass::oid")
    rows = execute(conninfo, "SELECT user_id, total_events FROM user_features ORDER BY user_id")
    keys = execute(
        conninfo,
        "SELECT conname FROM pg_constraint "
        "WHERE conrelid = 'user_features'::regclass AND contype = 'p'",
    )
    return {"oid": oid, "rows": rows, "primary_key": [name for (name,) in keys]}


def staging_exists(conninfo):
    [(exists,)] = execute(conninfo, "SELECT to_regclass('user_features_staging') IS NOT NULL")
    return exists


def connect_to(conninfo, **kwargs):
    return lambda: psycopg.connect(conninfo, **kwargs)


def test_swap_replaces_live_table(publish, db):
    before = live_state(db)
    make_staging(db, [(1, 11), (2, 21), (3, 31)])

    publish.swap_staging_into_live(connect_to(db))

    after = live_state(db)
    assert after["oid"] != before["oid"]
    assert after["rows"] == [(1, 11), (2, 21), (3, 31)]
    assert after["primary_key"] == ["user_features_pkey"]
    assert not staging_exists(db)


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([], "Staging table is empty"),
        ([(1, 11), (1, 12)], "duplicate or NULL user_id"),
        ([(None, 11), (2, 21)], "duplicate or NULL user_id"),
    ],
    ids=["empty", "duplicate", "null_user_id"],
)
def test_bad_staging_leaves_live_table_unchanged(publish, db, rows, message):
    before = live_state(db)
    make_staging(db, rows)

    with pytest.raises(RuntimeError, match=message):
        publish.swap_staging_into_live(connect_to(db))

    assert live_state(db) == before


def test_held_lock_times_out_and_leaves_live_table_unchanged(publish, db):
    before = live_state(db)
    make_staging(db, [(1, 11)])

    holder = psycopg.connect(db)
    try:
        holder.execute("SELECT 1 FROM user_features LIMIT 1")  # opens a transaction
        # Backstop: without the swap's own SET LOCAL the wait would end only after 30s.
        started = time.monotonic()
        with pytest.raises(errors.LockNotAvailable):
            publish.swap_staging_into_live(
                connect_to(db, options="-c lock_timeout=30s"), lock_timeout="1s"
            )
        waited = time.monotonic() - started
    finally:
        holder.rollback()
        holder.close()

    assert 0.9 <= waited < 5, waited
    assert live_state(db) == before


def test_two_swaps_in_a_row(publish, db):
    oids = [live_state(db)["oid"]]

    for rows in ([(1, 11), (2, 21)], [(5, 50)]):
        make_staging(db, rows)
        publish.swap_staging_into_live(connect_to(db))
        state = live_state(db)
        oids.append(state["oid"])
        assert state["rows"] == rows
        assert state["primary_key"] == ["user_features_pkey"]

    assert len(set(oids)) == 3


def test_gold_path_resolves_from_project1_root(monkeypatch, tmp_path):
    path = ROOT / "spark-batch" / "publish_gold_to_postgres.py"

    def load():
        spec = importlib.util.spec_from_file_location("publish_root_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    monkeypatch.delenv("PROJECT1_ROOT", raising=False)
    assert load().project_path("data/gold/user_features") == str(ROOT / "data/gold/user_features")

    monkeypatch.setenv("PROJECT1_ROOT", str(tmp_path))
    assert load().project_path("data/gold/user_features") == str(
        tmp_path / "data/gold/user_features"
    )
