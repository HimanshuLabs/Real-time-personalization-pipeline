"""The opt-in shrink guard of spark-batch/publish_gold_to_postgres.py (PUBLISH_REFUSE_SHRINK)."""

import importlib.util
import os
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def publish():
    spec = importlib.util.spec_from_file_location(
        "publish_shrink_under_test", ROOT / "spark-batch" / "publish_gold_to_postgres.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- unit tests: fake connection, no database ----------------------------------------

SET_TIMEOUT = "SET LOCAL lock_timeout = '10s'"
CHECK_STAGING = 'SELECT COUNT(*), COUNT(DISTINCT user_id) FROM "user_features_staging"'
LIVE_EXISTS = "SELECT to_regclass('user_features')"
COUNT_LIVE = 'SELECT COUNT(*) FROM "user_features"'
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
SWAP = [ADD_PK, DROP_LIVE, RENAME_TABLE, RENAME_PK]


class FakeDatabase:
    """Records every statement and how the transaction ended; answers the three SELECTs."""

    def __init__(self, staging_users=3, live_users=3, live_exists=True):
        self.results = {
            CHECK_STAGING: (staging_users, staging_users),
            LIVE_EXISTS: ("user_features" if live_exists else None,),
            COUNT_LIVE: (live_users,),
        }
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


class FakeTransaction:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db.log.append("BEGIN")
        return self

    def __exit__(self, exc_type, *exc):
        self.db.log.append("ROLLBACK" if exc_type else "COMMIT")
        return False


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

    def fetchone(self):
        return self.db.results[self.db.log[-1]]


def test_without_the_flag_the_statements_are_todays(publish):
    for kwargs in ({}, {"refuse_shrink": False}):
        db = FakeDatabase(staging_users=1, live_users=3)

        publish.swap_staging_into_live(db.connect, **kwargs)

        # A shrink is swapped, with no extra query: exactly the plain swap.
        assert db.log == ["BEGIN", SET_TIMEOUT, CHECK_STAGING, *SWAP, "COMMIT", "CLOSE"]


@pytest.mark.parametrize(("staging_users", "live_users"), [(3, 3), (4, 3)], ids=["equal", "more"])
def test_with_the_flag_equal_or_more_users_are_swapped(publish, staging_users, live_users):
    db = FakeDatabase(staging_users=staging_users, live_users=live_users)

    publish.swap_staging_into_live(db.connect, refuse_shrink=True)

    # The live count is read inside the transaction, before any change.
    assert db.log == [
        "BEGIN",
        SET_TIMEOUT,
        CHECK_STAGING,
        LIVE_EXISTS,
        COUNT_LIVE,
        *SWAP,
        "COMMIT",
        "CLOSE",
    ]


def test_with_the_flag_a_first_publish_is_swapped(publish):
    db = FakeDatabase(staging_users=1, live_exists=False)

    publish.swap_staging_into_live(db.connect, refuse_shrink=True)

    assert db.log == ["BEGIN", SET_TIMEOUT, CHECK_STAGING, LIVE_EXISTS, *SWAP, "COMMIT", "CLOSE"]


def test_with_the_flag_fewer_users_are_refused_before_any_change(publish):
    db = FakeDatabase(staging_users=2, live_users=3)

    with pytest.raises(publish.ShrinkRefused) as refused:
        publish.swap_staging_into_live(db.connect, refuse_shrink=True)

    assert (refused.value.staging_users, refused.value.live_users) == (2, 3)
    assert db.log == [
        "BEGIN",
        SET_TIMEOUT,
        CHECK_STAGING,
        LIVE_EXISTS,
        COUNT_LIVE,
        "ROLLBACK",
        "CLOSE",
    ]


def test_swap_or_exit_reports_a_refused_shrink_and_exits_1(publish, capsys):
    db = FakeDatabase(staging_users=2, live_users=3)

    with pytest.raises(SystemExit) as exit_info:
        publish.swap_or_exit(db.connect, refuse_shrink=True)

    assert exit_info.value.code == 1
    assert db.log[-2:] == ["ROLLBACK", "CLOSE"]
    captured = capsys.readouterr()
    expected = (
        "Staging user_features_staging has 2 users but the live user_features has 3. "
        "Refusing to publish fewer users (PUBLISH_REFUSE_SHRINK=1). The transaction was "
        "rolled back and the live user_features table is unchanged.\n"
    )
    assert captured.err == expected
    assert "Traceback" not in captured.err
    # Only the swap's own staging line, printed before the refusal.
    assert captured.out == "Staging user_features_staging: 2 rows, 2 distinct user_id\n"


def test_swap_or_exit_without_the_flag_swaps_a_shrink(publish):
    db = FakeDatabase(staging_users=1, live_users=3)

    publish.swap_or_exit(db.connect)

    assert db.log == ["BEGIN", SET_TIMEOUT, CHECK_STAGING, *SWAP, "COMMIT", "CLOSE"]


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        ({}, False),
        ({"PUBLISH_REFUSE_SHRINK": ""}, False),
        ({"PUBLISH_REFUSE_SHRINK": "0"}, False),
        ({"PUBLISH_REFUSE_SHRINK": "1"}, True),
    ],
    ids=["unset", "empty", "zero", "one"],
)
def test_refuse_shrink_setting(publish, environ, expected):
    assert publish.refuse_shrink_setting(environ) is expected


@pytest.mark.parametrize("value", ["yes", "true", "2", " 1"])
def test_refuse_shrink_setting_rejects_other_values(publish, value):
    with pytest.raises(ValueError) as error:
        publish.refuse_shrink_setting({"PUBLISH_REFUSE_SHRINK": value})

    assert str(error.value) == f"PUBLISH_REFUSE_SHRINK must be 0 or 1, got {value!r}"


# --- integration tests: real PostgreSQL ----------------------------------------------
#
# Same pattern as tests/test_publish_swap.py: P1_TEST_DATABASE_URL points at a
# maintenance database; the tests create TEST_DATABASE, work only there and drop it.
# Skipped when the variable is unset, except in CI.

TEST_DATABASE = "p1_shrink_test"
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


LIVE_ROWS = [(1, 10), (2, 20), (3, 30)]


@pytest.fixture
def db(test_db):
    # A live table of three users, as a previous publish leaves it, and no staging.
    execute(test_db, "DROP TABLE IF EXISTS user_features, user_features_staging")
    execute(test_db, "CREATE TABLE user_features (user_id integer, total_events bigint)")
    execute(
        test_db, "ALTER TABLE user_features ADD CONSTRAINT user_features_pkey PRIMARY KEY (user_id)"
    )
    with psycopg.connect(test_db, autocommit=True) as conn:
        conn.cursor().executemany("INSERT INTO user_features VALUES (%s, %s)", LIVE_ROWS)
    return test_db


def make_staging(conninfo, rows):
    # As the JDBC write leaves it: no primary key.
    execute(conninfo, "CREATE TABLE user_features_staging (user_id integer, total_events bigint)")
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.cursor().executemany("INSERT INTO user_features_staging VALUES (%s, %s)", rows)


def live_state(conninfo):
    [(oid,)] = execute(conninfo, "SELECT 'user_features'::regclass::oid")
    rows = execute(conninfo, "SELECT user_id, total_events FROM user_features ORDER BY user_id")
    return {"oid": oid, "rows": rows}


def connect_to(conninfo):
    return lambda: psycopg.connect(conninfo)


def test_flag_refuses_fewer_users_and_leaves_live_unchanged(publish, db):
    before = live_state(db)
    make_staging(db, [(1, 11), (2, 21)])

    with pytest.raises(publish.ShrinkRefused):
        publish.swap_staging_into_live(connect_to(db), refuse_shrink=True)

    assert live_state(db) == before


@pytest.mark.parametrize(
    "rows",
    [[(1, 11), (2, 21), (3, 31)], [(1, 11), (2, 21), (3, 31), (4, 41)]],
    ids=["equal", "more"],
)
def test_flag_swaps_equal_or_more_users(publish, db, rows):
    before = live_state(db)
    make_staging(db, rows)

    publish.swap_staging_into_live(connect_to(db), refuse_shrink=True)

    after = live_state(db)
    assert after["oid"] != before["oid"]
    assert after["rows"] == rows


def test_flag_swaps_a_first_publish(publish, db):
    execute(db, "DROP TABLE user_features")
    make_staging(db, [(1, 11)])

    publish.swap_staging_into_live(connect_to(db), refuse_shrink=True)

    assert live_state(db)["rows"] == [(1, 11)]


def test_without_the_flag_fewer_users_are_swapped(publish, db):
    make_staging(db, [(5, 50)])

    publish.swap_staging_into_live(connect_to(db))

    assert live_state(db)["rows"] == [(5, 50)]
