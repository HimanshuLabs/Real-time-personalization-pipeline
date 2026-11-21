"""The opt-in keep-previous swap of the publish job (PUBLISH_KEEP_PREVIOUS)."""

import importlib.util
import os
from pathlib import Path

import psycopg
import pytest
from psycopg import errors, sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from test_bronze_export_check import bash_command, dag_tree, operators

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def publish():
    spec = importlib.util.spec_from_file_location(
        "publish_keep_previous_under_test", ROOT / "spark-batch" / "publish_gold_to_postgres.py"
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
DROP_PREVIOUS = 'DROP TABLE IF EXISTS "user_features_previous"'
LIVE_TO_PREVIOUS = 'ALTER TABLE "user_features" RENAME TO "user_features_previous"'
PREVIOUS_PK = (
    'ALTER TABLE "user_features_previous" RENAME CONSTRAINT "user_features_pkey" '
    'TO "user_features_previous_pkey"'
)
RENAME_TABLE = 'ALTER TABLE "user_features_staging" RENAME TO "user_features"'
RENAME_PK = (
    'ALTER TABLE "user_features" RENAME CONSTRAINT "user_features_staging_pkey" '
    'TO "user_features_pkey"'
)
KEEP = [DROP_PREVIOUS, LIVE_TO_PREVIOUS, PREVIOUS_PK]
FINISH = [RENAME_TABLE, RENAME_PK, "COMMIT", "CLOSE"]


class FakeDatabase:
    """Records every statement and how the transaction ended; answers the SELECTs."""

    def __init__(self, staging_users=3, live_users=3, live_exists=True, fail_on=None):
        self.results = {
            CHECK_STAGING: (staging_users, staging_users),
            LIVE_EXISTS: ("user_features" if live_exists else None,),
            COUNT_LIVE: (live_users,),
        }
        self.fail_on = fail_on  # (statement, exception to raise)
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
        if self.db.fail_on and text == self.db.fail_on[0]:
            raise self.db.fail_on[1]

    def fetchone(self):
        return self.db.results[self.db.log[-1]]


def test_without_the_flag_the_statements_are_todays(publish):
    for kwargs in ({}, {"keep_previous": False}):
        db = FakeDatabase()

        publish.swap_staging_into_live(db.connect, **kwargs)

        assert db.log == ["BEGIN", SET_TIMEOUT, CHECK_STAGING, ADD_PK, DROP_LIVE, *FINISH]


def test_with_the_flag_the_live_table_becomes_the_previous_one(publish):
    db = FakeDatabase()

    publish.swap_staging_into_live(db.connect, keep_previous=True)

    assert db.log == ["BEGIN", SET_TIMEOUT, CHECK_STAGING, ADD_PK, LIVE_EXISTS, *KEEP, *FINISH]
    assert DROP_LIVE not in db.log


def test_with_the_flag_and_no_live_table_the_previous_one_is_left_alone(publish):
    db = FakeDatabase(live_exists=False)

    publish.swap_staging_into_live(db.connect, keep_previous=True)

    # No DROP of user_features_previous: there is no live table to replace it.
    assert db.log == ["BEGIN", SET_TIMEOUT, CHECK_STAGING, ADD_PK, LIVE_EXISTS, *FINISH]


def test_the_dag_configuration_runs_both_guards(publish):
    db = FakeDatabase(staging_users=3, live_users=3)

    publish.swap_staging_into_live(db.connect, refuse_shrink=True, keep_previous=True)

    assert db.log == [
        "BEGIN",
        SET_TIMEOUT,
        CHECK_STAGING,
        LIVE_EXISTS,
        COUNT_LIVE,
        ADD_PK,
        LIVE_EXISTS,
        *KEEP,
        *FINISH,
    ]


@pytest.mark.parametrize("failing", [DROP_PREVIOUS, LIVE_TO_PREVIOUS, PREVIOUS_PK, RENAME_TABLE])
def test_any_failure_with_the_flag_rolls_back(publish, failing):
    db = FakeDatabase(fail_on=(failing, psycopg.OperationalError("simulated failure")))

    with pytest.raises(psycopg.OperationalError):
        publish.swap_staging_into_live(db.connect, keep_previous=True)

    assert db.log[-3:] == [failing, "ROLLBACK", "CLOSE"]
    assert "COMMIT" not in db.log


def test_swap_or_exit_passes_the_flag_on(publish):
    db = FakeDatabase()

    publish.swap_or_exit(db.connect, keep_previous=True)

    assert db.log == ["BEGIN", SET_TIMEOUT, CHECK_STAGING, ADD_PK, LIVE_EXISTS, *KEEP, *FINISH]


def test_lock_timeout_with_the_flag_exits_1_with_the_clean_message(publish, capsys):
    db = FakeDatabase(fail_on=(LIVE_TO_PREVIOUS, errors.LockNotAvailable("driver text s3cret")))

    with pytest.raises(SystemExit) as exit_info:
        publish.swap_or_exit(db.connect, keep_previous=True)

    assert exit_info.value.code == 1
    assert db.log[-2:] == ["ROLLBACK", "CLOSE"]
    err = capsys.readouterr().err
    assert err.startswith("Publish aborted: could not lock user_features within 10s")
    assert "driver text" not in err and "s3cret" not in err and "Traceback" not in err


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        ({}, False),
        ({"PUBLISH_KEEP_PREVIOUS": ""}, False),
        ({"PUBLISH_KEEP_PREVIOUS": "0"}, False),
        ({"PUBLISH_KEEP_PREVIOUS": "1"}, True),
    ],
    ids=["unset", "empty", "zero", "one"],
)
def test_keep_previous_setting(publish, environ, expected):
    assert publish.keep_previous_setting(environ) is expected


@pytest.mark.parametrize("value", ["yes", "true", "2", " 1"])
def test_keep_previous_setting_rejects_other_values(publish, value):
    with pytest.raises(ValueError) as error:
        publish.keep_previous_setting({"PUBLISH_KEEP_PREVIOUS": value})

    assert str(error.value) == f"PUBLISH_KEEP_PREVIOUS must be 0 or 1, got {value!r}"


def test_dag_publish_keeps_the_previous_table():
    publish_task = operators(dag_tree())["gold_to_postgres"]
    expected = (
        "PUBLISH_KEEP_PREVIOUS=1 PUBLISH_REFUSE_SHRINK=1 spark-submit --packages "
        "org.postgresql:postgresql:42.7.4 spark-batch/publish_gold_to_postgres.py"
    )
    assert expected in bash_command(publish_task)


# --- integration tests: real PostgreSQL ----------------------------------------------
#
# Same pattern as tests/test_publish_swap.py: P1_TEST_DATABASE_URL points at a
# maintenance database; the tests create TEST_DATABASE, work only there and drop it.
# Skipped when the variable is unset, except in CI.

TEST_DATABASE = "p1_previous_test"
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


def make_table(conninfo, table, rows, primary_key=True):
    execute(conninfo, f"CREATE TABLE {table} (user_id integer, total_events bigint)")
    if primary_key:
        execute(conninfo, f"ALTER TABLE {table} ADD CONSTRAINT {table}_pkey PRIMARY KEY (user_id)")
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.cursor().executemany(f"INSERT INTO {table} VALUES (%s, %s)", rows)


@pytest.fixture
def db(test_db):
    # A live table as a previous publish leaves it; no staging, no previous table.
    execute(
        test_db,
        "DROP TABLE IF EXISTS user_features, user_features_staging, user_features_previous",
    )
    make_table(test_db, "user_features", [(1, 10), (2, 20)])
    return test_db


def state(conninfo, table):
    """OID, rows and primary-key names of a table, or None if it does not exist."""
    [(exists,)] = execute(conninfo, "SELECT to_regclass(%s) IS NOT NULL", (table,))
    if not exists:
        return None
    [(oid,)] = execute(conninfo, "SELECT %s::regclass::oid", (table,))
    rows = execute(conninfo, f"SELECT user_id, total_events FROM {table} ORDER BY user_id")
    keys = execute(
        conninfo,
        "SELECT conname FROM pg_constraint WHERE conrelid = %s::regclass AND contype = 'p'",
        (table,),
    )
    return {"oid": oid, "rows": rows, "primary_key": [name for (name,) in keys]}


def connect_to(conninfo, **kwargs):
    return lambda: psycopg.connect(conninfo, **kwargs)


def test_publish_with_the_flag_keeps_the_replaced_table(publish, db):
    before = state(db, "user_features")
    make_table(db, "user_features_staging", [(1, 11), (2, 21), (3, 31)], primary_key=False)

    publish.swap_staging_into_live(connect_to(db), keep_previous=True)

    previous = state(db, "user_features_previous")
    live = state(db, "user_features")
    assert previous == {**before, "primary_key": ["user_features_previous_pkey"]}
    assert live["rows"] == [(1, 11), (2, 21), (3, 31)]
    assert live["primary_key"] == ["user_features_pkey"]
    assert live["oid"] != before["oid"]
    assert state(db, "user_features_staging") is None


def test_the_next_publish_replaces_the_previous_table(publish, db):
    first = state(db, "user_features")
    make_table(db, "user_features_staging", [(5, 50)], primary_key=False)
    publish.swap_staging_into_live(connect_to(db), keep_previous=True)
    second = state(db, "user_features")
    make_table(db, "user_features_staging", [(6, 60)], primary_key=False)

    publish.swap_staging_into_live(connect_to(db), keep_previous=True)

    assert state(db, "user_features_previous") == {
        **second,
        "primary_key": ["user_features_previous_pkey"],
    }
    assert state(db, "user_features")["rows"] == [(6, 60)]
    # The first table is gone: exactly one previous table is kept.
    [(count,)] = execute(db, "SELECT count(*) FROM pg_class WHERE oid = %s", (first["oid"],))
    assert count == 0


def test_without_a_live_table_an_existing_previous_table_is_left_alone(publish, db):
    execute(db, "DROP TABLE user_features")
    make_table(db, "user_features_previous", [(9, 90)])
    kept = state(db, "user_features_previous")
    make_table(db, "user_features_staging", [(1, 11)], primary_key=False)

    publish.swap_staging_into_live(connect_to(db), keep_previous=True)

    assert state(db, "user_features_previous") == kept
    assert state(db, "user_features")["rows"] == [(1, 11)]
    assert state(db, "user_features")["primary_key"] == ["user_features_pkey"]


def test_bad_staging_with_the_flag_changes_nothing(publish, db):
    make_table(db, "user_features_previous", [(7, 70)])
    before = (state(db, "user_features"), state(db, "user_features_previous"))
    make_table(db, "user_features_staging", [(1, 11), (1, 12)], primary_key=False)

    with pytest.raises(RuntimeError, match="duplicate or NULL user_id"):
        publish.swap_staging_into_live(connect_to(db), keep_previous=True)

    assert (state(db, "user_features"), state(db, "user_features_previous")) == before


def test_held_lock_with_the_flag_times_out_and_changes_nothing(publish, db):
    make_table(db, "user_features_previous", [(7, 70)])
    before = (state(db, "user_features"), state(db, "user_features_previous"))
    make_table(db, "user_features_staging", [(1, 11)], primary_key=False)

    holder = psycopg.connect(db)
    try:
        holder.execute("SELECT 1 FROM user_features LIMIT 1")  # opens a transaction
        with pytest.raises(errors.LockNotAvailable):
            publish.swap_staging_into_live(
                connect_to(db, options="-c lock_timeout=30s"),
                lock_timeout="1s",
                keep_previous=True,
            )
    finally:
        holder.rollback()
        holder.close()

    assert (state(db, "user_features"), state(db, "user_features_previous")) == before


def test_the_dag_configuration_on_a_real_database(publish, db):
    before = state(db, "user_features")
    make_table(db, "user_features_staging", [(1, 11), (2, 21)], primary_key=False)

    publish.swap_staging_into_live(connect_to(db), refuse_shrink=True, keep_previous=True)

    assert state(db, "user_features_previous")["oid"] == before["oid"]
    assert state(db, "user_features")["rows"] == [(1, 11), (2, 21)]
