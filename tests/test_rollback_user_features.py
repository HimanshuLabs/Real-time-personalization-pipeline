"""spark-batch/rollback_user_features.py: swap user_features and user_features_previous back."""

import importlib.util
import os
import time
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

ROOT = Path(__file__).resolve().parents[1]

# A value that appears only in table rows, never in the script's output.
ROW_MARKER = 777777


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def rollback():
    return load(ROOT / "spark-batch" / "rollback_user_features.py", "rollback_under_test")


# --- without a database ----------------------------------------------------------------


def test_uses_the_publish_lock_limit(rollback):
    publish = load(ROOT / "spark-batch" / "publish_gold_to_postgres.py", "publish_for_rollback")
    assert rollback.SWAP_LOCK_TIMEOUT == publish.SWAP_LOCK_TIMEOUT == "10s"
    assert rollback.PREVIOUS_TABLE == publish.POSTGRES_PREVIOUS_TABLE == "user_features_previous"
    assert rollback.LIVE_TABLE == publish.POSTGRES_TABLE


def test_importing_does_not_connect(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("connected at import")

    monkeypatch.setattr(psycopg, "connect", refuse)
    module = load(ROOT / "spark-batch" / "rollback_user_features.py", "rollback_import_check")
    assert callable(module.main)


# --- integration tests: real PostgreSQL ----------------------------------------------
#
# Same pattern as tests/test_publish_swap.py: P1_TEST_DATABASE_URL points at a
# maintenance database; the tests create TEST_DATABASE, work only there and drop it.
# Skipped when the variable is unset, except in CI.

TEST_DATABASE = "p1_rollback_test"
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


COLUMNS = "user_id integer, total_events bigint"


def make_table(conninfo, table, rows, primary_key=True, columns=COLUMNS):
    execute(conninfo, f"CREATE TABLE {table} ({columns})")
    if primary_key:
        execute(conninfo, f"ALTER TABLE {table} ADD CONSTRAINT {table}_pkey PRIMARY KEY (user_id)")
    if rows:
        placeholders = ", ".join(["%s"] * len(rows[0]))
        with psycopg.connect(conninfo, autocommit=True) as conn:
            conn.cursor().executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)


NEW_ROWS = [(1, 11), (2, 21), (3, ROW_MARKER)]
OLD_ROWS = [(1, 10), (2, 20)]


@pytest.fixture
def db(test_db):
    # As a publish with PUBLISH_KEEP_PREVIOUS=1 leaves them: the new table served,
    # the replaced one kept as user_features_previous.
    execute(test_db, "DROP TABLE IF EXISTS user_features, user_features_previous")
    execute(test_db, "DROP TABLE IF EXISTS user_features_rollback_tmp")
    make_table(test_db, "user_features", NEW_ROWS)
    make_table(test_db, "user_features_previous", OLD_ROWS)
    return test_db


def state(conninfo, table):
    """OID, rows and primary-key names of a table, or None if it does not exist."""
    [(exists,)] = execute(conninfo, "SELECT to_regclass(%s) IS NOT NULL", (table,))
    if not exists:
        return None
    [(oid,)] = execute(conninfo, "SELECT %s::regclass::oid", (table,))
    rows = execute(conninfo, f"SELECT * FROM {table} ORDER BY user_id")
    keys = execute(
        conninfo,
        "SELECT conname FROM pg_constraint WHERE conrelid = %s::regclass AND contype = 'p'",
        (table,),
    )
    return {"oid": oid, "rows": rows, "primary_key": [name for (name,) in keys]}


def tables(conninfo):
    return (
        state(conninfo, "user_features"),
        state(conninfo, "user_features_previous"),
        state(conninfo, "user_features_rollback_tmp"),
    )


def connect_to(conninfo, **kwargs):
    return lambda: psycopg.connect(conninfo, **kwargs)


def run(rollback, conninfo, options=None, lock_timeout="10s"):
    """rollback_or_exit; returns the exit code (0 when it returns normally)."""
    connect = connect_to(conninfo, options=options) if options else connect_to(conninfo)
    try:
        rollback.rollback_or_exit(connect, lock_timeout)
    except SystemExit as exit_info:
        return exit_info.code
    return 0


def test_rollback_serves_the_previous_table_again(rollback, db, capsys):
    new, old, _ = tables(db)

    assert run(rollback, db) == 0

    live, previous, tmp = tables(db)
    assert live == {**old, "primary_key": ["user_features_pkey"]}
    assert previous == {**new, "primary_key": ["user_features_previous_pkey"]}
    assert tmp is None
    out = capsys.readouterr().out
    assert out == (
        "Rolled back: user_features now serves the 2 rows of the previous table; the "
        "replaced 3 rows are kept as user_features_previous (run again to roll forward).\n"
    )


def test_a_second_rollback_rolls_forward(rollback, db):
    before = tables(db)

    assert run(rollback, db) == 0
    assert run(rollback, db) == 0

    assert tables(db) == before


def test_without_a_live_table_the_previous_one_is_served(rollback, db, capsys):
    execute(db, "DROP TABLE user_features")
    _, old, _ = tables(db)

    assert run(rollback, db) == 0

    assert tables(db) == ({**old, "primary_key": ["user_features_pkey"]}, None, None)
    assert capsys.readouterr().out == (
        "Rolled back: user_features now serves the 2 rows of user_features_previous.\n"
    )


def assert_refused(rollback, conninfo, capsys, message):
    before = tables(conninfo)

    assert run(rollback, conninfo) == 1

    assert tables(conninfo) == before
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == f"Rollback refused: {message} Nothing was changed.\n"


def test_refused_without_a_previous_table(rollback, db, capsys):
    execute(db, "DROP TABLE user_features_previous")
    assert_refused(rollback, db, capsys, "there is no user_features_previous to roll back to.")


def test_refused_when_the_previous_table_is_empty(rollback, db, capsys):
    execute(db, "DELETE FROM user_features_previous")
    assert_refused(rollback, db, capsys, "user_features_previous is empty.")


@pytest.mark.parametrize(
    "rows", [[(1, 10), (1, 11)], [(None, 10), (2, 20)]], ids=["duplicate", "null_user_id"]
)
def test_refused_when_the_previous_user_id_is_not_unique(rollback, db, capsys, rows):
    execute(db, "DROP TABLE user_features_previous")
    make_table(db, "user_features_previous", rows, primary_key=False)
    assert_refused(
        rollback,
        db,
        capsys,
        "user_features_previous has 2 rows but 1 distinct user_id (duplicate or NULL user_id).",
    )


@pytest.mark.parametrize(
    "columns",
    [
        "user_id integer, total_events bigint, extra integer",
        "user_id integer, total_events integer",
        "total_events bigint, user_id integer",
    ],
    ids=["extra_column", "other_type", "other_order"],
)
def test_refused_when_the_columns_differ(rollback, db, capsys, columns):
    execute(db, "DROP TABLE user_features_previous")
    make_table(db, "user_features_previous", [], columns=columns)
    execute(db, "INSERT INTO user_features_previous (user_id, total_events) VALUES (1, 10)")
    assert_refused(
        rollback,
        db,
        capsys,
        "the columns of user_features_previous differ from those of user_features "
        "(names, types or order).",
    )


def test_held_lock_times_out_and_changes_nothing(rollback, db, capsys):
    before = tables(db)

    holder = psycopg.connect(db)
    try:
        holder.execute("SELECT 1 FROM user_features LIMIT 1")  # opens a transaction
        started = time.monotonic()
        code = run(rollback, db, options="-c lock_timeout=30s", lock_timeout="1s")
        waited = time.monotonic() - started
    finally:
        holder.rollback()
        holder.close()

    assert code == 1
    assert 0.9 <= waited < 5, waited
    assert tables(db) == before
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "Rollback aborted: could not lock user_features or user_features_previous within 1s "
        "because another session holds it. The transaction was rolled back and nothing was "
        "changed. Rerun the rollback once that session ends.\n"
    )


def test_output_never_carries_row_values(rollback, db, capsys):
    assert run(rollback, db) == 0
    assert run(rollback, db) == 0

    captured = capsys.readouterr()
    assert str(ROW_MARKER) not in captured.out + captured.err
