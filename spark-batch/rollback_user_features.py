"""Roll user_features back to user_features_previous, atomically and reversibly.

A publish with PUBLISH_KEEP_PREVIOUS=1 keeps the table it replaces as
user_features_previous. This script swaps the two in one transaction, under the
publish's lock limit (SWAP_LOCK_TIMEOUT), so running it again rolls forward. It
refuses, changing nothing, when there is no previous table, when the previous
table is empty or its user_id is not unique, or when its columns (names, types,
order) differ from the live table's, so a rollback never changes the served
columns.

Exit 0 after a rollback; 1 for every refusal and for a lock timeout, with one
clean line and no driver text. It prints row counts only, never rows. Settings:
the publish job's POSTGRES_* variables (same defaults).
"""

import os
import sys

import psycopg
from psycopg import errors, sql

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "personalization_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "de_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "de_password")

LIVE_TABLE = "user_features"
PREVIOUS_TABLE = "user_features_previous"
TMP_TABLE = "user_features_rollback_tmp"

# The publish's lock limit (spark-batch/publish_gold_to_postgres.py), so a
# rollback waits for readers exactly as long as a publish does.
SWAP_LOCK_TIMEOUT = "10s"


class RollbackRefused(RuntimeError):
    """The rollback was refused before any change; the message says why."""


def connect_to_postgres():
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def columns(cur, table):
    cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = %s "
        "ORDER BY ordinal_position",
        (table,),
    )
    return cur.fetchall()


def rename(cur, table, new_name):
    old, new = sql.Identifier(table), sql.Identifier(new_name)
    cur.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(old, new))
    cur.execute(
        sql.SQL("ALTER TABLE {} RENAME CONSTRAINT {} TO {}").format(
            new, sql.Identifier(f"{table}_pkey"), sql.Identifier(f"{new_name}_pkey")
        )
    )


def rollback(connect=connect_to_postgres, lock_timeout=SWAP_LOCK_TIMEOUT):
    """Swap user_features and user_features_previous in one transaction.

    Returns (rows now served, rows kept as user_features_previous, or None when
    there was no live table). Raises RollbackRefused before any change.
    """
    with connect() as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(lock_timeout)))

        cur.execute(
            "SELECT to_regclass(%s) IS NOT NULL, to_regclass(%s) IS NOT NULL",
            (PREVIOUS_TABLE, LIVE_TABLE),
        )
        previous_exists, live_exists = cur.fetchone()
        if not previous_exists:
            raise RollbackRefused(f"there is no {PREVIOUS_TABLE} to roll back to.")

        cur.execute(
            sql.SQL("SELECT COUNT(*), COUNT(DISTINCT user_id) FROM {}").format(
                sql.Identifier(PREVIOUS_TABLE)
            )
        )
        previous_rows, previous_users = cur.fetchone()
        if previous_rows == 0:
            raise RollbackRefused(f"{PREVIOUS_TABLE} is empty.")
        if previous_rows != previous_users:
            raise RollbackRefused(
                f"{PREVIOUS_TABLE} has {previous_rows} rows but {previous_users} distinct "
                "user_id (duplicate or NULL user_id)."
            )

        live_rows = None
        if live_exists:
            if columns(cur, PREVIOUS_TABLE) != columns(cur, LIVE_TABLE):
                raise RollbackRefused(
                    f"the columns of {PREVIOUS_TABLE} differ from those of {LIVE_TABLE} "
                    "(names, types or order)."
                )
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(LIVE_TABLE)))
            (live_rows,) = cur.fetchone()
            rename(cur, LIVE_TABLE, TMP_TABLE)

        rename(cur, PREVIOUS_TABLE, LIVE_TABLE)
        if live_exists:
            rename(cur, TMP_TABLE, PREVIOUS_TABLE)

    return previous_rows, live_rows


def rollback_or_exit(connect=connect_to_postgres, lock_timeout=SWAP_LOCK_TIMEOUT):
    """Run the rollback; on a refusal or a lock timeout, explain and exit 1."""
    try:
        served, kept = rollback(connect, lock_timeout)
    except RollbackRefused as exc:
        print(f"Rollback refused: {exc} Nothing was changed.", file=sys.stderr, flush=True)
        sys.exit(1)
    except errors.LockNotAvailable:
        # The driver's own text is not printed.
        print(
            f"Rollback aborted: could not lock {LIVE_TABLE} or {PREVIOUS_TABLE} within "
            f"{lock_timeout} because another session holds it. The transaction was rolled "
            "back and nothing was changed. Rerun the rollback once that session ends.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    if kept is None:
        print(f"Rolled back: {LIVE_TABLE} now serves the {served} rows of {PREVIOUS_TABLE}.")
    else:
        print(
            f"Rolled back: {LIVE_TABLE} now serves the {served} rows of the previous table; "
            f"the replaced {kept} rows are kept as {PREVIOUS_TABLE} (run again to roll forward)."
        )


def main():
    rollback_or_exit()


if __name__ == "__main__":
    main()
