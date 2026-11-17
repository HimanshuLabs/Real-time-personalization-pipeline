import os
import sys
from pathlib import Path

import psycopg
from psycopg import errors, sql
from pyspark.sql import SparkSession

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "personalization_db")
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_TABLE = "user_features"
POSTGRES_STAGING_TABLE = "user_features_staging"
POSTGRES_USER = os.environ.get("POSTGRES_USER", "de_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "de_password")

# data/ and logs/ resolve from PROJECT1_ROOT (default: the repository root), like
# spark-streaming/read_stream.py, so the script works from any directory.
PROJECT_ROOT = os.environ.get("PROJECT1_ROOT", str(Path(__file__).resolve().parents[1]))


def project_path(relative_path):
    return os.path.join(PROJECT_ROOT, relative_path)


# Longest the swap waits for a lock. DROP TABLE needs an exclusive lock, so
# without a limit one open reader would block the swap, and every API read
# queued behind it, indefinitely.
SWAP_LOCK_TIMEOUT = "10s"


def connect_to_postgres():
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def swap_staging_into_live(connect=connect_to_postgres, lock_timeout=SWAP_LOCK_TIMEOUT):
    """Validate staging and atomically replace the live table with it.

    Runs in one transaction: any failure, including a lock wait longer than
    lock_timeout, rolls back and leaves the live table untouched.
    """
    staging = sql.Identifier(POSTGRES_STAGING_TABLE)
    live = sql.Identifier(POSTGRES_TABLE)
    staging_pkey = sql.Identifier(f"{POSTGRES_STAGING_TABLE}_pkey")
    live_pkey = sql.Identifier(f"{POSTGRES_TABLE}_pkey")

    with connect() as conn, conn.transaction(), conn.cursor() as cur:
        # SET takes no bind parameters; the value is a quoted literal.
        cur.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(lock_timeout)))

        cur.execute(sql.SQL("SELECT COUNT(*), COUNT(DISTINCT user_id) FROM {}").format(staging))
        row_count, distinct_users = cur.fetchone()
        print(
            f"Staging {POSTGRES_STAGING_TABLE}: {row_count} rows, "
            f"{distinct_users} distinct user_id"
        )

        if row_count == 0:
            raise RuntimeError("Staging table is empty. Refusing to swap.")

        if row_count != distinct_users:
            raise RuntimeError(
                f"Staging has {row_count} rows but {distinct_users} distinct user_id "
                "(duplicate or NULL user_id). Refusing to swap."
            )

        cur.execute(
            sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY (user_id)").format(
                staging, staging_pkey
            )
        )
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(live))
        cur.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(staging, live))
        cur.execute(
            sql.SQL("ALTER TABLE {} RENAME CONSTRAINT {} TO {}").format(
                live, staging_pkey, live_pkey
            )
        )


def main():
    spark = (
        SparkSession.builder.appName("PublishGoldUserFeaturesToPostgres")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("WARN")

        gold_df = spark.read.parquet(project_path("data/gold/user_features"))

        gold_count = gold_df.count()
        print(f"Gold user_features row count before PostgreSQL publish: {gold_count}")

        if gold_count == 0:
            raise RuntimeError("Gold user_features table is empty. Refusing to publish 0 rows.")

        (
            gold_df.write.format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", POSTGRES_STAGING_TABLE)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

        print(f"Gold user_features written to {POSTGRES_STAGING_TABLE}.")
    finally:
        # Spark is done before the swap, which only needs PostgreSQL.
        spark.stop()

    swap_or_exit()

    print("Gold user_features published to PostgreSQL successfully.")


def swap_or_exit(connect=connect_to_postgres, lock_timeout=SWAP_LOCK_TIMEOUT):
    """Run the swap; on a lock timeout, explain and exit 1 instead of a traceback."""
    try:
        swap_staging_into_live(connect, lock_timeout)
    except errors.LockNotAvailable:
        # The driver's own text is not printed.
        print(
            f"Publish aborted: could not lock {POSTGRES_TABLE} within {lock_timeout} "
            "because another session holds it. The transaction was rolled back and the "
            f"live {POSTGRES_TABLE} table is unchanged. Rerun the publish once that "
            "session ends.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
