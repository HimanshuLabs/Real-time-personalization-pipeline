import os
import time
from prometheus_client import Gauge, start_http_server
import psycopg


EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "8001"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))

DB_HOST = os.getenv("POSTGRES_HOST", "host.docker.internal")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
DB_NAME = os.getenv("POSTGRES_DB", "personalization_db")
DB_USER = os.getenv("POSTGRES_USER", "de_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "de_password")


exporter_up = Gauge(
    "project1_feature_exporter_up",
    "Whether the Project 1 feature freshness exporter can query PostgreSQL. 1 = up, 0 = down.",
)

user_features_rows = Gauge(
    "project1_user_features_rows",
    "Total rows currently available in PostgreSQL user_features table.",
)

latest_feature_event_timestamp_unix = Gauge(
    "project1_latest_feature_event_timestamp_unix",
    "Unix timestamp of the newest last_event_timestamp in PostgreSQL user_features table.",
)

feature_freshness_seconds = Gauge(
    "project1_feature_freshness_seconds",
    "Age in seconds of the newest event timestamp available in PostgreSQL user_features table.",
)


def collect_metrics() -> None:
    try:
        with psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=5,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*)::bigint AS row_count,
                        EXTRACT(EPOCH FROM MAX(last_event_timestamp))::double precision AS latest_event_epoch
                    FROM user_features;
                    """
                )
                row_count, latest_event_epoch = cur.fetchone()

        user_features_rows.set(row_count or 0)

        if latest_event_epoch is not None:
            latest_feature_event_timestamp_unix.set(float(latest_event_epoch))
            feature_freshness_seconds.set(max(time.time() - float(latest_event_epoch), 0))
        else:
            latest_feature_event_timestamp_unix.set(0)
            feature_freshness_seconds.set(-1)

        exporter_up.set(1)

    except Exception as exc:
        exporter_up.set(0)
        print(f"[feature-exporter] PostgreSQL scrape failed: {exc}", flush=True)


def main() -> None:
    start_http_server(EXPORTER_PORT)
    print(f"[feature-exporter] Listening on :{EXPORTER_PORT}", flush=True)

    while True:
        collect_metrics()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
