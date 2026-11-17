import logging
import os
from typing import Annotated

import psycopg
from fastapi import FastAPI, HTTPException, Query
from prometheus_fastapi_instrumentator import Instrumentator
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Real-Time Personalization Feature API",
    description="Serves user-level personalization features from PostgreSQL",
    version="1.0.0",
)

# Prometheus metrics endpoint: /metrics. Its own scrapes are left out of every
# HTTP metric, so request rates and latencies describe real API traffic only.
Instrumentator(excluded_handlers=["/metrics"]).instrument(app).expose(
    app, endpoint="/metrics", include_in_schema=False
)


DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5433")),
    "dbname": os.environ.get("POSTGRES_DB", "personalization_db"),
    "user": os.environ.get("POSTGRES_USER", "de_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", "de_password"),
}

# Seconds to wait for PostgreSQL to accept a connection, so an unreachable
# database fails a request quickly instead of holding a worker thread.
DB_CONNECT_TIMEOUT_SECONDS = 3

PAGE_LIMIT_DEFAULT = 100
PAGE_LIMIT_MAX = 1000

# The columns of /features/{user_id} plus features_computed_at; never
# user_name or membership_tier.
FEATURE_PAGE_QUERY = """
    SELECT
        user_id,
        total_events,
        page_view_count,
        product_view_count,
        add_to_cart_count,
        purchase_count,
        search_count,
        avg_event_price,
        max_event_price,
        last_event_timestamp,
        unique_products_interacted,
        avg_engagement_score,
        avg_purchase_probability,
        total_spend,
        session_count,
        recency_hours,
        features_computed_at
    FROM user_features
    WHERE user_id > %s
    ORDER BY user_id
    LIMIT %s
"""


def get_db_connection():
    return psycopg.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
        row_factory=dict_row,
    )


@app.get("/")
def root():
    return {
        "message": "Feature serving API is running",
        "available_endpoint": "/features/{user_id}",
    }


@app.get("/health")
def health_check():
    # Healthy only if the DB connects and user_features exists with rows.
    # Error details are logged server-side, never returned to the client.
    reason = None

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT to_regclass('user_features') IS NOT NULL AS table_exists")

                if not cursor.fetchone()["table_exists"]:
                    reason = "user_features missing"
                else:
                    cursor.execute("SELECT EXISTS (SELECT 1 FROM user_features) AS has_rows")

                    if not cursor.fetchone()["has_rows"]:
                        reason = "user_features empty"

    except Exception:
        logger.exception("Health check failed: PostgreSQL unavailable")
        reason = "database unavailable"

    if reason is not None:
        raise HTTPException(status_code=503, detail=reason)

    return {"status": "healthy", "postgres": "connected"}


@app.get("/features")
def list_user_features(
    after_user_id: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=PAGE_LIMIT_MAX)] = PAGE_LIMIT_DEFAULT,
):
    # Keyset pagination by user_id: pass next_after_user_id back as
    # after_user_id. One extra row is fetched to tell whether another page
    # exists, so next_after_user_id is null on the last page.
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(FEATURE_PAGE_QUERY, (after_user_id, limit + 1))
                rows = cursor.fetchall()

    except Exception:
        logger.exception("Feature page lookup failed after user_id=%s", after_user_id)
        raise HTTPException(status_code=500, detail="Feature lookup failed")

    items = rows[:limit]
    has_more = len(rows) > limit

    return {
        "items": items,
        "count": len(items),
        "next_after_user_id": items[-1]["user_id"] if has_more else None,
    }


@app.get("/features/{user_id}")
def get_user_features(user_id: int):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        user_id,
                        total_events,
                        page_view_count,
                        product_view_count,
                        add_to_cart_count,
                        purchase_count,
                        search_count,
                        avg_event_price,
                        max_event_price,
                        last_event_timestamp,
                        unique_products_interacted,
                        avg_engagement_score,
                        avg_purchase_probability,
                        total_spend,
                        session_count,
                        recency_hours
                    FROM user_features
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )

                result = cursor.fetchone()

        if result is None:
            raise HTTPException(status_code=404, detail=f"No features found for user_id={user_id}")

        return {"user_id": user_id, "features": result}

    except HTTPException:
        raise

    except Exception:
        logger.exception("Feature lookup failed for user_id=%s", user_id)
        raise HTTPException(status_code=500, detail="Feature lookup failed")
