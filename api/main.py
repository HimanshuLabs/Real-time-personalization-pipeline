import logging
import os

from fastapi import FastAPI, HTTPException
import psycopg
from psycopg.rows import dict_row
from prometheus_fastapi_instrumentator import Instrumentator

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Real-Time Personalization Feature API",
    description="Serves user-level personalization features from PostgreSQL",
    version="1.0.0"
)

# Prometheus metrics endpoint: /metrics
Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)


DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5433")),
    "dbname": os.environ.get("POSTGRES_DB", "personalization_db"),
    "user": os.environ.get("POSTGRES_USER", "de_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", "de_password")
}


def get_db_connection():
    return psycopg.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        row_factory=dict_row
    )


@app.get("/")
def root():
    return {
        "message": "Feature serving API is running",
        "available_endpoint": "/features/{user_id}"
    }


@app.get("/health")
def health_check():
    # Healthy only if the DB connects and user_features exists with rows.
    # Error details are logged server-side, never returned to the client.
    reason = None

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT to_regclass('user_features') IS NOT NULL AS table_exists"
                )

                if not cursor.fetchone()["table_exists"]:
                    reason = "user_features missing"
                else:
                    cursor.execute(
                        "SELECT EXISTS (SELECT 1 FROM user_features) AS has_rows"
                    )

                    if not cursor.fetchone()["has_rows"]:
                        reason = "user_features empty"

    except Exception:
        logger.exception("Health check failed: PostgreSQL unavailable")
        reason = "database unavailable"

    if reason is not None:
        raise HTTPException(
            status_code=503,
            detail=reason
        )

    return {
        "status": "healthy",
        "postgres": "connected"
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
                        avg_purchase_probability
                    FROM user_features
                    WHERE user_id = %s
                    """,
                    (user_id,)
                )

                result = cursor.fetchone()

        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"No features found for user_id={user_id}"
            )

        return {
            "user_id": user_id,
            "features": result
        }

    except HTTPException:
        raise

    except Exception:
        logger.exception("Feature lookup failed for user_id=%s", user_id)
        raise HTTPException(
            status_code=500,
            detail="Feature lookup failed"
        )
