from fastapi import FastAPI, HTTPException
import psycopg
from psycopg.rows import dict_row
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(
    title="Real-Time Personalization Feature API",
    description="Serves user-level personalization features from PostgreSQL",
    version="1.0.0"
)

# Prometheus metrics endpoint: /metrics
Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)


DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "personalization_db",
    "user": "de_user",
    "password": "de_password"
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
    try:
        with get_db_connection():
            return {
                "status": "healthy",
                "postgres": "connected"
            }
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"PostgreSQL connection failed: {str(error)}"
        )


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
                        unique_products_interacted
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

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Feature lookup failed: {str(error)}"
        )
