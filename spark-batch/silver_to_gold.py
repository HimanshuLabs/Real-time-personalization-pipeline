import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

# Must match COMMERCE_EVENT_TYPES in data-generator/producer.py.
# Price features are computed over these events only.
COMMERCE_EVENT_TYPES = [
    "product_view",
    "image_zoom",
    "wishlist_add",
    "wishlist_remove",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "payment_initiated",
    "payment_success",
    "payment_failed",
    "order_placed",
    "order_cancelled",
    "return_requested",
    "review_submitted",
    "rating_given",
    "recommendation_impression",
    "recommendation_click",
    "search_result_click",
]

# data/ and logs/ resolve from PROJECT1_ROOT (default: the repository root), like
# spark-streaming/read_stream.py, so the script works from any directory.
PROJECT_ROOT = os.environ.get("PROJECT1_ROOT", str(Path(__file__).resolve().parents[1]))


def project_path(relative_path):
    return os.path.join(PROJECT_ROOT, relative_path)


SILVER_PATH = "data/silver/user_events"
GOLD_PATH = "data/gold/user_features"

commerce_event_types_sql = ", ".join(f"'{event_type}'" for event_type in COMMERCE_EVENT_TYPES)

# Averages and sums run on DECIMAL so the result does not depend on the order
# Spark adds the rows; each is rounded, then cast back to DOUBLE.
GOLD_FEATURES_SQL = f"""

    SELECT

        user_id,

        MAX_BY(user_name, event_timestamp) AS user_name,

        MAX_BY(membership_tier, event_timestamp) AS membership_tier,

        COUNT(*) AS total_events,

        SUM(
            CASE
                WHEN event_type = 'home_page_view'
                THEN 1
                ELSE 0
            END
        ) AS page_view_count,

        SUM(
            CASE
                WHEN event_type = 'product_view'
                THEN 1
                ELSE 0
            END
        ) AS product_view_count,

        SUM(
            CASE
                WHEN event_type = 'add_to_cart'
                THEN 1
                ELSE 0
            END
        ) AS add_to_cart_count,

        SUM(
            CASE
                WHEN event_type = 'payment_success'
                THEN 1
                ELSE 0
            END
        ) AS purchase_count,

        SUM(
            CASE
                WHEN event_type = 'search'
                THEN 1
                ELSE 0
            END
        ) AS search_count,

        -- Commerce events only; NULL for a user without any.
        CAST(
            ROUND(
                AVG(
                    CASE
                        WHEN event_type IN ({commerce_event_types_sql})
                        THEN CAST(discounted_price AS DECIMAL(18, 4))
                    END
                ),
                2
            ) AS DOUBLE
        ) AS avg_event_price,

        ROUND(
            MAX(
                CASE
                    WHEN event_type IN ({commerce_event_types_sql})
                    THEN discounted_price
                END
            ),
            2
        ) AS max_event_price,

        CAST(
            ROUND(
                AVG(CAST(engagement_score AS DECIMAL(18, 4))),
                2
            ) AS DOUBLE
        ) AS avg_engagement_score,

        CAST(
            ROUND(
                AVG(CAST(purchase_probability AS DECIMAL(18, 4))),
                4
            ) AS DOUBLE
        ) AS avg_purchase_probability,

        MAX(event_timestamp) AS last_event_timestamp,

        COUNT(
            DISTINCT product_name
        ) AS unique_products_interacted,

        CAST(
            ROUND(
                COALESCE(
                    SUM(
                        CASE
                            WHEN event_type = 'payment_success'
                            THEN CAST(cart_value AS DECIMAL(18, 2))
                        END
                    ),
                    0
                ),
                2
            ) AS DOUBLE
        ) AS total_spend,

        COUNT(
            DISTINCT session_id
        ) AS session_count,

        :features_computed_at AS features_computed_at,

        CAST(
            ROUND(
                (unix_micros(:features_computed_at) - unix_micros(MAX(event_timestamp)))
                / 3600000000.0,
                2
            ) AS DOUBLE
        ) AS recency_hours

    FROM silver_user_events

    GROUP BY
        user_id

"""


def main():
    spark = (
        SparkSession.builder.appName("SilverToGoldUserFeatures")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("WARN")

        silver_path = Path(project_path(SILVER_PATH))
        if not silver_path.exists():
            raise FileNotFoundError(
                f"Silver path does not exist. Run spark-batch/bronze_to_silver.py first: {silver_path}"
            )

        spark.read.parquet(str(silver_path)).createOrReplaceTempView("silver_user_events")

        # One timestamp for the whole run; recency_hours is measured against it.
        features_computed_at = datetime.now(timezone.utc)

        gold_user_features = spark.sql(
            GOLD_FEATURES_SQL, args={"features_computed_at": features_computed_at}
        )

        gold_user_features.write.mode("overwrite").parquet(project_path(GOLD_PATH))

        # Print the Gold actually written: each action on gold_user_features
        # would run the whole aggregation again.
        written_gold = spark.read.parquet(project_path(GOLD_PATH))

        print("Gold user_features written successfully")
        print(f"Features computed at: {features_computed_at.isoformat()}")
        print(f"Gold user feature count: {written_gold.count()}")

        written_gold.show(truncate=False)
        gold_user_features.printSchema()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
