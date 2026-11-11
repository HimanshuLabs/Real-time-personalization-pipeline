from datetime import datetime, timezone

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
    "search_result_click"
]

spark = (
    SparkSession.builder
    .appName("SilverToGoldUserFeatures")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

silver_df = spark.read.parquet(
    "data/silver/user_events"
)

silver_df.createOrReplaceTempView(
    "silver_user_events"
)

# One timestamp for the whole run; recency_hours is measured against it.
features_computed_at = datetime.now(timezone.utc)

commerce_event_types_sql = ", ".join(f"'{event_type}'" for event_type in COMMERCE_EVENT_TYPES)

# Averages and sums run on DECIMAL so the result does not depend on the order
# Spark adds the rows; each is rounded, then cast back to DOUBLE.
gold_user_features = spark.sql(f"""

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

""", args={"features_computed_at": features_computed_at})

gold_user_features.write.mode(
    "overwrite"
).parquet(
    "data/gold/user_features"
)

print("Gold user_features written successfully")

print(f"Features computed at: {features_computed_at.isoformat()}")

print(
    f"Gold user feature count: {gold_user_features.count()}"
)

gold_user_features.show(truncate=False)

gold_user_features.printSchema()
