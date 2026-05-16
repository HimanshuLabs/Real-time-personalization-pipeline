from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SilverToGoldUserFeatures")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

silver_df = spark.read.parquet(
    "data/silver/user_events"
)

silver_df.createOrReplaceTempView(
    "silver_user_events"
)

gold_user_features = spark.sql("""

    SELECT

        user_id,

        user_name,

        membership_tier,

        COUNT(*) AS total_events,

        SUM(
            CASE
                WHEN event_type = 'page_view'
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
                WHEN event_type = 'purchase'
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

        ROUND(
            AVG(discounted_price),
            2
        ) AS avg_event_price,

        ROUND(
            MAX(discounted_price),
            2
        ) AS max_event_price,

        ROUND(
            AVG(engagement_score),
            2
        ) AS avg_engagement_score,

        ROUND(
            AVG(purchase_probability),
            4
        ) AS avg_purchase_probability,

        MAX(event_timestamp) AS last_event_timestamp,

        COUNT(
            DISTINCT product_name
        ) AS unique_products_interacted

    FROM silver_user_events

    GROUP BY
        user_id,
        user_name,
        membership_tier

""")

gold_user_features.write.mode(
    "overwrite"
).parquet(
    "data/gold/user_features"
)

print("Gold user_features written successfully")

print(
    f"Gold user feature count: {gold_user_features.count()}"
)

gold_user_features.show(truncate=False)

gold_user_features.printSchema()
