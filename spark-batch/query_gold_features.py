from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("QueryGoldUserFeatures")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

gold_df = spark.read.parquet(
    "data/gold/user_features"
)

gold_df.createOrReplaceTempView(
    "gold_user_features"
)

print("Top users by total events:")

spark.sql("""

    SELECT

        user_id,
        user_name,
        membership_tier,

        total_events,
        purchase_count,
        add_to_cart_count,
        unique_products_interacted,
        last_event_timestamp

    FROM gold_user_features

    ORDER BY total_events DESC

    LIMIT 20

""").show(truncate=False)

print("Users with strongest purchase intent:")

spark.sql("""

    SELECT

        user_id,
        user_name,
        membership_tier,

        total_events,
        product_view_count,
        add_to_cart_count,
        purchase_count,

        ROUND(
            CAST(add_to_cart_count AS DOUBLE)
            / total_events,
            2
        ) AS cart_ratio,

        ROUND(
            CAST(purchase_count AS DOUBLE)
            / total_events,
            2
        ) AS purchase_ratio

    FROM gold_user_features

    WHERE total_events > 0

    ORDER BY
        cart_ratio DESC,
        purchase_ratio DESC

    LIMIT 20

""").show(truncate=False)

print("High-value users by average event price:")

spark.sql("""

    SELECT

        user_id,
        user_name,
        membership_tier,

        total_events,
        avg_event_price,
        max_event_price,
        unique_products_interacted

    FROM gold_user_features

    ORDER BY avg_event_price DESC

    LIMIT 20

""").show(truncate=False)
