import os
import signal
import threading
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    from_json,
    length,
    lit,
    trim,
    try_to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)

PROJECT_ROOT = os.environ.get(
    "PROJECT1_ROOT",
    str(Path(__file__).resolve().parents[1]),
)

BRONZE_PATH = os.path.join(PROJECT_ROOT, "data/bronze/user_events")
BRONZE_CHECKPOINT = os.path.join(PROJECT_ROOT, "checkpoints/bronze_user_events")
DLQ_CHECKPOINT = os.path.join(PROJECT_ROOT, "checkpoints/dlq")

# Must match EVENT_TYPES in data-generator/producer.py.
EVENT_TYPES = [
    "home_page_view",
    "search",
    "search_result_click",
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
    "ad_impression",
    "ad_click",
    "coupon_applied",
    "delivery_tracking_view",
    "login",
    "logout"
]

SUPPORTED_SCHEMA_VERSIONS = ["4.0"]

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
StructField("email", StringType(), True),
StructField("gender", StringType(), True),
StructField("age", IntegerType(), True),
StructField("membership_tier", StringType(), True),
StructField("loyalty_points", IntegerType(), True),
StructField("preferred_language", StringType(), True),
StructField("home_city", StringType(), True),
StructField("home_state", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_journey_stage", StringType(), True),
    StructField("user_segment", StringType(), True),
    StructField("is_prime_user", BooleanType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("original_price", DoubleType(), True),
    StructField("discount_percent", DoubleType(), True),
    StructField("discounted_price", DoubleType(), True),
    StructField("cart_value", DoubleType(), True),
    StructField("inventory_remaining", IntegerType(), True),
    StructField("search_query", StringType(), True),
    StructField("time_on_page_sec", DoubleType(), True),
    StructField("scroll_depth_percent", IntegerType(), True),
    StructField("hover_duration_ms", IntegerType(), True),
    StructField("session_duration_sec", IntegerType(), True),
    StructField("items_viewed_in_session", IntegerType(), True),
    StructField("repeat_product_view_count", IntegerType(), True),
    StructField("time_since_last_event_ms", IntegerType(), True),
    StructField("recommendation_rank", IntegerType(), True),
    StructField("recommendation_clicked", BooleanType(), True),
    StructField("recommendation_algorithm", StringType(), True),
    StructField("click_position", IntegerType(), True),
    StructField("engagement_score", DoubleType(), True),
    StructField("purchase_probability", DoubleType(), True),
    StructField("cart_abandonment_probability", DoubleType(), True),
    StructField("ab_test_group", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("operating_system", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("network_type", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("traffic_source", StringType(), True),
    StructField("campaign_id", IntegerType(), True),
    StructField("api_latency_ms", IntegerType(), True),
    StructField("page_load_time_ms", IntegerType(), True),
    StructField("fraud_score", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("schema_version", StringType(), True),
    StructField("source", StringType(), True),
])


def between(name, low, high):
    return (col(name) >= low) & (col(name) <= high)


def parse_events(raw_df):
    # raw_df has the Kafka "value" column (binary or string).
    return raw_df.select(
        col("value").cast("string").alias("raw_value"),
        from_json(
            col("value").cast("string"),
            event_schema
        ).alias("data")
    )


def build_validated(parsed_df):
    # try_to_timestamp yields NULL (not an error) for malformed event_time,
    # so those rows fail the validity check below and go to the DLQ.
    flattened_df = (
        parsed_df
        .select(
            "raw_value",
            "data.*"
        )
        .withColumn(
            "event_timestamp",
            try_to_timestamp(col("event_time"))
        )
    )

    required_fields_valid = (
        col("event_id").isNotNull() &
        col("session_id").isNotNull() &
        col("user_id").isNotNull() &
        col("event_type").isNotNull() &
        col("product_name").isNotNull() &
        col("discounted_price").isNotNull() &
        col("event_time").isNotNull() &
        col("event_timestamp").isNotNull() &
        col("source").isNotNull() &
        col("schema_version").isNotNull()
    )

    value_rules_valid = (
        (length(trim(col("event_id"))) > 0) &
        col("event_type").isin(EVENT_TYPES) &
        col("schema_version").isin(SUPPORTED_SCHEMA_VERSIONS) &
        (col("user_id") >= 1) &
        (col("quantity") >= 1) &
        (col("original_price") >= 0) &
        (col("discounted_price") >= 0) &
        (col("discounted_price") <= col("original_price")) &
        between("discount_percent", 0, 100) &
        between("purchase_probability", 0, 1) &
        between("cart_abandonment_probability", 0, 1) &
        between("fraud_score", 0, 1)
    )

    # A comparison on a NULL column is NULL, and a NULL filter drops the row from
    # both the valid and the ~valid branch. coalesce(..., False) sends it to the DLQ.
    return flattened_df.withColumn(
        "is_valid",
        coalesce(required_fields_valid & value_rules_valid, lit(False))
    )


def split_valid_invalid(validated_df):
    # Bronze stays lossless: dedup by event_id happens in Silver. No streaming
    # watermark here, because with a stateful step it would drop late events.
    valid_df = (
        validated_df
        .filter(col("is_valid"))
        .drop("raw_value", "is_valid")
    )

    invalid_df = (
        validated_df
        .filter(~col("is_valid"))
        .selectExpr("CAST(raw_value AS STRING) AS value")
    )

    return valid_df, invalid_df


stop_requested = threading.Event()


def request_stop(signum, frame):
    # Only set a flag. A Spark/Py4J call from a signal handler re-enters the
    # socket the main thread is blocked on (this also replaces PySpark's own
    # SIGINT handler, which does exactly that via cancelAllJobs()).
    stop_requested.set()


def main():

    spark = (
        SparkSession.builder
        .appName("KafkaStructuredStreamWithDLQ")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "user_events")
        # Only used when there is no checkpoint yet: read the whole topic so
        # events produced before the first run aren't skipped. Re-reads are
        # safe because Silver drops duplicate event_ids.
        .option("startingOffsets", "earliest")
        .load()
    )

    valid_df, invalid_df = split_valid_invalid(
        build_validated(parse_events(raw_df))
    )

    valid_query = (
        valid_df.writeStream
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .queryName("bronze_user_events_writer")
        .start()
    )

    invalid_query = (
        invalid_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "user_events_dlq")
        .option("checkpointLocation", DLQ_CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .queryName("invalid_events_dlq_stream")
        .start()
    )

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    try:
        # Returns True once any query has terminated; raises if it failed.
        while not stop_requested.is_set():
            if spark.streams.awaitAnyTermination(timeout=1):
                break
    finally:
        for query in (valid_query, invalid_query):
            query.stop()
        spark.stop()

    print("[stream] stopped cleanly", flush=True)


if __name__ == "__main__":
    main()
