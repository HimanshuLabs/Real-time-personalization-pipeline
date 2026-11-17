import operator
import os
import signal
import threading
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as abs_,
)
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    expr,
    from_json,
    length,
    lit,
    trim,
    try_to_timestamp,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

PROJECT_ROOT = os.environ.get(
    "PROJECT1_ROOT",
    str(Path(__file__).resolve().parents[1]),
)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

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
    "logout",
]

# Must match COMMERCE_EVENT_TYPES in data-generator/producer.py.
# These events must carry a product and its prices; the others may omit them.
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

COMMERCE_REQUIRED_FIELDS = ["product_id", "product_name", "original_price", "discounted_price"]

# Must match CART_EVENT_TYPES in data-generator/producer.py.
# These events must carry cart_value = discounted_price * quantity.
CART_EVENT_TYPES = [
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "payment_initiated",
    "payment_success",
    "payment_failed",
    "order_placed",
]

# The producer rounds cart_value to 2 decimals.
CART_VALUE_TOLERANCE = 0.01

# event_time may run ahead of the stream's clock by at most this much.
MAX_CLOCK_SKEW = "INTERVAL 1 HOUR"

SUPPORTED_SCHEMA_VERSIONS = ["4.0"]

event_schema = StructType(
    [
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
    ]
)


def between(name, low, high):
    return (col(name) >= low) & (col(name) <= high)


def all_of(conditions):
    return reduce(operator.and_, conditions)


def product_price_rules():
    # Each rule is NULL when a field it reads is NULL.
    return [
        col("quantity") >= 1,
        col("original_price") >= 0,
        col("discounted_price") >= 0,
        col("discounted_price") <= col("original_price"),
        between("discount_percent", 0, 100),
    ]


def parse_events(raw_df):
    # raw_df has the Kafka "value" column (binary or string).
    return raw_df.select(
        col("value").cast("string").alias("raw_value"),
        from_json(col("value").cast("string"), event_schema).alias("data"),
    )


def build_validated(parsed_df):
    # try_to_timestamp yields NULL (not an error) for malformed event_time,
    # so those rows fail the validity check below and go to the DLQ.
    flattened_df = parsed_df.select("raw_value", "data.*").withColumn(
        "event_timestamp", try_to_timestamp(col("event_time"))
    )

    required_fields_valid = (
        col("event_id").isNotNull()
        & col("session_id").isNotNull()
        & col("user_id").isNotNull()
        & col("event_type").isNotNull()
        & col("event_time").isNotNull()
        & col("event_timestamp").isNotNull()
        & col("source").isNotNull()
        & col("schema_version").isNotNull()
    )

    value_rules_valid = (
        (length(trim(col("event_id"))) > 0)
        & col("event_type").isin(EVENT_TYPES)
        & col("schema_version").isin(SUPPORTED_SCHEMA_VERSIONS)
        & (col("user_id") >= 1)
        & between("purchase_probability", 0, 1)
        & between("cart_abandonment_probability", 0, 1)
        & between("fraud_score", 0, 1)
        & (col("engagement_score") >= 0)
        & (col("event_timestamp") <= current_timestamp() + expr(MAX_CLOCK_SKEW))
    )

    # Commerce events: product fields present and every product/price rule
    # holds (a NULL rule fails). Other events: a NULL rule means the field is
    # absent, which is allowed; a present value must still pass.
    product_rules_valid = when(
        col("event_type").isin(COMMERCE_EVENT_TYPES),
        all_of(
            [col(name).isNotNull() for name in COMMERCE_REQUIRED_FIELDS] + product_price_rules()
        ),
    ).otherwise(all_of([coalesce(rule, lit(True)) for rule in product_price_rules()]))

    # Cart events: cart_value present, not negative, and equal to
    # discounted_price * quantity within rounding. Other events: NULL or >= 0.
    cart_value_valid = when(
        col("event_type").isin(CART_EVENT_TYPES),
        col("cart_value").isNotNull()
        & (col("cart_value") >= 0)
        & (
            abs_(col("cart_value") - col("discounted_price") * col("quantity"))
            <= CART_VALUE_TOLERANCE
        ),
    ).otherwise(coalesce(col("cart_value") >= 0, lit(True)))

    # A comparison on a NULL column is NULL, and a NULL filter drops the row from
    # both the valid and the ~valid branch. coalesce(..., False) sends it to the DLQ.
    return flattened_df.withColumn(
        "is_valid",
        coalesce(
            required_fields_valid & value_rules_valid & product_rules_valid & cart_value_valid,
            lit(False),
        ),
    )


def split_valid_invalid(validated_df):
    # Bronze stays lossless: dedup by event_id happens in Silver. No streaming
    # watermark here, because with a stateful step it would drop late events.
    valid_df = validated_df.filter(col("is_valid")).drop("raw_value", "is_valid")

    invalid_df = validated_df.filter(~col("is_valid")).selectExpr(
        "CAST(raw_value AS STRING) AS value"
    )

    return valid_df, invalid_df


stop_requested = threading.Event()


def request_stop(signum, frame):
    # Only set a flag. A Spark/Py4J call from a signal handler re-enters the
    # socket the main thread is blocked on (this also replaces PySpark's own
    # SIGINT handler, which does exactly that via cancelAllJobs()).
    stop_requested.set()


def should_stop():
    # A stop comes either as a signal to this process (request_stop) or as the
    # stop file scripts/run_stream.sh writes when it catches Ctrl+C / SIGTERM,
    # which also covers a signal sent before this process was running.
    stop_file = os.environ.get("STREAM_STOP_FILE")
    if stop_file and os.path.exists(stop_file):
        stop_requested.set()
    return stop_requested.is_set()


def start_bronze_query(valid_df):
    return (
        valid_df.writeStream.format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .queryName("bronze_user_events_writer")
        .start()
    )


def start_dlq_query(invalid_df):
    return (
        invalid_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", "user_events_dlq")
        .option("checkpointLocation", DLQ_CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .queryName("invalid_events_dlq_stream")
        .start()
    )


def install_stop_handlers():
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)


def main():
    # First, so a Ctrl+C or SIGTERM during start-up (Spark and the queries take
    # several seconds to start) is noticed instead of ignored.
    install_stop_handlers()

    # Stopped before this process started: don't start Spark at all.
    if not should_stop():
        run_stream()

    print("[stream] stopped cleanly", flush=True)


def run_stream():
    spark = (
        SparkSession.builder.appName("KafkaStructuredStreamWithDLQ")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    # Creating the SparkContext installs PySpark's own SIGINT handler, which calls
    # cancelAllJobs() from inside the handler; put ours back. A signal that
    # arrived while Spark started has already set stop_requested.
    install_stop_handlers()

    queries = []
    try:
        spark.sparkContext.setLogLevel("WARN")

        if not should_stop():
            raw_df = (
                spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", "user_events")
                # Only used when there is no checkpoint yet: read the whole topic so
                # events produced before the first run aren't skipped. Re-reads are
                # safe because Silver drops duplicate event_ids.
                .option("startingOffsets", "earliest")
                .load()
            )

            valid_df, invalid_df = split_valid_invalid(build_validated(parse_events(raw_df)))

            # Each query starts only if no stop was requested meanwhile.
            for start, df in ((start_bronze_query, valid_df), (start_dlq_query, invalid_df)):
                if should_stop():
                    break
                queries.append(start(df))

        # Returns True once any query has terminated; raises if it failed.
        while not should_stop():
            if spark.streams.awaitAnyTermination(timeout=1):
                break
    finally:
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
