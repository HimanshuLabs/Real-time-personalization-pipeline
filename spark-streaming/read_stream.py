from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)

spark = (
    SparkSession.builder
    .appName("KafkaStructuredStreamWithDLQ")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

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

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "user_events")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = raw_df.select(
    col("value").cast("string").alias("raw_value"),
    from_json(
        col("value").cast("string"),
        event_schema
    ).alias("data")
)

flattened_df = parsed_df.select(
    "raw_value",
    "data.*"
)

required_fields_valid = (
    col("event_id").isNotNull() &
    col("session_id").isNotNull() &
    col("user_id").isNotNull() &
    col("event_type").isNotNull() &
    col("product_name").isNotNull() &
    col("discounted_price").isNotNull() &
    col("event_time").isNotNull() &
    col("source").isNotNull() &
    col("schema_version").isNotNull()
)

valid_df = (
    flattened_df
    .filter(required_fields_valid)
    .drop("raw_value")
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"))
    )
    .withWatermark("event_timestamp", "10 minutes")
)

invalid_df = (
    flattened_df
    .filter(~required_fields_valid)
    .selectExpr("CAST(raw_value AS STRING) AS value")
)

valid_query = (
    valid_df.writeStream
    .format("parquet")
    .option("path", "data/bronze/user_events")
    .option("checkpointLocation", "checkpoints/bronze_user_events")
    .outputMode("append")
    .queryName("bronze_user_events_writer")
    .start()
)

invalid_query = (
    invalid_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "user_events_dlq")
    .option("checkpointLocation", "checkpoints/dlq")
    .outputMode("append")
    .queryName("invalid_events_dlq_stream")
    .start()
)

spark.streams.awaitAnyTermination()