from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("KafkaStructuredStreamWithDLQ")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", StringType(), True),
    StructField("source", StringType(), True),
    StructField("schema_version", StringType(), True)
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

valid_df = (
    parsed_df
    .filter(col("data").isNotNull())
    .select("data.*")
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"))
    )
    .withWatermark("event_timestamp", "10 minutes")
)

flattened_df = parsed_df.select(
    "raw_value",
    "data.*"
)

valid_df = (
    flattened_df
    .filter(
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("event_type").isNotNull() &
        col("product").isNotNull() &
        col("price").isNotNull() &
        col("event_time").isNotNull() &
        col("source").isNotNull() &
        col("schema_version").isNotNull()
    )
    .drop("raw_value")
    .withColumn("event_timestamp", to_timestamp(col("event_time")))
    .withWatermark("event_timestamp", "10 minutes")
)

invalid_df = (
    flattened_df
    .filter(
        col("event_id").isNull() |
        col("user_id").isNull() |
        col("event_type").isNull() |
        col("product").isNull() |
        col("price").isNull() |
        col("event_time").isNull() |
        col("source").isNull() |
        col("schema_version").isNull()
    )
    .selectExpr("CAST(raw_value AS STRING) AS value")
)

valid_query = (
    valid_df.writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("append")
    .queryName("valid_events_stream")
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