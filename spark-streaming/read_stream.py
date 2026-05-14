from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("KafkaStructuredStream")
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
    from_json(
        col("value").cast("string"),
        event_schema
    ).alias("data")
)

final_df = parsed_df.select("data.*")

query = (
    final_df.writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("append")
    .start()
)

query.awaitTermination()