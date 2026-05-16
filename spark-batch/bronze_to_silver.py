from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = (
    SparkSession.builder
    .appName("BronzeToSilverUserEvents")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_df = spark.read.parquet("data/bronze/user_events")

silver_df = (
    bronze_df
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"))
    )

    .dropDuplicates(["event_id"])

    .filter(col("event_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("event_type").isNotNull())
    .filter(col("product_name").isNotNull())
    .filter(col("discounted_price").isNotNull())
    .filter(col("event_timestamp").isNotNull())

    .select(
        "event_id",
        "user_id",
        "user_name",
        "membership_tier",
        "event_type",
        "product_name",
        "category",
        "discounted_price",
        "engagement_score",
        "purchase_probability",
        "recommendation_algorithm",
        "event_timestamp",
        "source",
        "schema_version"
    )
)

silver_df.write.mode("overwrite").parquet(
    "data/silver/user_events"
)

print("Silver user_events written successfully")

print(f"Silver record count: {silver_df.count()}")

silver_df.show(truncate=False)

silver_df.printSchema()
