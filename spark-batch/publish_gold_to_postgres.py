from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PublishGoldUserFeaturesToPostgres")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

gold_df = spark.read.parquet(
    "data/gold/user_features"
)

# Improve JDBC parallelism

gold_df = gold_df.repartition(4)

postgres_url = (
    "jdbc:postgresql://localhost:5433/personalization_db"
)

postgres_properties = {
    "user": "de_user",
    "password": "de_password",
    "driver": "org.postgresql.Driver"
}

(
    gold_df.write
    .mode("overwrite")
    .option("truncate", "true")
    .jdbc(
        url=postgres_url,
        table="user_features",
        properties=postgres_properties
    )
)

print(
    "Gold user features published to PostgreSQL successfully."
)

print(
    f"Published record count: {gold_df.count()}"
)

gold_df.show(
    10,
    truncate=False
)