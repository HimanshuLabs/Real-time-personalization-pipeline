from pyspark.sql import SparkSession


POSTGRES_URL = "jdbc:postgresql://localhost:5433/personalization_db"
POSTGRES_TABLE = "user_features"
POSTGRES_USER = "de_user"
POSTGRES_PASSWORD = "de_password"


spark = (
    SparkSession.builder
    .appName("PublishGoldUserFeaturesToPostgres")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

gold_df = spark.read.parquet("data/gold/user_features")

gold_count = gold_df.count()
print(f"Gold user_features row count before PostgreSQL publish: {gold_count}")

if gold_count == 0:
    raise RuntimeError("Gold user_features table is empty. Refusing to publish 0 rows.")

(
    gold_df.write
    .format("jdbc")
    .option("url", POSTGRES_URL)
    .option("dbtable", POSTGRES_TABLE)
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

print("Gold user_features published to PostgreSQL successfully.")

spark.stop()
