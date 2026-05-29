from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp


BRONZE_PATH = "data/bronze/user_events"
USERS_DIM_PATH = "data/dim/users"
PRODUCTS_DIM_PATH = "data/dim/products"
SILVER_PATH = "data/silver/user_events"
REPORT_PATH = "logs/silver_enrichment_report.json"


def require_path(path: str, message: str) -> None:
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"{message}: {path}")


spark = (
    SparkSession.builder
    .appName("BronzeToSilverEnrichedEvents")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

require_path(BRONZE_PATH, "Bronze path does not exist. Run the Bronze ingestion first")
require_path(USERS_DIM_PATH, "User dimension does not exist. Run reference-data/create_reference_data.py first")
require_path(PRODUCTS_DIM_PATH, "Product dimension does not exist. Run reference-data/create_reference_data.py first")

bronze_df = spark.read.parquet(BRONZE_PATH)
users_dim = spark.read.parquet(USERS_DIM_PATH)
products_dim = spark.read.parquet(PRODUCTS_DIM_PATH)

df = bronze_df

# ---------------------------------------------------------------------
# NORMALIZE BASIC EVENT COLUMNS
# ---------------------------------------------------------------------

if "user_id" not in df.columns:
    raise ValueError("Bronze data is missing required column: user_id")

if "event_type" not in df.columns:
    raise ValueError("Bronze data is missing required column: event_type")

df = df.withColumn("user_id", col("user_id").cast("int"))

if "event_timestamp" not in df.columns:
    if "event_time" in df.columns:
        df = df.withColumn("event_timestamp", to_timestamp(col("event_time")))
    elif "timestamp" in df.columns:
        df = df.withColumn("event_timestamp", to_timestamp(col("timestamp")))
    else:
        raise ValueError("Bronze data is missing event timestamp column")

if "price" in df.columns:
    df = df.withColumn("price", col("price").cast("double"))
else:
    df = df.withColumn("price", lit(None).cast("double"))

if "product" not in df.columns and "product_id" not in df.columns:
    df = df.withColumn("product", lit(None).cast("string"))


# ---------------------------------------------------------------------
# QUALITY FILTERS + DEDUPLICATION
# ---------------------------------------------------------------------

df = (
    df
    .where(col("user_id").isNotNull())
    .where(col("event_type").isNotNull())
    .where(col("event_timestamp").isNotNull())
)

if "event_id" in df.columns:
    df = df.dropDuplicates(["event_id"])
    # ---------------------------------------------------------------------
# DROP EXISTING ENRICHMENT COLUMNS BEFORE DIMENSION JOINS
# This makes the job safe to rerun and prevents ambiguous column names.
# ---------------------------------------------------------------------

existing_enrichment_columns = [
    "user_segment",
    "user_city",
    "device_preference",
    "signup_date",
    "account_status",
    "reference_product_id",
    "product_category",
    "product_brand",
    "reference_price",
    "is_active",
    "is_user_reference_missing",
    "is_product_reference_missing",
]

for column_name in existing_enrichment_columns:
    if column_name in df.columns:
        df = df.drop(column_name)


# ---------------------------------------------------------------------
# USER ENRICHMENT
# ---------------------------------------------------------------------

users_dim = users_dim.select(
    "user_id",
    "user_segment",
    "user_city",
    "device_preference",
    "signup_date",
    "account_status"
)

df = df.join(users_dim, on="user_id", how="left")


# ---------------------------------------------------------------------
# PRODUCT ENRICHMENT
# ---------------------------------------------------------------------

if "product" in df.columns and "product" in products_dim.columns:
    product_ref = products_dim.withColumnRenamed("product_id", "reference_product_id")

    df = df.join(
        product_ref.select(
            "product",
            "reference_product_id",
            "product_category",
            "product_brand",
            "reference_price",
            "is_active"
        ),
        on="product",
        how="left"
    )

elif "product_id" in df.columns and "product_id" in products_dim.columns:
    product_ref = products_dim.withColumnRenamed("product_id", "_product_id_join")

    df = (
        df
        .join(
            product_ref.select(
                "_product_id_join",
                "product",
                "product_category",
                "product_brand",
                "reference_price",
                "is_active"
            ),
            df["product_id"] == product_ref["_product_id_join"],
            "left"
        )
        .drop("_product_id_join")
        .withColumn("reference_product_id", col("product_id"))
    )

else:
    df = df.withColumn("reference_product_id", lit(None).cast("int"))
    df = df.withColumn("product_category", lit(None).cast("string"))
    df = df.withColumn("product_brand", lit(None).cast("string"))
    df = df.withColumn("reference_price", lit(None).cast("double"))
    df = df.withColumn("is_active", lit(None).cast("boolean"))


# ---------------------------------------------------------------------
# ENRICHMENT QUALITY FLAGS
# ---------------------------------------------------------------------

if "product_category" not in df.columns:
    df = df.withColumn("product_category", lit(None).cast("string"))

if "product_brand" not in df.columns:
    df = df.withColumn("product_brand", lit(None).cast("string"))

if "reference_price" not in df.columns:
    df = df.withColumn("reference_price", lit(None).cast("double"))

if "reference_product_id" not in df.columns:
    df = df.withColumn("reference_product_id", lit(None).cast("int"))

df = (
    df
    .withColumn("is_user_reference_missing", col("user_segment").isNull())
    .withColumn("is_product_reference_missing", col("product_category").isNull())
)


# ---------------------------------------------------------------------
# FINAL SILVER COLUMN ORDER
# ---------------------------------------------------------------------

preferred_columns = [
    "event_id",
    "user_id",
    "user_segment",
    "user_city",
    "device_preference",
    "account_status",
    "event_type",
    "product",
    "reference_product_id",
    "product_category",
    "product_brand",
    "price",
    "reference_price",
    "event_timestamp",
    "source",
    "schema_version",
    "is_active",
    "is_user_reference_missing",
    "is_product_reference_missing",
]

final_columns = [column for column in preferred_columns if column in df.columns]
remaining_columns = [column for column in df.columns if column not in final_columns]

silver_df = df.select(final_columns + remaining_columns)

silver_df.write.mode("overwrite").parquet(SILVER_PATH)

silver_count = silver_df.count()
missing_user_ref_count = silver_df.where(col("is_user_reference_missing")).count()
missing_product_ref_count = silver_df.where(col("is_product_reference_missing")).count()

report = {
    "silver_record_count": silver_count,
    "missing_user_reference_count": missing_user_ref_count,
    "missing_product_reference_count": missing_product_ref_count,
    "silver_path": SILVER_PATH,
    "users_dimension_path": USERS_DIM_PATH,
    "products_dimension_path": PRODUCTS_DIM_PATH,
}

Path("logs").mkdir(exist_ok=True)

with open(REPORT_PATH, "w", encoding="utf-8") as file:
    json.dump(report, file, indent=2)

print("Silver enriched user_events written successfully")
print(f"Silver record count: {silver_count}")
print(f"Missing user reference count: {missing_user_ref_count}")
print(f"Missing product reference count: {missing_product_ref_count}")
print(f"Report written to: {REPORT_PATH}")

silver_df.show(20, truncate=False)
silver_df.printSchema()

spark.stop()