from datetime import date, timedelta
from pathlib import Path
import json
import random
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# The dims are built from the producer's own catalog and user profiles, so every
# id the producer can emit has a reference row. Importing producer.py does not
# connect to Kafka: its Producer is only created inside main().
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data-generator"))
from producer import NUM_USERS, PRODUCT_IDS, PRODUCTS, get_user_profile  # noqa: E402


USERS_CSV_PATH = "data/reference/users"
PRODUCTS_CSV_PATH = "data/reference/products"

USERS_DIM_PATH = "data/dim/users"
PRODUCTS_DIM_PATH = "data/dim/products"

REPORT_PATH = "logs/reference_data_report.json"

# Fixed anchor so signup_date doesn't change from one run to the next.
SIGNUP_ANCHOR_DATE = date(2026, 1, 1)

DEVICE_PREFERENCES = ["mobile", "desktop", "tablet"]

PRODUCT_BRANDS = ["NovaTech", "UrbanCart", "FitCore", "HomeNest", "PrimeStyle"]

USERS_SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_segment", StringType(), True),
    StructField("user_city", StringType(), True),
    StructField("device_preference", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("account_status", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_brand", StringType(), True),
    StructField("reference_price", DoubleType(), True),
    StructField("is_active", BooleanType(), True),
])


def build_user_row(user_id):
    profile = get_user_profile(user_id)

    # Own seed: random.Random(user_id) is the profile's stream, and reusing it
    # would make device_preference mirror gender (same first 3-way choice).
    rng = random.Random(f"users-dim:{user_id}")

    return (
        user_id,
        profile["user_segment"],
        profile["home_city"],
        rng.choice(DEVICE_PREFERENCES),
        SIGNUP_ANCHOR_DATE - timedelta(days=rng.randint(0, 1199)),
        "inactive" if rng.randint(1, 25) == 1 else "active",
    )


def build_product_row(category, product_name):
    product_id = PRODUCT_IDS[product_name]
    rng = random.Random(f"products-dim:{product_id}")

    return (
        product_id,
        product_name,
        category,
        rng.choice(PRODUCT_BRANDS),
        float(rng.randint(999, 90998)),
        product_id % 20 != 0,
    )


spark = (
    SparkSession.builder
    .appName("CreateReferenceData")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------------------
# USER DIMENSION
# ---------------------------------------------------------------------

users_dim = spark.createDataFrame(
    [build_user_row(user_id) for user_id in range(1, NUM_USERS + 1)],
    USERS_SCHEMA
)


# ---------------------------------------------------------------------
# PRODUCT DIMENSION
# ---------------------------------------------------------------------

products_dim = spark.createDataFrame(
    [
        build_product_row(category, product_name)
        for category, product_names in PRODUCTS.items()
        for product_name in product_names
    ],
    PRODUCTS_SCHEMA
)


# ---------------------------------------------------------------------
# WRITE OUTPUTS
# ---------------------------------------------------------------------

users_dim.coalesce(1).write.mode("overwrite").option("header", True).csv(USERS_CSV_PATH)
products_dim.coalesce(1).write.mode("overwrite").option("header", True).csv(PRODUCTS_CSV_PATH)

users_dim.write.mode("overwrite").parquet(USERS_DIM_PATH)
products_dim.write.mode("overwrite").parquet(PRODUCTS_DIM_PATH)

user_count = users_dim.count()
product_count = products_dim.count()

report = {
    "user_reference_count": user_count,
    "product_reference_count": product_count,
    "users_csv_path": USERS_CSV_PATH,
    "products_csv_path": PRODUCTS_CSV_PATH,
    "users_dimension_path": USERS_DIM_PATH,
    "products_dimension_path": PRODUCTS_DIM_PATH,
}

Path("logs").mkdir(exist_ok=True)

with open(REPORT_PATH, "w", encoding="utf-8") as file:
    json.dump(report, file, indent=2)

print("Reference datasets created successfully")
print(f"User dimension count: {user_count}")
print(f"Product dimension count: {product_count}")
print(f"Report written to: {REPORT_PATH}")

users_dim.show(10, truncate=False)
products_dim.show(10, truncate=False)

spark.stop()
