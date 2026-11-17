import json
import os
import random
import sys
from datetime import date, timedelta
from pathlib import Path

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
from producer import (  # noqa: E402
    NUM_USERS,
    PRODUCT_IDS,
    PRODUCT_REFERENCE_PRICES,
    PRODUCTS,
    get_user_profile,
    is_product_active,
    user_reference_attributes,
)

# data/ and logs/ resolve from PROJECT1_ROOT (default: the repository root), like
# spark-streaming/read_stream.py, so the script works from any directory.
PROJECT_ROOT = os.environ.get("PROJECT1_ROOT", str(Path(__file__).resolve().parents[1]))


def project_path(relative_path):
    return os.path.join(PROJECT_ROOT, relative_path)


USERS_DIM_PATH = "data/dim/users"
PRODUCTS_DIM_PATH = "data/dim/products"

REPORT_PATH = "logs/reference_data_report.json"

# Fixed anchor so signup_date doesn't change from one run to the next.
SIGNUP_ANCHOR_DATE = date(2026, 1, 1)

PRODUCT_BRANDS = ["NovaTech", "UrbanCart", "FitCore", "HomeNest", "PrimeStyle"]

USERS_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("user_segment", StringType(), True),
        StructField("user_city", StringType(), True),
        StructField("device_preference", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("account_status", StringType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_brand", StringType(), True),
        StructField("reference_price", DoubleType(), True),
        StructField("is_active", BooleanType(), True),
    ]
)


def build_user_row(user_id):
    profile = get_user_profile(user_id)

    # Device, signup offset and account status come from the producer, so the
    # producer can skip inactive accounts.
    attributes = user_reference_attributes(user_id)

    return (
        user_id,
        profile["user_segment"],
        profile["home_city"],
        attributes["device_preference"],
        SIGNUP_ANCHOR_DATE - timedelta(days=attributes["signup_days_ago"]),
        attributes["account_status"],
    )


def build_product_row(category, product_name):
    product_id = PRODUCT_IDS[product_name]
    rng = random.Random(f"products-dim:{product_id}")

    return (
        product_id,
        product_name,
        category,
        rng.choice(PRODUCT_BRANDS),
        PRODUCT_REFERENCE_PRICES[product_name],
        is_product_active(product_id),
    )


def main():
    spark = (
        SparkSession.builder.appName("CreateReferenceData")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("WARN")

        users_dim = spark.createDataFrame(
            [build_user_row(user_id) for user_id in range(1, NUM_USERS + 1)], USERS_SCHEMA
        )

        products_dim = spark.createDataFrame(
            [
                build_product_row(category, product_name)
                for category, product_names in PRODUCTS.items()
                for product_name in product_names
            ],
            PRODUCTS_SCHEMA,
        )

        users_dim.write.mode("overwrite").parquet(project_path(USERS_DIM_PATH))
        products_dim.write.mode("overwrite").parquet(project_path(PRODUCTS_DIM_PATH))

        user_count = users_dim.count()
        product_count = products_dim.count()

        report = {
            "user_reference_count": user_count,
            "product_reference_count": product_count,
            "users_dimension_path": USERS_DIM_PATH,
            "products_dimension_path": PRODUCTS_DIM_PATH,
        }

        Path(project_path("logs")).mkdir(exist_ok=True)

        with open(project_path(REPORT_PATH), "w", encoding="utf-8") as file:
            json.dump(report, file, indent=2)

        print("Reference datasets created successfully")
        print(f"User dimension count: {user_count}")
        print(f"Product dimension count: {product_count}")
        print(f"Report written to: {REPORT_PATH}")

        users_dim.show(10, truncate=False)
        products_dim.show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
