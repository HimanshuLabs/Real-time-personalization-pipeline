from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, expr, row_number
from pyspark.sql.window import Window


BRONZE_PATH = "data/bronze/user_events"

USERS_CSV_PATH = "data/reference/users"
PRODUCTS_CSV_PATH = "data/reference/products"

USERS_DIM_PATH = "data/dim/users"
PRODUCTS_DIM_PATH = "data/dim/products"

REPORT_PATH = "logs/reference_data_report.json"


def has_parquet_files(path: str) -> bool:
    target = Path(path)
    return target.exists() and any(target.rglob("*.parquet"))


spark = (
    SparkSession.builder
    .appName("CreateReferenceData")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_df = None

if has_parquet_files(BRONZE_PATH):
    bronze_df = spark.read.parquet(BRONZE_PATH)
    print(f"Loaded Bronze events from: {BRONZE_PATH}")
else:
    print("Bronze data not found. Creating default reference datasets.")


# ---------------------------------------------------------------------
# USER DIMENSION
# ---------------------------------------------------------------------

if bronze_df is not None and "user_id" in bronze_df.columns:
    users_base = (
        bronze_df
        .select(col("user_id").cast("int").alias("user_id"))
        .where(col("user_id").isNotNull())
        .distinct()
    )

    if users_base.count() == 0:
        users_base = spark.range(1, 501).withColumnRenamed("id", "user_id")
else:
    users_base = spark.range(1, 501).withColumnRenamed("id", "user_id")


users_dim = (
    users_base
    .withColumn(
        "user_segment",
        expr("""
            CASE pmod(user_id, 4)
                WHEN 0 THEN 'new_user'
                WHEN 1 THEN 'regular_user'
                WHEN 2 THEN 'high_value_user'
                ELSE 'discount_seeker'
            END
        """)
    )
    .withColumn(
        "user_city",
        expr("""
            CASE pmod(user_id, 6)
                WHEN 0 THEN 'Hyderabad'
                WHEN 1 THEN 'Bengaluru'
                WHEN 2 THEN 'Mumbai'
                WHEN 3 THEN 'Delhi'
                WHEN 4 THEN 'Pune'
                ELSE 'Chennai'
            END
        """)
    )
    .withColumn(
        "device_preference",
        expr("""
            CASE pmod(user_id, 3)
                WHEN 0 THEN 'mobile'
                WHEN 1 THEN 'desktop'
                ELSE 'tablet'
            END
        """)
    )
    .withColumn(
        "signup_date",
        expr("date_sub(current_date(), CAST(pmod(user_id * 17, 1200) AS INT))")
    )
    .withColumn(
        "account_status",
        expr("""
            CASE
                WHEN pmod(user_id, 25) = 0 THEN 'inactive'
                ELSE 'active'
            END
        """)
    )
)


# ---------------------------------------------------------------------
# PRODUCT DIMENSION
# ---------------------------------------------------------------------

default_products = [
    ("wireless_headphones",),
    ("gaming_laptop",),
    ("running_shoes",),
    ("smart_watch",),
    ("mechanical_keyboard",),
    ("coffee_maker",),
    ("office_chair",),
    ("fitness_tracker",),
    ("bluetooth_speaker",),
    ("backpack",),
]

if bronze_df is not None and "product" in bronze_df.columns:
    products_base = (
        bronze_df
        .select(col("product").cast("string").alias("product"))
        .where(col("product").isNotNull())
        .distinct()
    )

    if products_base.count() == 0:
        products_base = spark.createDataFrame(default_products, ["product"])

elif bronze_df is not None and "product_id" in bronze_df.columns:
    products_base = (
        bronze_df
        .select(col("product_id").cast("int").alias("product_id"))
        .where(col("product_id").isNotNull())
        .distinct()
        .withColumn("product", concat(lit("product_"), col("product_id")))
    )

else:
    products_base = spark.createDataFrame(default_products, ["product"])


if "product_id" not in products_base.columns:
    products_base = products_base.withColumn(
        "product_id",
        row_number().over(Window.orderBy("product"))
    )


products_dim = (
    products_base
    .withColumn(
        "product_category",
        expr("""
            CASE pmod(abs(hash(product)), 5)
                WHEN 0 THEN 'electronics'
                WHEN 1 THEN 'fashion'
                WHEN 2 THEN 'home'
                WHEN 3 THEN 'fitness'
                ELSE 'accessories'
            END
        """)
    )
    .withColumn(
        "product_brand",
        expr("""
            CASE pmod(abs(hash(product)), 5)
                WHEN 0 THEN 'NovaTech'
                WHEN 1 THEN 'UrbanCart'
                WHEN 2 THEN 'FitCore'
                WHEN 3 THEN 'HomeNest'
                ELSE 'PrimeStyle'
            END
        """)
    )
    .withColumn(
        "reference_price",
        expr("CAST(pmod(abs(hash(product)), 90000) + 999 AS DOUBLE)")
    )
    .withColumn(
        "is_active",
        expr("""
            CASE
                WHEN pmod(product_id, 20) = 0 THEN false
                ELSE true
            END
        """)
    )
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