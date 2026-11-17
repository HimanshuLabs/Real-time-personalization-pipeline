import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# data/ and logs/ resolve from PROJECT1_ROOT (default: the repository root), like
# spark-streaming/read_stream.py, so the script works from any directory.
PROJECT_ROOT = os.environ.get("PROJECT1_ROOT", str(Path(__file__).resolve().parents[1]))


def project_path(relative_path):
    return os.path.join(PROJECT_ROOT, relative_path)


BRONZE_PATH = "data/bronze/user_events"
USERS_DIM_PATH = "data/dim/users"
PRODUCTS_DIM_PATH = "data/dim/products"
SILVER_PATH = "data/silver/user_events"
REPORT_PATH = "logs/silver_enrichment_report.json"

# Silver's leading columns, in this order; the other Bronze columns follow.
LEADING_COLUMNS = [
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
    "reference_price",
    "event_timestamp",
    "source",
    "schema_version",
    "is_active",
    "is_user_reference_missing",
    "is_product_reference_missing",
]


def require_path(path: str, message: str) -> None:
    target = Path(project_path(path))
    if not target.exists():
        raise FileNotFoundError(f"{message}: {target}")


def build_silver(bronze_df, users_dim, products_dim):
    """Filter, dedup and enrich schema-4.0 Bronze. Returns (silver_df, counts)."""

    # The file listing is fixed when Bronze is read, so these counts and Silver
    # all see the same Bronze even while the stream keeps appending.
    bronze_rows = bronze_df.count()

    df = (
        bronze_df.where(col("user_id").isNotNull())
        .where(col("event_type").isNotNull())
        .where(col("event_timestamp").isNotNull())
    )
    rows_after_filters = df.count()

    df = df.dropDuplicates(["event_id"])
    rows_after_dedup = df.count()

    # User enrichment. user_segment is not taken from the dim: Silver keeps the
    # event's own value.
    df = df.join(
        users_dim.select(
            "user_id", "user_city", "device_preference", "signup_date", "account_status"
        ),
        on="user_id",
        how="left",
    )

    # Product enrichment by product_id. Non-commerce events have none and get NULLs.
    product_ref = products_dim.withColumnRenamed("product_id", "_product_id_join")
    df = (
        df.join(
            product_ref.select(
                "_product_id_join",
                "product",
                "product_category",
                "product_brand",
                "reference_price",
                "is_active",
            ),
            df["product_id"] == product_ref["_product_id_join"],
            "left",
        )
        .drop("_product_id_join")
        .withColumn("reference_product_id", col("product_id"))
    )

    # Only an event that names a product can be missing its product reference.
    df = df.withColumn("is_user_reference_missing", col("user_city").isNull()).withColumn(
        "is_product_reference_missing",
        col("product_id").isNotNull() & col("product_category").isNull(),
    )

    remaining_columns = [column for column in df.columns if column not in LEADING_COLUMNS]
    silver_df = df.select(LEADING_COLUMNS + remaining_columns)

    counts = {
        "bronze_rows": bronze_rows,
        "rows_dropped_by_filters": bronze_rows - rows_after_filters,
        "duplicates_removed": rows_after_filters - rows_after_dedup,
    }
    return silver_df, counts


def main():
    spark = (
        SparkSession.builder.appName("BronzeToSilverEnrichedEvents")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("WARN")

        require_path(BRONZE_PATH, "Bronze path does not exist. Run the Bronze ingestion first")
        require_path(
            USERS_DIM_PATH,
            "User dimension does not exist. Run reference-data/create_reference_data.py first",
        )
        require_path(
            PRODUCTS_DIM_PATH,
            "Product dimension does not exist. Run reference-data/create_reference_data.py first",
        )

        silver_df, counts = build_silver(
            spark.read.parquet(project_path(BRONZE_PATH)),
            spark.read.parquet(project_path(USERS_DIM_PATH)),
            spark.read.parquet(project_path(PRODUCTS_DIM_PATH)),
        )

        silver_df.write.mode("overwrite").parquet(project_path(SILVER_PATH))

        # Report on the Silver actually written, not on a re-run of the plan.
        written_silver = spark.read.parquet(project_path(SILVER_PATH))
        silver_count = written_silver.count()
        missing_user_ref_count = written_silver.where(col("is_user_reference_missing")).count()
        missing_product_ref_count = written_silver.where(
            col("is_product_reference_missing")
        ).count()

        report = {
            **counts,
            "silver_record_count": silver_count,
            "missing_user_reference_count": missing_user_ref_count,
            "missing_product_reference_count": missing_product_ref_count,
            "silver_path": SILVER_PATH,
            "users_dimension_path": USERS_DIM_PATH,
            "products_dimension_path": PRODUCTS_DIM_PATH,
        }

        Path(project_path("logs")).mkdir(exist_ok=True)

        with open(project_path(REPORT_PATH), "w", encoding="utf-8") as file:
            json.dump(report, file, indent=2)

        print("Silver enriched user_events written successfully")
        print(f"Bronze rows: {report['bronze_rows']}")
        print(f"Rows dropped by filters: {report['rows_dropped_by_filters']}")
        print(f"Duplicates removed: {report['duplicates_removed']}")
        print(f"Silver record count: {silver_count}")
        print(f"Missing user reference count: {missing_user_ref_count}")
        print(f"Missing product reference count: {missing_product_ref_count}")
        print(f"Report written to: {REPORT_PATH}")

        silver_df.show(20, truncate=False)
        silver_df.printSchema()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
