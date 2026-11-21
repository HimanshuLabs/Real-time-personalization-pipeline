"""Check that every user in Bronze is a customer of the shop export. Read-only.

create_reference_data.py rebuilds the users dim from customers.csv in
SHOP_MASTER_DIR (default: the committed reference-data/shop_master). If Bronze
was produced from another export, Silver would lose most user references, so
the Airflow DAG runs this check first: exit 1 lists how many Bronze users the
export lacks, before anything is overwritten. It writes nothing.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# The same customers create_reference_data.py builds the users dim from.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data-generator"))
from producer import PROJECT_ROOT, SHOP_MASTER_DIR, USER_IDS  # noqa: E402

BRONZE_PATH = "data/bronze/user_events"
SAMPLE_SIZE = 5


def missing_users(spark, bronze_path, customer_ids):
    """(distinct Bronze users, Bronze users not in customer_ids, the first few of them)."""
    bronze = spark.read.parquet(bronze_path)
    bronze_users = bronze.select("user_id").where(col("user_id").isNotNull()).distinct()
    customers = spark.createDataFrame([(user_id,) for user_id in customer_ids], "user_id int")
    missing = bronze_users.join(customers, on="user_id", how="left_anti")
    sample = [row.user_id for row in missing.orderBy("user_id").limit(SAMPLE_SIZE).collect()]
    return bronze_users.count(), missing.count(), sample


def main():
    bronze_path = os.path.join(PROJECT_ROOT, BRONZE_PATH)
    customers_csv = os.path.join(PROJECT_ROOT, SHOP_MASTER_DIR, "customers.csv")
    if not Path(bronze_path).exists():
        print(f"[bronze-check] error: Bronze path does not exist: {bronze_path}", file=sys.stderr)
        return 1

    spark = (
        SparkSession.builder.appName("CheckBronzeAgainstExport")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("WARN")
        distinct, missing, sample = missing_users(spark, bronze_path, USER_IDS)
    finally:
        spark.stop()

    if missing:
        print(
            f"[bronze-check] error: {missing} of {distinct} distinct user_id in Bronze are not "
            f"customers in {customers_csv} (first: {', '.join(map(str, sample))}). The users dim "
            "would not match Bronze. Set SHOP_MASTER_DIR to the export Bronze was produced "
            "from. Nothing was written.",
            file=sys.stderr,
            flush=True,
        )
        return 1

    print(
        f"[bronze-check] ok: all {distinct} distinct user_id in Bronze are among the "
        f"{len(USER_IDS)} customers in {customers_csv}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
