from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, literal

PROJECT_ROOT = os.environ.get(
    "PROJECT1_ROOT",
    str(Path(__file__).resolve().parents[1]),
)

# No task passes env=, so every task inherits the environment Airflow was
# started with. The scale settings are set there, never in this file:
# SHOP_MASTER_DIR (the export Bronze was produced from; unset means the
# committed reference-data/shop_master), SPARK_DRIVER_MEMORY (unset: Spark's
# default) and the other settings in the README's "Running at scale".
# verify_bronze_matches_export fails the run before anything is overwritten
# when Bronze holds users that export does not have.

DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="project1_personalization_batch_pipeline",
    description="Orchestrates Bronze to Silver to Gold to PostgreSQL for Project 1 real-time personalization pipeline.",
    start_date=pendulum.datetime(2026, 5, 29, tz="Asia/Kolkata"),
    # Triggered by hand after a stream run; see the README's Airflow notes.
    schedule=None,
    catchup=False,
    # Every task overwrites shared Silver/Gold/Postgres outputs.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["project1", "spark", "lakehouse", "feature-store"],
) as dag:
    verify_bronze_exists = BashOperator(
        task_id="verify_bronze_exists",
        cwd=PROJECT_ROOT,
        bash_command=literal(
            """
            set -euo pipefail

            echo "Checking Bronze layer..."
            test -d data/bronze/user_events

            FILE_COUNT=$(find data/bronze/user_events -type f -name "*.parquet" | wc -l)

            if [ "$FILE_COUNT" -eq 0 ]; then
              echo "No Bronze parquet files found in data/bronze/user_events"
              exit 1
            fi

            echo "Bronze layer exists with $FILE_COUNT parquet file(s)."
            """
        ),
    )

    # Read-only. Runs before create_reference_data, the first task that writes.
    # A mismatch fails the same way every time, so it is not retried.
    verify_bronze_matches_export = BashOperator(
        task_id="verify_bronze_matches_export",
        cwd=PROJECT_ROOT,
        retries=0,
        bash_command=literal(
            """
            set -euo pipefail

            echo "SHOP_MASTER_DIR=${SHOP_MASTER_DIR:-<unset: reference-data/shop_master>}"
            echo "SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-<unset: Spark default>}"
            python -c "import confluent_kafka, psycopg, pyspark" || { echo "The python on PATH ($(command -v python || echo none)) cannot import confluent_kafka, psycopg and pyspark: start Airflow with scripts/run_airflow.sh, which puts the project's venv first on PATH." >&2; exit 1; }
            echo "Checking that every Bronze user is a customer of the shop export..."
            spark-submit reference-data/check_bronze_against_export.py

            echo "Bronze matches the shop export."
            """
        ),
    )

    create_reference_data = BashOperator(
        task_id="create_reference_data",
        cwd=PROJECT_ROOT,
        bash_command=literal(
            """
            set -euo pipefail

            echo "Creating reference user/product dimensions..."
            spark-submit reference-data/create_reference_data.py

            echo "Reference data completed."
            """
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        cwd=PROJECT_ROOT,
        bash_command=literal(
            """
            set -euo pipefail

            echo "Running Bronze to Silver Spark job..."
            spark-submit spark-batch/bronze_to_silver.py

            echo "Bronze to Silver completed."
            """
        ),
    )

    verify_silver_exists = BashOperator(
        task_id="verify_silver_exists",
        cwd=PROJECT_ROOT,
        bash_command=literal(
            """
            set -euo pipefail

            echo "Checking Silver layer..."
            test -d data/silver/user_events

            FILE_COUNT=$(find data/silver/user_events -type f -name "*.parquet" | wc -l)

            if [ "$FILE_COUNT" -eq 0 ]; then
              echo "No Silver parquet files found in data/silver/user_events"
              exit 1
            fi

            echo "Silver layer exists with $FILE_COUNT parquet file(s)."
            """
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        cwd=PROJECT_ROOT,
        bash_command=literal(
            """
            set -euo pipefail

            echo "Running Silver to Gold Spark job..."
            spark-submit spark-batch/silver_to_gold.py

            echo "Silver to Gold completed."
            """
        ),
    )

    verify_gold_exists = BashOperator(
        task_id="verify_gold_exists",
        cwd=PROJECT_ROOT,
        bash_command=literal(
            """
            set -euo pipefail

            echo "Checking Gold layer..."
            test -d data/gold/user_features

            FILE_COUNT=$(find data/gold/user_features -type f -name "*.parquet" | wc -l)

            if [ "$FILE_COUNT" -eq 0 ]; then
              echo "No Gold parquet files found in data/gold/user_features"
              exit 1
            fi

            echo "Gold layer exists with $FILE_COUNT parquet file(s)."
            """
        ),
    )

    gold_to_postgres = BashOperator(
        task_id="gold_to_postgres",
        cwd=PROJECT_ROOT,
        bash_command=literal(
            """
            set -euo pipefail

            echo "Publishing Gold user features to PostgreSQL..."
            PUBLISH_REFUSE_SHRINK=1 spark-submit --packages org.postgresql:postgresql:42.7.4 spark-batch/publish_gold_to_postgres.py

            echo "Gold to PostgreSQL completed."
            """
        ),
    )

    # Checks the serving table only; it does not call the API.
    verify_postgres_table = BashOperator(
        task_id="verify_postgres_table",
        cwd=PROJECT_ROOT,
        bash_command=literal(
            """
            set -euo pipefail

            echo "Verifying PostgreSQL user_features table..."

            python - <<'PY'
import os

import psycopg
from psycopg.rows import dict_row

conn = psycopg.connect(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5433")),
    dbname=os.environ.get("POSTGRES_DB", "personalization_db"),
    user=os.environ.get("POSTGRES_USER", "de_user"),
    password=os.environ.get("POSTGRES_PASSWORD", "de_password"),
    row_factory=dict_row,
)

with conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS row_count FROM user_features;")
        result = cur.fetchone()
        row_count = result["row_count"]

        if row_count == 0:
            raise RuntimeError("PostgreSQL user_features table is empty.")

        print(f"PostgreSQL user_features row count: {row_count}")
PY

            echo "PostgreSQL verification completed."
            """
        ),
    )

    (
        verify_bronze_exists
        >> verify_bronze_matches_export
        >> create_reference_data
        >> bronze_to_silver
        >> verify_silver_exists
        >> silver_to_gold
        >> verify_gold_exists
        >> gold_to_postgres
        >> verify_postgres_table
    )
