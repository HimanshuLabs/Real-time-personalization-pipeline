from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, literal


PROJECT_ROOT = "/home/manshu/Desktop/Project-1- Real time suggestions"

DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="project1_personalization_batch_pipeline",
    description="Orchestrates Bronze to Silver to Gold to PostgreSQL for Project 1 real-time personalization pipeline.",
    start_date=pendulum.datetime(2026, 5, 29, tz="Asia/Kolkata"),
    schedule="@daily",
    catchup=False,
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
            spark-submit --packages org.postgresql:postgresql:42.7.4 spark-batch/publish_gold_to_postgres.py

            echo "Gold to PostgreSQL completed."
            """
        ),
    )

    verify_postgres_api_path = BashOperator(
        task_id="verify_postgres_api_path",
        cwd=PROJECT_ROOT,
        bash_command=literal(
            """
            set -euo pipefail

            echo "Verifying PostgreSQL user_features table..."

            python - <<'PY'
import psycopg
from psycopg.rows import dict_row

conn = psycopg.connect(
    host="localhost",
    port=5433,
    dbname="personalization_db",
    user="de_user",
    password="de_password",
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
        >> bronze_to_silver
        >> verify_silver_exists
        >> silver_to_gold
        >> verify_gold_exists
        >> gold_to_postgres
        >> verify_postgres_api_path
    )
