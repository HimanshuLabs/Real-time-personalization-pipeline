# Architecture

The pipeline has three parts:

1. **Streaming ingestion**: Kafka → Spark Structured Streaming → Bronze.
2. **Batch feature computation**: Bronze → Silver → Gold → PostgreSQL.
3. **API serving**: FastAPI reads the PostgreSQL feature table.

Features are computed in batch. The streaming job only validates and lands events; it does not update the serving table.

Diagram: [diagrams/project1_system_architecture.mmd](diagrams/project1_system_architecture.mmd)

## Streaming ingestion

`spark-streaming/read_stream.py`, started with `scripts/run_stream.sh`:

- Reads the Kafka topic `user_events` from `localhost:9092`. With no checkpoint it starts at the earliest offset; after that it resumes from its checkpoint.
- Parses the JSON value with a typed schema and derives `event_timestamp` with `try_to_timestamp`, so a malformed `event_time` becomes NULL instead of failing the query.
- Builds one `is_valid` column from the required-field and value rules, wrapped in `coalesce(..., false)` so a NULL in any checked column counts as invalid.
- Valid rows are appended to Bronze Parquet (`data/bronze/user_events`). Invalid rows are written to the Kafka topic `user_events_dlq` as their original JSON string.
- Both sinks trigger every 10 seconds and keep their own checkpoint under `checkpoints/`.
- There is no streaming deduplication and no watermark. Bronze is lossless; duplicates are removed in Silver.
- SIGINT and SIGTERM only set a stop flag. The main loop then stops both queries and the Spark session and prints `[stream] stopped cleanly`.

## Batch feature computation

Run manually or through the Airflow DAG `project1_personalization_batch_pipeline` (`dags/project1_personalization_pipeline.py`, daily, `max_active_runs=1`):

1. `reference-data/create_reference_data.py` writes `data/dim/users` (user_id 1..`NUM_USERS`) and `data/dim/products` (the producer's catalog). Both are generated from `data-generator/producer.py`, so every id the producer can emit has a reference row.
2. `spark-batch/bronze_to_silver.py` drops rows without `user_id`, `event_type` or `event_timestamp`, removes duplicate `event_id`s, and left-joins both dims. It keeps the event's own `user_segment` and sets `is_user_reference_missing` / `is_product_reference_missing`.
3. `spark-batch/silver_to_gold.py` aggregates Silver to one row per `user_id` (`data/gold/user_features`).
4. `spark-batch/publish_gold_to_postgres.py` writes Gold to `user_features_staging` over JDBC, then in one PostgreSQL transaction checks that staging is non-empty with unique `user_id`s, adds the primary key, drops `user_features`, renames staging to `user_features` and renames the key to `user_features_pkey`. Any failure rolls back and leaves the live table as it was.

Each batch run reprocesses all of Bronze. Spark sessions run in UTC.

## Serving

`api/main.py` reads `user_features` with one connection per request (no pool):

- `GET /health`: 200 only if PostgreSQL is reachable and `user_features` exists and has rows; otherwise 503 with a short reason.
- `GET /features/{user_id}`: 200 with the user's features, 404 if unknown, 422 if `user_id` is not an integer, 500 if the lookup fails.
- `GET /metrics`: Prometheus metrics.

Error responses never include exception text; details are logged server-side.

## Monitoring

`monitoring/run_monitoring.sh` starts four containers on the host network:

- **kafka-exporter (:9308):** topic offsets for `user_events` and `user_events_dlq`.
- **feature-freshness-exporter (:8001):** `user_features` row count and the age of the newest `last_event_timestamp`. If the database is unreachable it reports `project1_feature_exporter_up 0` and NaN for the other gauges.
- **Prometheus (:9090):** scrapes the API, both exporters and itself.
- **Grafana (:3000):** dashboard provisioned from `monitoring/grafana/dashboards/project1-observability.json`.

Spark keeps its Kafka offsets in checkpoints, not in a consumer group, so the consumer-group lag panel normally shows no data. Feature freshness is the signal for the batch-to-serving path.
