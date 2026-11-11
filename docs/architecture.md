# Architecture

The pipeline has three parts:

1. **Streaming ingestion**: Kafka → Spark Structured Streaming → Bronze.
2. **Batch feature computation**: Bronze → Silver → Gold → PostgreSQL.
3. **API serving**: FastAPI reads the PostgreSQL feature table.

Features are computed in batch. The streaming job only validates and lands events; it does not update the serving table.

Diagram: [diagrams/project1_system_architecture.mmd](diagrams/project1_system_architecture.mmd)

## Synthetic events

`data-generator/producer.py` keeps up to 20 sessions active (`MAX_ACTIVE_SESSIONS`) and on each loop emits the next event of a randomly chosen session, keyed by `user_id`:

- A session has one user, one `session_id` and one product, with `original_price` (the product's INR list price in `PRODUCT_REFERENCE_PRICES` × 0.9 to 1.1) and `discount_percent` fixed for the session.
- Its events follow a purchase funnel: `login` or `home_page_view` (sometimes `ad_impression` / `ad_click`), `search`, one to three `product_view`s (with optional recommendation, `image_zoom` and `wishlist_add` events), `add_to_cart`, then `remove_from_cart` or `checkout_started`, `coupon_applied`, `payment_initiated`, `payment_success` + `order_placed` or `payment_failed`, an occasional review or rating, and `logout`.
- `PRODUCT_FIELDS` are NULL on events outside `COMMERCE_EVENT_TYPES`. Recommendation fields, `search_query`, session counters, `time_since_last_event_ms`, `inventory_remaining` and `purchase_probability` follow the session, user and product state.
- `user_journey_stage` follows the event type (`JOURNEY_STAGE_BY_EVENT_TYPE`: `discovery`, `consideration`, `checkout`, `post_purchase`); `logout` keeps the session's latest stage.
- `device_type`, `operating_system`, `browser`, `network_type` and `app_version` are drawn once per session from combinations that fit together (no desktop OS on mobile or tablet, `safari` only on `ios` and `macos`, `ethernet` only on desktop).
- `payment_method` is drawn once per session and set only on `PAYMENT_EVENT_TYPES` (`payment_initiated`, `payment_success`, `payment_failed`, `order_placed`); it is NULL elsewhere.
- `account_status` and `is_product_active` are defined in the producer and used by the reference dims; inactive accounts and products are never emitted.
- The producer emits 21 of the 26 `EVENT_TYPES`; `search_result_click`, `wishlist_remove`, `order_cancelled`, `return_requested` and `delivery_tracking_view` are never emitted.

## Streaming ingestion

`spark-streaming/read_stream.py`, started with `scripts/run_stream.sh`:

- Reads the Kafka topic `user_events` from `localhost:9092`. With no checkpoint it starts at the earliest offset; after that it resumes from its checkpoint.
- Parses the JSON value with a typed schema and derives `event_timestamp` with `try_to_timestamp`, so a malformed `event_time` becomes NULL instead of failing the query.
- Builds one `is_valid` column from the required-field and value rules, wrapped in `coalesce(..., false)` so a NULL in any checked column counts as invalid.
- Product and price fields are required only on `COMMERCE_EVENT_TYPES` (`product_id`, `product_name`, `original_price`, `discounted_price`, with every range rule holding). Other events may leave them NULL; a value that is present must pass the same range rules.
- Valid rows are appended to Bronze Parquet (`data/bronze/user_events`). Invalid rows are written to the Kafka topic `user_events_dlq` as their original JSON string.
- Both sinks trigger every 10 seconds and keep their own checkpoint under `checkpoints/`.
- There is no streaming deduplication and no watermark. Bronze is lossless; duplicates are removed in Silver.
- SIGINT and SIGTERM only set a stop flag. The main loop then stops both queries and the Spark session and prints `[stream] stopped cleanly`.

## Batch feature computation

Run manually or through the Airflow DAG `project1_personalization_batch_pipeline` (`dags/project1_personalization_pipeline.py`, daily, `max_active_runs=1`):

1. `reference-data/create_reference_data.py` writes `data/dim/users` (user_id 1..`NUM_USERS`) and `data/dim/products` (the producer's catalog). Both are generated from `data-generator/producer.py`, including list prices and status, so every id the producer can emit has a reference row.
2. `spark-batch/bronze_to_silver.py` drops rows without `user_id`, `event_type` or `event_timestamp`, removes duplicate `event_id`s, and left-joins both dims. It keeps the event's own `user_segment` and sets `is_user_reference_missing` and `is_product_reference_missing`; the product flag applies only to events that have a `product_id`.
3. `spark-batch/silver_to_gold.py` aggregates Silver to one row per `user_id` (`data/gold/user_features`, 19 columns). `avg_event_price` and `max_event_price` use commerce events only and are NULL for a user without any. Averages and `total_spend` are computed on DECIMAL and stored as DOUBLE, so reruns on the same Silver give identical values. The last four columns are `total_spend`, `session_count`, `features_computed_at` (one timestamp per run) and `recency_hours` (hours from `last_event_timestamp` to `features_computed_at`).
4. `spark-batch/publish_gold_to_postgres.py` writes Gold to `user_features_staging` over JDBC, then in one PostgreSQL transaction checks that staging is non-empty with unique `user_id`s, adds the primary key, drops `user_features`, renames staging to `user_features` and renames the key to `user_features_pkey`. Any failure rolls back and leaves the live table as it was.

Each batch run reprocesses all of Bronze. Spark sessions run in UTC.

## Serving

`api/main.py` reads `user_features` with one connection per request (no pool):

- `GET /health`: 200 only if PostgreSQL is reachable and `user_features` exists and has rows; otherwise 503 with a short reason.
- `GET /features/{user_id}`: 200 with the user's features (every `user_features` column except `user_name`, `membership_tier` and `features_computed_at`, so including `total_spend`, `session_count` and `recency_hours`), 404 if unknown, 422 if `user_id` is not an integer, 500 if the lookup fails.
- `GET /metrics`: Prometheus metrics.

Error responses never include exception text; details are logged server-side.

## Monitoring

`monitoring/run_monitoring.sh` starts four containers on the host network:

- **kafka-exporter (:9308):** topic offsets for `user_events` and `user_events_dlq`.
- **feature-freshness-exporter (:8001):** `user_features` row count and the age of the newest `last_event_timestamp`. If the database is unreachable it reports `project1_feature_exporter_up 0` and NaN for the other gauges.
- **Prometheus (:9090):** scrapes the API, both exporters and itself.
- **Grafana (:3000):** dashboard provisioned from `monitoring/grafana/dashboards/project1-observability.json`.

Spark keeps its Kafka offsets in checkpoints, not in a consumer group, so the consumer-group lag panel normally shows no data. Feature freshness is the signal for the batch-to-serving path.
