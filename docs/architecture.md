# Architecture

The pipeline has three parts:

1. **Streaming ingestion**: Kafka → Spark Structured Streaming → Bronze.
2. **Batch feature computation**: Bronze → Silver → Gold → PostgreSQL.
3. **API serving**: FastAPI reads the PostgreSQL feature table.

Features are computed in batch. The streaming job only validates and lands events; it does not update the serving table.

Diagram: [diagrams/project1_system_architecture.mmd](diagrams/project1_system_architecture.mmd)

## Synthetic events

This pipeline is the live clickstream of the shop in the batch lakehouse ([Batch-Lakehouse-Marketing-Analytics](https://github.com/HimanshuLabs/Batch-Lakehouse-Marketing-Analytics)). Users and products come from `reference-data/shop_master/customers.csv` (497 customers, 24 of them inactive) and `products.csv` (147 products: 138 active, 9 discontinued), a read-only export of its warehouse (the current SCD2 rows of `warehouse.dim_customer` and `warehouse.dim_product`, exported on 2026-11-18 from its `main` at `c1e117a`). Events carry its customer IDs, names, cities, products and list prices; only the behaviour is simulated, without Faker. The producer loads both files once from `PROJECT1_ROOT` and exits 2 if a file or a required column is missing. For a scale run, `reference-data/export_shop_master.py` writes a fresh export of the batch lakehouse (as `p2_reader`, one read-only snapshot, the same CSV format) into a git-ignored folder named by `SHOP_MASTER_DIR`, which the producer and the reference dims then read instead; unset, they read the committed files. This pipeline's events are live product behaviour for personalization, ML and AI; the batch lakehouse's own web events are its marketing feed, tying sessions to campaigns. This pipeline's purchases are not reconciled with the batch lakehouse's orders.

`data-generator/producer.py` keeps up to 20 sessions active (`MAX_ACTIVE_SESSIONS`) and on each loop emits the next event of a randomly chosen session, keyed by `user_id`:

- By default the producer runs until stopped and each new session goes to a random free active user. With `PRODUCER_ONE_PASS=1` it runs to a session target instead: the active users are shuffled by `PRODUCER_SEED`, shard `i/n` (`PRODUCER_SHARD`) takes every n-th of them and gives each exactly one session, then starts extra sessions with the default rule among its own users until its share of `PRODUCER_TOTAL_SESSIONS`, and exits once the last session ends. A user is never in two concurrent sessions, and the seed makes the users, sessions and event draws repeatable. `PRODUCER_PAUSE_SECONDS` and `PRODUCER_QUIET` set the pause and turn off the per-event print.
- A session has one user (picked from the customer IDs in `customers.csv`, which are not contiguous), one `session_id` and one product, with `original_price` (the product's INR list price from `products.csv`) and `discount_percent` fixed for the session; only the discount varies between sessions.
- `country` is India on about 95% of events and `city` the customer's `home_city` on about 85%; other Indian events use another city where the shop has customers, and events from abroad use a fixed list of foreign (country, city) pairs.
- Its events follow a purchase funnel: `login` or `home_page_view` (sometimes `ad_impression` / `ad_click`), `search`, one to three `product_view`s (with optional recommendation, `image_zoom` and `wishlist_add` events), `add_to_cart`, then `remove_from_cart` or `checkout_started`, `coupon_applied`, `payment_initiated`, `payment_success` + `order_placed` or `payment_failed`, an occasional review or rating, and `logout`.
- `PRODUCT_FIELDS` are NULL on events outside `COMMERCE_EVENT_TYPES`. Recommendation fields, `search_query`, session counters, `time_since_last_event_ms`, `inventory_remaining` and `purchase_probability` follow the session, user and product state.
- `user_journey_stage` follows the event type (`JOURNEY_STAGE_BY_EVENT_TYPE`: `discovery`, `consideration`, `checkout`, `post_purchase`); `logout` keeps the session's latest stage.
- `device_type`, `operating_system`, `browser`, `network_type` and `app_version` are drawn once per session from combinations that fit together (no desktop OS on mobile or tablet, `safari` only on `ios` and `macos`, `ethernet` only on desktop).
- `payment_method` is drawn once per session and set only on `PAYMENT_EVENT_TYPES` (`payment_initiated`, `payment_success`, `payment_failed`, `order_placed`); it is NULL elsewhere.
- `account_status` (a seeded draw per customer, 1 in 25 inactive) and `is_product_active` (`product_status` is `active` in `products.csv`) are defined in the producer and used by the reference dims; inactive accounts and discontinued products are never emitted.
- The producer emits 21 of the 26 `EVENT_TYPES`; `search_result_click`, `wishlist_remove`, `order_cancelled`, `return_requested` and `delivery_tracking_view` are never emitted.

## Streaming ingestion

`spark-streaming/read_stream.py`, started with `scripts/run_stream.sh`:

- Reads the Kafka topic `user_events` from `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`). With no checkpoint it starts at the earliest offset; after that it resumes from its checkpoint.
- Parses the JSON value with a typed schema and derives `event_timestamp` with `try_to_timestamp`, so a malformed `event_time` becomes NULL instead of failing the query.
- Builds one `is_valid` column from the required-field and value rules, wrapped in `coalesce(..., false)` so a NULL in any checked column counts as invalid. A Kafka value that isn't valid JSON, is empty or is NULL parses to all-NULL fields and is invalid too.
- `event_time` may be at most one hour ahead of the stream's clock (`MAX_CLOCK_SKEW`); a later event is invalid.
- `engagement_score` must be present and `>= 0` on every event; `purchase_probability`, `cart_abandonment_probability` and `fraud_score` must be between 0 and 1.
- Product and price fields are required only on `COMMERCE_EVENT_TYPES` (`product_id`, `product_name`, `original_price`, `discounted_price`, with every range rule holding). Other events may leave them NULL; a value that is present must pass the same range rules.
- On `CART_EVENT_TYPES` (`add_to_cart`, `remove_from_cart`, `checkout_started`, `payment_initiated`, `payment_success`, `payment_failed`, `order_placed`; defined identically in the producer), `cart_value` must be present, `>= 0` and within 0.01 of `discounted_price × quantity`. On other events it must be NULL or `>= 0`.
- Valid rows are appended to Bronze Parquet (`data/bronze/user_events`). Invalid rows are written to the Kafka topic `user_events_dlq` with their original value unchanged (the JSON string, or the raw, empty or NULL value).
- Both sinks trigger every 10 seconds and keep their own checkpoint under `checkpoints/`. The Bronze file sink is exactly-once across restarts (it commits each batch to `_spark_metadata`); the Kafka DLQ sink is at-least-once, so a restart can write an invalid event to `user_events_dlq` twice.
- Before `spark-submit`, `scripts/run_stream.sh` runs `scripts/repair_stream_checkpoints.py`, because Spark 4.1.x crashes on restart (a `NullPointerException` in `KafkaMicroBatchStream.metrics`, [apache/spark#55236](https://github.com/apache/spark/issues/55236)) when a checkpoint holds a planned but uncommitted batch (`offsets/N` without `commits/N`). For the highest such batch: if Bronze's `_spark_metadata` already has `N` or `N.compact`, it writes `commits/N` as a copy of the latest commit; otherwise, and always for the DLQ, it moves `offsets/N` (and its `.crc`) into `checkpoints/<query>/_repaired/`, so Spark replans the batch from commit N-1. It never deletes anything, and a failure stops the start. The step is to be removed once a Spark release fixes the issue.
- There is no streaming deduplication and no watermark. Bronze is lossless; duplicates are removed in Silver.
- SIGINT and SIGTERM only set a stop flag. `main()` installs the handlers first, so a signal during start-up stops the job before any query starts, and installs them again right after the SparkSession is created, because creating the SparkContext replaces the SIGINT handler with PySpark's own (which calls `cancelAllJobs()` from inside the handler). `scripts/run_stream.sh` runs `spark-submit` with SIGINT/SIGTERM ignored, so the JVM is never killed mid-batch, and catches both signals itself: it writes a stop file at a fresh per-run path (`STREAM_STOP_FILE`, removed when the script exits). The driver checks that file at start (and then doesn't start Spark), before starting the queries and every second, so a stop sent before the Python driver is running is not lost. The main loop then stops the running queries and the Spark session and prints `[stream] stopped cleanly`.

## Batch feature computation

Run manually or through the Airflow DAG `project1_personalization_batch_pipeline` (`dags/project1_personalization_pipeline.py`, triggered by hand with `schedule=None`, `max_active_runs=1`, started with `scripts/run_airflow.sh` so every task runs on the project's venv):

1. `reference-data/check_bronze_against_export.py` (the DAG task `verify_bronze_matches_export`, not retried) checks that every distinct `user_id` in Bronze is a customer of the export the dims are built from (`customers.csv` in `SHOP_MASTER_DIR`), reading only Bronze's `user_id` column. It writes nothing and exits 1 on a mismatch, before any dim, Silver, Gold or PostgreSQL table is overwritten. In the DAG it is preceded by a check that the `python` on `PATH` can import `confluent_kafka`, `psycopg` and `pyspark`.
2. `reference-data/create_reference_data.py` writes `data/dim/users` (every customer in `customers.csv`) and `data/dim/products` (every product in `products.csv`, with its brand). Both are built from the data `data-generator/producer.py` loads, including list prices, signup dates and status, so every id the producer can emit has a reference row.
3. `spark-batch/bronze_to_silver.py` drops rows without `user_id`, `event_type` or `event_timestamp`, removes duplicate `event_id`s (`dropDuplicates` keeps an arbitrary row when duplicates differ in content), and left-joins both dims. It keeps the event's own `user_segment` and sets `is_user_reference_missing` and `is_product_reference_missing`; the product flag applies only to events that have a `product_id`.
4. `spark-batch/silver_to_gold.py` aggregates Silver to one row per `user_id` (`data/gold/user_features`, 19 columns). `avg_event_price` and `max_event_price` use commerce events only and are NULL for a user without any. Averages and `total_spend` are computed on DECIMAL and stored as DOUBLE, so reruns on the same Silver give identical feature values; only `features_computed_at` and `recency_hours` change per run. `user_name` and `membership_tier` come from `MAX_BY(..., event_timestamp)`; a tie on the latest timestamp picks an arbitrary value. The last four columns are `total_spend`, `session_count`, `features_computed_at` (one timestamp per run) and `recency_hours` (hours from `last_event_timestamp` to `features_computed_at`).
5. `spark-batch/publish_gold_to_postgres.py` writes Gold to `user_features_staging` over JDBC, then in one PostgreSQL transaction checks that staging is non-empty with unique `user_id`s, adds the primary key, drops `user_features`, renames staging to `user_features` and renames the key to `user_features_pkey`. Any failure rolls back and leaves the live table as it was. The transaction starts with `SET LOCAL lock_timeout = '10s'` (`SWAP_LOCK_TIMEOUT`), so a session holding `user_features` makes the publish fail with a clear message and a non-zero exit after 10 seconds instead of blocking readers indefinitely. With `PUBLISH_REFUSE_SHRINK=1`, which the DAG sets on this task, the same transaction also refuses a staging table with fewer users than the live table (equal, more or a first publish pass), and the publish exits 1 with the live table unchanged.
6. The DAG's last task, `verify_postgres_table`, checks that `user_features` has rows; it does not call the API.

Each batch run reprocesses all of Bronze. Spark sessions run in UTC. Every script resolves `data/` and `logs/` from `PROJECT1_ROOT` (default: the repository root).

## Serving

`api/main.py` reads `user_features` with one connection per request (no pool, 3-second connect timeout):

- `GET /health`: 200 only if PostgreSQL is reachable and `user_features` exists and has rows; otherwise 503 with a short reason.
- `GET /features/{user_id}`: 200 with the user's features (every `user_features` column except `user_name`, `membership_tier` and `features_computed_at`, so including `total_spend`, `session_count` and `recency_hours`), 404 if unknown, 422 if `user_id` is not an integer, 500 if the lookup fails.
- `GET /features?after_user_id=&limit=`: all users in `user_id` order, one page at a time (keyset pagination; `after_user_id` ≥ 0, default 0; `limit` 1–1000, default 100). Returns `{"items", "count", "next_after_user_id"}`; each item has the `/features/{user_id}` fields plus `features_computed_at`, never `user_name` or `membership_tier`. `next_after_user_id` is null on the last page. 422 for invalid parameters, 500 if the lookup fails. The decision-intelligence layer ([AI-Decision-Intelligence-Layer](https://github.com/HimanshuLabs/AI-Decision-Intelligence-Layer)) reads this endpoint (`GET /features`, paged by `user_id`) and holds no database credentials for this pipeline.
- `GET /metrics`: Prometheus metrics.

Error responses never include exception text; details are logged server-side.

## Monitoring

`monitoring/run_monitoring.sh` starts four containers on the host network, each listening on 127.0.0.1 only; `monitoring/stop_monitoring.sh` removes exactly these four. The start script waits for all four endpoints and exits 1 with a hint if one never answers (for example the Kafka exporter when Kafka is down):

- **kafka-exporter (`project1_kafka_exporter`, :9308):** topic offsets for `user_events` and `user_events_dlq`.
- **feature-freshness-exporter (`project1_feature_exporter`, :8001):** `user_features` row count, the age of the newest `last_event_timestamp` (`project1_feature_freshness_seconds`) and the age of the newest `features_computed_at` (`project1_features_computed_age_seconds`, time since the last published Gold run). If the database is unreachable it reports `project1_feature_exporter_up 0` and NaN for the other gauges. Built as `project1-feature-freshness-exporter:1.1`, a fixed tag.
- **Prometheus (`project1_prometheus`, :9090):** scrapes the API, both exporters and itself.
- **Grafana (`project1_grafana`, :3000):** dashboard provisioned from `monitoring/grafana/dashboards/project1-observability.json`.

Spark keeps its Kafka offsets in checkpoints, not in a consumer group, so the dashboard has no consumer-group lag panel (it would never have data); it shows messages written per topic instead. Feature freshness and features computed age are the signals for the batch-to-serving path. The API leaves its own `/metrics` scrapes out of every HTTP metric (`excluded_handlers=["/metrics"]`), so the request-rate, p95 latency (from the high-resolution histogram) and 5xx panels show real API traffic only.
