# Real-Time Personalization Pipeline

[![CI](https://github.com/HimanshuLabs/Real-time-personalization-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/HimanshuLabs/Real-time-personalization-pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Spark](https://img.shields.io/badge/Spark-4.1.2-E25A1C)
![Kafka](https://img.shields.io/badge/Kafka-4.1.2-231F20)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

An end-to-end data pipeline that turns a stream of e-commerce events into per-user personalization features and serves them over an API.

**Kafka → Spark Structured Streaming → Bronze → Silver → Gold → PostgreSQL → FastAPI**, with a dead-letter topic, an atomic serving-table swap, Airflow orchestration and Prometheus/Grafana monitoring. Everything runs locally with Docker. The events are simulated, as the live clickstream of the shop in the batch lakehouse ([Batch-Lakehouse-Marketing-Analytics](https://github.com/HimanshuLabs/Batch-Lakehouse-Marketing-Analytics)): its real customer and product master data, with simulated behaviour.

> Streaming ingestion lands and validates events continuously. Features are computed in batch and published atomically, so the API always reads a complete, consistent table.

## Scale run

The same code has been run end to end on the batch lakehouse's full customer base, about 4 lakh customers instead of the default 497. **Synthetic data, one local machine (16 threads), not production.**

| | Count |
|---|---|
| Source: export of the batch lakehouse's warehouse | 3,99,997 customers and 147 products |
| Active users | 3,83,965 |
| Sessions | 6,52,740 |
| Events | 39,71,944 |
| Kafka `user_events`, Bronze, Silver | 39,71,944 each (no loss) |
| Gold and PostgreSQL `user_features` | 3,83,965 users; their `total_events` add up to 39,71,944 |
| Dead-letter topic | 0 |
| Feature API | `GET /features` walked end to end: 3,83,965 users |
| Verification | 31 of 31 checks passed |

Counts use Indian digit grouping: 39,71,944 is 3,971,944.

What the 31 checks prove:

- **Kafka and Bronze (4 checks):** the producer's delivered count equals the Kafka end offsets, Bronze has exactly one row per Kafka record with no repeated `event_id`, and the dead-letter topic is empty. An empty dead-letter topic is the expected result here: every Kafka record is in Bronze, so none was invalid.
- **Dims and users (7):** the users dim holds every exported customer; the active users in the dim are exactly the users the producer was given; every active user has at least one event and no inactive user has any; Bronze holds exactly the planned number of sessions.
- **Silver (8):** the same row count as Bronze, no repeated `event_id`, no missing user or product reference, and the Silver report agrees (0 rows dropped by filters, 0 duplicates removed).
- **Gold (4):** one row per active user; the users' `total_events` add up to the Bronze row count and their `session_count` to the session count.
- **PostgreSQL publish (5):** `user_features` has Gold's 19 columns in the same order and the same number of rows, and the two tables are identical on every column of every row, compared in both directions.
- **Export consistency (3):** every active user's profile in the events is a row of the export, and every published name and membership tier comes from the export.

The API walk is a separate check, not one of the 31. The check-by-check counts are in [docs/scale_run_evidence.md](docs/scale_run_evidence.md), and the settings the run needed are in [Running at scale](#running-at-scale).

**What CI proves.** CI runs the tests at the default size (the committed export, about 500 customers) on every push and pull request. It proves the code, not the scale. The numbers above come from one local run, checked by a local verification script.

## Highlights

- **The live clickstream of a real shop.** Users and products are the batch lakehouse's: the committed export of its warehouse has 497 customers and 147 products, with the same IDs, names, cities and list prices. The generator runs up to 20 concurrent sessions; each follows one customer and one product through a purchase funnel (entry, search, product views, cart, checkout, payment, order) at the product's INR list price. Inactive accounts and discontinued products never appear in the stream.
- **No silent data loss.** Every event lands in exactly one place: Bronze if valid, the dead-letter topic if not. Malformed timestamps and NULLs are routed to the DLQ instead of crashing the stream or vanishing.
- **Lossless Bronze, dedup in Silver.** Bronze keeps every valid event; duplicates are removed by `event_id` in Silver, so no late event is ever dropped.
- **Spend, session and recency features.** Gold adds `total_spend`, `session_count` and `recency_hours` to the per-user counts. Price features cover commerce events only, and averages are computed on DECIMAL, so rerunning Gold on the same Silver gives identical feature values; only `features_computed_at` and `recency_hours` change from run to run.
- **Atomic publishing.** Gold is written to a staging table, validated (non-empty, unique `user_id`) and swapped into the live table in one PostgreSQL transaction. A failed publish leaves the old table untouched.
- **Graceful operations.** Ctrl+C stops the stream cleanly, and a restart resumes from its checkpoints, after a checkpoint repair that works around a Spark 4.1 restart crash. Bronze gets no duplicates or gaps across a restart, because Spark's file sink is exactly-once; the DLQ sink is at-least-once, so a restart can write an invalid event to the DLQ twice. Kafka and PostgreSQL data persist on Docker volumes.
- **Contracts enforced by tests.** 338 tests run the real Spark scripts on small datasets and check exact results, plus the event schema, session and funnel rules, validation rules, API behaviour and the publish swap and the export from the batch lakehouse against a real PostgreSQL. All 338 pass locally with `P1_TEST_DATABASE_URL` set (0 skipped). CI runs them on every push to `main` and every pull request to `main`, and there they always all run.
- **Observable.** Prometheus scrapes the API and two exporters; Grafana shows API request rate, p95 latency and 5xx rate, Kafka throughput, DLQ rate, feature freshness and the age of the last published feature run.

## Architecture

```mermaid
flowchart LR
    GEN["producer.py<br/>session-based events"] -- "key = user_id" --> TOPIC["Kafka<br/>user_events"]
    TOPIC --> STREAM["read_stream.py<br/>parse + validate"]
    STREAM -- "valid" --> BRONZE[("Bronze<br/>Parquet")]
    STREAM -- "invalid" --> DLQ["Kafka<br/>user_events_dlq"]
    REF["create_reference_data.py"] --> DIMS[("users / products<br/>dims")]
    BRONZE --> SILVER[("Silver<br/>dedup + enrichment")]
    DIMS --> SILVER
    SILVER --> GOLD[("Gold<br/>one row per user")]
    GOLD -- "staging swap" --> PG[("PostgreSQL<br/>user_features")]
    PG --> API["FastAPI"]
```

| Stage | Code | Output |
|---|---|---|
| Event generation | `data-generator/producer.py` | Kafka `user_events` (3 partitions, keyed by `user_id`) |
| Validation and landing | `spark-streaming/read_stream.py` (via `scripts/run_stream.sh`) | `data/bronze/user_events`, Kafka `user_events_dlq` |
| Bronze check | `reference-data/check_bronze_against_export.py` | exit 1 if Bronze has users that are not customers of the export; writes nothing |
| Reference data | `reference-data/create_reference_data.py` | `data/dim/users`, `data/dim/products` |
| Dedup and enrichment | `spark-batch/bronze_to_silver.py` | `data/silver/user_events` |
| User features | `spark-batch/silver_to_gold.py` | `data/gold/user_features` |
| Publish | `spark-batch/publish_gold_to_postgres.py` | PostgreSQL `user_features` |
| Serving | `api/main.py` | HTTP API |
| Orchestration | `dags/project1_personalization_pipeline.py` | Airflow DAG `project1_personalization_batch_pipeline` |
| Monitoring | `monitoring/run_monitoring.sh` | Prometheus and Grafana |

The full diagram, including monitoring, is in [docs/diagrams/project1_system_architecture.mmd](docs/diagrams/project1_system_architecture.mmd), and component details are in [docs/architecture.md](docs/architecture.md).

## Example run

One end-to-end run at the default size (the committed 497-customer export), following the quick start below. The run on the batch lakehouse's full customer base is in [Scale run](#scale-run).

| Stage | Result |
|---|---|
| Kafka `user_events` | 2,793 events across 3 partitions (971 / 964 / 858) |
| Dead-letter topic | 0 events |
| Stream restart | the stream crashed on a restart (Spark issue [#55236](https://github.com/apache/spark/issues/55236)); the checkpoint repair ran and the stream resumed |
| Bronze | 2,793 rows, 2,793 distinct `event_id`s |
| Silver | 2,793 rows (0 dropped by filters, 0 duplicates), 0 missing user references, 0 missing product references |
| Gold and PostgreSQL | 321 users in 545 sessions, one row per user |
| Purchases | 26 |
| Total spend | 90,052.69 INR |
| Browse-only users | 13 users with no commerce event, so their price features are NULL |
| Match with the batch lakehouse | every published user matches its customer there (name and tier) |
| API | `/health` 200; `/features/22` 200; `/features/1` 404 (customer 1 was quarantined in the batch lakehouse, so this pipeline never produces it); `/features/abc` 422; `GET /features` paged all 321 users in 4 requests (`limit=100`), each once, in ascending `user_id` order, with no `user_name`, `membership_tier` or `email` |

Counts vary from run to run, because the producer generates random sessions.

### See the dead-letter topic in action

With the stream running, send a malformed event and watch it land in `user_events_dlq` instead of Bronze:

```bash
echo '{"event_id":"demo-bad-1","event_time":"not-a-date"}' | docker exec -i project1_kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic user_events
sleep 15
docker exec project1_kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic user_events_dlq
```

One partition of `user_events_dlq` goes up by 1.

## Tech stack

| Layer | Technology |
|---|---|
| Messaging | Apache Kafka 4.1.2 (KRaft, single broker) |
| Processing | Apache Spark 4.1.2 (Structured Streaming and batch), Parquet |
| Serving | PostgreSQL 16.14, FastAPI 0.115, psycopg 3 |
| Orchestration | Apache Airflow 3.2 |
| Monitoring | Prometheus 3.5, Grafana 13, kafka-exporter 1.10, custom feature-freshness exporter |
| Quality | pytest, Ruff, GitHub Actions |
| Runtime | Docker Compose, Python 3.11+, Java 17 |

## Quick start

Requirements: Docker, Java 17 and Python 3.11 or newer. Run everything from the repository root.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-ci.txt -r api/requirements.txt
```

1. Start Kafka (`127.0.0.1:9092`, container `project1_kafka`) and PostgreSQL (`127.0.0.1:5433`, container `project1_postgres`):

   ```bash
   docker compose up -d
   ```

2. Create both topics with 3 partitions:

   ```bash
   docker exec project1_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic user_events --partitions 3 --replication-factor 1
   docker exec project1_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic user_events_dlq --partitions 3 --replication-factor 1
   ```

3. Start the stream in its own terminal (stop it with Ctrl+C):

   ```bash
   scripts/run_stream.sh
   ```

4. Produce events in another terminal (stop with Ctrl+C; it prints a `delivered / failed / undelivered` summary and exits 0 if every event was delivered, 1 otherwise; a second Ctrl+C during that final flush is ignored):

   ```bash
   python data-generator/producer.py
   ```

5. Build the reference data, Silver and Gold:

   ```bash
   spark-submit reference-data/create_reference_data.py
   spark-submit spark-batch/bronze_to_silver.py
   spark-submit spark-batch/silver_to_gold.py
   ```

6. Publish to PostgreSQL:

   ```bash
   spark-submit --packages org.postgresql:postgresql:42.7.4 spark-batch/publish_gold_to_postgres.py
   ```

7. Start the API in terminal A. `uvicorn` keeps running in the foreground until Ctrl+C:

   ```bash
   uvicorn api.main:app --host 127.0.0.1 --port 8000
   ```

   Query it from terminal B (activate the venv there too if you run Python):

   ```bash
   curl http://127.0.0.1:8000/health
   curl "http://127.0.0.1:8000/features?limit=3"
   ```

   The second call returns the first three users' features. For a single user, call `GET /features/{user_id}` with any `user_id` from those results.

8. Start monitoring (optional). Prometheus runs at `127.0.0.1:9090` and Grafana at `127.0.0.1:3000`; `monitoring/stop_monitoring.sh` removes the four `project1_*` monitoring containers (`project1_kafka_exporter`, `project1_feature_exporter`, `project1_prometheus`, `project1_grafana`) and nothing else.

   ```bash
   monitoring/run_monitoring.sh
   ```

The batch scripts resolve `data/` and `logs/` from `PROJECT1_ROOT` (default: the repository root), like the stream, so they can be run from any directory. `spark-batch/query_gold_features.py` runs a few example queries on Gold.

### Running at scale

The quick start above runs at the default size. The [scale run](#scale-run) used the same scripts in the same order, with the settings below. None of them is needed at the default size; left unset, every script behaves as in the quick start.

In every terminal, before any command:

```bash
source venv/bin/activate
ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)"
export SCALE_TMP=/path/without/spaces/p1-scale-tmp
mkdir -p "$SCALE_TMP/spark" "$SCALE_TMP/tmp"
export SPARK_LOCAL_DIRS="$SCALE_TMP/spark"
export TMPDIR="$SCALE_TMP/tmp"
export SPARK_SUBMIT_OPTS="-Djava.io.tmpdir=$SCALE_TMP/tmp"
export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python
```

1. Export the batch lakehouse's customers and products into a git-ignored folder. Its database must be running and `P2_EXPORT_PASSWORD` set in the environment:

   ```bash
   SHOP_MASTER_DIR=reference-data/shop_master_full python reference-data/export_shop_master.py
   ```

2. Start Kafka and PostgreSQL and create both topics (quick start steps 1 and 2), then start the stream in its own terminal with a larger driver heap and bounded micro-batches:

   ```bash
   SPARK_DRIVER_MEMORY=4g STREAM_MAX_OFFSETS_PER_TRIGGER=300000 scripts/run_stream.sh
   ```

3. Produce to a session target in another terminal, with no pause and no per-event print. The seed is any integer; the same seed repeats the same users, sessions and events, so the counts of the scale run belong to its own seed:

   ```bash
   SHOP_MASTER_DIR=reference-data/shop_master_full PRODUCER_ONE_PASS=1 PRODUCER_SEED=1 PRODUCER_TOTAL_SESSIONS=652740 PRODUCER_PAUSE_SECONDS=0 PRODUCER_QUIET=1 python data-generator/producer.py
   ```

4. The producer exits on its own once every session has ended and prints its summary. The stream has caught up when the offsets of its latest committed micro-batch add up to the same number as Kafka's end offsets of `user_events`, and that number equals the producer's `delivered` count. Run both from the repository root and compare:

   ```bash
   docker exec project1_kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic user_events | awk -F: '{s+=$3} END{print s}'
   n=$(ls checkpoints/bronze_user_events/commits | grep -E '^[0-9]+$' | sort -n | tail -1); tail -n 1 "checkpoints/bronze_user_events/offsets/$n" | grep -oE '"[0-9]+":[0-9]+' | awk -F: '{s+=$2} END{print s}'
   ```

   If the second number is lower, the stream is still reading the backlog (at most `STREAM_MAX_OFFSETS_PER_TRIGGER` records per 10-second micro-batch); run it again later. Then stop the stream with Ctrl+C in its terminal (see *Operations*), so the batch jobs read a Bronze that no longer changes and do not share the machine with the stream.

5. Check that every Bronze user is a customer of the export (it writes nothing and exits 1 on a mismatch), then build the dims, Silver and Gold, and publish:

   ```bash
   export SPARK_DRIVER_MEMORY=5g
   SHOP_MASTER_DIR=reference-data/shop_master_full spark-submit reference-data/check_bronze_against_export.py
   SHOP_MASTER_DIR=reference-data/shop_master_full spark-submit reference-data/create_reference_data.py
   spark-submit spark-batch/bronze_to_silver.py
   spark-submit spark-batch/silver_to_gold.py
   PUBLISH_KEEP_PREVIOUS=1 spark-submit --packages org.postgresql:postgresql:42.7.4 spark-batch/publish_gold_to_postgres.py
   ```

| Setting | Why |
|---|---|
| `SHOP_MASTER_DIR` on the export, the producer, the Bronze check and the reference dims | These four read the shop CSVs. Unset, each one silently uses the committed 497-customer export, so it is written on every command that reads it. |
| `PRODUCER_PAUSE_SECONDS=0` | The default pause is a random 0.05 to 1.5 seconds after every event, which would take weeks for 39,71,944 events. |
| `PRODUCER_QUIET=1` | The default prints every event as indented JSON. The progress and summary lines on stderr stay. |
| `SPARK_DRIVER_MEMORY` | The scripts start Spark in local mode with Spark's default driver heap of 1 GB. The run used 4g for the stream and 5g for the batch jobs. `scripts/run_stream.sh` passes no extra options to `spark-submit`, so the environment variable is the way to set it. |
| `STREAM_MAX_OFFSETS_PER_TRIGGER` | Caps the Kafka records per micro-batch, so a backlog is read in bounded batches. |
| `SPARK_LOCAL_DIRS`, `TMPDIR`, `java.io.tmpdir` | Spark's shuffle files and the JVM's temporary files go to `/tmp` by default. On a machine where `/tmp` is small or memory-backed, point all three at a folder on a real disk. |
| `ulimit -n 65536` | Raises the shell's open-file limit; a common default is 1024. If the hard limit is lower, the shell's hard limit is used. |
| `PYSPARK_PYTHON=python`, `PYSPARK_DRIVER_PYTHON=python` | The plain name, with the venv first on `PATH`. Do not set them to a full path if the path has a space in it: Spark's launcher scripts break on it. |
| `PUBLISH_KEEP_PREVIOUS=1` on the publish | The table it replaces is kept as `user_features_previous`, so a bad publish can be rolled back (see *Operations*, Rollback). |
| No spaces in `SCALE_TMP` | For the same reason, and because `SPARK_SUBMIT_OPTS` is split on spaces. The repository folder itself may have spaces: the scripts quote their own paths. |

The Airflow DAG runs step 5. Start Airflow with `scripts/run_airflow.sh` from a terminal that has the settings above: its tasks inherit that environment, including `SHOP_MASTER_DIR` and `SPARK_DRIVER_MEMORY` (see *Operations*, Airflow).

## How it works

### Synthetic events

**The shop.** This pipeline is the live clickstream of the batch lakehouse's shop. Its users and catalogue come from `reference-data/shop_master/customers.csv` (497 customers) and `products.csv` (147 products: 138 active, 9 discontinued), a read-only export of its warehouse: the current SCD2 rows of `warehouse.dim_customer` and `warehouse.dim_product`, exported on 2026-11-18 from its `main` at `c1e117a`. The events therefore carry those customer IDs, names, cities, products and list prices; only the behaviour is simulated (no Faker). The producer loads both files once from `PROJECT1_ROOT` and exits 2 with a one-line error if a file or a required column is missing.

**Role next to the batch lakehouse.** This pipeline's events are live product behaviour, for personalization, ML and AI. The batch lakehouse's own web events are its marketing feed, which ties sessions to campaigns. Both share the same customers and products.

`data-generator/producer.py` keeps up to 20 sessions active (`MAX_ACTIVE_SESSIONS`). Each loop picks one active session at random, emits its next event with the current UTC time, and replaces a finished session with a new one. A session belongs to one user (within one producer process, a user has at most one active session at a time), shares one `session_id` across its events, and follows one product, whose price is fixed for the session.

Each session follows this funnel, one event per step:

1. `login` (30%) or `home_page_view` (70%); after `home_page_view`, sometimes `ad_impression` (20%), then sometimes `ad_click` (25% of impressions).
2. `search` (50%), with a query that fits the session's category.
3. One to three `product_view`s. Each may be preceded by `recommendation_impression` → `recommendation_click` (25%) and followed by `image_zoom` (20%) or `wishlist_add` (5%; half of those sessions stop there).
4. `add_to_cart` (18% of sessions, quantity 1 to 3). Then either `remove_from_cart` (10% of carts; the session stops) or `checkout_started` (60% of the remaining carts).
5. After checkout: `coupon_applied` (20%), then `payment_initiated`, then `payment_success` + `order_placed` (85%) or `payment_failed`. After an order, sometimes `review_submitted` or `rating_given` (5%).
6. `logout` ends 40% of sessions.

Products and prices:

- **Users:** the customer IDs in `customers.csv`. They are not contiguous (IDs 1–3 are quarantined in the batch lakehouse), so a session picks from that list. A profile is the customer's row: `user_name`, `email`, `gender`, `age`, `membership_tier`, `loyalty_points`, `preferred_language`, `home_city`, `home_state`, `user_segment` (the batch lakehouse's `customer_segment`) and `is_prime_user`.
- **Catalog:** the 147 products of `products.csv` in 6 categories (Electronics, Fashion, Home, Beauty, Fitness, Books); `product_id` is the batch lakehouse's `product_id`.
- **Prices:** every product's list price is its `original_price` in `products.csv` (`PRODUCT_REFERENCE_PRICES`; INR, the events carry no currency field), and `CATEGORY_PRICE_BANDS` is the lowest and highest list price per category. A session's `original_price` is the list price; only the discount varies per session: `discount_percent` is 0–15% in 85% of sessions and 15–40% otherwise.
- **Product fields:** `PRODUCT_FIELDS` (`product_id`, `product_name`, `category`, `quantity`, `original_price`, `discount_percent`, `discounted_price`, `cart_value`) are NULL on non-commerce events. On commerce events, `quantity` is 1 until `add_to_cart` and the cart quantity from then on. `cart_value` = `discounted_price × quantity` is set only on cart events (`add_to_cart`, `remove_from_cart`, `checkout_started`, `payment_initiated`, `payment_success`, `payment_failed`, `order_placed`).
- **Status:** `producer.py` defines `account_status` (a seeded draw per customer, 1 account in 25 inactive; 24 of the 497 customers) and `is_product_active` (`product_status` is `active` in `products.csv`; 9 products are discontinued). The reference dims use the same functions. The producer never starts a session for an inactive account and never picks a discontinued product.

Other fields follow the session and user state:

- `recommendation_rank`, `recommendation_clicked` and `recommendation_algorithm` are set only on recommendation events; a click repeats the rank and algorithm of its impression. `click_position` is set only on clicks.
- `search_query` is set only on `search` events.
- `time_since_last_event_ms` is the real gap since the same user's previous event. It is NULL on a user's first event since the producer started, and for a gap too large for Bronze's 32-bit column.
- `session_duration_sec` counts seconds since the session's first event; `items_viewed_in_session` and `repeat_product_view_count` count the session's product views so far.
- `purchase_probability` is a logistic function of the funnel stage plus small noise, so it rises from browsing to payment and stays strictly between 0 and 1.
- `country` is `"India"` on about 95% of events, and `city` is the user's `home_city` on about 85%. The other Indian events use another city where the shop has customers; events from abroad use a fixed list of foreign (country, city) pairs.
- `inventory_remaining` is a per-product stock level (starting at 50 to 500) that drops by the cart quantity on `payment_success` and is restocked once it falls below 5. It is NULL on non-commerce events.
- `user_journey_stage` follows the event type (`JOURNEY_STAGE_BY_EVENT_TYPE`): `discovery` for `home_page_view`, `login`, `search`, `ad_impression` and `ad_click`; `consideration` for product views, `image_zoom`, `wishlist_add` and recommendation events; `checkout` from `add_to_cart` through the payment events; `post_purchase` for `order_placed`, `review_submitted` and `rating_given`. `logout` keeps the session's latest stage. The `comparison` value in `USER_JOURNEY_STAGES` is never emitted.
- `device_type`, `operating_system`, `browser`, `network_type` and `app_version` are drawn once per session and fit together: mobile and tablet run `android` or `ios` on `wifi`, `4g` or `5g`; desktop runs `windows`, `macos` or `linux` on `wifi` or `ethernet`; `safari` appears only on `ios` and `macos`.
- `payment_method` is drawn once per session and set only on `payment_initiated`, `payment_success`, `payment_failed` and `order_placed` (`PAYMENT_EVENT_TYPES`); it is NULL on every other event.

**Event types.** The stream accepts all 26 types in `EVENT_TYPES`. The producer emits 21 of them: `home_page_view`, `login`, `logout`, `search`, `ad_impression`, `ad_click`, `product_view`, `image_zoom`, `wishlist_add`, `recommendation_impression`, `recommendation_click`, `add_to_cart`, `remove_from_cart`, `checkout_started`, `coupon_applied`, `payment_initiated`, `payment_success`, `payment_failed`, `order_placed`, `review_submitted` and `rating_given`.

`COMMERCE_EVENT_TYPES`, the events that concern a product, are `product_view`, `image_zoom`, `wishlist_add`, `wishlist_remove`, `add_to_cart`, `remove_from_cart`, `checkout_started`, `payment_initiated`, `payment_success`, `payment_failed`, `order_placed`, `order_cancelled`, `return_requested`, `review_submitted`, `rating_given`, `recommendation_impression`, `recommendation_click` and `search_result_click`. Tests keep `EVENT_TYPES` and `COMMERCE_EVENT_TYPES` identical in the producer and the stream, and `COMMERCE_EVENT_TYPES` identical in Gold.

<details>
<summary><b>Example event</b> (schema 4.0, an <code>add_to_cart</code> for customer 196 of the batch lakehouse)</summary>

```json
{
  "event_id": "17d6fbd9-ca5d-4b3f-a041-776ed0a1d6c2",
  "session_id": "338109cf-e8c1-4244-b9de-99949a2d5959",
  "user_id": 196,
  "user_name": "Samarth Kunda",
  "email": "samarth.kunda196@example.net",
  "gender": "Male",
  "age": 34,
  "membership_tier": "Bronze",
  "loyalty_points": 1618,
  "preferred_language": "Marathi",
  "home_city": "Nashik",
  "home_state": "Maharashtra",
  "event_time": "2026-11-11T07:09:53.610454+00:00",
  "event_type": "add_to_cart",
  "user_journey_stage": "checkout",
  "user_segment": "Working Professional",
  "is_prime_user": false,
  "product_id": 92,
  "product_name": "Vastrio Cotton Kurta",
  "category": "Fashion",
  "quantity": 1,
  "original_price": 1549.0,
  "discount_percent": 0.66,
  "discounted_price": 1538.78,
  "cart_value": 1538.78,
  "inventory_remaining": 494,
  "search_query": null,
  "time_on_page_sec": 64.65,
  "scroll_depth_percent": 42,
  "hover_duration_ms": 12580,
  "session_duration_sec": 17,
  "items_viewed_in_session": 2,
  "repeat_product_view_count": 2,
  "time_since_last_event_ms": 15172,
  "recommendation_rank": null,
  "recommendation_clicked": null,
  "recommendation_algorithm": null,
  "click_position": null,
  "engagement_score": 4.25,
  "purchase_probability": 0.3436,
  "cart_abandonment_probability": 0.8169,
  "ab_test_group": "A",
  "payment_method": null,
  "device_type": "tablet",
  "operating_system": "android",
  "browser": "chrome",
  "network_type": "4g",
  "app_version": "1.9",
  "traffic_source": "instagram_ads",
  "campaign_id": 7893,
  "api_latency_ms": 91,
  "page_load_time_ms": 5176,
  "fraud_score": 0.2179,
  "country": "India",
  "city": "Nashik",
  "ip_address": "62.105.10.253",
  "schema_version": "4.0",
  "source": "real-time-personalization-generator"
}
```

</details>

### Streaming ingestion and validation

`scripts/run_stream.sh` starts `spark-streaming/read_stream.py` with the Spark Kafka connector (`spark-sql-kafka-0-10_2.13:4.1.2`). The stream reads `user_events` (from the earliest offset on first start, then from its checkpoint), parses each message with a typed schema and routes it every 10 seconds:

- **Valid** events are appended to Bronze Parquet with their 58 fields plus `event_timestamp`.
- **Invalid** events are written to `user_events_dlq` as their original JSON.

An event is valid only if all of these hold:

- **Required fields:** `event_id`, `session_id`, `user_id`, `event_type`, `event_time`, `source` and `schema_version` are present.
- **Timestamp:** `event_time` parses (`try_to_timestamp`; a malformed value becomes NULL and fails the check instead of stopping the stream), and it is at most one hour ahead of the stream's clock (`MAX_CLOCK_SKEW`), so an event from the future can't produce negative recency or make features look fresh.
- **Contract:** `event_id` is not blank, `event_type` is one of the 26 `EVENT_TYPES`, `schema_version` is `"4.0"`, and `user_id >= 1`.
- **Scores:** `purchase_probability`, `cart_abandonment_probability` and `fraud_score` are each between 0 and 1; `engagement_score` is present and `>= 0`.
- **Product and price fields:** the rules are `quantity >= 1`, `original_price >= 0`, `discounted_price >= 0`, `discounted_price <= original_price` and `0 <= discount_percent <= 100`.
  - **Commerce events** (`COMMERCE_EVENT_TYPES`) must have `product_id`, `product_name`, `original_price` and `discounted_price`, and every rule must hold (a NULL `quantity` or `discount_percent` fails its rule).
  - **Other events** may leave product and price fields NULL; a value that is present must pass the same rules.
- **Cart value:** on cart events (`CART_EVENT_TYPES`: `add_to_cart`, `remove_from_cart`, `checkout_started`, `payment_initiated`, `payment_success`, `payment_failed`, `order_placed`), `cart_value` is present, `>= 0` and within 0.01 of `discounted_price × quantity` (the producer rounds it to 2 decimals). On other events it is NULL or `>= 0`. This protects `total_spend`, which sums `cart_value` over `payment_success`.

All rules feed one `is_valid` column wrapped in `coalesce(..., false)`, so a NULL in any checked column, a message that isn't valid JSON, an empty value or a NULL Kafka value goes to the DLQ instead of disappearing from both outputs.

**Checkpoint repair before each start.** On Spark 4.1.x, restarting a query whose checkpoint holds a planned but uncommitted batch (`offsets/N` without `commits/N`, which a stop can leave behind) crashes with a `NullPointerException` in `KafkaMicroBatchStream.metrics`: replaying that batch calls `metrics()` before `latestOffset()` ([apache/spark#55236](https://github.com/apache/spark/issues/55236)). So `scripts/run_stream.sh` first runs `scripts/repair_stream_checkpoints.py`, which looks only at the highest batch of each checkpoint under `PROJECT1_ROOT`. If Bronze's file sink already recorded that batch (`data/bronze/user_events/_spark_metadata/N` or `N.compact`), the data is already in Bronze, so the script writes `commits/N` as a copy of the latest commit file. Otherwise, and always for the DLQ (an at-least-once Kafka sink), it moves `offsets/N` and its `.N.crc` into `checkpoints/<query>/_repaired/`, and Spark replans batch N from commit N-1. It never deletes anything, prints one line per action and does nothing when no batch is pending. If it fails, the stream does not start.

### Batch feature computation

1. **Bronze check:** `check_bronze_against_export.py` reads only the `user_id` column of Bronze and checks that every distinct user is a customer of the export the dims are built from (`customers.csv` in `SHOP_MASTER_DIR`). It writes nothing. If Bronze was produced from another export, it exits 1 with the number of missing users and the first five, before any dim, Silver, Gold or PostgreSQL table is overwritten.
2. **Reference data:** `create_reference_data.py` builds the users dim (every customer in `customers.csv`) and the products dim (every product in `products.csv`, brand included) from the data the producer loads, with its list prices and status functions, so every id the producer can emit has a reference row. `signup_date` comes from the CSV; `device_preference` and `account_status` use fixed seeds, so the output is identical on every run.
3. **Silver:** `bronze_to_silver.py` drops rows missing `user_id`, `event_type` or `event_timestamp`, removes duplicate `event_id`s, left-joins both dims and keeps the event's own `user_segment`. Its report (`logs/silver_enrichment_report.json`) counts `bronze_rows` and `rows_dropped_by_filters` in one pass over Bronze, `duplicates_removed`, and the Silver rows and missing references in one pass over the Silver actually written; the sample it prints is read back from that Silver too, so the dedup and joins run once. `is_user_reference_missing` flags users without a dim row; `is_product_reference_missing` flags events that have a `product_id` without a dim row, so non-commerce events are never flagged.
4. **Gold:** `silver_to_gold.py` aggregates Silver to one row per `user_id`. Price features use commerce events only. Averages and `total_spend` are computed on DECIMAL, rounded half up and stored as DOUBLE, so the result doesn't depend on the order Spark adds the rows. One `features_computed_at` timestamp is taken per run, and `recency_hours` is measured against it. The count and sample it prints are read back from the Gold actually written, so the aggregation runs once.
5. **Publish:** `publish_gold_to_postgres.py` writes Gold to `user_features_staging` over JDBC, then in one transaction checks that staging is non-empty with unique `user_id`s, adds the primary key, drops `user_features`, renames staging to `user_features` and renames the key to `user_features_pkey`. Any failure rolls back and leaves the live table as it was. Queries already running finish first, because the swap waits for their locks; queries that arrive during the swap wait for its brief lock and then read the new table. The swap waits at most 10 seconds for a lock (`SET LOCAL lock_timeout`, `SWAP_LOCK_TIMEOUT`): if a session holds `user_features` longer, the publish rolls back, leaves the live table unchanged and exits non-zero with a message saying so, instead of blocking the swap and every API read queued behind it. With `PUBLISH_REFUSE_SHRINK=1` (the Airflow DAG sets it), the same transaction also refuses a staging table with fewer users than the live `user_features` holds: the publish rolls back and exits 1 with a message saying so, so a bad run cannot replace a good table. Equal or more users, or a first publish, pass. Unset, the swap runs exactly as described above. With `PUBLISH_KEEP_PREVIOUS=1`, the replaced table is kept as `user_features_previous` (see *Operations*, Rollback).

Every Spark job runs with `spark.sql.session.timeZone=UTC`. The Airflow DAG `project1_personalization_batch_pipeline` runs these steps when triggered by hand (`schedule=None`, so unpausing it starts nothing), with `max_active_runs=1`:
`verify_bronze_exists → verify_bronze_matches_export → create_reference_data → bronze_to_silver → verify_silver_exists → silver_to_gold → verify_gold_exists → gold_to_postgres → verify_postgres_table`. `verify_bronze_matches_export` first checks that the `python` on `PATH` can import `confluent_kafka`, `psycopg` and `pyspark` (it fails with a hint to start Airflow with `scripts/run_airflow.sh` otherwise), then runs the Bronze check; it is not retried, because a mismatch fails the same way every time. The last task checks that the PostgreSQL `user_features` table has rows; it does not call the API.

### Feature table: PostgreSQL `user_features`

| Column | Type | Meaning |
|---|---|---|
| `user_id` | integer | primary key (`user_features_pkey`) |
| `user_name` | text | from the user's latest event |
| `membership_tier` | text | from the user's latest event |
| `total_events` | bigint | all events |
| `page_view_count` | bigint | `home_page_view` events |
| `product_view_count` | bigint | `product_view` events |
| `add_to_cart_count` | bigint | `add_to_cart` events |
| `purchase_count` | bigint | `payment_success` events |
| `search_count` | bigint | `search` events |
| `avg_event_price` | double precision | average `discounted_price` over commerce events, 2 decimals; NULL for a user without commerce events |
| `max_event_price` | double precision | highest `discounted_price` over commerce events, 2 decimals; NULL for a user without commerce events |
| `avg_engagement_score` | double precision | over all events, 2 decimals |
| `avg_purchase_probability` | double precision | over all events, 4 decimals |
| `last_event_timestamp` | timestamp with time zone | latest event time |
| `unique_products_interacted` | bigint | distinct product names (0 for a user without commerce events) |
| `total_spend` | double precision | sum of `cart_value` over `payment_success` events, 2 decimals; 0 if none |
| `session_count` | bigint | distinct `session_id`s |
| `features_computed_at` | timestamp with time zone | when the Gold run computed the features; the same for every row |
| `recency_hours` | double precision | hours from `last_event_timestamp` to `features_computed_at`, 2 decimals |

### API

| Endpoint | Status | Body |
|---|---|---|
| `GET /` | 200 | service message |
| `GET /health` | 200 | `{"status": "healthy", "postgres": "connected"}` |
| `GET /health` | 503 | `{"detail": "database unavailable"}`, `"user_features missing"` or `"user_features empty"` |
| `GET /features/{user_id}` | 200 | `{"user_id": ..., "features": {...}}` |
| `GET /features/{user_id}` | 404 | `{"detail": "No features found for user_id=<id>"}`; also for any id outside PostgreSQL's `integer` range (the type of `user_id`), which simply matches no row |
| `GET /features/{user_id}` | 422 | `user_id` is not an integer |
| `GET /features/{user_id}` | 500 | `{"detail": "Feature lookup failed"}` |
| `GET /features?after_user_id=<int>&limit=<int>` | 200 | `{"items": [...], "count": n, "next_after_user_id": <id or null>}`; an `after_user_id` at or past the last `user_id`, however large, gives an empty last page (`{"items": [], "count": 0, "next_after_user_id": null}`) |
| `GET /features?after_user_id=<int>&limit=<int>` | 422 | `after_user_id` below 0, `limit` outside 1–1000, or either not an integer |
| `GET /features?after_user_id=<int>&limit=<int>` | 500 | `{"detail": "Feature lookup failed"}` |
| `GET /metrics` | 200 | Prometheus metrics |

`features` contains every column above except `user_name`, `membership_tier` and `features_computed_at`. That is `user_id`, the six counts, `avg_event_price`, `max_event_price`, `last_event_timestamp`, `unique_products_interacted`, `avg_engagement_score`, `avg_purchase_probability`, `total_spend`, `session_count` and `recency_hours`. Error responses never include exception text; details are logged server-side.

`GET /features` pages through the whole table in `user_id` order (keyset pagination: `WHERE user_id > after_user_id ORDER BY user_id`; defaults `after_user_id=0`, `limit=100`). Each item has the same fields as `features` above plus `features_computed_at`. The API fetches `limit + 1` rows and returns `limit`; the extra row only tells whether another page exists. Pass `next_after_user_id` back as `after_user_id` until it is `null`, which happens on the last page, so a walk never ends with an empty request. It lets a downstream reader fetch every user's features over HTTP; The decision-intelligence layer ([AI-Decision-Intelligence-Layer](https://github.com/HimanshuLabs/AI-Decision-Intelligence-Layer)) reads this endpoint (`GET /features`, paged by `user_id`) and holds no database credentials for this pipeline.

## Configuration

| Variable | Default | Used by |
|---|---|---|
| `POSTGRES_HOST` | `localhost` | API, publish job, DAG verification task, feature-freshness exporter |
| `POSTGRES_PORT` | `5433` | same as above |
| `POSTGRES_DB` | `personalization_db` | same as above |
| `POSTGRES_USER` | `de_user` | same as above |
| `POSTGRES_PASSWORD` | `de_password` | same as above |
| `PROJECT1_ROOT` | the repository root | where `data/`, `checkpoints/`, `logs/` and `reference-data/shop_master/` live: `producer.py`, `read_stream.py`, `scripts/repair_stream_checkpoints.py`, the Bronze check, the reference-data, Silver, Gold, publish and query scripts, and the Airflow DAG (task working directory) |
| `NUM_USERS` | unset (every customer) | `producer.py` only: an optional cap to the first N customers of `customers.csv`. Must be at least 1. The producer runs at most 20 sessions and never more than the number of active accounts; with no active account it exits 2 with a one-line error. The users dim always holds every customer |
| `SHOP_MASTER_DIR` | unset (`reference-data/shop_master`) | `producer.py`, and through it `create_reference_data.py` and `check_bronze_against_export.py`: the folder of `customers.csv` and `products.csv`. A relative path resolves from `PROJECT1_ROOT`, an absolute one is used as is. Required by `reference-data/export_shop_master.py`, which writes there and refuses the committed folder; `reference-data/shop_master_*/` is git-ignored |
| `PRODUCER_ONE_PASS` | unset (run until stopped) | `producer.py`: `1` runs to a session target. The active users are shuffled by `PRODUCER_SEED`; shard `i/n` takes every n-th of them, gives each exactly one session (the pass), then starts extra sessions with the default rule (a random free user of the shard) until its share of `PRODUCER_TOTAL_SESSIONS` has started, and exits after the last session ends. Never a user in two concurrent sessions. The seed also seeds the event draws |
| `PRODUCER_SEED`, `PRODUCER_TOTAL_SESSIONS`, `PRODUCER_SHARD` | unset; shard `0/1` | `producer.py`, only with `PRODUCER_ONE_PASS=1` (otherwise exit 2): an integer seed; the total sessions of all shards, split evenly and never below a shard's active users; this process's shard `i/n`. Processes with the same seed and different shards never share a user |
| `PRODUCER_PAUSE_SECONDS` | unset (random 0.05–1.5 s) | `producer.py`: a fixed pause between events, `0` for none |
| `PRODUCER_QUIET` | unset | `producer.py`: `1` prints no events; the stderr summary is unchanged |
| `STREAM_MAX_OFFSETS_PER_TRIGGER` | unset (no cap) | `read_stream.py`: the most Kafka records one micro-batch reads, for a backlog. A positive integer, otherwise the stream exits before Spark starts |
| `PUBLISH_REFUSE_SHRINK` | unset (off) | `publish_gold_to_postgres.py`: `1` refuses to publish fewer users than the live `user_features` holds (equal, more or a first publish pass); `0` or empty is off; any other value exits 2 before Spark starts. The Airflow DAG sets it on its publish task |
| `PUBLISH_KEEP_PREVIOUS` | unset (off) | `publish_gold_to_postgres.py`: `1` keeps the replaced table as `user_features_previous` for `rollback_user_features.py`; `0` or empty is off; any other value exits 2 before Spark starts. The Airflow DAG sets it on its publish task |
| `AIRFLOW_HOME` | `airflow_home/` in the repository | `scripts/run_airflow.sh`: Airflow's configuration and metadata folder; a value already set is kept. The script ignores inherited `AIRFLOW__*` variables, so settings go in `airflow_home/airflow.cfg` |
| `P2_EXPORT_HOST`, `P2_EXPORT_PORT`, `P2_EXPORT_DB`, `P2_EXPORT_USER`, `P2_EXPORT_PASSWORD` | `127.0.0.1`, `5434`, `marketing_analytics`, `p2_reader`, none (required, never printed) | `reference-data/export_shop_master.py`: the batch lakehouse's database, read in one `REPEATABLE READ READ ONLY` transaction with `default_transaction_read_only=on` and a 2-minute statement timeout |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `producer.py` and `read_stream.py` (both the `user_events` source and the `user_events_dlq` sink) |
| `SPARK_KAFKA_VERSION` | `4.1.2` | `scripts/run_stream.sh` (Spark Kafka connector version) |
| `GRAFANA_ADMIN_PASSWORD` | generated and printed once if unset | `monitoring/run_monitoring.sh` |

The feature-freshness exporter's own default host is `host.docker.internal`; `run_monitoring.sh` starts it with `POSTGRES_HOST=localhost` and `POSTGRES_PORT=5433`. Docker Compose creates the database with the default name, user and password above.

**Credentials.** `de_user` / `de_password` are local-demo defaults, set in `docker-compose.yml` and used as fallbacks in the code. Every `POSTGRES_*` value above can be overridden with an environment variable; `.env` and `.env.*` files are git-ignored (except `.env.example`). The API, the feature-freshness exporter, the publish job and the DAG's verification task all connect as `de_user`, the superuser that owns `personalization_db`: this is a single-user local demo with one database account. The anomaly-detection pipeline ([ML-Pipeline-Anomaly-Detection](https://github.com/HimanshuLabs/ML-Pipeline-Anomaly-Detection)) shows the production pattern: a dedicated read-only role (`project5_reader`) for downstream readers.

## Operations

- **Stop the stream:** press Ctrl+C (or `kill -TERM -<pgid>`) in the `scripts/run_stream.sh` terminal. Both queries and Spark stop, the stream prints `[stream] stopped cleanly` and exits 0. This works at any moment, including start-up: the script catches the signal and writes a per-run stop file (`STREAM_STOP_FILE`, in a fresh `mktemp -d` folder that is removed when the script exits), which `read_stream.py` checks at start, before starting the queries and every second, so a signal sent while `spark-submit` is still launching the JVM stops the job before any query starts. The JVM ignores SIGINT/SIGTERM, so it is never killed mid-batch; the Python driver also handles a signal that reaches it directly and shuts down in order. The script exits with `spark-submit`'s status.
- **Restart the stream:** it resumes from `checkpoints/bronze_user_events` and `checkpoints/dlq`, after the checkpoint repair (see *Streaming ingestion and validation*); batches it set aside are kept in `checkpoints/<query>/_repaired/`. Bronze is exactly-once across restarts (the file sink commits each batch to `_spark_metadata`); the DLQ sink is at-least-once, so a batch that was in flight when the stream stopped can be written to `user_events_dlq` again.
- **Reset the stream** (re-read Kafka from the start): stop it, then move **both** `checkpoints/` **and** `data/bronze/user_events` out of the repository. The Bronze folder holds the file sink's own log (`_spark_metadata`); a new checkpoint paired with an old `_spark_metadata` makes the sink skip new batches without an error. After a reset, the DLQ query re-reads the topic too, so invalid events already in the DLQ are written there again.
- **Data persistence:** Kafka data lives on the `kafka_data` volume and PostgreSQL data on `postgres_data`; both survive container recreation. Kafka keeps every event (`KAFKA_LOG_RETENTION_MS: -1`; the broker default would delete them after 7 days), so a stream reset can always re-read the whole topic. Never run `docker compose down -v`, which deletes both volumes.
- **Rollback:** with `PUBLISH_KEEP_PREVIOUS=1` (the Airflow DAG sets it, and the scale publish command uses it), a publish keeps the table it replaces as `user_features_previous` instead of dropping it, in the same transaction as the swap. `python spark-batch/rollback_user_features.py` swaps it back atomically: it takes the same 10-second lock limit as a publish and refuses, changing nothing and exiting 1, if there is no previous table, if it is empty, if its `user_id` is not unique, or if its columns differ from the live table's. A second rollback rolls forward again. Only one previous table is kept: the next publish replaces it. A rollback target exists only after a publish with the setting on; a table published before that has none. Readers see the same brief lock as during a publish, and the served table keeps its name, columns and primary key. Without the setting, a publish drops the old table as before.
- **Manual safety copy:** before a risky manual operation on the serving table, a one-off `pg_dump -t public.user_features -Fc` to a file outside the repository gives a second way back (`pg_restore --clean --if-exists --single-transaction`). Unlike the rollback, that restore blocks readers for its whole duration with no lock limit.
- **Rebuild instead of rollback:** Silver, Gold and the dims are derived and are rebuilt rather than rolled back. Rerun the batch steps from Bronze; at the scale run's size the jobs take about 2.5 minutes. Bronze is append-only; Kafka keeps every event, so a stream reset re-reads it, including any bad events a producer run wrote.
- **Airflow:** Airflow runs in its own venv, `venv-airflow`, because Airflow's official constraints pin different versions of packages this pipeline pins (for example `fastapi` 0.136.1, against the API's 0.115.6). That venv holds only `requirements-airflow.txt`: `apache-airflow==3.2.2` and `apache-airflow-providers-standard==1.13.1`, the version in Airflow's official 3.2.2 constraints file. Install it with the constraints file of your Python minor version (the command below is for Python 3.14; use `constraints-3.11.txt` on Python 3.11):

  ```bash
  python3 -m venv venv-airflow
  venv-airflow/bin/pip install -r requirements-airflow.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.14.txt"
  ```

  CI checks on Python 3.11 and 3.14 that `requirements-airflow.txt` resolves against these constraints. Run Airflow through `scripts/run_airflow.sh`, for example `scripts/run_airflow.sh standalone`. The script puts this repository's `venv/bin` first on `PATH`, sets `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to `python`, sets `AIRFLOW_HOME` to `airflow_home/` unless it is already set, and runs `venv-airflow/bin/airflow`. It ignores every inherited `AIRFLOW__*` variable and prints their names, never their values, so a setting exported for another Airflow cannot reach this one; this Airflow's settings go in `airflow_home/airflow.cfg`. Every task's `spark-submit` and `python` therefore run on the project's venv, and the jobs never need packages in Airflow's venv. Tasks inherit the rest of that environment, so for a scale run set `SHOP_MASTER_DIR`, `SPARK_DRIVER_MEMORY` and the other *Running at scale* settings before starting it. The DAG is triggered by hand. If Bronze holds users that are not customers of the export, `verify_bronze_matches_export` fails before anything is written. The publish task sets `PUBLISH_REFUSE_SHRINK=1`, so a run with fewer users cannot replace the live table. Airflow's web server listens on port 8080 by default; stop any other Airflow on that port first. The file is `dags/project1_personalization_pipeline.py` but the `dag_id` is `project1_personalization_batch_pipeline`; they differ on purpose, because the local Airflow metadata database records past runs under that `dag_id` and renaming it would orphan them.
- **Monitoring:** the Grafana dashboard covers API request rate, p95 latency from the high-resolution histogram and 5xx rate (the API leaves its own `/metrics` scrapes out of every HTTP metric), `user_events` and DLQ throughput, messages written per topic, `user_features` row count, feature freshness (age of the newest `last_event_timestamp`), features computed age (age of the newest `features_computed_at`, i.e. of the last published Gold run) and exporter health. There is no consumer-group lag panel: Spark keeps its Kafka offsets in checkpoints, not a consumer group, so that metric never has data. `run_monitoring.sh` builds the exporter as `project1-feature-freshness-exporter:1.1`, a fixed tag to bump when the exporter changes.

## Design decisions

| Decision | Why |
|---|---|
| Lossless Bronze, dedup in Silver | A streaming watermark with stateful dedup would silently drop late events from the raw layer. |
| One coalesced `is_valid` column | Every event lands in exactly one of Bronze and the DLQ; a NULL can't slip past both filters. |
| Product fields required only on commerce events | Logins, searches and page views carry no product, and a product or price that is present is still checked. |
| Staging-table swap | Readers never see a missing, empty or partially written serving table, and a failed publish changes nothing. |
| Reference dims built from the producer's shop data | The producer and the dims load the same export from the batch lakehouse, so every id the producer emits has a reference row with the same list price and status, and rebuilding them is deterministic. |
| Averages on DECIMAL | Rounding doesn't depend on the order Spark adds the rows, so the same Silver always gives the same feature values (only `features_computed_at` and `recency_hours` change per run). |
| Spark sessions in UTC | Timestamps mean the same thing on every machine; PostgreSQL stores `timestamp with time zone`. |
| Signals handled by the Python driver only | A terminal Ctrl+C stops the queries in order instead of killing the JVM mid-batch. |

## Testing and CI

```bash
ruff check .
python -m pytest -q
```

| File | Tests | Covers |
|---|---:|---|
| `tests/test_batch_pipeline.py` | 16 | the real reference, Silver, Gold and query scripts on a tiny Bronze, run from another directory with `PROJECT1_ROOT` set (nothing written outside it): duplicate removed, event `user_segment` kept, no column named just `price` while the dim's list price is present as `reference_price`, product-reference flag only for an unknown product, Gold schema (19 columns, names and types in order), commerce-only and DECIMAL-rounded price features, exact `total_spend`, `session_count` and `recency_hours`, dim status equal to the producer's functions, dims equal to the shop CSVs (every customer with its `signup_date`; every product with its name, category, brand, list price and status), Silver report counts (Bronze rows, rows dropped by filters, duplicates removed, Silver rows), parquet dims only (no CSV copies), every batch script has `main()` and starts no Spark on import, Gold fails clearly without Silver |
| `tests/test_export_shop_master.py` | 12 | the export from the batch lakehouse. Integration (real PostgreSQL, a throwaway `p1_export_test` database with the batch lakehouse's column types, the committed rows loaded in shuffled order plus its unknown and invalid members and closed SCD2 versions): the export equals the committed `customers.csv` and `products.csv` byte for byte and prints no password; the session is read-only with a 2-minute statement timeout; a NULL required value, a non-numeric id or two current rows for one id exit 2 and leave the folder unchanged. Without a database: the committed folder (absolute or relative), a missing folder or password and a bad port are refused before connecting; the export columns equal the producer's and the committed headers; `reference-data/shop_master_*/` is git-ignored and the committed folder is not |
| `tests/test_stream_validation.py` | 50 | one DLQ case per validation rule (required fields, contract, scores, `engagement_score`, future `event_time`, product and price rules, `cart_value` NULL / negative / mismatched), non-JSON, empty and NULL Kafka values; valid cases at the edges (login without product fields, `cart_value` within rounding, `engagement_score` 0, `event_time` 30 minutes ahead); the DLQ holds exactly the invalid cases, valid + invalid = input, Bronze/DLQ columns unchanged, `KAFKA_BOOTSTRAP_SERVERS` read for both the source and the DLQ sink, importing the stream module starts no Spark, SIGINT or SIGTERM during start-up stops cleanly without starting a query; with a real local SparkSession, `main()` has its own SIGINT/SIGTERM handlers back in place after PySpark installs its SIGINT handler; the launcher's stop file stops the job cleanly when present at start (Spark not started), during start-up (no query started) and while running (both queries stopped); `scripts/run_stream.sh` with a fake `spark-submit` and `PROJECT1_ROOT` in a temporary folder (the repository's checkpoints are never repaired by a test): valid bash, a fresh stop-file path per run that doesn't exist yet and is removed at exit, `spark-submit`'s exit status passed through, a group SIGINT or SIGTERM (sent twice) turned into the stop file while `spark-submit` ignores both signals; `STREAM_MAX_OFFSETS_PER_TRIGGER` sets `maxOffsetsPerTrigger` only when given, and a bad value stops the job before Spark starts |
| `tests/test_stream_validation_gaps.py` | 14 | validation rules without a dedicated case elsewhere: NULL `event_id`, `user_id`, `event_type`, `event_time` and `schema_version`; out-of-range `cart_abandonment_probability`; NULL `purchase_probability`; negative `discounted_price`; NULL `original_price`, `discounted_price` and `discount_percent` on a commerce event; a wrong JSON type; truncated JSON. Each must reach the dead-letter branch only, with its original text unchanged; a guard test checks the unchanged base event is valid |
| `tests/test_stream_checkpoints.py` | 15 | the checkpoint repair on fake checkpoints: a pending Bronze batch already in the sink log (`N` or `N.compact`) gets `commits/N` with the latest commit's bytes and checksum; without sink metadata, and always for the DLQ, `offsets/N` and its `.crc` move to `_repaired/`; one line per action; a clean, missing or empty checkpoint is left unchanged; a second run changes nothing more; an earlier repair is never overwritten; the first commit uses Spark's default format; `scripts/run_stream.sh` repairs before `spark-submit` and does not start it when the repair fails |
| `tests/test_feature_api.py` | 33 | every endpoint through real HTTP requests to the ASGI app, with a fake database: `GET /` message; `GET /metrics` Prometheus text, with series for `/features/{user_id}` and none for `/metrics` itself; `GET /health` 200 and each 503 reason (database unavailable, table missing, table empty); `GET /features/{user_id}` 200 with exactly the 16 feature keys and no `user_name` or `membership_tier`, 404, 422 for a non-integer id (no database call), 500; error bodies carry no credentials or exception text; `GET /features` first, middle, last and empty pages, an exactly full last page ends the walk, full walks with no gaps, repeats or empty extra request, limit and `after_user_id` bounds (422), item keys, 500, parameterised SQL; `POSTGRES_*` honoured and every connection uses the 3-second connect timeout |
| `tests/test_feature_exporter.py` | 4 | the feature-freshness exporter with a fake database: DB up sets `project1_features_computed_age_seconds` from `MAX(features_computed_at)` and the other gauges exactly; an empty table gives -1 ages; DB down after a good scrape gives `exporter_up 0` and NaN for every gauge; the age comes back when the DB does |
| `tests/test_producer_contract.py` | 79 | 58 keys in order, schema 4.0, catalog prices and bands, product fields NULL on non-commerce events, one user, product and price per session, funnel order, field consistency rules, journey stage by event type, device context fixed and consistent per session, `payment_method` only on payment events, no Kafka connection on import; the shop: every event's profile fields equal its `customers.csv` row, every product id, name, category and list price matches `products.csv`, no inactive account or discontinued product, only customer IDs from the CSV, home and abroad locations, `NUM_USERS` caps to the first customers, no Faker import, a missing CSV or required column exits 2 with a one-line error; running the producer with a stub Kafka client: exit 0 when all delivered, 1 on a failed or undelivered event, SIGTERM stops like Ctrl+C (flush and summary), a second SIGINT/SIGTERM (real signals to a producer process whose final flush is slow) still gives the summary, exit 0 and no traceback, `NUM_USERS=10` runs without hanging, `NUM_USERS=0` and no active accounts give a clear error, `KAFKA_BOOTSTRAP_SERVERS` and `enable.idempotence` in the producer config; `SHOP_MASTER_DIR` absolute, relative to `PROJECT1_ROOT`, unset or missing (exit 2 naming it); the user pick: the default draws exactly like the old `random.choice` over the free users (same seed, same events) without walking the user list, never one user in two concurrent sessions (default and target runs), a `PRODUCER_ONE_PASS` run gives every active user exactly one session first, then extra sessions to exactly the target, all finished, the same seed gives the same sessions and events, shards are disjoint, cover every active user and split the target evenly, a target below a shard's users is a clear error; `run()` stops at the target with the sessions line and an unchanged summary; quiet mode prints no events; default settings are today's; every bad run setting is refused with one line and exit 2 |
| `tests/test_project_structure.py` | 5 | required files exist, runtime folders are git-ignored, `EVENT_TYPES` and `COMMERCE_EVENT_TYPES` match across the producer, stream and Gold, `CART_EVENT_TYPES` matches across the producer and stream and is a subset of `COMMERCE_EVENT_TYPES` |
| `tests/test_publish_swap.py` | 17 | the publish swap. Unit (fake connection): statement order with `SET LOCAL lock_timeout` first, empty and duplicate staging refused before any change, a failure at any step rolls back, a lock timeout exits 1 with the clean message and no driver text. Integration (real PostgreSQL, a throwaway `p1_publish_test` database): the swap replaces the table (new OID, `user_features_pkey`, correct rows), empty / duplicate / NULL-`user_id` staging leaves the live table unchanged, a held lock times out and leaves it unchanged, two swaps in a row succeed; the Gold path resolves from `PROJECT1_ROOT` |
| `tests/test_python_syntax.py` | 1 | every Python file parses |
| `tests/test_bronze_export_check.py` | 6 | the Bronze check run as a script on a tiny Bronze in a temporary `PROJECT1_ROOT`: every user a customer passes; unknown users exit 1 with the exact message and nothing is written; the export in `SHOP_MASTER_DIR` is the one read; no Spark on import. The DAG read with `ast` (Airflow is not installed in CI): the exact task order with the check before the first write, the check's import preflight and command, `retries=0` on the check only, no task replaces the environment, the publish command sets `PUBLISH_REFUSE_SHRINK=1`, `schedule=None`, `catchup=False`, `max_active_runs=1` |
| `tests/test_publish_keep_previous.py` | 25 | the opt-in keep-previous swap. Unit (fake connection): without the flag the statements are exactly the plain swap's; with it the live table becomes `user_features_previous` (its key renamed first) and replaces the one before, and without a live table an existing previous table is left alone; the DAG configuration runs both guards; a failure at any new statement rolls back; a lock timeout exits 1 with the clean message; the setting accepts only unset, empty, `0` and `1`; the DAG's publish command sets `PUBLISH_KEEP_PREVIOUS=1`. Integration (real PostgreSQL, a throwaway `p1_previous_test` database): the replaced table kept with its rows and `user_features_previous_pkey`, the next publish replacing it, no live table leaving the previous one untouched, bad staging and a held lock changing nothing, the DAG configuration |
| `tests/test_rollback_user_features.py` | 14 | `rollback_user_features.py`: the publish's lock limit and table names; no connection at import. Integration (real PostgreSQL, a throwaway `p1_rollback_test` database): the previous table served again with `user_features_pkey` and the replaced one kept; a second rollback rolls forward; no live table; every refusal (no previous table, empty, duplicate or NULL `user_id`, different columns) and a held lock exit 1 with one clean line and change nothing; no row value in the output |
| `tests/test_publish_shrink_guard.py` | 20 | the opt-in shrink guard. Unit (fake connection): without the flag the statements are exactly the plain swap's; with it, equal or more users and a first publish are swapped, fewer users are refused before any change, `swap_or_exit` exits 1 with the exact message and no traceback, and the setting accepts only unset, empty, `0` and `1`. Integration (real PostgreSQL, a throwaway `p1_shrink_test` database): fewer users refused with the live table unchanged, equal and more swapped, a first publish, fewer users swapped without the flag |
| `tests/test_feature_api_postgres.py` | 20 | the API against a real PostgreSQL (a throwaway `p1_api_test` database, `user_id integer` as in `user_features`): the largest `integer` id served; ids no user can have (above the `integer` range, above 64 bits, negative) answered 404 with the usual detail; a cursor at or past the last id gives an empty last page; negative cursors and numbers too long to parse answered 422; no body carries exception text |
| `tests/test_run_airflow.py` | 7 | `scripts/run_airflow.sh` with a fake `airflow`, in a folder whose path has spaces: the project's `venv/bin` first on `PATH`, `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` set to `python` (replacing a full path), `AIRFLOW_HOME` defaulted or kept, inherited `AIRFLOW__*` variables dropped with their names (never values) on stderr, the arguments passed through unchanged, a clear error when either venv is missing, and the script is executable |

The 6 integration tests in `tests/test_publish_swap.py`, the 5 in `tests/test_export_shop_master.py`, the 5 in `tests/test_publish_shrink_guard.py`, the 20 in `tests/test_feature_api_postgres.py`, the 6 in `tests/test_publish_keep_previous.py` and the 12 in `tests/test_rollback_user_features.py` need `P1_TEST_DATABASE_URL`, pointing at a maintenance database (such as `postgres`) on a server where the role may create databases. They create `p1_publish_test`, `p1_export_test`, `p1_shrink_test`, `p1_api_test`, `p1_previous_test` and `p1_rollback_test`, work only there and drop them at the end. Without the variable they are skipped locally; in CI they must run. Against the Docker Compose PostgreSQL:

```bash
P1_TEST_DATABASE_URL=postgresql://de_user:de_password@127.0.0.1:5433/postgres python -m pytest -q
```

GitHub Actions (`.github/workflows/ci.yml`) runs on every push and pull request to `main`:
- **Python lint and validation:** runs on Python 3.11 and 3.14 (a matrix; one version failing doesn't cancel the other), with Java 17 (Temurin), `requirements-ci.txt`, `ruff check .` and `pytest -q -rs`, and a `postgres:16.14` service container with `P1_TEST_DATABASE_URL` set, so the publish-swap, shrink-guard, keep-previous, rollback, export and feature-API integration tests always run. A skipped test fails the job: every test must run in CI, and the test files import their dependencies directly instead of skipping when one is missing. Its last step resolves `requirements-airflow.txt` against Airflow's official 3.2.2 constraints for that Python version with `pip install --dry-run` (nothing is installed; a conflict fails the job).
- **Docker image build validation:** builds `api/Dockerfile` (the API runs as a non-root user) and `monitoring/exporters/Dockerfile.feature_exporter`.

## Project structure

```text
.
├── api/                    FastAPI service and Dockerfile
├── dags/                   Airflow DAG for the batch pipeline
├── data-generator/         Kafka event producer
├── docs/                   architecture notes and full diagram
├── monitoring/             Prometheus, Grafana, exporters, start/stop scripts
├── reference-data/         shop master export from the batch lakehouse, its export script, the Bronze check and the users and products dims
├── scripts/                stream launcher (clean Ctrl+C handling), checkpoint repair and Airflow launcher
├── spark-batch/            Bronze → Silver → Gold → PostgreSQL jobs and the serving-table rollback
├── spark-streaming/        Kafka → Bronze / DLQ streaming job
├── tests/                  pytest suite
└── docker-compose.yml      Kafka and PostgreSQL
```

## Known limits and next steps

| Limitation | Production next step |
|---|---|
| Features are recomputed in batch; the stream doesn't update `user_features`. | Incremental upserts from the stream (`foreachBatch` + `INSERT … ON CONFLICT`) for near-real-time counters. |
| `recency_hours` is computed at batch time against `features_computed_at`, so it grows stale between Gold runs. | Compute recency at request time from `last_event_timestamp`. |
| Every batch run reprocesses all of Bronze. | Incremental processing with a table format such as Delta Lake and `MERGE`. |
| Bronze is many small files: each 10-second micro-batch writes one Parquet file per Kafka partition and nothing compacts them (one default-size run: 184 files, about 21 KB each, for 2,323 events). Every batch run lists and opens all of them. | Compaction (Delta Lake `OPTIMIZE`, or a periodic rewrite into larger files), or a longer trigger interval. |
| Bronze stores parsed columns, not the raw Kafka payload. | Keep the raw value alongside the parsed columns for full replay. |
| The dead-letter sink is at-least-once: if the stream restarts in the middle of a batch, that batch's dead-letter records can be written a second time. Bronze, a file sink, is exactly-once. | Deduplicate dead-letter records downstream, or write them with a transactional sink. |
| DLQ messages don't record which rule rejected them. | Add the failed rule as a Kafka header on each DLQ message. |
| A session follows one product, so `items_viewed_in_session` and `repeat_product_view_count` always carry the same value. | Let a session browse several products and count views per product. |
| 5 of the 26 event types are never emitted: `search_result_click`, `wishlist_remove`, `order_cancelled`, `return_requested` and `delivery_tracking_view`. | Add funnel steps for them. |
| Some context fields are drawn independently for each event: `traffic_source`, `ab_test_group`, `campaign_id`, `api_latency_ms`, `page_load_time_ms`, `cart_abandonment_probability`, `fraud_score` and `ip_address`. | Keep campaign and experiment fields constant per session and model latency from the device and network. |
| Each producer process keeps its own sessions, stock levels and per-user gaps, so parallel producers don't share them. | Keep that state in a shared store, or run a single producer. |
| Every stream start runs `scripts/repair_stream_checkpoints.py` to work around a Spark 4.1.x crash on restart from a planned but uncommitted batch ([apache/spark#55236](https://github.com/apache/spark/issues/55236), closed upstream without a fix). | Remove the repair step from `scripts/run_stream.sh` once a Spark release fixes it. |
| This pipeline's purchases are simulated and are not reconciled with the batch lakehouse's orders: the two share customers and products, not transactions. | Feed these purchases into the batch lakehouse's order flow, or reconcile them against its orders. |
| Single Kafka broker, replication factor 1. | A multi-broker cluster with replication factor 3. |
| CI proves the code at the default size (about 500 customers). The scale run and the dead-letter check are local evidence, recorded in [docs/scale_run_evidence.md](docs/scale_run_evidence.md), and are not run in CI. | A scheduled large-data job, or a CI job with a Kafka service, if the run time is acceptable. |
| The Airflow DAG is not executed in CI: CI parses it and checks its task order, but Airflow is not installed there. | A DAG integrity test and a containerised Airflow in the quick start. |
| Silver keeps one row per `event_id` with `dropDuplicates`, so when two events share an `event_id` but differ in content, which one survives is arbitrary. | Deduplicate on the full payload, or keep the earliest by Kafka offset and send conflicting duplicates to the DLQ. |
| `user_name` and `membership_tier` come from `MAX_BY(..., event_timestamp)`, so if a user's latest events share the same timestamp with different values, the one picked is arbitrary. | Break ties with a second ordering key, such as the Kafka offset or `event_id`. |
| A rollback target exists only after a publish with `PUBLISH_KEEP_PREVIOUS=1`, and exactly one previous table is kept: the next such publish replaces it (see *Operations*, Rollback). | Keep several generations, or a dump per publish, if a rollback further back is ever needed. |
| On Python 3.14, pytest shows about 770 `DeprecationWarning`s from inside the pinned FastAPI and Starlette, which call `asyncio.iscoroutinefunction`. They are harmless today. | Removing them needs a FastAPI and Starlette upgrade: the pinned FastAPI 0.115.6 requires Starlette below 0.42. |
| One database account: the API, exporter, publish job and DAG all use `de_user`, the superuser that owns `personalization_db`, with demo credentials. | Separate roles per component: a writer for the publish job and a read-only role with `SELECT` on `user_features` for the API, exporter and downstream readers, with passwords from a secrets manager. |

Operational caveats (the JVM and signals, IDs after a new export, the Grafana password, base-image tags) are listed under *Operational notes* below.

Security and privacy: this is a local demo with default credentials, and the serving table includes `user_name`, the customer's name from the batch lakehouse. A production deployment would use a secrets manager and serve IDs and features only.

## Operational notes

- `scripts/run_stream.sh` makes the JVM ignore SIGINT and SIGTERM and turns them into the stop file, so the Python driver can stop the queries in order. If the driver ever hangs, only SIGKILL stops the JVM.
- `product_id`s and `user_id`s are the batch lakehouse's IDs from the `reference-data/shop_master/` export. A new export can add, retire or change rows; the dims rebuild on every run, but events already in Bronze keep the values they were produced with.
- The Grafana admin password is passed as an environment variable and is visible through `docker inspect project1_grafana`.
- The `python:3.11-slim` (API) and `python:3.12-slim` (exporter) base images are tagged by minor version only, so a rebuild can pick up a newer patch release; pin a patch tag or digest for reproducible builds.

## License

MIT. See [LICENSE](LICENSE).
