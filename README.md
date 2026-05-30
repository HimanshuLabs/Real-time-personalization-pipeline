# Real-Time Personalization Pipeline

## 1. Overview

This project is an end-to-end real-time data engineering pipeline that simulates user activity events, streams them through Kafka, processes them using Spark Structured Streaming, applies schema validation and Dead Letter Queue routing, stores clean data in Medallion layers, publishes Gold-level user features to PostgreSQL, and exposes them through a FastAPI feature-serving endpoint.

---

## 2. Architecture

```text
Python Event Producer
        ↓
Kafka topic: user_events
        ↓
Spark Structured Streaming
        ↓
Schema parsing + validation + event-time watermarking
        ↓
Valid events → Bronze Parquet
Invalid events → Kafka DLQ topic: user_events_dlq
        ↓
Bronze → Silver transformation
        ↓
Silver → Gold user feature aggregation
        ↓
PostgreSQL user_features table
        ↓
FastAPI /features/{user_id}
```

---

## 3. Tech Stack

| Layer | Technology |
|---|---|
| Event generation | Python |
| Messaging | Apache Kafka |
| Stream processing | Apache Spark Structured Streaming |
| Batch processing | PySpark |
| Storage format | Parquet |
| Lakehouse pattern | Bronze, Silver, Gold Medallion Architecture |
| Serving database | PostgreSQL |
| API layer | FastAPI |
| Infrastructure | Docker Compose |
| Version control | Git |

---

## 4. Project Structure

```text
.
├── api/
│   ├── main.py
│   └── requirements.txt
├── data-generator/
│   ├── producer.py
│   ├── requirements.txt
│   └── venv/
├── spark-streaming/
│   └── read_stream.py
├── spark-batch/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   ├── query_gold_features.py
│   └── publish_gold_to_postgres.py
├── docker-compose.yml
├── README.md
└── .gitignore
```

---

## 5. Data Flow

### 5.1 Event Generation

`data-generator/producer.py` generates synthetic user behavior events.

Example event:

```json
{
  "event_id": "uuid",
  "user_id": 412,
  "event_type": "add_to_cart",
  "product": "iphone",
  "price": 62814.04,
  "event_time": "2026-05-14T18:40:58.855061+00:00",
  "source": "python-generator",
  "schema_version": "1.0"
}
```

Supported event types:

```text
page_view
product_view
add_to_cart
purchase
search
```

---

### 5.2 Kafka Ingestion

The producer writes events to the Kafka topic:

```text
user_events
```

Invalid records are routed by Spark to the Dead Letter Queue topic:

```text
user_events_dlq
```

---

### 5.3 Spark Structured Streaming

`spark-streaming/read_stream.py` reads from Kafka and performs:

- JSON parsing
- schema validation
- required-field checks
- event-time timestamp conversion
- watermarking
- valid/invalid stream split
- valid event writing to Bronze Parquet
- invalid event writing to Kafka DLQ

---

### 5.4 Bronze Layer

Bronze stores raw but valid ingested events.

Output path:

```text
data/bronze/user_events
```

Purpose:

- preserve raw valid events
- keep ingestion close to source format
- provide replayable lakehouse foundation

---

### 5.5 Silver Layer

`spark-batch/bronze_to_silver.py` reads Bronze data and creates a cleaned Silver table.

Silver transformations include:

- timestamp standardization
- duplicate removal using `event_id`
- null filtering
- column selection
- schema cleanup

Output path:

```text
data/silver/user_events
```

---

### 5.6 Gold Layer

`spark-batch/silver_to_gold.py` creates user-level personalization features.

Output path:

```text
data/gold/user_features
```

Generated features:

| Feature | Meaning |
|---|---|
| `total_events` | Total user activity events |
| `page_view_count` | Number of page views |
| `product_view_count` | Number of product views |
| `add_to_cart_count` | Number of add-to-cart actions |
| `purchase_count` | Number of purchases |
| `search_count` | Number of searches |
| `avg_event_price` | Average price across user events |
| `max_event_price` | Maximum price seen in user events |
| `last_event_timestamp` | Most recent event timestamp |
| `unique_products_interacted` | Count of distinct products interacted with |

---

### 5.7 PostgreSQL Serving Table

`spark-batch/publish_gold_to_postgres.py` publishes Gold features into PostgreSQL.

Target table:

```text
user_features
```

This table supports low-latency feature lookup by `user_id`.

---

### 5.8 FastAPI Feature Serving

`api/main.py` exposes the serving layer.

Main endpoint:

```text
GET /features/{user_id}
```

Example response:

```json
{
  "user_id": 412,
  "features": {
    "user_id": 412,
    "total_events": 1,
    "page_view_count": 0,
    "product_view_count": 0,
    "add_to_cart_count": 1,
    "purchase_count": 0,
    "search_count": 0,
    "avg_event_price": 62814.04,
    "max_event_price": 62814.04,
    "last_event_timestamp": "2026-05-14T18:40:58.855061+00:00",
    "unique_products_interacted": 1
  }
}
```

---

## 6. How to Run

### 6.1 Start Docker Services

```bash
docker-compose up -d
```

Verify:

```bash
docker ps
```

Expected containers:

```text
kafka
postgres
```

---

### 6.2 Verify Kafka Topics

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Expected topics:

```text
user_events
user_events_dlq
```

---

### 6.3 Activate Python Environment

```bash
source data-generator/venv/bin/activate
```

---

### 6.4 Run Producer

```bash
cd data-generator
python producer.py
```

---

### 6.5 Run Spark Streaming Ingestion

From project root:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  spark-streaming/read_stream.py
```

This writes valid events to:

```text
data/bronze/user_events
```

and invalid events to:

```text
user_events_dlq
```

---

### 6.6 Run Bronze to Silver Transformation

```bash
spark-submit spark-batch/bronze_to_silver.py
```

---

### 6.7 Run Silver to Gold Aggregation

```bash
spark-submit spark-batch/silver_to_gold.py
```

---

### 6.8 Query Gold Features with Spark SQL

```bash
spark-submit spark-batch/query_gold_features.py
```

---

### 6.9 Publish Gold Features to PostgreSQL

```bash
spark-submit \
  --packages org.postgresql:postgresql:42.7.4 \
  spark-batch/publish_gold_to_postgres.py
```

---

### 6.10 Verify PostgreSQL Table

```bash
docker exec -it postgres psql -U de_user -d personalization_db
```

Inside PostgreSQL:

```sql
\dt

SELECT COUNT(*) FROM user_features;

SELECT * FROM user_features LIMIT 10;

\q
```

---

### 6.11 Run FastAPI

From project root:

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

---

### 6.12 Test API

Health check:

```text
http://localhost:8000/health
```

Feature lookup:

```text
http://localhost:8000/features/412
```

Replace `412` with an actual `user_id` from PostgreSQL.

---

## 7. PostgreSQL Configuration

The project PostgreSQL container uses host port `5433` because local port `5432` may already be occupied by a system PostgreSQL service.

Connection details:

```text
Host: localhost
Port: 5433
Database: personalization_db
User: de_user
Password: de_password
```

Docker internal PostgreSQL port remains `5432`.

---

## 8. Dead Letter Queue Testing

To test DLQ routing, temporarily replace this line in `data-generator/producer.py`:

```python
event = generate_event()
```

with:

```python
event = {
    "user_id": "INVALID_STRING",
    "price": "NOT_A_NUMBER"
}
```

Run producer and Spark streaming.

Then consume DLQ:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic user_events_dlq \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

After testing, restore:

```python
event = generate_event()
```

---

## 9. Key Data Engineering Concepts Demonstrated

- real-time event ingestion
- Kafka topic-based architecture
- Spark Structured Streaming
- schema parsing
- schema validation
- Dead Letter Queue routing
- event-time processing
- watermarking
- Bronze/Silver/Gold Medallion Architecture
- batch transformation
- deduplication
- feature aggregation
- Spark SQL analytics
- PostgreSQL serving sink
- FastAPI feature serving
- Dockerized infrastructure
- Git-based incremental development

---

## 10. Current Pipeline State

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Bronze Parquet
  ↓
Silver Parquet
  ↓
Gold User Features
  ↓
PostgreSQL
  ↓
FastAPI
```

---

## 11. Status

Current version supports local end-to-end execution from event generation to API-based feature lookup.

The pipeline can:

- generate synthetic user events
- stream events through Kafka
- validate and route bad records to DLQ
- persist valid records to Bronze
- clean and deduplicate data into Silver
- aggregate user features into Gold
- publish feature tables to PostgreSQL
- serve user features through FastAPI

---
## Observability: Prometheus + Grafana

This project includes a local observability layer for the real-time personalization pipeline using Prometheus and Grafana.

The purpose of this layer is not just to show graphs. It gives operational visibility into the serving API, Kafka topics, DLQ flow, PostgreSQL feature-serving table, and feature freshness.

### Monitoring stack

| Component | Port | Purpose |
|---|---:|---|
| FastAPI `/metrics` | `8000` | Exposes API request count and latency metrics |
| Kafka Exporter | `9308` | Exposes Kafka topic offsets, topic activity, and consumer-group lag when offsets are committed |
| Feature Freshness Exporter | `8001` | Queries PostgreSQL and exposes serving-table health metrics |
| Prometheus | `9090` | Scrapes FastAPI, Kafka exporter, and feature freshness exporter |
| Grafana | `3000` | Visualizes API, Kafka, DLQ, PostgreSQL, and freshness metrics |

### Metrics covered

| Metric area | What it tells us |
|---|---|
| FastAPI request throughput | Whether the feature-serving API is receiving traffic |
| FastAPI p95 latency | Whether API responses are staying fast enough for serving use cases |
| Kafka `user_events` offset | Whether user event data is entering Kafka |
| Kafka `user_events` throughput | Whether the main event stream is actively moving |
| Kafka `user_events_dlq` offset | Whether bad records are reaching the DLQ topic |
| Kafka DLQ ingress rate | Whether bad-record volume is increasing |
| Kafka consumer-group lag | Visible only when consumers commit offsets to Kafka |
| PostgreSQL `user_features` row count | Whether the serving database has feature rows available |
| Feature freshness seconds | How stale or fresh the served features are |
| Prometheus target health | Whether Prometheus can scrape all configured targets |
| Grafana dashboard health | Whether the observability dashboard is available |

### Important note on Spark consumer lag

Kafka exporter reports consumer-group lag only when a consumer commits offsets back to Kafka.

Spark Structured Streaming commonly stores offsets in checkpoint files instead of committing them to Kafka as a normal consumer group. Because of that, the Grafana Kafka consumer-group lag panel may show `No data`.

That does not automatically mean the pipeline is broken.

For this project, feature freshness is the more reliable signal for the Spark-to-serving path because it tells us how old the latest feature data is inside PostgreSQL.

### Start required base services

Start Kafka and PostgreSQL:

```bash
docker start kafka postgres
