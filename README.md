# Real-Time E-Commerce Event Streaming Pipeline

> **Tech Stack:** Python · Apache Kafka · PySpark Structured Streaming · MySQL 8 · Docker  
> **Year:** 2026

---

## 📌 Project Overview

This project implements a **production-grade, real-time streaming data pipeline** for an e-commerce platform.  
User clickstream events (page views, product clicks, add-to-cart, purchases, etc.) are continuously generated, streamed through Apache Kafka, processed by PySpark Structured Streaming, and persisted into a MySQL database for analytical dashboarding.

---

## 🏗️ Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        E-Commerce Platform (Simulated)                    │
│                         [ kafka_producer.py ]                             │
│   Generates: page_view | product_click | add_to_cart | purchase | search  │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │  JSON Events (gzip-compressed)
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                         Apache Kafka Broker                               │
│                    Topic: ecommerce_clickstream                           │
│                    Partitions: 3 | Retention: 7 days                      │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │  Kafka Consumer (Spark Structured Streaming)
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    PySpark Structured Streaming                           │
│                       [ spark_streaming.py ]                              │
│                                                                           │
│  1. Parse JSON payload → typed DataFrame                                  │
│  2. Watermark(30s) + dropDuplicates(event_id) → Deduplication             │
│  3. Windowed Aggregations (5-min sliding, 1-min slide):                   │
│     ├── Product-level  : views, clicks, cart, purchases, revenue          │
│     ├── User-level     : total events, spend, sessions, device            │
│     └── Category-level : events, revenue, unique users/products           │
│  4. foreachBatch() → MySQL writes per micro-batch (every 10 seconds)      │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │  JDBC / mysql-connector-python
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           MySQL 8 Database                                │
│                      [ ecommerce_streaming ]                              │
│                                                                           │
│  Tables:                                                                  │
│  ├── raw_clickstream_events        (deduplicated raw events)              │
│  ├── product_event_aggregates      (per-product windowed metrics)         │
│  ├── user_session_aggregates       (per-user windowed metrics)            │
│  ├── category_event_aggregates     (per-category windowed metrics)        │
│  └── pipeline_run_metadata         (observability / batch tracking)       │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
data project/
├── config.py               # Centralized configuration (Kafka, Spark, MySQL)
├── kafka_producer.py        # Clickstream event producer → Kafka
├── spark_streaming.py       # PySpark Structured Streaming pipeline
├── mysql_sink.py            # MySQL write helpers (dedup, upsert, metadata)
├── schema.sql               # MySQL DDL — all 5 tables
├── docker-compose.yml       # Zookeeper + Kafka + Kafka-UI + MySQL
├── requirements.txt         # Python dependencies
└── README.md                # This file
```

---

## ⚙️ Prerequisites

| Requirement | Version |
|-------------|---------|
| Python      | 3.9+    |
| Java (JDK)  | 11 or 17 (required by Spark) |
| Docker + Docker Compose | Latest |
| pip         | 23+     |

---

## 🚀 Quick Start (Step-by-Step)

### Step 1 — Clone / Open the Project

```bash
cd "data project"
```

### Step 2 — Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 3 — Start Infrastructure (Kafka + MySQL)

```bash
docker-compose up -d
```

Wait for all services to be healthy (≈ 30–60 seconds):

```bash
docker-compose ps
```

You should see all containers in `Up (healthy)` state.

| Service    | Port  | Purpose                      |
|------------|-------|------------------------------|
| Zookeeper  | 2181  | Kafka coordination           |
| Kafka      | 9092  | Message broker               |
| Kafka UI   | 8080  | Web UI for Kafka monitoring  |
| MySQL      | 3306  | Analytical sink database     |

> 💡 **Kafka UI**: Open http://localhost:8080 in your browser to monitor topics, consumer groups, and message throughput in real time.

### Step 4 — Initialize MySQL Schema (if not auto-run by Docker)

```bash
mysql -h 127.0.0.1 -P 3306 -u root -proot123 < schema.sql
```

### Step 5 — Start the PySpark Streaming Pipeline

Open Terminal 1:

```bash
python spark_streaming.py
```

> **First run note:** Spark will automatically download the Kafka and MySQL connector JARs (≈ 50 MB). Subsequent runs use the local cache.

### Step 6 — Start the Kafka Producer

Open Terminal 2:

```bash
python kafka_producer.py
```

The producer will begin emitting **5 events/second** into the `ecommerce_clickstream` Kafka topic.

---

## 📊 What Happens at Runtime

```
Terminal 1 (Spark):
  2026-01-15 10:00:00 [INFO] SparkStreaming - Step 1/7 — Initializing MySQL schema ...
  2026-01-15 10:00:01 [INFO] SparkStreaming - SparkSession created: EcommerceStreamingPipeline
  2026-01-15 10:00:03 [INFO] SparkStreaming - Pipeline is LIVE. Active streaming queries:
       • raw_clickstream_sink
       • product_agg_sink
       • user_agg_sink
       • category_agg_sink
  2026-01-15 10:00:13 [INFO] MySQLSink - Raw events: attempted=47  inserted=47  duplicates_skipped=0
  2026-01-15 10:00:23 [INFO] MySQLSink - Product aggregates upserted: 12 records
  ...

Terminal 2 (Producer):
  2026-01-15 10:00:05 [INFO] KafkaProducer - Events produced so far: 100
  2026-01-15 10:00:25 [INFO] KafkaProducer - Events produced so far: 200
  ...
```

---

## 🗄️ MySQL Output Tables

### 1. `raw_clickstream_events`
Deduplicated raw events. Each `event_id` is stored only once (INSERT IGNORE).

| Column           | Type           | Description                     |
|------------------|----------------|---------------------------------|
| event_id         | VARCHAR(64)    | UUID — deduplication key        |
| user_id          | VARCHAR(64)    | Anonymized user identifier      |
| event_type       | VARCHAR(32)    | page_view / product_click / ... |
| product_id       | VARCHAR(64)    | Product SKU                     |
| price            | DECIMAL(10,2)  | Product price (INR)             |
| event_timestamp  | DATETIME(3)    | Millisecond-precision event time|

### 2. `product_event_aggregates`
Per-product windowed metrics updated every micro-batch.

| Column           | Type           | Description                     |
|------------------|----------------|---------------------------------|
| window_start     | DATETIME       | Window interval start           |
| window_end       | DATETIME       | Window interval end             |
| product_id       | VARCHAR(64)    | Product SKU                     |
| total_views      | BIGINT         | Page views in window            |
| total_purchases  | BIGINT         | Purchases in window             |
| total_revenue    | DECIMAL(15,2)  | Revenue in window               |
| unique_users     | BIGINT         | Distinct users in window        |

### 3. `user_session_aggregates`
Per-user windowed behavior metrics.

### 4. `category_event_aggregates`
Per-category windowed sales and engagement metrics.

### 5. `pipeline_run_metadata`
Observability table — one row per Spark micro-batch.

| Column               | Description                        |
|----------------------|------------------------------------|
| batch_id             | Spark batch sequence number        |
| events_processed     | Events in this batch               |
| events_deduplicated  | Duplicates caught this batch       |
| status               | SUCCESS / FAILED                   |
| duration_ms          | Processing latency (milliseconds)  |

---

## 🔧 Configuration Reference

All settings are in [`config.py`](config.py):

| Parameter              | Default            | Description                          |
|------------------------|--------------------|--------------------------------------|
| `KAFKA_TOPIC`          | ecommerce_clickstream | Kafka topic name                  |
| `EVENTS_PER_SECOND`    | 5                  | Producer throughput                  |
| `MICRO_BATCH_INTERVAL` | 10 seconds         | Spark trigger interval               |
| `WATERMARK_DELAY`      | 30 seconds         | Late event tolerance                 |
| `WINDOW_DURATION`      | 5 minutes          | Aggregation window size              |
| `WINDOW_SLIDE_INTERVAL`| 1 minute           | Window slide interval                |
| `MYSQL_PASSWORD`       | root123            | MySQL root password                  |

---

## 🧠 Key Technical Concepts (Interview Q&A)

### Q: How does deduplication work?
**A:** Spark's `dropDuplicates(["event_id"])` with a **watermark of 30 seconds** maintains an in-memory state store of seen `event_id` values. Any event arriving with a duplicate `event_id` within the watermark window is dropped before writing to MySQL. At the MySQL layer, `INSERT IGNORE` provides a second line of defence using the `UNIQUE KEY` on `event_id`.

### Q: What is a watermark in Structured Streaming?
**A:** A watermark defines how late data can arrive before Spark stops waiting for it. Setting `withWatermark("event_timestamp", "30 seconds")` tells Spark: *"discard any event whose timestamp is more than 30 seconds behind the current maximum observed timestamp."* This bounds the state size and allows Spark to finalize aggregation results.

### Q: Why use `foreachBatch` instead of the JDBC sink?
**A:** The native JDBC sink only supports `append` output mode and doesn't support upserts. `foreachBatch` gives us full control: we collect each micro-batch as a Python list and call `INSERT ... ON DUPLICATE KEY UPDATE` for idempotent upserts — critical for `update` output mode aggregations.

### Q: What is `outputMode("update")` vs `outputMode("append")`?
**A:** 
- **append**: Only new rows that are finalized (past the watermark) are emitted. Low latency but delayed.
- **update**: Emits all rows that changed since the last trigger, including partial aggregation results being updated in real time.

### Q: How are events partitioned in Kafka?
**A:** The producer uses `user_id` as the **Kafka message key**. This guarantees that all events from the same user are routed to the same partition, preserving event ordering per user.

### Q: What happens if the Spark job crashes mid-batch?
**A:** Structured Streaming uses **checkpointing** (written to `./spark_checkpoint`). On restart, Spark reads the checkpoint to determine the last committed Kafka offset and resumes from there — guaranteeing **exactly-once** semantics when combined with idempotent MySQL upserts.

---

## 🛑 Stopping the Pipeline

```bash
# Terminal 2 — stop producer
Ctrl + C

# Terminal 1 — stop Spark (graceful shutdown)
Ctrl + C

# Stop Docker containers
docker-compose down
```

To also delete all stored data (volumes):
```bash
docker-compose down -v
```

---

## 📈 Sample Analytical Queries

```sql
-- Top 5 best-selling products by revenue in the last window
SELECT product_id, category, total_revenue, total_purchases
FROM product_event_aggregates
ORDER BY window_end DESC, total_revenue DESC
LIMIT 5;

-- Most active users in the current hour
SELECT user_id, total_events, purchases, total_spend
FROM user_session_aggregates
WHERE window_start >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
ORDER BY total_spend DESC
LIMIT 10;

-- Category performance comparison
SELECT category, SUM(total_revenue) AS revenue, SUM(total_purchases) AS purchases
FROM category_event_aggregates
WHERE window_start >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY category
ORDER BY revenue DESC;

-- Pipeline health — recent batch latencies
SELECT batch_id, events_processed, events_deduplicated, status, duration_ms
FROM pipeline_run_metadata
ORDER BY batch_timestamp DESC
LIMIT 20;
```

---

## 📝 Resume Bullet Points (Reference)

- **Designed** a streaming data architecture using Apache Kafka to produce and consume continuous user clickstream events with 5+ event types (page_view, product_click, add_to_cart, purchase, search).
- **Utilized** PySpark Structured Streaming to process events in micro-batches (10-second intervals), performing real-time windowed aggregations (product, user, category) and stateful deduplication via watermark + `dropDuplicates`.
- **Persisted** refined, low-latency streaming outputs into a transactional MySQL 8 database across 5 normalized tables using idempotent `foreachBatch` upserts for analytical business dashboarding.

---

*Built with ❤️ for the data engineering interview portfolio.*
