# =============================================================================
# spark_streaming.py
# PySpark Structured Streaming — consumes Kafka topic, deduplicates events,
# computes windowed aggregations, and persists results to MySQL.
# =============================================================================

import logging
import os
import sys
import time
from datetime import datetime

# ---------------------------------------------------------------------------
# Force PySpark workers to use the SAME Python as this script (Python 3.13).
# Without this, Spark picks up a different Python version from PATH (e.g. 3.11).
# ---------------------------------------------------------------------------
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# ---------------------------------------------------------------------------
# Fix SPARK_HOME — always use the pip-installed PySpark, not any other Spark.
# This overrides any stale SPARK_HOME from other projects (e.g. sentiment_analysis).
# ---------------------------------------------------------------------------
import pyspark as _pyspark
os.environ["SPARK_HOME"] = os.path.dirname(_pyspark.__file__)

# ---------------------------------------------------------------------------
# Auto-set JAVA_HOME so PySpark can find Java on Windows
# ---------------------------------------------------------------------------
_java_candidates = [
    r"C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot",  # Java 17 LTS — best for PySpark 3.5.x
    r"C:\Program Files\Eclipse Adoptium\jdk-17",
    r"C:\Program Files\Java\jdk-17",
    r"C:\Program Files\Java\jdk-11",
    r"C:\Program Files\Java\jdk-25.0.3",                   # fallback only
]
for _jpath in _java_candidates:
    if os.path.isfile(os.path.join(_jpath, "bin", "java.exe")):
        os.environ["JAVA_HOME"] = _jpath
        os.environ["PATH"] = os.path.join(_jpath, "bin") + os.pathsep + os.environ.get("PATH", "")
        break

# ---------------------------------------------------------------------------
# Setup HADOOP_HOME with winutils.exe for Windows (suppress Hadoop warnings)
# ---------------------------------------------------------------------------
_hadoop_home = r"C:\hadoop"
_winutils_path = os.path.join(_hadoop_home, "bin", "winutils.exe")
os.makedirs(os.path.join(_hadoop_home, "bin"), exist_ok=True)
if not os.path.isfile(_winutils_path):
    # Download winutils.exe for Hadoop 3.3.x
    try:
        import urllib.request
        _url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"
        urllib.request.urlretrieve(_url, _winutils_path)
    except Exception:
        # If download fails, create a dummy placeholder (suppresses the warning)
        with open(_winutils_path, "wb") as _f:
            _f.write(b"")
os.environ["HADOOP_HOME"] = _hadoop_home
os.environ["PATH"] = os.path.join(_hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType, LongType,
)

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_AUTO_OFFSET_RESET,
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_CHECKPOINT_DIR,
    MICRO_BATCH_INTERVAL,
    WATERMARK_DELAY,
    WINDOW_DURATION,
    WINDOW_SLIDE_INTERVAL,
    MYSQL_JDBC_URL,
    MYSQL_USER,
    MYSQL_PASSWORD,
    SPARK_PACKAGES,
)
from mysql_sink import (
    initialize_schema,
    write_raw_events,
    write_product_aggregates,
    write_user_aggregates,
    write_category_aggregates,
    log_batch_metadata,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SparkStreaming")

# ---------------------------------------------------------------------------
# JSON schema — must match kafka_producer.py payload exactly
# ---------------------------------------------------------------------------
CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id",        StringType(),    nullable=False),
    StructField("user_id",         StringType(),    nullable=False),
    StructField("session_id",      StringType(),    nullable=False),
    StructField("event_type",      StringType(),    nullable=False),
    StructField("product_id",      StringType(),    nullable=True),
    StructField("product_name",    StringType(),    nullable=True),
    StructField("category",        StringType(),    nullable=True),
    StructField("price",           DoubleType(),    nullable=True),
    StructField("quantity",        IntegerType(),   nullable=True),
    StructField("device_type",     StringType(),    nullable=True),
    StructField("browser",         StringType(),    nullable=True),
    StructField("country",         StringType(),    nullable=True),
    StructField("city",            StringType(),    nullable=True),
    StructField("referrer",        StringType(),    nullable=True),
    StructField("event_timestamp", StringType(),    nullable=False),  # parsed later
])


# ---------------------------------------------------------------------------
# Spark Session factory
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    """
    Create a SparkSession with Kafka and MySQL connector packages.
    Spark downloads the JARs automatically on first run via --packages.
    """
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.jars.packages", SPARK_PACKAGES)
        # Reduce Spark log verbosity in terminal
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.checkpointLocation", SPARK_CHECKPOINT_DIR)
        # Kafka source options
        .config("spark.kafka.consumer.group.id", KAFKA_GROUP_ID)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created: %s", SPARK_APP_NAME)
    return spark


# ---------------------------------------------------------------------------
# Read raw stream from Kafka
# ---------------------------------------------------------------------------

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Subscribe to the Kafka topic and return a streaming DataFrame.
    Each row = one Kafka message (key, value as binary).
    """
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_AUTO_OFFSET_RESET)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 1000)   # Limit per micro-batch for back-pressure
        .load()
    )
    return raw_stream


# ---------------------------------------------------------------------------
# Parse and clean raw Kafka bytes → typed DataFrame
# ---------------------------------------------------------------------------

def parse_events(raw_stream: DataFrame) -> DataFrame:
    """
    Deserialise Kafka value (binary) → JSON → typed struct → flat columns.
    Also casts event_timestamp string to TimestampType.
    """
    parsed = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                CLICKSTREAM_SCHEMA
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),  # Kafka broker timestamp
        )
        .select("data.*", "kafka_timestamp")
        # Cast ISO-8601 string to proper Spark TimestampType
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        )
        # Drop rows where mandatory fields are null (malformed messages)
        .filter(
            F.col("event_id").isNotNull() &
            F.col("user_id").isNotNull() &
            F.col("event_timestamp").isNotNull()
        )
    )
    return parsed


# ---------------------------------------------------------------------------
# Deduplication using Spark watermark + dropDuplicates
# ---------------------------------------------------------------------------

def deduplicate_events(parsed_stream: DataFrame) -> DataFrame:
    """
    Apply watermark-based deduplication on event_id.
    Events arriving more than WATERMARK_DELAY late are dropped.
    This is Spark Structured Streaming's stateful deduplication.
    """
    deduped = (
        parsed_stream
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
    )
    return deduped


# ---------------------------------------------------------------------------
# Windowed aggregations
# ---------------------------------------------------------------------------

def compute_product_aggregates(deduped_stream: DataFrame) -> DataFrame:
    """
    Sliding window aggregations per product_id.
    Only considers product-interaction events (excludes page_view, search).
    """
    product_events = deduped_stream.filter(F.col("product_id").isNotNull())

    agg = (
        product_events
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window("event_timestamp", WINDOW_DURATION, WINDOW_SLIDE_INTERVAL),
            F.col("product_id"),
            F.col("category"),
        )
        .agg(
            F.count(F.when(F.col("event_type") == "page_view",      True)).alias("total_views"),
            F.count(F.when(F.col("event_type") == "product_click",  True)).alias("total_clicks"),
            F.count(F.when(F.col("event_type") == "add_to_cart",    True)).alias("total_add_to_cart"),
            F.count(F.when(F.col("event_type") == "purchase",       True)).alias("total_purchases"),
            F.sum(
                F.when(F.col("event_type") == "purchase",
                       F.col("price") * F.col("quantity"))
                .otherwise(0.0)
            ).alias("total_revenue"),
            F.approx_count_distinct("user_id").alias("unique_users"),
            F.avg("price").alias("avg_price"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "product_id", "category",
            "total_views", "total_clicks", "total_add_to_cart", "total_purchases",
            "total_revenue", "unique_users", "avg_price",
        )
    )
    return agg


def compute_user_aggregates(deduped_stream: DataFrame) -> DataFrame:
    """Sliding window aggregations per user_id."""
    agg = (
        deduped_stream
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window("event_timestamp", WINDOW_DURATION, WINDOW_SLIDE_INTERVAL),
            F.col("user_id"),
        )
        .agg(
            F.count("*").alias("total_events"),
            F.count(F.when(F.col("event_type") == "page_view",     True)).alias("page_views"),
            F.count(F.when(F.col("event_type") == "product_click", True)).alias("product_clicks"),
            F.count(F.when(F.col("event_type") == "add_to_cart",   True)).alias("cart_additions"),
            F.count(F.when(F.col("event_type") == "purchase",      True)).alias("purchases"),
            F.sum(
                F.when(F.col("event_type") == "purchase",
                       F.col("price") * F.col("quantity"))
                .otherwise(0.0)
            ).alias("total_spend"),
            F.approx_count_distinct("session_id").alias("unique_sessions"),
            # Most-used device for this user in this window
            F.first("device_type").alias("primary_device"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "user_id", "total_events", "page_views", "product_clicks",
            "cart_additions", "purchases", "total_spend", "unique_sessions", "primary_device",
        )
    )
    return agg


def compute_category_aggregates(deduped_stream: DataFrame) -> DataFrame:
    """Sliding window aggregations per product category."""
    cat_events = deduped_stream.filter(F.col("category").isNotNull())

    agg = (
        cat_events
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window("event_timestamp", WINDOW_DURATION, WINDOW_SLIDE_INTERVAL),
            F.col("category"),
        )
        .agg(
            F.count("*").alias("total_events"),
            F.count(F.when(F.col("event_type") == "page_view",  True)).alias("total_views"),
            F.count(F.when(F.col("event_type") == "purchase",   True)).alias("total_purchases"),
            F.sum(
                F.when(F.col("event_type") == "purchase",
                       F.col("price") * F.col("quantity"))
                .otherwise(0.0)
            ).alias("total_revenue"),
            F.approx_count_distinct("user_id").alias("unique_users"),
            F.approx_count_distinct("product_id").alias("unique_products"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "category", "total_events", "total_views",
            "total_purchases", "total_revenue", "unique_users", "unique_products",
        )
    )
    return agg


# ---------------------------------------------------------------------------
# foreachBatch sink — writes each micro-batch to MySQL
# ---------------------------------------------------------------------------

def _df_to_dicts(df: DataFrame) -> list:
    """Convert a Spark DataFrame batch to a list of plain Python dicts."""
    rows = df.collect()
    return [row.asDict() for row in rows]


def make_batch_writer(batch_write_fn):
    """
    Factory: returns a foreachBatch function that calls batch_write_fn(records).
    Handles empty batches gracefully.
    """
    def write_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            logger.info("[batch %d] Empty batch — nothing to write.", batch_id)
            return
        records = _df_to_dicts(batch_df)
        logger.info("[batch %d] Writing %d records via %s",
                    batch_id, len(records), batch_write_fn.__name__)
        batch_write_fn(records)
    return write_batch


def raw_events_batch_writer(batch_df: DataFrame, batch_id: int):
    """
    foreachBatch sink for the raw deduplicated event stream.
    Also logs pipeline metadata per micro-batch.
    """
    start_ts = time.time()
    if batch_df.isEmpty():
        logger.info("[batch %d] Empty raw events batch.", batch_id)
        return

    records = _df_to_dicts(batch_df)

    # Compute window bounds for metadata from the batch
    timestamps = [r["event_timestamp"] for r in records if r.get("event_timestamp")]
    window_start = min(timestamps) if timestamps else None
    window_end   = max(timestamps) if timestamps else None

    # Format for MySQL datetime
    def _fmt(ts):
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        return str(ts)

    # Prepare records for MySQL (rename/format columns)
    mysql_records = []
    for r in records:
        mysql_records.append({
            "event_id":        r.get("event_id"),
            "user_id":         r.get("user_id"),
            "session_id":      r.get("session_id"),
            "event_type":      r.get("event_type"),
            "product_id":      r.get("product_id"),
            "category":        r.get("category"),
            "price":           r.get("price"),
            "quantity":        r.get("quantity"),
            "device_type":     r.get("device_type"),
            "browser":         r.get("browser"),
            "country":         r.get("country"),
            "city":            r.get("city"),
            "referrer":        r.get("referrer"),
            "event_timestamp": _fmt(r.get("event_timestamp")),
        })

    inserted = write_raw_events(mysql_records)
    duration_ms = int((time.time() - start_ts) * 1000)

    log_batch_metadata(
        batch_id=batch_id,
        events_processed=len(records),
        events_deduplicated=len(records) - inserted,
        window_start=_fmt(window_start),
        window_end=_fmt(window_end),
        status="SUCCESS",
        duration_ms=duration_ms,
    )


def product_agg_batch_writer(batch_df: DataFrame, batch_id: int):
    """foreachBatch sink for product window aggregations."""
    if batch_df.isEmpty():
        return

    def _fmt(ts):
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        return str(ts)

    records = _df_to_dicts(batch_df)
    mysql_records = []
    for r in records:
        mysql_records.append({
            "window_start":     _fmt(r.get("window_start")),
            "window_end":       _fmt(r.get("window_end")),
            "product_id":       r.get("product_id"),
            "category":         r.get("category"),
            "total_views":      int(r.get("total_views", 0) or 0),
            "total_clicks":     int(r.get("total_clicks", 0) or 0),
            "total_add_to_cart":int(r.get("total_add_to_cart", 0) or 0),
            "total_purchases":  int(r.get("total_purchases", 0) or 0),
            "total_revenue":    float(r.get("total_revenue", 0.0) or 0.0),
            "unique_users":     int(r.get("unique_users", 0) or 0),
            "avg_price":        float(r.get("avg_price", 0.0) or 0.0),
        })

    write_product_aggregates(mysql_records)


def user_agg_batch_writer(batch_df: DataFrame, batch_id: int):
    """foreachBatch sink for user session window aggregations."""
    if batch_df.isEmpty():
        return

    def _fmt(ts):
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        return str(ts)

    records = _df_to_dicts(batch_df)
    mysql_records = []
    for r in records:
        mysql_records.append({
            "window_start":   _fmt(r.get("window_start")),
            "window_end":     _fmt(r.get("window_end")),
            "user_id":        r.get("user_id"),
            "total_events":   int(r.get("total_events", 0) or 0),
            "page_views":     int(r.get("page_views", 0) or 0),
            "product_clicks": int(r.get("product_clicks", 0) or 0),
            "cart_additions": int(r.get("cart_additions", 0) or 0),
            "purchases":      int(r.get("purchases", 0) or 0),
            "total_spend":    float(r.get("total_spend", 0.0) or 0.0),
            "unique_sessions":int(r.get("unique_sessions", 0) or 0),
            "primary_device": r.get("primary_device"),
        })

    write_user_aggregates(mysql_records)


def category_agg_batch_writer(batch_df: DataFrame, batch_id: int):
    """foreachBatch sink for category window aggregations."""
    if batch_df.isEmpty():
        return

    def _fmt(ts):
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        return str(ts)

    records = _df_to_dicts(batch_df)
    mysql_records = []
    for r in records:
        mysql_records.append({
            "window_start":    _fmt(r.get("window_start")),
            "window_end":      _fmt(r.get("window_end")),
            "category":        r.get("category"),
            "total_events":    int(r.get("total_events", 0) or 0),
            "total_views":     int(r.get("total_views", 0) or 0),
            "total_purchases": int(r.get("total_purchases", 0) or 0),
            "total_revenue":   float(r.get("total_revenue", 0.0) or 0.0),
            "unique_users":    int(r.get("unique_users", 0) or 0),
            "unique_products": int(r.get("unique_products", 0) or 0),
        })

    write_category_aggregates(mysql_records)


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_pipeline():
    """
    Main pipeline orchestrator:
    1. Initialize MySQL schema
    2. Create SparkSession
    3. Read from Kafka
    4. Parse + Deduplicate
    5. Fork into aggregation streams
    6. Attach foreachBatch sinks
    7. Await termination
    """
    # Step 1 — MySQL schema
    logger.info("Step 1/7 — Initializing MySQL schema ...")
    initialize_schema("schema.sql")

    # Step 2 — Spark
    logger.info("Step 2/7 — Creating SparkSession ...")
    spark = create_spark_session()

    # Step 3 — Kafka source
    logger.info("Step 3/7 — Connecting to Kafka topic '%s' ...", KAFKA_TOPIC)
    raw_stream = read_kafka_stream(spark)

    # Step 4 — Parse + Deduplicate
    logger.info("Step 4/7 — Parsing and deduplicating event stream ...")
    parsed_stream = parse_events(raw_stream)
    deduped_stream = deduplicate_events(parsed_stream)

    # Step 5 — Aggregations
    logger.info("Step 5/7 — Computing windowed aggregations ...")
    product_agg_stream  = compute_product_aggregates(deduped_stream)
    user_agg_stream     = compute_user_aggregates(deduped_stream)
    category_agg_stream = compute_category_aggregates(deduped_stream)

    # Step 6 — Attach sinks
    logger.info("Step 6/7 — Attaching foreachBatch MySQL sinks ...")

    # Raw deduplicated events sink
    raw_query = (
        deduped_stream
        .writeStream
        .outputMode("append")
        .trigger(processingTime=MICRO_BATCH_INTERVAL)
        .foreachBatch(raw_events_batch_writer)
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/raw_events")
        .queryName("raw_clickstream_sink")
        .start()
    )

    # Product aggregations sink
    product_query = (
        product_agg_stream
        .writeStream
        .outputMode("update")
        .trigger(processingTime=MICRO_BATCH_INTERVAL)
        .foreachBatch(product_agg_batch_writer)
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/product_agg")
        .queryName("product_agg_sink")
        .start()
    )

    # User aggregations sink
    user_query = (
        user_agg_stream
        .writeStream
        .outputMode("update")
        .trigger(processingTime=MICRO_BATCH_INTERVAL)
        .foreachBatch(user_agg_batch_writer)
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/user_agg")
        .queryName("user_agg_sink")
        .start()
    )

    # Category aggregations sink
    category_query = (
        category_agg_stream
        .writeStream
        .outputMode("update")
        .trigger(processingTime=MICRO_BATCH_INTERVAL)
        .foreachBatch(category_agg_batch_writer)
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/category_agg")
        .queryName("category_agg_sink")
        .start()
    )

    # Step 7 — Await termination
    logger.info(
        "Step 7/7 — Pipeline is LIVE. Active streaming queries:\n"
        "  • %s\n  • %s\n  • %s\n  • %s",
        raw_query.name, product_query.name,
        user_query.name, category_query.name,
    )
    logger.info("Press Ctrl+C to gracefully stop the pipeline.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Stopping all streaming queries ...")
        for q in [raw_query, product_query, user_query, category_query]:
            q.stop()
        spark.stop()
        logger.info("Pipeline stopped gracefully.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_pipeline()
