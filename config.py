# =============================================================================
# config.py
# Centralized configuration for the Real-Time E-Commerce Streaming Pipeline
# =============================================================================

# ---------------------
# Kafka Configuration
# ---------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce_clickstream"
KAFKA_GROUP_ID = "ecommerce_spark_consumer"
KAFKA_AUTO_OFFSET_RESET = "earliest"

# ---------------------
# MySQL Configuration
# ---------------------
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "ecommerce_streaming"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root123"
MYSQL_JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    f"?useSSL=false&allowPublicKeyRetrieval=true"
)

# ---------------------
# Spark Configuration
# ---------------------
SPARK_APP_NAME = "EcommerceStreamingPipeline"
SPARK_MASTER = "local[*]"
SPARK_CHECKPOINT_DIR = "./spark_checkpoint"

# Micro-batch trigger interval (seconds)
MICRO_BATCH_INTERVAL = "10 seconds"

# Watermark delay for late events
WATERMARK_DELAY = "30 seconds"

# ---------------------
# Streaming Window
# ---------------------
WINDOW_DURATION = "5 minutes"
WINDOW_SLIDE_INTERVAL = "1 minute"

# ---------------------
# Producer Configuration
# ---------------------
EVENTS_PER_SECOND = 5          # Number of events to produce per second
PRODUCER_SLEEP_INTERVAL = 0.2  # seconds between event emissions (1/EVENTS_PER_SECOND)
TOTAL_EVENTS = 10000           # Total events before producer stops (0 = infinite)

# ---------------------
# Kafka JAR Packages (for PySpark)
# ---------------------
SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "mysql:mysql-connector-java:8.0.33"
)
