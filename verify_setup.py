#!/usr/bin/env python3
# =============================================================================
# verify_setup.py
# Pre-flight checklist — verifies all infrastructure is ready before running
# the pipeline. Run this BEFORE starting kafka_producer.py or spark_streaming.py
# =============================================================================

import sys
import json
import socket
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Verify")

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
WARN = "\033[93m[WARN]\033[0m"

results = []


def check(label, fn):
    try:
        msg = fn()
        print(f"  {PASS}  {label}: {msg}")
        results.append((label, True))
    except Exception as e:
        print(f"  {FAIL}  {label}: {e}")
        results.append((label, False))


# ---------------------------------------------------------------------------
# 1. Python version
# ---------------------------------------------------------------------------
def check_python():
    v = sys.version_info
    assert v >= (3, 9), f"Need Python 3.9+, got {v.major}.{v.minor}"
    return f"Python {v.major}.{v.minor}.{v.micro}"


# ---------------------------------------------------------------------------
# 2. Required packages importable
# ---------------------------------------------------------------------------
def check_kafka():
    from kafka import KafkaProducer  # noqa: F401
    import kafka
    return f"kafka-python-ng {kafka.__version__}"


def check_pyspark():
    import pyspark
    return f"PySpark {pyspark.__version__}"


def check_mysql_connector():
    import mysql.connector
    return f"mysql-connector-python {mysql.connector.__version__}"


# ---------------------------------------------------------------------------
# 3. TCP port reachable
# ---------------------------------------------------------------------------
def port_open(host, port, timeout=3):
    s = socket.create_connection((host, port), timeout=timeout)
    s.close()


def check_kafka_broker():
    port_open("localhost", 9092)
    return "localhost:9092 reachable"


def check_zookeeper():
    port_open("localhost", 2181)
    return "localhost:2181 reachable"


def check_mysql_port():
    port_open("localhost", 3306)
    return "localhost:3306 reachable"


# ---------------------------------------------------------------------------
# 4. MySQL login + tables
# ---------------------------------------------------------------------------
def check_mysql_tables():
    import mysql.connector
    from config import MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        connection_timeout=5,
    )
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    expected = {
        "raw_clickstream_events",
        "product_event_aggregates",
        "user_session_aggregates",
        "category_event_aggregates",
        "pipeline_run_metadata",
    }
    missing = expected - set(tables)
    if missing:
        raise RuntimeError(f"Missing tables: {missing}. Run: python -c \"from mysql_sink import initialize_schema; initialize_schema()\"")
    return f"All 5 tables present in '{MYSQL_DATABASE}'"


# ---------------------------------------------------------------------------
# 5. Kafka topic exists / is reachable
# ---------------------------------------------------------------------------
def check_kafka_topic():
    from kafka import KafkaAdminClient
    from kafka.errors import UnknownTopicOrPartitionError
    from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        request_timeout_ms=5000,
        client_id="verify_setup",
    )
    topics = admin.list_topics()
    admin.close()

    if KAFKA_TOPIC in topics:
        return f"Topic '{KAFKA_TOPIC}' exists"
    else:
        return f"Topic '{KAFKA_TOPIC}' will be auto-created on first produce (auto.create.topics=true)"


# ---------------------------------------------------------------------------
# 6. Quick event generation smoke test
# ---------------------------------------------------------------------------
def check_event_generation():
    from kafka_producer import generate_event
    event = generate_event()
    assert "event_id" in event
    assert "user_id" in event
    assert "event_type" in event
    return f"Sample event OK: type={event['event_type']}, user={event['user_id']}"


# ---------------------------------------------------------------------------
# Run all checks
# ---------------------------------------------------------------------------
def main():
    print("\n" + "="*60)
    print("  Real-Time E-Commerce Streaming Pipeline - Preflight Check")
    print("="*60 + "\n")

    print("[PKG] Python Environment:")
    check("Python version",       check_python)
    check("kafka-python-ng",      check_kafka)
    check("PySpark",              check_pyspark)
    check("mysql-connector",      check_mysql_connector)

    print("\n[DOCKER] Infrastructure:")
    check("Kafka broker (9092)",  check_kafka_broker)
    check("Zookeeper (2181)",     check_zookeeper)
    check("MySQL (3306)",         check_mysql_port)

    print("\n[DB] MySQL Database:")
    check("MySQL login + tables", check_mysql_tables)

    print("\n[KAFKA] Topic Check:")
    check("Kafka topic",          check_kafka_topic)

    print("\n[CODE] Pipeline Code:")
    check("Event generation",     check_event_generation)

    print()
    print("="*60)
    passed = sum(1 for _, ok in results if ok)
    total  = len(results)
    if passed == total:
        print(f"  ALL {total}/{total} CHECKS PASSED -- Pipeline is ready to run!")
        print()
        print("  Start order:")
        print("    Terminal 1 --> python spark_streaming.py")
        print("    Terminal 2 --> python kafka_producer.py")
        print("    Browser    --> http://localhost:8080  (Kafka UI)")
    else:
        failed = [(label, ok) for label, ok in results if not ok]
        print(f"  FAILED: {len(failed)}/{total} checks did not pass:")
        for label, _ in failed:
            print(f"     - {label}")
        print()
        print("  Fix the above issues and re-run: python verify_setup.py")
    print("="*60 + "\n")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
