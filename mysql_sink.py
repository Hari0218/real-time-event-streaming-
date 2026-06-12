# =============================================================================
# mysql_sink.py
# MySQL helper — schema initialization + batch write utilities for the pipeline
# =============================================================================

import logging
import os
import time
import mysql.connector
from mysql.connector import Error as MySQLError
from datetime import datetime

from config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_DATABASE,
    MYSQL_USER,
    MYSQL_PASSWORD,
)

logger = logging.getLogger("MySQLSink")


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

def get_connection(retries: int = 5, delay: int = 3):
    """
    Create and return a MySQL connection with retry logic.
    Retries up to `retries` times with `delay` seconds between attempts.
    """
    for attempt in range(1, retries + 1):
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                database=MYSQL_DATABASE,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                autocommit=False,
                connection_timeout=10,
                charset="utf8mb4",
            )
            logger.info("MySQL connection established (attempt %d).", attempt)
            return conn
        except MySQLError as err:
            logger.warning(
                "MySQL connection attempt %d/%d failed: %s", attempt, retries, err
            )
            if attempt < retries:
                time.sleep(delay)
    raise ConnectionError(
        f"Could not connect to MySQL at {MYSQL_HOST}:{MYSQL_PORT} after {retries} attempts."
    )


# ---------------------------------------------------------------------------
# Schema initializer — run once at startup
# ---------------------------------------------------------------------------

def initialize_schema(schema_file: str = "schema.sql"):
    """
    Execute schema.sql against MySQL to create all tables if they don't exist.
    Splits on semicolons to execute multi-statement DDL files.
    Looks for schema.sql in the same directory as this script if not found in CWD.
    """
    # Resolve path: try CWD first, then the script's own directory
    if not os.path.isabs(schema_file):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        if not os.path.exists(schema_file):
            schema_file = os.path.join(script_dir, os.path.basename(schema_file))

    logger.info("Initializing MySQL schema from '%s' ...", schema_file)
    try:
        with open(schema_file, "r", encoding="utf-8") as f:
            raw_sql = f.read()

        # Strip comment lines and split on ';'
        lines = []
        for line in raw_sql.splitlines():
            stripped = line.strip()
            if not stripped.startswith("--"):
                lines.append(line)
        cleaned_sql = "\n".join(lines)

        statements = [
            stmt.strip()
            for stmt in cleaned_sql.split(";")
            if stmt.strip()  # skip blank/empty statements
        ]

        conn = get_connection()
        cursor = conn.cursor()

        for stmt in statements:
            if stmt:
                try:
                    cursor.execute(stmt)
                except MySQLError as err:
                    # Suppress "table already exists" warnings during re-runs
                    if err.errno == 1050:
                        logger.debug("Table already exists — skipping.")
                    else:
                        logger.error("DDL error: %s\nStatement: %s", err, stmt[:120])

        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Schema initialization complete.")

    except FileNotFoundError:
        logger.error("schema.sql not found at: %s", schema_file)
        raise


# ---------------------------------------------------------------------------
# Raw events insert (batch upsert with deduplication via IGNORE)
# ---------------------------------------------------------------------------

RAW_EVENTS_INSERT = """
    INSERT IGNORE INTO raw_clickstream_events
        (event_id, user_id, session_id, event_type, product_id, category,
         price, quantity, device_type, browser, country, city, referrer, event_timestamp)
    VALUES
        (%(event_id)s, %(user_id)s, %(session_id)s, %(event_type)s, %(product_id)s, %(category)s,
         %(price)s, %(quantity)s, %(device_type)s, %(browser)s, %(country)s, %(city)s, %(referrer)s,
         %(event_timestamp)s)
"""


def write_raw_events(events: list) -> int:
    """
    Batch-insert raw events into raw_clickstream_events.
    Uses INSERT IGNORE for server-side deduplication on event_id.
    Returns the number of rows actually inserted.
    """
    if not events:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0

    try:
        cursor.executemany(RAW_EVENTS_INSERT, events)
        conn.commit()
        inserted = cursor.rowcount
        logger.info("Raw events: attempted=%d  inserted=%d  duplicates_skipped=%d",
                    len(events), inserted, len(events) - inserted)
    except MySQLError as err:
        conn.rollback()
        logger.error("Failed to write raw events: %s", err)
        raise
    finally:
        cursor.close()
        conn.close()

    return inserted


# ---------------------------------------------------------------------------
# Product aggregates upsert
# ---------------------------------------------------------------------------

PRODUCT_AGG_UPSERT = """
    INSERT INTO product_event_aggregates
        (window_start, window_end, product_id, category,
         total_views, total_clicks, total_add_to_cart, total_purchases,
         total_revenue, unique_users, avg_price)
    VALUES
        (%(window_start)s, %(window_end)s, %(product_id)s, %(category)s,
         %(total_views)s, %(total_clicks)s, %(total_add_to_cart)s, %(total_purchases)s,
         %(total_revenue)s, %(unique_users)s, %(avg_price)s)
    ON DUPLICATE KEY UPDATE
        total_views         = VALUES(total_views),
        total_clicks        = VALUES(total_clicks),
        total_add_to_cart   = VALUES(total_add_to_cart),
        total_purchases     = VALUES(total_purchases),
        total_revenue       = VALUES(total_revenue),
        unique_users        = VALUES(unique_users),
        avg_price           = VALUES(avg_price),
        computed_at         = CURRENT_TIMESTAMP
"""


def write_product_aggregates(records: list) -> None:
    """Upsert windowed product-level aggregations into MySQL."""
    if not records:
        return

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(PRODUCT_AGG_UPSERT, records)
        conn.commit()
        logger.info("Product aggregates upserted: %d records", len(records))
    except MySQLError as err:
        conn.rollback()
        logger.error("Failed to write product aggregates: %s", err)
        raise
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# User session aggregates upsert
# ---------------------------------------------------------------------------

USER_AGG_UPSERT = """
    INSERT INTO user_session_aggregates
        (window_start, window_end, user_id, total_events, page_views,
         product_clicks, cart_additions, purchases, total_spend,
         unique_sessions, primary_device)
    VALUES
        (%(window_start)s, %(window_end)s, %(user_id)s, %(total_events)s, %(page_views)s,
         %(product_clicks)s, %(cart_additions)s, %(purchases)s, %(total_spend)s,
         %(unique_sessions)s, %(primary_device)s)
    ON DUPLICATE KEY UPDATE
        total_events    = VALUES(total_events),
        page_views      = VALUES(page_views),
        product_clicks  = VALUES(product_clicks),
        cart_additions  = VALUES(cart_additions),
        purchases       = VALUES(purchases),
        total_spend     = VALUES(total_spend),
        unique_sessions = VALUES(unique_sessions),
        primary_device  = VALUES(primary_device),
        computed_at     = CURRENT_TIMESTAMP
"""


def write_user_aggregates(records: list) -> None:
    """Upsert windowed user-level aggregations into MySQL."""
    if not records:
        return

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(USER_AGG_UPSERT, records)
        conn.commit()
        logger.info("User aggregates upserted: %d records", len(records))
    except MySQLError as err:
        conn.rollback()
        logger.error("Failed to write user aggregates: %s", err)
        raise
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# Category aggregates upsert
# ---------------------------------------------------------------------------

CATEGORY_AGG_UPSERT = """
    INSERT INTO category_event_aggregates
        (window_start, window_end, category, total_events, total_views,
         total_purchases, total_revenue, unique_users, unique_products)
    VALUES
        (%(window_start)s, %(window_end)s, %(category)s, %(total_events)s, %(total_views)s,
         %(total_purchases)s, %(total_revenue)s, %(unique_users)s, %(unique_products)s)
    ON DUPLICATE KEY UPDATE
        total_events    = VALUES(total_events),
        total_views     = VALUES(total_views),
        total_purchases = VALUES(total_purchases),
        total_revenue   = VALUES(total_revenue),
        unique_users    = VALUES(unique_users),
        unique_products = VALUES(unique_products),
        computed_at     = CURRENT_TIMESTAMP
"""


def write_category_aggregates(records: list) -> None:
    """Upsert windowed category-level aggregations into MySQL."""
    if not records:
        return

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(CATEGORY_AGG_UPSERT, records)
        conn.commit()
        logger.info("Category aggregates upserted: %d records", len(records))
    except MySQLError as err:
        conn.rollback()
        logger.error("Failed to write category aggregates: %s", err)
        raise
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# Pipeline metadata tracker
# ---------------------------------------------------------------------------

METADATA_INSERT = """
    INSERT INTO pipeline_run_metadata
        (batch_id, events_processed, events_deduplicated, window_start, window_end,
         status, error_message, duration_ms)
    VALUES
        (%(batch_id)s, %(events_processed)s, %(events_deduplicated)s,
         %(window_start)s, %(window_end)s, %(status)s, %(error_message)s, %(duration_ms)s)
"""


def log_batch_metadata(
    batch_id: int,
    events_processed: int,
    events_deduplicated: int,
    window_start,
    window_end,
    status: str = "SUCCESS",
    error_message: str = None,
    duration_ms: int = 0,
) -> None:
    """Record metadata about each Spark micro-batch run for observability."""
    record = {
        "batch_id":             batch_id,
        "events_processed":     events_processed,
        "events_deduplicated":  events_deduplicated,
        "window_start":         window_start,
        "window_end":           window_end,
        "status":               status,
        "error_message":        error_message,
        "duration_ms":          duration_ms,
    }
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(METADATA_INSERT, record)
        conn.commit()
    except MySQLError as err:
        conn.rollback()
        logger.error("Failed to log batch metadata: %s", err)
    finally:
        cursor.close()
        conn.close()
