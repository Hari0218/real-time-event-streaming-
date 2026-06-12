-- =============================================================================
-- schema.sql
-- MySQL Database Schema for Real-Time E-Commerce Streaming Pipeline
--
-- NOTE: Run this file AFTER connecting to the ecommerce_streaming database.
--       The python pipeline (mysql_sink.py) already connects to that DB.
--       The docker-compose.yml creates the DB automatically via MYSQL_DATABASE env var.
-- =============================================================================

-- =============================================================================
-- Table 1: raw_clickstream_events
-- Stores deduplicated raw events from Kafka
-- =============================================================================
CREATE TABLE IF NOT EXISTS raw_clickstream_events (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    event_id        VARCHAR(64)     NOT NULL UNIQUE COMMENT 'UUID - used for deduplication',
    user_id         VARCHAR(64)     NOT NULL,
    session_id      VARCHAR(64)     NOT NULL,
    event_type      VARCHAR(32)     NOT NULL COMMENT 'page_view|product_click|add_to_cart|purchase|search',
    product_id      VARCHAR(64),
    category        VARCHAR(64),
    price           DECIMAL(10, 2),
    quantity        INT             DEFAULT 1,
    device_type     VARCHAR(32)     COMMENT 'mobile|desktop|tablet',
    browser         VARCHAR(32),
    country         VARCHAR(64),
    city            VARCHAR(64),
    referrer        VARCHAR(255),
    event_timestamp DATETIME(3)     NOT NULL COMMENT 'Millisecond precision event time',
    ingested_at     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP COMMENT 'Time record was written to DB',
    PRIMARY KEY (id),
    INDEX idx_event_id      (event_id),
    INDEX idx_user_id       (user_id),
    INDEX idx_event_type    (event_type),
    INDEX idx_event_timestamp (event_timestamp),
    INDEX idx_product_id    (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Deduplicated raw clickstream events from Kafka';

-- =============================================================================
-- Table 2: product_event_aggregates
-- Aggregated product-level metrics per time window (updated per micro-batch)
-- =============================================================================
CREATE TABLE IF NOT EXISTS product_event_aggregates (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    window_start    DATETIME        NOT NULL COMMENT 'Start of the aggregation window',
    window_end      DATETIME        NOT NULL COMMENT 'End of the aggregation window',
    product_id      VARCHAR(64)     NOT NULL,
    category        VARCHAR(64),
    total_views     BIGINT          DEFAULT 0,
    total_clicks    BIGINT          DEFAULT 0,
    total_add_to_cart BIGINT        DEFAULT 0,
    total_purchases BIGINT          DEFAULT 0,
    total_revenue   DECIMAL(15, 2)  DEFAULT 0.00,
    unique_users    BIGINT          DEFAULT 0,
    avg_price       DECIMAL(10, 2)  DEFAULT 0.00,
    computed_at     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_window_product (window_start, window_end, product_id),
    INDEX idx_product_agg       (product_id),
    INDEX idx_window_start      (window_start),
    INDEX idx_category_agg      (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Windowed product-level aggregations from Spark Structured Streaming';

-- =============================================================================
-- Table 3: user_session_aggregates
-- Aggregated user/session-level metrics per time window
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_session_aggregates (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    window_start    DATETIME        NOT NULL,
    window_end      DATETIME        NOT NULL,
    user_id         VARCHAR(64)     NOT NULL,
    total_events    BIGINT          DEFAULT 0,
    page_views      BIGINT          DEFAULT 0,
    product_clicks  BIGINT          DEFAULT 0,
    cart_additions  BIGINT          DEFAULT 0,
    purchases       BIGINT          DEFAULT 0,
    total_spend     DECIMAL(15, 2)  DEFAULT 0.00,
    unique_sessions BIGINT          DEFAULT 0,
    primary_device  VARCHAR(32),
    computed_at     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_window_user (window_start, window_end, user_id),
    INDEX idx_user_agg      (user_id),
    INDEX idx_window_user   (window_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Windowed user-level aggregations from Spark Structured Streaming';

-- =============================================================================
-- Table 4: category_event_aggregates
-- Aggregated category-level metrics per time window
-- =============================================================================
CREATE TABLE IF NOT EXISTS category_event_aggregates (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    window_start    DATETIME        NOT NULL,
    window_end      DATETIME        NOT NULL,
    category        VARCHAR(64)     NOT NULL,
    total_events    BIGINT          DEFAULT 0,
    total_views     BIGINT          DEFAULT 0,
    total_purchases BIGINT          DEFAULT 0,
    total_revenue   DECIMAL(15, 2)  DEFAULT 0.00,
    unique_users    BIGINT          DEFAULT 0,
    unique_products BIGINT          DEFAULT 0,
    computed_at     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_window_category (window_start, window_end, category),
    INDEX idx_category      (category),
    INDEX idx_window_cat    (window_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Windowed category-level aggregations from Spark Structured Streaming';

-- =============================================================================
-- Table 5: pipeline_run_metadata
-- Tracks each Spark micro-batch execution for observability
-- =============================================================================
CREATE TABLE IF NOT EXISTS pipeline_run_metadata (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    batch_id        BIGINT          NOT NULL,
    batch_timestamp TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    events_processed BIGINT         DEFAULT 0,
    events_deduplicated BIGINT      DEFAULT 0,
    window_start    DATETIME,
    window_end      DATETIME,
    status          VARCHAR(16)     DEFAULT 'SUCCESS' COMMENT 'SUCCESS|FAILED',
    error_message   TEXT,
    duration_ms     BIGINT,
    PRIMARY KEY (id),
    INDEX idx_batch_id  (batch_id),
    INDEX idx_batch_ts  (batch_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Pipeline observability and batch tracking metadata';

-- All tables created successfully.
