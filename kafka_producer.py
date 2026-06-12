# =============================================================================
# kafka_producer.py
# Simulates real-time e-commerce user clickstream events into Apache Kafka
# =============================================================================

import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    EVENTS_PER_SECOND,
    PRODUCER_SLEEP_INTERVAL,
    TOTAL_EVENTS,
)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KafkaProducer")

# ---------------------------------------------------------------------------
# Sample data pools — mimics a real e-commerce catalogue
# ---------------------------------------------------------------------------
EVENT_TYPES = [
    "page_view",
    "product_click",
    "add_to_cart",
    "purchase",
    "search",
    "wishlist_add",
    "checkout_start",
]

# Weighted distribution so page_view >> purchase (realistic funnel)
EVENT_WEIGHTS = [35, 25, 15, 8, 10, 4, 3]

CATEGORIES = [
    "Electronics",
    "Fashion",
    "Home & Kitchen",
    "Books",
    "Sports & Outdoors",
    "Beauty",
    "Toys & Games",
    "Automotive",
]

PRODUCTS = {
    "Electronics": [
        ("P1001", "Wireless Headphones",   2999.00),
        ("P1002", "Bluetooth Speaker",     1499.00),
        ("P1003", "Smartwatch",            8999.00),
        ("P1004", "USB-C Hub",              899.00),
        ("P1005", "Mechanical Keyboard",   3499.00),
    ],
    "Fashion": [
        ("P2001", "Running Shoes",         3999.00),
        ("P2002", "Denim Jacket",          2499.00),
        ("P2003", "Sunglasses",            1299.00),
        ("P2004", "Backpack",              1999.00),
        ("P2005", "Wristwatch",            5999.00),
    ],
    "Home & Kitchen": [
        ("P3001", "Air Fryer",             4499.00),
        ("P3002", "Coffee Maker",          2999.00),
        ("P3003", "Robot Vacuum",         12999.00),
        ("P3004", "Blender",               2199.00),
        ("P3005", "Instant Pot",           5499.00),
    ],
    "Books": [
        ("P4001", "Clean Code",             699.00),
        ("P4002", "Designing Data-Intensive Applications", 1299.00),
        ("P4003", "The Pragmatic Programmer", 799.00),
        ("P4004", "Python Crash Course",    899.00),
        ("P4005", "Atomic Habits",          599.00),
    ],
    "Sports & Outdoors": [
        ("P5001", "Yoga Mat",               999.00),
        ("P5002", "Resistance Bands",       499.00),
        ("P5003", "Trekking Poles",        1899.00),
        ("P5004", "Fitness Tracker",       3499.00),
        ("P5005", "Cycling Helmet",        2299.00),
    ],
    "Beauty": [
        ("P6001", "Face Serum",            1499.00),
        ("P6002", "Hair Dryer",            2799.00),
        ("P6003", "Perfume",               3999.00),
        ("P6004", "Electric Toothbrush",   1999.00),
        ("P6005", "Moisturizer SPF 50",     999.00),
    ],
    "Toys & Games": [
        ("P7001", "LEGO Architecture Set", 3499.00),
        ("P7002", "Board Game Catan",      1999.00),
        ("P7003", "RC Car",                2299.00),
        ("P7004", "Jigsaw Puzzle 1000pc",   799.00),
        ("P7005", "Action Figure Set",      999.00),
    ],
    "Automotive": [
        ("P8001", "Car Dash Cam",          2499.00),
        ("P8002", "Tyre Inflator",         1599.00),
        ("P8003", "Car Phone Holder",       399.00),
        ("P8004", "Leather Seat Cover",    3299.00),
        ("P8005", "Jump Starter",          4999.00),
    ],
}

DEVICES    = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [55, 35, 10]

BROWSERS   = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
BROWSER_WEIGHTS = [50, 20, 15, 10, 5]

COUNTRIES  = ["India", "USA", "UK", "Germany", "Australia", "Canada", "Singapore"]
CITIES = {
    "India":     ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Pune"],
    "USA":       ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
    "UK":        ["London", "Manchester", "Birmingham", "Leeds"],
    "Germany":   ["Berlin", "Munich", "Hamburg", "Frankfurt"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth"],
    "Canada":    ["Toronto", "Vancouver", "Montreal", "Calgary"],
    "Singapore": ["Singapore"],
}

REFERRERS  = [
    "https://google.com",
    "https://facebook.com",
    "https://instagram.com",
    "direct",
    "https://youtube.com",
    "email_campaign",
    "https://twitter.com",
    "affiliate_partner",
]


# ---------------------------------------------------------------------------
# Event generator
# ---------------------------------------------------------------------------

def _generate_user_pool(size: int = 500) -> list:
    """Pre-generate a pool of user_ids to create realistic repeat sessions."""
    return [f"U{str(uuid.uuid4().int)[:8]}" for _ in range(size)]


USER_POOL = _generate_user_pool(500)


def generate_event() -> dict:
    """
    Generate a single synthetic e-commerce clickstream event.
    Returns a dictionary representing the JSON payload.
    """
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    category   = random.choice(CATEGORIES)
    product    = random.choice(PRODUCTS[category])
    country    = random.choice(COUNTRIES)
    city       = random.choice(CITIES[country])

    product_id, product_name, price = product

    # For non-product-interaction events, null out product fields
    if event_type in ("page_view", "search"):
        product_id   = None
        product_name = None
        price        = None
        quantity     = None
        category     = None
    elif event_type in ("add_to_cart", "purchase"):
        quantity = random.randint(1, 5)
    else:
        # product_click, wishlist_add, checkout_start — single unit
        quantity = 1

    return {
        "event_id":        str(uuid.uuid4()),          # Unique — used for deduplication
        "user_id":         random.choice(USER_POOL),
        "session_id":      str(uuid.uuid4()),
        "event_type":      event_type,
        "product_id":      product_id,
        "product_name":    product_name,
        "category":        category,
        "price":           float(price) if price else None,
        "quantity":        quantity,
        "device_type":     random.choices(DEVICES,   weights=DEVICE_WEIGHTS,   k=1)[0],
        "browser":         random.choices(BROWSERS,  weights=BROWSER_WEIGHTS,  k=1)[0],
        "country":         country,
        "city":            city,
        "referrer":        random.choice(REFERRERS),
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }


# ---------------------------------------------------------------------------
# Delivery report callbacks
# ---------------------------------------------------------------------------

def on_send_success(record_metadata):
    logger.debug(
        "Event delivered → topic=%s  partition=%d  offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(exc: KafkaError):
    logger.error("Failed to deliver message: %s", exc)


# ---------------------------------------------------------------------------
# Producer bootstrap
# ---------------------------------------------------------------------------

def create_producer() -> KafkaProducer:
    """Create and return a configured KafkaProducer instance."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        acks="all",                   # Wait for all replicas to acknowledge
        retries=5,
        retry_backoff_ms=300,
        max_block_ms=10_000,          # 10 s connection timeout
        compression_type="gzip",      # Compress payloads
        linger_ms=10,                 # Micro-batch up to 10 ms for throughput
        batch_size=16_384,            # 16 KB batch size
    )


# ---------------------------------------------------------------------------
# Main producer loop
# ---------------------------------------------------------------------------

def run_producer():
    """
    Continuously produce clickstream events to Kafka.
    Sends TOTAL_EVENTS events (or runs infinitely if TOTAL_EVENTS == 0).
    """
    logger.info("Starting Kafka Producer → bootstrap=%s, topic=%s",
                KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    producer = create_producer()
    events_sent = 0

    try:
        while True:
            event     = generate_event()
            event_key = event["user_id"]   # Partition by user for ordering

            (
                producer.send(KAFKA_TOPIC, key=event_key, value=event)
                        .add_callback(on_send_success)
                        .add_errback(on_send_error)
            )

            events_sent += 1

            if events_sent % 100 == 0:
                producer.flush()
                logger.info("Events produced so far: %d", events_sent)

            if TOTAL_EVENTS and events_sent >= TOTAL_EVENTS:
                logger.info("Reached target of %d events. Stopping producer.", TOTAL_EVENTS)
                break

            time.sleep(PRODUCER_SLEEP_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user after %d events.", events_sent)

    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed. Total events sent: %d", events_sent)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_producer()
