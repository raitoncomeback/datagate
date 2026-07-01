# =============================================================================
# src/streaming/consumer.py
# DataGate streaming consumer
#
# Reads raw tick events from Kafka topic market.ticks.raw,
# runs each event through DataGate's quality gate in real-time,
# and routes:
#   PASS  → DuckDB live_ticks table + Kafka market.ticks.verified
#   FAIL  → DuckDB stream_quarantine table + Kafka market.ticks.quarantine
#
# This is the streaming equivalent of gate.py — same 4 checks,
# same failure codes, but operating on individual events rather
# than daily batch files.
#
# Run with: python -m src.streaming.consumer
# =============================================================================

import json
import time
import uuid
from datetime import datetime, timezone, timedelta
from loguru import logger

from kafka import KafkaConsumer, KafkaProducer

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_RAW = "market.ticks.raw"
TOPIC_VERIFIED = "market.ticks.verified"
TOPIC_QUARANTINE = "market.ticks.quarantine"
CONSUMER_GROUP = "datagate-gate-consumer"

# Per-field validation rules for streaming tick events
PRICE_FIELDS = ["open", "high", "low", "close"]
REQUIRED_FIELDS = {"ticker", "timestamp", "open", "high", "low", "close", "volume"}

# Freshness SLA for streaming ticks — 15 min delay is expected (yfinance)
# We allow up to 20 minutes to give some buffer
FRESHNESS_MAX_MINUTES = 20


# ---------------------------------------------------------------------------
# Gate checks (streaming version)
# ---------------------------------------------------------------------------

def check_schema(event: dict) -> tuple[bool, str | None]:
    missing = [f for f in REQUIRED_FIELDS if f not in event or event[f] is None]
    if missing:
        return False, f"missing fields: {missing}"
    return True, None


def check_range(event: dict) -> tuple[bool, str | None]:
    for field in PRICE_FIELDS:
        val = event.get(field)
        if val is None or float(val) <= 0:
            return False, f"{field}={val} must be > 0"

    if event["volume"] < 0:
        return False, f"volume={event['volume']} cannot be negative"

    if float(event["high"]) < float(event["low"]):
        return False, f"high ({event['high']}) < low ({event['low']})"

    # Sanity check — price shouldn't move more than 20% in one 5-min bar
    open_price = float(event["open"])
    close_price = float(event["close"])
    if open_price > 0:
        bar_change_pct = abs((close_price - open_price) / open_price) * 100
        if bar_change_pct > 20:
            return False, f"suspicious bar change: {bar_change_pct:.1f}% in one interval"

    return True, None


def check_freshness(event: dict) -> tuple[bool, str | None]:
    try:
        # Parse the bar timestamp
        ts_raw = event.get("timestamp", "")
        if not ts_raw:
            return False, "missing timestamp"

        bar_time = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        if bar_time.tzinfo is None:
            bar_time = bar_time.replace(tzinfo=timezone.utc)

        age_minutes = (datetime.now(timezone.utc) - bar_time).total_seconds() / 60

        if age_minutes > FRESHNESS_MAX_MINUTES:
            return False, f"bar is {age_minutes:.1f} min old, exceeds {FRESHNESS_MAX_MINUTES} min SLA"

        # Also catch future timestamps
        if age_minutes < -2:
            return False, f"timestamp is {abs(age_minutes):.1f} min in the future"

    except Exception as e:
        return False, f"could not parse timestamp: {e}"

    return True, None


def run_streaming_gate(event: dict) -> tuple[bool, str | None, str | None]:
    """
    Runs schema → range → freshness checks on a streaming tick event.
    Returns (passed, failure_code, failure_detail).
    """
    passed, detail = check_schema(event)
    if not passed:
        return False, "SCHEMA_MISSING_FIELD", detail

    passed, detail = check_range(event)
    if not passed:
        return False, "RANGE_VIOLATION", detail

    passed, detail = check_freshness(event)
    if not passed:
        return False, "STALE", detail

    return True, None, None


# ---------------------------------------------------------------------------
# DuckDB schema
# ---------------------------------------------------------------------------

def ensure_streaming_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS live_ticks (
            event_id      VARCHAR PRIMARY KEY,
            ticker        VARCHAR NOT NULL,
            bar_timestamp TIMESTAMPTZ NOT NULL,
            polled_at     TIMESTAMPTZ,
            open          DOUBLE,
            high          DOUBLE,
            low           DOUBLE,
            close         DOUBLE,
            volume        BIGINT,
            gate_passed   BOOLEAN,
            received_at   TIMESTAMPTZ
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stream_quarantine (
            event_id         VARCHAR PRIMARY KEY,
            ticker           VARCHAR,
            failure_code     VARCHAR,
            failure_detail   VARCHAR,
            raw_event        JSON,
            quarantined_at   TIMESTAMPTZ
        )
    """)

    logger.debug("Streaming tables verified/created")


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def run():
    from src.utils.storage import get_duckdb_connection

    logger.info("DataGate streaming consumer starting...")
    logger.info(f"Reading from: {TOPIC_RAW}")
    logger.info(f"Consumer group: {CONSUMER_GROUP}")

    # Setup Kafka consumer
    consumer = KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=5000,  # return from poll after 5s if no messages
    )

    # Setup Kafka producer for routing verified/quarantine events
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    conn = get_duckdb_connection()
    ensure_streaming_tables(conn)

    passed_count = 0
    failed_count = 0
    total_count = 0

    logger.info("Consumer ready — waiting for events...")

    try:
        while True:
            records = consumer.poll(timeout_ms=5000)

            if not records:
                logger.debug("No messages — waiting...")
                continue

            for topic_partition, messages in records.items():
                for message in messages:
                    total_count += 1
                    event = message.value
                    ticker = message.key or event.get("ticker", "unknown")

                    # Run the gate
                    passed, failure_code, failure_detail = run_streaming_gate(event)
                    received_at = datetime.now(timezone.utc).isoformat()

                    if passed:
                        passed_count += 1

                        # Write to DuckDB live_ticks
                        conn.execute("""
                            INSERT OR REPLACE INTO live_ticks
                                (event_id, ticker, bar_timestamp, polled_at,
                                 open, high, low, close, volume,
                                 gate_passed, received_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, true, ?)
                        """, [
                            event.get("event_id", str(uuid.uuid4())),
                            ticker,
                            event.get("timestamp"),
                            event.get("polled_at"),
                            event.get("open"),
                            event.get("high"),
                            event.get("low"),
                            event.get("close"),
                            event.get("volume"),
                            received_at,
                        ])

                        # Forward to verified topic
                        verified_event = {**event, "gate_passed": True, "received_at": received_at}
                        producer.send(TOPIC_VERIFIED, key=ticker, value=verified_event)

                        logger.info(
                            f"✓ {ticker} ₹{event.get('close')} — "
                            f"passed [{passed_count}P/{failed_count}F/{total_count}T]"
                        )

                    else:
                        failed_count += 1

                        # Write to stream_quarantine
                        conn.execute("""
                            INSERT OR REPLACE INTO stream_quarantine
                                (event_id, ticker, failure_code,
                                 failure_detail, raw_event, quarantined_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, [
                            event.get("event_id", str(uuid.uuid4())),
                            ticker,
                            failure_code,
                            failure_detail,
                            json.dumps(event),
                            received_at,
                        ])

                        # Forward to quarantine topic
                        quarantine_event = {
                            **event,
                            "gate_passed": False,
                            "failure_code": failure_code,
                            "failure_detail": failure_detail,
                            "received_at": received_at,
                        }
                        producer.send(TOPIC_QUARANTINE, key=ticker, value=quarantine_event)

                        logger.warning(
                            f"✗ {ticker} — {failure_code}: {failure_detail} "
                            f"[{passed_count}P/{failed_count}F/{total_count}T]"
                        )

            producer.flush()

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()
        producer.close()
        conn.close()
        logger.info(
            f"Session summary: {passed_count} passed, "
            f"{failed_count} failed, {total_count} total"
        )


if __name__ == "__main__":
    run()