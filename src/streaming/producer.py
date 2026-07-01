# =============================================================================
# src/streaming/producer.py
# DataGate streaming producer
#
# Polls 25 NSE tickers via yfinance every 5 minutes during market hours
# (9:15 AM - 3:30 PM IST) and publishes individual tick events to Kafka.
#
# Design note: yfinance provides 15-minute delayed intraday data during
# market hours. We poll every 5 minutes so the stream reflects price
# movement throughout the trading day, not just EOD snapshots.
# This is the standard approach for portfolio-grade streaming systems
# where a live paid feed isn't available.
#
# Topics published to:
#   market.ticks.raw  — one event per ticker per poll
#
# Run with: python -m src.streaming.producer
# =============================================================================

import json
import time
import uuid
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import yfinance as yf
from kafka import KafkaProducer
from loguru import logger

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_RAW = "market.ticks.raw"
POLL_INTERVAL_SECONDS = 300  # 5 minutes
IST = ZoneInfo("Asia/Kolkata")

# 25 NSE tickers — Nifty's most liquid stocks
TICKERS = [
    # Original 10 (enriched in gold mart)
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "HINDUNILVR.NS", "SBIN.NS", "BAJFINANCE.NS", "WIPRO.NS", "ADANIENT.NS",
    # Additional 15 (streamed through gate, not enriched)
    "BAJAJFINSV.NS", "TITAN.NS", "ASIANPAINT.NS", "KOTAKBANK.NS", "LT.NS",
    "AXISBANK.NS", "MARUTI.NS", "NTPC.NS", "ONGC.NS", "POWERGRID.NS",
    "SUNPHARMA.NS", "TATAMOTORS.NS", "TATASTEEL.NS", "TECHM.NS", "ULTRACEMCO.NS",
]

MARKET_OPEN = (9, 15)   # 9:15 AM IST
MARKET_CLOSE = (15, 30) # 3:30 PM IST


# ---------------------------------------------------------------------------
# Market hours check
# ---------------------------------------------------------------------------

# def is_market_open() -> bool:
#     """Returns True if NSE is currently open."""
#     now = datetime.now(IST)

#     # Skip weekends
#     if now.weekday() >= 5:
#         return False

#     open_time = now.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0)
#     close_time = now.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0)

#     return open_time <= now <= close_time

def is_market_open() -> bool:
    """TESTING MODE — always returns True to test outside market hours."""
    return True
def time_until_market_open() -> int:
    """Returns seconds until next market open."""
    now = datetime.now(IST)
    today_open = now.replace(
        hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0
    )

    if now < today_open and now.weekday() < 5:
        return int((today_open - now).total_seconds())

    # Next weekday
    days_ahead = 1
    while (now.weekday() + days_ahead) % 7 >= 5:
        days_ahead += 1

    next_open = (now + timedelta(days=days_ahead)).replace(
        hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0
    )
    return int((next_open - now).total_seconds())


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


# ---------------------------------------------------------------------------
# Fetch and publish
# ---------------------------------------------------------------------------

def fetch_and_publish(producer: KafkaProducer) -> int:
    """
    Fetches current price for all tickers and publishes one event
    per ticker to Kafka. Returns number of events published.
    """
    published = 0
    poll_id = str(uuid.uuid4())[:8]
    polled_at = datetime.now(timezone.utc).isoformat()

    logger.info(f"Poll {poll_id} — fetching {len(TICKERS)} tickers...")

    for ticker in TICKERS:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d", interval="5m")

            if hist.empty:
                logger.warning(f"{ticker} — no intraday data returned")
                continue

            latest = hist.iloc[-1]
            bar_time = hist.index[-1]

            event = {
                "event_id":   str(uuid.uuid4()),
                "poll_id":    poll_id,
                "ticker":     ticker,
                "timestamp":  bar_time.isoformat(),
                "polled_at":  polled_at,
                "open":       round(float(latest["Open"]), 2),
                "high":       round(float(latest["High"]), 2),
                "low":        round(float(latest["Low"]), 2),
                "close":      round(float(latest["Close"]), 2),
                "volume":     int(latest["Volume"]),
                "source":     "yfinance_intraday_15min_delayed",
            }

            producer.send(
                topic=TOPIC_RAW,
                key=ticker,
                value=event,
            )
            published += 1
            logger.info(f"  {ticker} ₹{event['close']} → published")

        except Exception as e:
            logger.error(f"  {ticker} — failed: {e}")
            continue

    producer.flush()
    logger.info(f"Poll {poll_id} complete — {published}/{len(TICKERS)} events published")
    return published


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run():
    logger.info("DataGate streaming producer starting...")
    logger.info(f"Tickers: {len(TICKERS)} | Interval: {POLL_INTERVAL_SECONDS}s | Topic: {TOPIC_RAW}")

    producer = create_producer()
    logger.info("Kafka producer connected")

    total_published = 0

    try:
        while True:
            if is_market_open():
                count = fetch_and_publish(producer)
                total_published += count
                logger.info(f"Total events published this session: {total_published}")
                logger.info(f"Sleeping {POLL_INTERVAL_SECONDS}s until next poll...")
                time.sleep(POLL_INTERVAL_SECONDS)
            else:
                wait_seconds = time_until_market_open()
                wait_minutes = wait_seconds // 60
                logger.info(
                    f"Market closed. Next open in ~{wait_minutes} minutes. "
                    f"Sleeping until then..."
                )
                # Sleep in 60s chunks so we can respond to Ctrl+C
                for _ in range(min(wait_seconds, 3600)):
                    time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    finally:
        producer.close()
        logger.info(f"Session total: {total_published} events published")


if __name__ == "__main__":
    run()