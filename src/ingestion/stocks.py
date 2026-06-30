# =============================================================================
# src/ingestion/stocks.py
# Fetches daily OHLCV data for Indian stocks from NSE via yfinance
# Writes raw JSON to MinIO bronze/stocks/YYYY-MM-DD.json
# Logs run to DuckDB pipeline_runs table
# =============================================================================

import os
import uuid
import json
from datetime import datetime, timezone, timedelta
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# --- Indian tickers on NSE (.NS suffix for yfinance) ---
TICKERS = [
    "RELIANCE.NS",   # Reliance Industries
    "TCS.NS",        # Tata Consultancy Services
    "HDFCBANK.NS",   # HDFC Bank
    "INFY.NS",       # Infosys
    "ICICIBANK.NS",  # ICICI Bank
    "HINDUNILVR.NS", # Hindustan Unilever
    "SBIN.NS",       # State Bank of India
    "BAJFINANCE.NS", # Bajaj Finance
    "WIPRO.NS",      # Wipro
    "ADANIENT.NS",   # Adani Enterprises
]


def fetch_stock_data() -> list[dict]:
    """
    Fetches the last 2 days of OHLCV data for all tickers.
    Returns a flat list of records, one per ticker per day.
    """
    records = []
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=5)  # 5 days back to ensure we get data
                                        # even on weekends/holidays

    for ticker in TICKERS:
        try:
            logger.info(f"Fetching {ticker}...")
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start.isoformat(), end=today.isoformat())

            if hist.empty:
                logger.warning(f"No data returned for {ticker}")
                continue

            # Take only the most recent trading day
            latest = hist.iloc[-1]

            record = {
                "ticker":       ticker,
                "ticker_clean": ticker.replace(".NS", ""),
                "date":         hist.index[-1].strftime("%Y-%m-%d"),
                "open":         round(float(latest["Open"]), 2),
                "high":         round(float(latest["High"]), 2),
                "low":          round(float(latest["Low"]), 2),
                "close":        round(float(latest["Close"]), 2),
                "volume":       int(latest["Volume"]),
                "ingested_at":  datetime.now(timezone.utc).isoformat(),
                "source":       "yfinance",
                "exchange":     "NSE",
            }
            records.append(record)
            logger.info(f"{ticker} — close: ₹{record['close']}")

        except Exception as e:
            logger.error(f"Failed to fetch {ticker}: {e}")
            continue

    return records


def run():
    """
    Main entry point — fetch, write to MinIO, log to DuckDB.
    """
    run_id = str(uuid.uuid4())
    logger.info(f"Starting stocks ingestor — run_id: {run_id}")

    # --- Fetch ---
    records = fetch_stock_data()

    if not records:
        logger.error("No records fetched — aborting")
        return

    logger.info(f"Fetched {len(records)} stock records")

    # --- Write to MinIO ---
    from src.utils.storage import write_to_minio, today_key, log_pipeline_run

    try:
        key = today_key("stocks")
        write_to_minio(records, "bronze", key)
        logger.info(f"Written to MinIO: bronze/{key}")

        # --- Log success to DuckDB ---
        log_pipeline_run(run_id, "stocks", len(records), "success")
        logger.info("Run logged to DuckDB")

    except Exception as e:
        log_pipeline_run(run_id, "stocks", 0, "failed", str(e))
        logger.error(f"Failed to write to MinIO: {e}")
        raise


if __name__ == "__main__":
    run()