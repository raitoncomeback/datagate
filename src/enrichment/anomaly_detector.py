# =============================================================================
# src/enrichment/anomaly_detector.py
# Detects statistically unusual stock price movements by comparing today's
# close against a rolling 30-day average. For any ticker with a move
# beyond 2 standard deviations, calls Gemini to write a one-sentence
# plain-English explanation using that day's enriched news as context.
#
# This is the "circuit breaker context" layer — it answers the question
# "why did this stock move unusually today" which the AI advisor can cite.
# =============================================================================

import os
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from loguru import logger
from google import genai
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

# Primary model — switch to OpenRouter if Gemini daily quota exhausted
USE_OPENROUTER = True   # set False to use Gemini instead
OPENROUTER_MODEL = "openai/gpt-oss-120b:free"
GEMINI_MODEL = "gemini-2.5-flash"
ANOMALY_THRESHOLD_STD = 2.0   # flag if move > 2 standard deviations
DELAY_BETWEEN_CALLS = 7.0     # rate limit guard (same as news enricher)

PROMPT_TEMPLATE = """You are a financial analyst explaining an unusual stock price movement.

Ticker: {ticker}
Today's close: ₹{close}
30-day average close: ₹{avg_close}
Move: {pct_change:+.2f}% ({direction})
Standard deviations from mean: {std_devs:.1f}

Relevant news headlines from today (may be empty if no news found):
{news_context}

Write exactly ONE sentence in plain English explaining the most likely reason
for this unusual price movement, referencing specific news if available.
If no relevant news exists, note that the move appears technically driven.
Do not start with "The stock" — vary your opening.
Respond with ONLY the sentence, no preamble, no quotes."""


def init_client():
    if USE_OPENROUTER:
        from openai import OpenAI
        if not OPENROUTER_API_KEY or OPENROUTER_API_KEY == "your_openrouter_key_here":
            raise ValueError("OPENROUTER_API_KEY not set in .env")
        return OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=OPENROUTER_API_KEY,
        )
    else:
        from google import genai
        if not GEMINI_API_KEY or GEMINI_API_KEY == "your_gemini_api_key_here":
            raise ValueError("GEMINI_API_KEY not set in .env")
        return genai.Client(api_key=GEMINI_API_KEY)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def explain_anomaly(client, ticker: str, close: float, avg_close: float,
                    pct_change: float, std_devs: float, news_context: str) -> str:
    """Calls Gemini to explain an anomalous stock move."""
    direction = "above average" if pct_change > 0 else "below average"
    prompt = PROMPT_TEMPLATE.format(
        ticker=ticker.replace(".NS", ""),
        close=close,
        avg_close=round(avg_close, 2),
        pct_change=pct_change,
        direction=direction,
        std_devs=std_devs,
        news_context=news_context if news_context else "No relevant news found for this ticker today.",
    )
    if USE_OPENROUTER:
        response = client.chat.completions.create(
            model=OPENROUTER_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        raw = response.choices[0].message.content.strip()
    else:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
        )
        raw = response.text.strip()
    return raw.strip('"').strip("'")


def ensure_anomaly_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_anomalies (
            anomaly_id         VARCHAR PRIMARY KEY,
            ticker             VARCHAR,
            date               DATE,
            close              DOUBLE,
            avg_close_30d      DOUBLE,
            pct_change         DOUBLE,
            std_devs           DOUBLE,
            gemini_explanation VARCHAR,
            detected_at        TIMESTAMPTZ
        )
    """)


def get_news_context_for_ticker(conn, ticker: str) -> str:
    """
    Pulls today's enriched news headlines relevant to a specific ticker.
    Falls back to bearish/bullish market-wide headlines if no ticker-specific news.
    """
    ticker_clean = ticker.replace(".NS", "")

    # First try: news directly mentioning this ticker
    rows = conn.execute("""
        SELECT bn.title, en.sentiment, en.confidence, en.market_implication
        FROM enriched_news en
        JOIN bronze_news bn ON en.article_id = bn.article_id
        WHERE en.tickers_mentioned LIKE ?
          AND en.confidence >= 0.5
        ORDER BY en.confidence DESC
        LIMIT 3
    """, [f'%{ticker}%']).fetchall()

    if rows:
        lines = []
        for title, sentiment, confidence, implication in rows:
            lines.append(f"- [{sentiment.upper()} {confidence:.0%}] {title}")
        return "\n".join(lines)

    # Fallback: top market-wide news with high confidence
    rows = conn.execute("""
        SELECT bn.title, en.sentiment, en.confidence
        FROM enriched_news en
        JOIN bronze_news bn ON en.article_id = bn.article_id
        WHERE en.confidence >= 0.7
          AND en.sentiment != 'neutral'
        ORDER BY en.confidence DESC
        LIMIT 3
    """).fetchall()

    if rows:
        lines = [f"- [{s.upper()} {c:.0%}] {t}" for t, s, c in rows]
        return "No ticker-specific news. Market-wide signals:\n" + "\n".join(lines)

    return ""


def run():
    """
    Main entry point.
    Reads stock prices from bronze_stocks, computes rolling statistics,
    flags anomalies, explains via Gemini, writes to stock_anomalies table.
    """
    from src.utils.storage import get_duckdb_connection

    logger.info("Starting anomaly detector...")
    client = init_client()
    logger.info(f"Using {'OpenRouter (' + OPENROUTER_MODEL + ')' if USE_OPENROUTER else 'Gemini (' + GEMINI_MODEL + ')'}")
    conn = get_duckdb_connection()
    ensure_anomaly_table(conn)

    # Get all available stock data grouped by ticker
    all_stocks = conn.execute("""
        SELECT ticker, date, close
        FROM bronze_stocks
        ORDER BY ticker, date
    """).fetchall()

    if not all_stocks:
        logger.warning("No stock data in bronze_stocks — run ingestion first")
        conn.close()
        return

    # Group by ticker
    from collections import defaultdict
    ticker_data = defaultdict(list)
    for ticker, date, close in all_stocks:
        ticker_data[ticker].append((date, close))

    logger.info(f"Checking {len(ticker_data)} tickers for anomalies...")

    anomalies_found = 0
    anomalies_explained = 0

    for ticker, records in ticker_data.items():
        if len(records) < 2:
            logger.debug(f"{ticker} — not enough history ({len(records)} records), skipping")
            continue

        # Sort by date, take most recent as "today"
        records_sorted = sorted(records, key=lambda x: x[0])
        today_date, today_close = records_sorted[-1]
        historical_closes = [r[1] for r in records_sorted[:-1]]

        # Need at least 5 data points for meaningful statistics
        if len(historical_closes) < 5:
            logger.debug(f"{ticker} — insufficient history for stats, skipping")
            continue

        # Compute rolling average and standard deviation
        avg_close = sum(historical_closes) / len(historical_closes)
        variance = sum((c - avg_close) ** 2 for c in historical_closes) / len(historical_closes)
        std_dev = variance ** 0.5

        if std_dev == 0:
            logger.debug(f"{ticker} — zero std dev (price unchanged), skipping")
            continue

        pct_change = ((today_close - avg_close) / avg_close) * 100
        std_devs_from_mean = abs(today_close - avg_close) / std_dev

        if std_devs_from_mean < ANOMALY_THRESHOLD_STD:
            logger.debug(f"{ticker} — {std_devs_from_mean:.1f} std devs, within normal range")
            continue

        # Anomaly detected
        anomalies_found += 1
        direction = "UP" if pct_change > 0 else "DOWN"
        logger.info(
            f"ANOMALY: {ticker} — ₹{today_close} is {std_devs_from_mean:.1f} std devs "
            f"from avg ₹{avg_close:.2f} ({pct_change:+.2f}% {direction})"
        )

        # Check if already explained for this ticker+date
        existing = conn.execute("""
            SELECT anomaly_id FROM stock_anomalies
            WHERE ticker = ? AND date = ?
        """, [ticker, str(today_date)]).fetchone()

        if existing:
            logger.info(f"{ticker} anomaly for {today_date} already explained, skipping")
            continue

        # Get news context for this ticker
        news_context = get_news_context_for_ticker(conn, ticker)

        try:
            explanation = explain_anomaly(
                client, ticker, today_close, avg_close,
                pct_change, std_devs_from_mean, news_context
            )

            import uuid
            conn.execute("""
                INSERT OR REPLACE INTO stock_anomalies
                    (anomaly_id, ticker, date, close, avg_close_30d,
                     pct_change, std_devs, gemini_explanation, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                str(uuid.uuid4()), ticker, str(today_date),
                today_close, avg_close, pct_change,
                std_devs_from_mean, explanation,
                datetime.now(timezone.utc).isoformat()
            ])

            logger.info(f"{ticker} explanation: {explanation}")
            anomalies_explained += 1

            time.sleep(DELAY_BETWEEN_CALLS)

        except Exception as e:
            logger.error(f"Failed to explain {ticker} anomaly: {e}")

    conn.close()

    logger.info("=" * 50)
    if anomalies_found == 0:
        logger.info("No anomalies detected — all tickers within normal range")
    else:
        logger.info(f"Anomaly detection complete: {anomalies_found} found, {anomalies_explained} explained")
    logger.info("=" * 50)


if __name__ == "__main__":
    run()