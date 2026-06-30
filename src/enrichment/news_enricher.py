# =============================================================================
# src/enrichment/news_enricher.py
# Reads verified news articles from bronze_news in DuckDB, calls Gemini
# to extract 5 structured fields per article, writes results back to
# DuckDB enriched_news table and MinIO enriched/news/ folder.
#
# Fields extracted per article:
#   sentiment          — bullish | bearish | neutral
#   confidence         — 0.0 to 1.0
#   tickers_mentioned  — list of NSE tickers from our tracked universe
#   topic_tags         — list of free-form topic labels
#   market_implication — one plain-English sentence on market impact
#
# Rate limiting: Gemini 3.5 Flash free tier = 10 RPM.
# We process one article at a time with a 7s delay = ~8.5 RPM, safely
# under the limit with margin for slow responses.
# =============================================================================

import os
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from loguru import logger
from google import genai
from google.genai import types
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

# Primary model — switch to OpenRouter if Gemini daily quota exhausted
USE_OPENROUTER = True   # set False to use Gemini instead
OPENROUTER_MODEL = "openai/gpt-oss-120b:free"
GEMINI_MODEL = "gemini-2.5-flash"
DELAY_BETWEEN_CALLS_SECONDS = 7.0
BATCH_LIMIT = None # set to an integer to process a subset, None for all

# The 10 NSE tickers we track — Gemini uses this list to constrain extraction
TRACKED_TICKERS = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "HINDUNILVR.NS", "SBIN.NS", "BAJFINANCE.NS", "WIPRO.NS", "ADANIENT.NS"
]

PROMPT_TEMPLATE = """You are a financial analyst AI processing Indian market news.

Analyze this news article and return a JSON object with exactly these 5 fields.

Article title: {title}
Article description: {description}
Source: {source}

Tracked NSE tickers: {tickers}

Return ONLY valid JSON, no markdown, no explanation, no preamble:
{{
  "sentiment": "bullish" | "bearish" | "neutral",
  "confidence": <float between 0.0 and 1.0>,
  "tickers_mentioned": [<only tickers from the tracked list above that are directly relevant>],
  "topic_tags": [<2-4 short topic labels, e.g. "earnings", "RBI policy", "FII flows", "merger", "results">],
  "market_implication": "<one plain-English sentence explaining the likely market impact of this news>"
}}

Rules:
- sentiment must be exactly one of: bullish, bearish, neutral
- confidence should reflect how clearly the article signals market direction (ambiguous = low, clear positive/negative = high)
- tickers_mentioned must only contain tickers from the tracked list, empty array if none are relevant
- topic_tags should be concise, lowercase, 2-4 tags maximum
- market_implication must be exactly one sentence, specific to Indian markets"""


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
def enrich_article(client, article_id: str, title: str, description: str, source: str) -> dict:
    """
    Calls Gemini to extract structured sentiment data from one article.
    Returns a dict with the 5 enrichment fields.
    Retries up to 5 times with exponential backoff on failure.
    """
    prompt = PROMPT_TEMPLATE.format(
        title=title or "No title",
        description=(description or "No description")[:600],
        source=source or "Unknown",
        tickers=", ".join(TRACKED_TICKERS),
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

    # Strip markdown fences if Gemini added them despite instructions
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    parsed = json.loads(raw)

    # Validate required fields exist
    required = {"sentiment", "confidence", "tickers_mentioned", "topic_tags", "market_implication"}
    missing = required - set(parsed.keys())
    if missing:
        raise ValueError(f"Gemini response missing fields: {missing}")

    # Validate sentiment value
    if parsed["sentiment"] not in ("bullish", "bearish", "neutral"):
        raise ValueError(f"Invalid sentiment value: {parsed['sentiment']}")

    # Clamp confidence to valid range
    parsed["confidence"] = max(0.0, min(1.0, float(parsed["confidence"])))

    return parsed


def ensure_enriched_table(conn):
    """Creates the enriched_news table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS enriched_news (
            article_id         VARCHAR PRIMARY KEY,
            sentiment          VARCHAR,
            confidence         DOUBLE,
            tickers_mentioned  JSON,
            topic_tags         JSON,
            market_implication VARCHAR,
            enriched_at        TIMESTAMPTZ
        )
    """)


def run():
    """
    Main entry point.
    Reads unenriched verified news from DuckDB, enriches via Gemini,
    writes results back to DuckDB and MinIO.
    """
    from src.utils.storage import get_duckdb_connection, write_to_minio, today_key

    logger.info("Starting news enricher...")
    client = init_client()
    logger.info(f"Using {'OpenRouter (' + OPENROUTER_MODEL + ')' if USE_OPENROUTER else 'Gemini (' + GEMINI_MODEL + ')'}")
    conn = get_duckdb_connection()
    ensure_enriched_table(conn)

    # Find articles in bronze_news that haven't been enriched yet
    query = """
        SELECT bn.article_id, bn.title, bn.description, bn.source_name
        FROM bronze_news bn
        LEFT JOIN enriched_news en ON bn.article_id = en.article_id
        WHERE en.article_id IS NULL
        ORDER BY bn.ingested_at
    """
    if BATCH_LIMIT:
        query += f" LIMIT {BATCH_LIMIT}"

    unenriched = conn.execute(query).fetchall()

    if not unenriched:
        logger.info("No unenriched articles found — nothing to do")
        conn.close()
        return

    logger.info(f"Found {len(unenriched)} articles to enrich")

    enriched_records = []
    success_count = 0
    failed_count = 0

    for i, (article_id, title, description, source_name) in enumerate(unenriched):
        call_start = time.time()
        try:
            result = enrich_article(client, article_id, title, description, source_name)

            enriched_at = datetime.now(timezone.utc).isoformat()

            conn.execute("""
                INSERT OR REPLACE INTO enriched_news
                    (article_id, sentiment, confidence, tickers_mentioned,
                     topic_tags, market_implication, enriched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                article_id,
                result["sentiment"],
                result["confidence"],
                json.dumps(result["tickers_mentioned"]),
                json.dumps(result["topic_tags"]),
                result["market_implication"],
                enriched_at,
            ])

            enriched_records.append({
                "article_id":         article_id,
                "title":              title,
                "sentiment":          result["sentiment"],
                "confidence":         result["confidence"],
                "tickers_mentioned":  result["tickers_mentioned"],
                "topic_tags":         result["topic_tags"],
                "market_implication": result["market_implication"],
                "enriched_at":        enriched_at,
            })

            elapsed = time.time() - call_start
            logger.info(
                f"[{i+1}/{len(unenriched)}] ({elapsed:.1f}s) "
                f"{result['sentiment'].upper()} ({result['confidence']:.2f}) "
                f"tickers={result['tickers_mentioned']} — {title[:60]}"
            )
            success_count += 1

        except Exception as e:
            elapsed = time.time() - call_start
            logger.error(f"[{i+1}/{len(unenriched)}] ({elapsed:.1f}s) Failed: {e} — {title[:60]}")
            failed_count += 1

        # Rate limit guard
        if i < len(unenriched) - 1:
            time.sleep(DELAY_BETWEEN_CALLS_SECONDS)

    # Write all enriched records to MinIO
    if enriched_records:
        key = today_key("news")
        write_to_minio(enriched_records, "enriched", key)
        logger.info(f"Written {len(enriched_records)} enriched records to MinIO enriched/{key}")

    conn.close()

    logger.info("=" * 50)
    logger.info(f"News enrichment complete: {success_count} enriched, {failed_count} failed")
    logger.info("=" * 50)


if __name__ == "__main__":
    run()