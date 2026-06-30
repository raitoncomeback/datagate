# =============================================================================
# src/ingestion/news.py
# Fetches Indian financial news headlines from NewsAPI
# Writes raw JSON to MinIO bronze/news/YYYY-MM-DD.json
# Logs run to DuckDB pipeline_runs table
# =============================================================================

import os
import uuid
import hashlib
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime, timezone
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY")
NEWSAPI_URL = "https://newsapi.org/v2/everything"

# Search terms relevant to Indian financial markets
QUERY_TERMS = "Nifty OR Sensex OR NSE OR BSE OR RBI"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def fetch_news_data() -> list[dict]:
    """
    Fetches top Indian financial news headlines.
    Returns a list of article records.
    """
    if not NEWSAPI_KEY or NEWSAPI_KEY == "your_newsapi_key_here":
        raise ValueError("NEWSAPI_KEY not set in .env")

    from datetime import timedelta
        # NewsAPI free tier embargoes the last 24 hours — articles only become
        # queryable after that delay. We fetch the 24-48 hour window instead
        # of "now minus 23 hours" to actually get results on this plan tier.
    since = (datetime.now(timezone.utc) - timedelta(hours=47)).strftime("%Y-%m-%dT%H:%M:%S")
    params = {
        "q": QUERY_TERMS,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 50,
        "from": since,
        "apiKey": NEWSAPI_KEY,
    }    
    logger.info("Fetching Indian financial news...")
    response = requests.get(NEWSAPI_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if data.get("status") != "ok":
        raise Exception(f"NewsAPI error: {data.get('message')}")

    articles = data.get("articles", [])
    logger.info(f"NewsAPI returned {len(articles)} articles")

    records = []
    for article in articles:
        # Create a stable ID from the URL so we can dedupe later
        article_id = hashlib.md5(article["url"].encode()).hexdigest()

        record = {
            "article_id":   article_id,
            "title":        article.get("title"),
            "description":  article.get("description"),
            "url":          article.get("url"),
            "published_at": article.get("publishedAt"),
            "source_name":  article.get("source", {}).get("name"),
            "ingested_at":  datetime.now(timezone.utc).isoformat(),
        }
        records.append(record)

    return records


def run():
    """
    Main entry point — fetch, write to MinIO, log to DuckDB.
    """
    run_id = str(uuid.uuid4())
    logger.info(f"Starting news ingestor — run_id: {run_id}")

    from src.utils.storage import write_to_minio, today_key, log_pipeline_run

    try:
        records = fetch_news_data()

        if not records:
            logger.warning("No articles fetched")
            log_pipeline_run(run_id, "news", 0, "success", "no articles returned")
            return

        key = today_key("news")
        write_to_minio(records, "bronze", key)
        logger.info(f"Written to MinIO: bronze/{key}")

        log_pipeline_run(run_id, "news", len(records), "success")
        logger.info(f"Run logged to DuckDB — {len(records)} articles")

    except Exception as e:
        log_pipeline_run(run_id, "news", 0, "failed", str(e))
        logger.error(f"News ingestor failed: {e}")
        raise


if __name__ == "__main__":
    run()