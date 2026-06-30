# =============================================================================
# src/ingestion/reddit.py
# Fetches posts from Indian investing subreddits using Reddit's public
# .json endpoints — no API key, no app registration required.
#
# DESIGN NOTE: This uses the unauthenticated public JSON interface
# (reddit.com/r/{sub}/hot.json) rather than PRAW + OAuth, since formal
# Reddit API researcher access was pending approval at build time.
# The function signature and output schema are intentionally identical
# to what a PRAW-based version would produce, so swapping the fetch
# implementation later (once API access is approved) requires no
# changes to the gate, enrichment, or any downstream code — only this
# file's internals would change. Rate-limited conservatively (2s delay
# between subreddit calls) to stay well within Reddit's tolerance for
# unauthenticated traffic.
# =============================================================================

import uuid
import time
import requests
from datetime import datetime, timezone
from loguru import logger

SUBREDDITS = ["IndiaInvestments", "DalalStreetTalks"]
POSTS_PER_SUBREDDIT = 25

HEADERS = {
    # Reddit blocks the default python-requests user agent — a descriptive
    # custom one is required even for unauthenticated public JSON access
    "User-Agent": "datagate-portfolio-project/0.1 (personal data pipeline, non-commercial)"
}


def fetch_subreddit_posts(subreddit: str, limit: int = 25) -> list[dict]:
    """
    Fetches hot posts from a subreddit's public .json endpoint.
    Returns a list of normalized post records.
    """
    url = f"https://www.reddit.com/r/{subreddit}/hot.json"
    params = {"limit": limit}

    logger.info(f"Fetching r/{subreddit}...")
    response = requests.get(url, headers=HEADERS, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()

    posts = data.get("data", {}).get("children", [])
    records = []

    for post in posts:
        p = post.get("data", {})

        # Skip stickied/pinned posts — usually subreddit rules/meta posts,
        # not real discussion content worth analyzing
        if p.get("stickied"):
            continue

        record = {
            "post_id":      p.get("id"),
            "subreddit":    subreddit,
            "title":        p.get("title"),
            "score":        p.get("score", 0),
            "num_comments": p.get("num_comments", 0),
            "upvote_ratio": p.get("upvote_ratio"),
            "created_utc":  datetime.fromtimestamp(
                                 p.get("created_utc", 0), tz=timezone.utc
                             ).isoformat(),
            "body_snippet": (p.get("selftext") or "")[:500],
            "ingested_at":  datetime.now(timezone.utc).isoformat(),
            "source":       "reddit_public_json",
        }
        records.append(record)

    logger.info(f"r/{subreddit} — {len(records)} posts (after filtering stickied)")
    return records


def fetch_reddit_data() -> list[dict]:
    """
    Fetches posts from all configured subreddits.
    Returns a combined, flat list of records.
    """
    all_records = []

    for i, subreddit in enumerate(SUBREDDITS):
        try:
            records = fetch_subreddit_posts(subreddit, POSTS_PER_SUBREDDIT)
            all_records.extend(records)
        except Exception as e:
            logger.error(f"Failed to fetch r/{subreddit}: {e}")

        # Be polite to the unauthenticated endpoint — small delay between
        # subreddits, not needed after the last one
        if i < len(SUBREDDITS) - 1:
            time.sleep(2)

    return all_records


def run():
    """
    Main entry point — fetch, write to MinIO, log to DuckDB.
    """
    run_id = str(uuid.uuid4())
    logger.info(f"Starting reddit ingestor — run_id: {run_id}")

    from src.utils.storage import write_to_minio, today_key, log_pipeline_run

    try:
        records = fetch_reddit_data()

        if not records:
            logger.warning("No Reddit posts fetched")
            log_pipeline_run(run_id, "reddit", 0, "success", "no posts returned")
            return

        key = today_key("reddit")
        write_to_minio(records, "bronze", key)
        logger.info(f"Written to MinIO: bronze/{key}")

        log_pipeline_run(run_id, "reddit", len(records), "success")
        logger.info(f"Run logged to DuckDB — {len(records)} posts")

    except Exception as e:
        log_pipeline_run(run_id, "reddit", 0, "failed", str(e))
        logger.error(f"Reddit ingestor failed: {e}")
        raise


if __name__ == "__main__":
    run()