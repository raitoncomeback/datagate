# =============================================================================
# src/ingestion/macro.py
# Scrapes RBI press release titles + dates from the official RBI press
# release page. Real scraped government data — no API, no key needed.
#
# Design choice: we scrape TITLES not numeric tables. RBI's actual rate/CPI
# figures live inside linked PDFs, which would require PDF parsing on top
# of scraping (fragile, heavy). Titles like "RBI kept repo rate unchanged"
# carry the signal we need and get interpreted later by Gemini enrichment,
# the same pattern already used for news — see src/enrichment/.
# =============================================================================

import uuid
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from loguru import logger

RBI_PRESS_URL = "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx"

# Keywords that indicate macro-relevant releases (vs routine auction notices)
MACRO_KEYWORDS = [
    "repo rate", "monetary policy", "inflation", "cpi", "gdp",
    "money supply", "reserve money", "mpc", "interest rate",
    "foreign exchange", "forex", "current account", "rupee",
]


def is_macro_relevant(title: str) -> bool:
    """Filters press release titles down to macro-relevant ones only."""
    title_lower = title.lower()
    return any(kw in title_lower for kw in MACRO_KEYWORDS)


def fetch_macro_data() -> list[dict]:
    """
    Scrapes the RBI press release page, filters to macro-relevant
    releases, and returns structured records.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    }

    logger.info("Fetching RBI press releases...")
    response = requests.get(RBI_PRESS_URL, headers=headers, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")

    # RBI's press release links sit inside <a> tags pointing to
    # BS_PressReleaseDisplay.aspx?prid=XXXXX
    links = soup.find_all("a", href=re.compile(r"prid=\d+"))

    records = []
    seen_ids = set()

    for link in links:
        title = link.get_text(strip=True)
        href = link.get("href", "")

        if not title or len(title) < 10:
            continue

        if not is_macro_relevant(title):
            continue

        prid_match = re.search(r"prid=(\d+)", href)
        if not prid_match:
            continue
        prid = prid_match.group(1)

        if prid in seen_ids:
            continue
        seen_ids.add(prid)

        full_url = f"https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx?prid={prid}"

        record = {
            "release_id":   prid,
            "title":        title,
            "url":          full_url,
            "scraped_at":   datetime.now(timezone.utc).isoformat(),
            "source":       "rbi_press_releases",
        }
        records.append(record)
        logger.info(f"Matched: {title[:70]}")

    logger.info(f"Found {len(records)} macro-relevant releases out of {len(links)} total")
    return records


def run():
    """
    Main entry point — scrape, write to MinIO, log to DuckDB.
    """
    run_id = str(uuid.uuid4())
    logger.info(f"Starting macro ingestor — run_id: {run_id}")

    from src.utils.storage import write_to_minio, today_key, log_pipeline_run

    try:
        records = fetch_macro_data()

        if not records:
            logger.warning("No macro-relevant releases found today")
            log_pipeline_run(run_id, "macro", 0, "success", "no macro releases matched")
            return

        key = today_key("macro")
        write_to_minio(records, "bronze", key)
        logger.info(f"Written to MinIO: bronze/{key}")

        log_pipeline_run(run_id, "macro", len(records), "success")
        logger.info(f"Run logged to DuckDB — {len(records)} releases")

    except Exception as e:
        log_pipeline_run(run_id, "macro", 0, "failed", str(e))
        logger.error(f"Macro ingestor failed: {e}")
        raise


if __name__ == "__main__":
    run()