# =============================================================================
# src/enrichment/quarantine_explainer.py
# Reads quarantine_log rows where gemini_explanation IS NULL, asks Gemini
# to write a one-sentence plain-English explanation of why the record was
# quarantined, and writes it back. Idempotent — safe to re-run; only
# processes rows that haven't been explained yet.
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
MODEL_NAME = "gemini-3.5-flash"
# Free tier: 15 requests/minute. We process one record at a time (not
# batched) since each quarantine reason is unique — small delay keeps
# us safely under the rate limit.
DELAY_BETWEEN_CALLS_SECONDS = 4.5


PROMPT_TEMPLATE = """You are explaining a data quality failure to a data engineer.

A record from the "{source}" data source was quarantined (rejected) by an
automated quality gate.

Failure code: {failure_code}
Technical detail: {failure_detail}

Raw record (for context, may be truncated): {raw_record}

Write exactly ONE sentence in plain English explaining what went wrong and
why it matters. Be specific and concrete — reference the actual values
involved where relevant. Do not use the word "quarantine" or restate the
failure code verbatim. Write as if explaining to a colleague glancing at
a dashboard.

Respond with ONLY the sentence, no preamble, no quotes, no markdown."""


def init_gemini():
    if not GEMINI_API_KEY or GEMINI_API_KEY == "your_gemini_api_key_here":
        raise ValueError("GEMINI_API_KEY not set in .env")
    return genai.Client(api_key=GEMINI_API_KEY)

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def explain_failure(client, source: str, failure_code: str, failure_detail: str, raw_record: str) -> str:
    """
    Calls Gemini to generate a one-sentence explanation for a single
    quarantined record. Returns the explanation text.
    Retries up to 5 times with exponential backoff (4s, 8s, 16s, 32s, 60s)
    to absorb transient 503 UNAVAILABLE errors from server-side overload.
    """
    # Truncate raw_record so we don't blow up the prompt on large records
    raw_record_truncated = raw_record[:800] if raw_record else "{}"

    prompt = PROMPT_TEMPLATE.format(
        source=source,
        failure_code=failure_code,
        failure_detail=failure_detail or "no additional detail provided",
        raw_record=raw_record_truncated,
    )

    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
    )
    explanation = response.text.strip()

    # Basic safety net — Gemini occasionally wraps in quotes despite instructions
    explanation = explanation.strip('"').strip("'")

    return explanation


def run():
    """
    Main entry point — finds unexplained quarantine rows, explains each
    via Gemini, writes the explanation back to DuckDB.
    """
    from src.utils.storage import get_duckdb_connection

    logger.info("Starting quarantine explainer...")
    client = init_gemini()
    conn = get_duckdb_connection()

# BATCH_LIMIT caps how many records get processed in one run.
    # Useful for testing pace/cost before committing to a full run.
    # Set to None to process everything.
    BATCH_LIMIT = 5

    query = """
        SELECT record_id, source, failure_code, failure_detail, raw_record
        FROM quarantine_log
        WHERE gemini_explanation IS NULL
        ORDER BY quarantined_at
    """
    if BATCH_LIMIT:
        query += f" LIMIT {BATCH_LIMIT}"

    unexplained = conn.execute(query).fetchall()
    if not unexplained:
        logger.info("No unexplained quarantine records found — nothing to do")
        conn.close()
        return

    logger.info(f"Found {len(unexplained)} quarantine records needing explanation")

    explained_count = 0
    failed_count = 0

    for i, (record_id, source, failure_code, failure_detail, raw_record) in enumerate(unexplained):
        call_start = time.time()
        try:
            # raw_record comes back from DuckDB as a JSON string already
            explanation = explain_failure(
                client, source, failure_code, failure_detail, raw_record
            )

            conn.execute("""
                UPDATE quarantine_log
                SET gemini_explanation = ?
                WHERE record_id = ?
            """, [explanation, record_id])

            elapsed = time.time() - call_start
            logger.info(f"[{i+1}/{len(unexplained)}] ({elapsed:.1f}s) {source}/{failure_code}: {explanation}")
            explained_count += 1
        except Exception as e:
            logger.error(f"Failed to explain record {record_id}: {e}")
            failed_count += 1

        # Rate limit guard — skip the wait after the very last call
        if i < len(unexplained) - 1:
            time.sleep(DELAY_BETWEEN_CALLS_SECONDS)

    conn.close()

    logger.info("=" * 50)
    logger.info(f"Quarantine explanations complete: {explained_count} explained, {failed_count} failed")
    logger.info("=" * 50)


if __name__ == "__main__":
    run()