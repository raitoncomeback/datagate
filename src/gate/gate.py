# =============================================================================
# src/gate/gate.py
# DataGate's quality gate — the heart of the whole project.
#
# Every record from every source passes through here before it's allowed
# to reach enrichment or the AI advisor. Four independent checks:
#
#   1. FRESHNESS  — is this record within its source's SLA window?
#   2. DUPLICATE  — have we already seen this exact record?
#   3. SCHEMA     — does it have all required fields, correct types?
#   4. RANGE      — are the values physically/logically plausible?
#
# Pass  -> written to bronze_verified/ in MinIO + corresponding bronze_* table
# Fail  -> written to quarantine/ in MinIO + quarantine_log table, with a
#          structured failure_code that downstream code (and Gemini) can
#          read without re-parsing free text.
# =============================================================================

import uuid
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from loguru import logger

from src.utils.storage import (
    get_duckdb_connection,
    write_to_minio,
    today_key,
)


# ---------------------------------------------------------------------------
# Per-source configuration — freshness SLA + required schema
# ---------------------------------------------------------------------------

FRESHNESS_SLA = {
    # Stocks: yfinance free tier returns END-OF-DAY data (date only, no
    # intraday timestamp). "Freshness" for this source means "is this
    # today's (or the most recent trading day's) close" — not literally
    # within the last hour, since the data itself has no hour granularity.
    # We treat anything from today or the prior trading day as fresh,
    # since markets close on weekends/holidays.
    "stocks": timedelta(days=3),

    "news":   timedelta(hours=48),
    "macro":  timedelta(days=7),
    "reddit": timedelta(hours=12),  # set now, used once reddit.py exists
}

REQUIRED_FIELDS = {
    "stocks": {"ticker", "date", "open", "high", "low", "close", "volume"},
    "news":   {"article_id", "title", "url", "published_at"},
    "macro":  {"release_id", "title", "url", "scraped_at"},
    "reddit": {"post_id", "subreddit", "title", "score", "created_utc"},
}

# Which field on each record represents "when this data point is from"
# Used for freshness checks. Different per source because the sources
# have genuinely different shapes.
TIMESTAMP_FIELD = {
    "stocks": "date",          # date the price is FOR, not when fetched
    "news":   "published_at",
    "macro":  "scraped_at",    # RBI doesn't give a clean "as of" date on titles
    "reddit": "created_utc",
}

# Which field(s) uniquely identify a record, for duplicate detection
DEDUPE_KEY = {
    "stocks": ["ticker", "date"],
    "news":   ["article_id"],
    "macro":  ["release_id"],
    "reddit": ["post_id"],
}


# ---------------------------------------------------------------------------
# Failure codes — structured, not free text. Gemini explains these later,
# but the CODE itself is what the circuit breaker and trust score key off.
# ---------------------------------------------------------------------------

class FailureCode:
    STALE = "STALE"
    DUPLICATE = "DUPLICATE"
    SCHEMA_MISSING_FIELD = "SCHEMA_MISSING_FIELD"
    SCHEMA_WRONG_TYPE = "SCHEMA_WRONG_TYPE"
    RANGE_VIOLATION = "RANGE_VIOLATION"


@dataclass
class GateResult:
    record_id: str
    source: str
    passed: bool
    failure_code: str | None
    failure_detail: str | None


# ---------------------------------------------------------------------------
# Check 1 — Freshness
# ---------------------------------------------------------------------------

def check_freshness(record: dict, source: str) -> tuple[bool, str | None]:
    """
    Returns (passed, detail). detail is None if passed.
    """
    ts_field = TIMESTAMP_FIELD.get(source)
    if not ts_field or ts_field not in record:
        return False, f"missing timestamp field '{ts_field}' needed for freshness check"

    raw_value = record[ts_field]

    try:
        if source == "stocks":
            # "date" is just YYYY-MM-DD, no time component
            record_time = datetime.strptime(raw_value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            # ISO format with timezone, e.g. "2026-06-30T12:34:50Z"
            cleaned = raw_value.replace("Z", "+00:00")
            record_time = datetime.fromisoformat(cleaned)
            if record_time.tzinfo is None:
                record_time = record_time.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError) as e:
        return False, f"could not parse timestamp '{raw_value}': {e}"

    now = datetime.now(timezone.utc)
    age = now - record_time
    sla = FRESHNESS_SLA[source]

    if age > sla:
        return False, f"record is {age} old, exceeds SLA of {sla}"

    # Also catch the opposite problem — a timestamp in the future,
    # which is itself a data quality signal worth catching
    if record_time > now + timedelta(minutes=5):
        return False, f"record timestamp is in the future ({record_time})"

    return True, None


# ---------------------------------------------------------------------------
# Check 2 — Duplicate detection
# ---------------------------------------------------------------------------

def check_duplicate(record: dict, source: str, conn) -> tuple[bool, str | None]:
    """
    Checks if a record with the same dedupe key already exists in the
    verified bronze table for this source. Returns (passed, detail).
    passed=True means NOT a duplicate (i.e. it's fine to proceed).
    """
    key_fields = DEDUPE_KEY[source]
    table = f"bronze_{source}"

    missing = [f for f in key_fields if f not in record]
    if missing:
        return False, f"cannot check duplicates, missing key fields: {missing}"

    where_clauses = " AND ".join([f"{f} = ?" for f in key_fields])
    values = [record[f] for f in key_fields]

    query = f"SELECT COUNT(*) FROM {table} WHERE {where_clauses}"
    try:
        count = conn.execute(query, values).fetchone()[0]
    except Exception:
        # Table might not have data yet — that's fine, not a duplicate
        count = 0

    if count > 0:
        return False, f"record with key {dict(zip(key_fields, values))} already exists"

    return True, None


# ---------------------------------------------------------------------------
# Check 3 — Schema validation
# ---------------------------------------------------------------------------

def check_schema(record: dict, source: str) -> tuple[bool, str | None]:
    """
    Confirms all required fields are present and not null.
    Returns (passed, detail).
    """
    required = REQUIRED_FIELDS[source]
    missing = [f for f in required if f not in record or record[f] is None]

    if missing:
        return False, f"missing required fields: {missing}"

    return True, None


# ---------------------------------------------------------------------------
# Check 4 — Range / sanity checks
# ---------------------------------------------------------------------------

def check_range(record: dict, source: str) -> tuple[bool, str | None]:
    """
    Source-specific plausibility checks. Returns (passed, detail).
    """
    if source == "stocks":
        for field in ["open", "high", "low", "close"]:
            val = record.get(field)
            if val is None or val <= 0:
                return False, f"{field}={val} is not a plausible price (must be > 0)"

        if record.get("volume", 0) < 0:
            return False, f"volume={record['volume']} cannot be negative"

        # High should be >= low, and both open/close should sit within range
        if record["high"] < record["low"]:
            return False, f"high ({record['high']}) is less than low ({record['low']})"

    elif source == "news":
        title = record.get("title", "")
        if not title or len(title.strip()) == 0:
            return False, "title is empty"

    elif source == "macro":
        title = record.get("title", "")
        if not title or len(title.strip()) == 0:
            return False, "title is empty"

    elif source == "reddit":
        score = record.get("score")
        if score is not None and not isinstance(score, (int, float)):
            return False, f"score={score} is not numeric"

    return True, None


# ---------------------------------------------------------------------------
# Main gate function — runs all 4 checks in order, stops at first failure
# ---------------------------------------------------------------------------

def run_gate_on_record(record: dict, source: str, conn) -> GateResult:
    """
    Runs all 4 checks on a single record in a fixed priority order:
    schema -> range -> freshness -> duplicate.

    Schema first because the other checks assume fields exist.
    Duplicate last because it's the most expensive check (a DB query).
    """
    record_id = str(uuid.uuid4())

    passed, detail = check_schema(record, source)
    if not passed:
        return GateResult(record_id, source, False, FailureCode.SCHEMA_MISSING_FIELD, detail)

    passed, detail = check_range(record, source)
    if not passed:
        return GateResult(record_id, source, False, FailureCode.RANGE_VIOLATION, detail)

    passed, detail = check_freshness(record, source)
    if not passed:
        return GateResult(record_id, source, False, FailureCode.STALE, detail)

    passed, detail = check_duplicate(record, source, conn)
    if not passed:
        return GateResult(record_id, source, False, FailureCode.DUPLICATE, detail)

    return GateResult(record_id, source, True, None, None)


# ---------------------------------------------------------------------------
# Batch processing — reads a source's bronze records, gates each one,
# writes pass/fail to the right place, logs everything to DuckDB
# ---------------------------------------------------------------------------

def gate_source(source: str, records: list[dict]) -> dict:
    """
    Runs the gate on a full batch of records for one source.
    Returns a summary dict with pass/fail counts.
    """
    conn = get_duckdb_connection()

    passed_records = []
    failed_records = []

    for record in records:
        result = run_gate_on_record(record, source, conn)

        # Log every gate decision to DuckDB regardless of pass/fail
        conn.execute("""
            INSERT OR REPLACE INTO gate_results
                (record_id, source, checked_at, passed, failure_code, gemini_explanation)
            VALUES (?, ?, current_timestamp, ?, ?, NULL)
        """, [result.record_id, source, result.passed, result.failure_code])

        if result.passed:
            passed_records.append(record)
        else:
            failed_records.append((record, result))
            # Log to quarantine_log with full detail immediately
            # gemini_explanation stays NULL here — enrichment phase fills it in
            import json
            conn.execute("""
                INSERT OR REPLACE INTO quarantine_log
                    (record_id, source, quarantined_at, failure_code,
                     failure_detail, gemini_explanation, raw_record)
                VALUES (?, ?, current_timestamp, ?, ?, NULL, ?)
            """, [
                result.record_id, source, result.failure_code,
                result.failure_detail,
                json.dumps(record, default=str)
            ])
    # --- Write passed records to bronze_verified in MinIO ---
    if passed_records:
        key = today_key(source)
        write_to_minio(passed_records, "bronze-verified", key)
        logger.info(f"[{source}] {len(passed_records)} records passed -> bronze-verified/{key}")

        # Also insert into the source-specific bronze table for querying
        _insert_into_bronze_table(conn, source, passed_records)

    # --- Write failed records to quarantine in MinIO ---
    if failed_records:
        key = today_key(source)
        quarantine_payload = [
            {**rec, "_failure_code": result.failure_code, "_failure_detail": result.failure_detail}
            for rec, result in failed_records
        ]
        write_to_minio(quarantine_payload, "quarantine", key)
        logger.warning(f"[{source}] {len(failed_records)} records FAILED -> quarantine/{key}")
        for rec, result in failed_records:
            logger.warning(f"  - {result.failure_code}: {result.failure_detail}")

    conn.close()

    return {
        "source": source,
        "total": len(records),
        "passed": len(passed_records),
        "failed": len(failed_records),
    }


def _insert_into_bronze_table(conn, source: str, records: list[dict]):
    """
    Inserts verified records into the source-specific bronze table
    so they're queryable via SQL, not just sitting in MinIO as JSON.
    """
    if source == "stocks":
        for r in records:
            conn.execute("""
                INSERT OR REPLACE INTO bronze_stocks
                    (ticker, date, open, high, low, close, volume, ingested_at, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                r["ticker"], r["date"], r["open"], r["high"], r["low"],
                r["close"], r["volume"], r["ingested_at"], today_key(source)
            ])

    elif source == "news":
        for r in records:
            conn.execute("""
                INSERT OR REPLACE INTO bronze_news
                    (article_id, title, description, url, published_at, source_name, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                r["article_id"], r["title"], r.get("description"),
                r["url"], r["published_at"], r.get("source_name"), r["ingested_at"]
            ])

    elif source == "macro":
        # macro doesn't have its own clean bronze table yet defined in storage.py
        # for now we skip the relational insert and rely on MinIO + gate_results
        pass

    elif source == "reddit":
        for r in records:
            conn.execute("""
                INSERT OR REPLACE INTO bronze_reddit
                    (post_id, subreddit, title, score, num_comments,
                     upvote_ratio, created_utc, body_snippet, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                r["post_id"], r["subreddit"], r["title"], r.get("score"),
                r.get("num_comments"), r.get("upvote_ratio"), r["created_utc"],
                r.get("body_snippet"), r["ingested_at"]
            ])


# ---------------------------------------------------------------------------
# CLI entry point — run the gate against today's data for all sources
# ---------------------------------------------------------------------------

def run():
    """
    Reads today's bronze data for each source from MinIO and gates it.
    """
    from src.utils.storage import read_from_minio, today_key as tk

    sources = ["stocks", "news", "macro"]  # reddit added once available
    summaries = []

    for source in sources:
        try:
            key = tk(source)
            records = read_from_minio("bronze", key)
            logger.info(f"[{source}] read {len(records)} records from bronze/{key}")
            summary = gate_source(source, records)
            summaries.append(summary)
        except Exception as e:
            logger.error(f"[{source}] gate run failed: {e}")
            summaries.append({"source": source, "error": str(e)})

    logger.info("=" * 50)
    logger.info("Gate run summary:")
    for s in summaries:
        if "error" in s:
            logger.info(f"  {s['source']}: ERROR — {s['error']}")
        else:
            logger.info(f"  {s['source']}: {s['passed']}/{s['total']} passed")
    logger.info("=" * 50)


if __name__ == "__main__":
    run()
