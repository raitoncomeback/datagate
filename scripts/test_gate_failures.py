# =============================================================================
# scripts/test_gate_failures.py
# Deliberately feeds the gate hand-crafted bad records to verify
# SCHEMA_MISSING_FIELD and RANGE_VIOLATION fire correctly.
# STALE and DUPLICATE were already proven via real ingestor runs —
# this script closes the gap on the other two failure codes.
#
# This is throwaway test data, not real ingested data — it does NOT
# go through write_to_minio's real "bronze" path, it calls the gate
# functions directly so we can inspect results immediately without
# needing a full ingest -> gate round trip.
#
# Usage:
#   python scripts/test_gate_failures.py
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timezone
from src.gate.gate import run_gate_on_record, FailureCode
from src.utils.storage import get_duckdb_connection


def test_case(label: str, record: dict, source: str, expected_code: str):
    """Runs one record through the gate and checks the result matches expectation."""
    conn = get_duckdb_connection()
    result = run_gate_on_record(record, source, conn)
    conn.close()

    status = "PASS" if result.failure_code == expected_code else "MISMATCH"
    icon = "✓" if status == "PASS" else "✗"

    print(f"\n{icon} {label}")
    print(f"   Expected: {expected_code}")
    print(f"   Got:      {result.failure_code}  (passed={result.passed})")
    print(f"   Detail:   {result.failure_detail}")

    return status == "PASS"


if __name__ == "__main__":
    print("=" * 60)
    print("DataGate — gate failure code verification")
    print("=" * 60)

    results = []

    # -------------------------------------------------------------------
    # Test 1: SCHEMA_MISSING_FIELD — stock record missing 'close'
    # -------------------------------------------------------------------
    bad_schema_record = {
        "ticker": "TESTCO.NS",
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "open": 100.0,
        "high": 105.0,
        "low": 98.0,
        # "close" deliberately missing
        "volume": 10000,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    results.append(test_case(
        "Stock record missing 'close' field",
        bad_schema_record,
        "stocks",
        FailureCode.SCHEMA_MISSING_FIELD,
    ))

    # -------------------------------------------------------------------
    # Test 2: RANGE_VIOLATION — negative price
    # -------------------------------------------------------------------
    negative_price_record = {
        "ticker": "TESTCO2.NS",
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "open": 100.0,
        "high": 105.0,
        "low": 98.0,
        "close": -50.0,  # physically impossible
        "volume": 10000,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    results.append(test_case(
        "Stock record with negative close price",
        negative_price_record,
        "stocks",
        FailureCode.RANGE_VIOLATION,
    ))

    # -------------------------------------------------------------------
    # Test 3: RANGE_VIOLATION — high less than low (logically impossible)
    # -------------------------------------------------------------------
    inverted_range_record = {
        "ticker": "TESTCO3.NS",
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "open": 100.0,
        "high": 90.0,   # high is LESS than low — impossible
        "low": 98.0,
        "close": 95.0,
        "volume": 10000,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    results.append(test_case(
        "Stock record where high < low",
        inverted_range_record,
        "stocks",
        FailureCode.RANGE_VIOLATION,
    ))

    # -------------------------------------------------------------------
    # Test 4: RANGE_VIOLATION — negative volume
    # -------------------------------------------------------------------
    negative_volume_record = {
        "ticker": "TESTCO4.NS",
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "open": 100.0,
        "high": 105.0,
        "low": 98.0,
        "close": 102.0,
        "volume": -500,  # impossible
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    results.append(test_case(
        "Stock record with negative volume",
        negative_volume_record,
        "stocks",
        FailureCode.RANGE_VIOLATION,
    ))

    # -------------------------------------------------------------------
    # Test 5: SCHEMA_MISSING_FIELD — news record missing 'title'
    # -------------------------------------------------------------------
    bad_news_record = {
        "article_id": "test-article-001",
        # "title" deliberately missing
        "description": "Some description",
        "url": "https://example.com/article",
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    results.append(test_case(
        "News record missing 'title' field",
        bad_news_record,
        "news",
        FailureCode.SCHEMA_MISSING_FIELD,
    ))

    # -------------------------------------------------------------------
    # Test 6: a record that SHOULD pass, as a sanity control
    # -------------------------------------------------------------------
    good_record = {
        "ticker": "TESTGOOD.NS",
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "open": 100.0,
        "high": 105.0,
        "low": 98.0,
        "close": 102.0,
        "volume": 10000,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    conn = get_duckdb_connection()
    good_result = run_gate_on_record(good_record, "stocks", conn)
    conn.close()
    control_pass = good_result.passed
    icon = "✓" if control_pass else "✗"
    print(f"\n{icon} Control: a genuinely valid record")
    print(f"   Expected: passed=True")
    print(f"   Got:      passed={good_result.passed}, code={good_result.failure_code}")
    results.append(control_pass)

    # -------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------
    print("\n" + "=" * 60)
    passed_count = sum(results)
    total = len(results)
    if passed_count == total:
        print(f"All {total}/{total} gate checks behaved correctly.")
    else:
        print(f"{passed_count}/{total} passed — review mismatches above.")
    print("=" * 60)