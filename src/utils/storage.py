# =============================================================================
# src/utils/storage.py
# Shared MinIO (object storage) and DuckDB (warehouse) clients.
# All pipeline code imports from here — one place to swap backends later.
# =============================================================================

import os
import json
import duckdb
import boto3
from botocore.client import Config
from loguru import logger
from pathlib import Path
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# MinIO client — S3-compatible, identical API to real AWS S3 or GCS
# ---------------------------------------------------------------------------

def get_minio_client():
    """
    Returns a boto3 S3 client pointed at the local MinIO instance.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "datagate"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "datagate123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def write_to_minio(data: list[dict], bucket: str, key: str) -> int:
    """
    Writes a list of records as JSON to MinIO.
    Returns the number of records written.
    """
    client = get_minio_client()
    body = json.dumps(data, default=str, indent=2).encode("utf-8")

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    logger.info(f"Wrote {len(data)} records to minio://{bucket}/{key}")
    return len(data)


def read_from_minio(bucket: str, key: str) -> list[dict]:
    """
    Reads a JSON file from MinIO and returns a list of dicts.
    """
    client = get_minio_client()
    response = client.get_object(Bucket=bucket, Key=key)
    data = json.loads(response["Body"].read().decode("utf-8"))
    logger.info(f"Read {len(data)} records from minio://{bucket}/{key}")
    return data


def list_minio_keys(bucket: str, prefix: str = "") -> list[str]:
    """
    Lists all object keys in a bucket with an optional prefix filter.
    """
    client = get_minio_client()
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]


def today_key(source: str) -> str:
    """
    Returns today's date-partitioned object key for a given source.
    """
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"{source}/{date_str}.json"


# ---------------------------------------------------------------------------
# DuckDB client — embedded warehouse, replaces BigQuery
# ---------------------------------------------------------------------------

DUCKDB_PATH = os.environ.get(
    "DUCKDB_PATH",
    str(Path(__file__).parent.parent.parent / "data" / "duckdb" / "datagate.db")
)


def get_duckdb_connection():
    """
    Returns a DuckDB connection to the DataGate warehouse file.
    Creates the file and schema if they don't exist.
    """
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: duckdb.DuckDBPyConnection):
    """
    Creates all DataGate tables if they don't already exist.
    """

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id      VARCHAR PRIMARY KEY,
            source      VARCHAR NOT NULL,
            run_at      TIMESTAMPTZ NOT NULL,
            records     INTEGER,
            status      VARCHAR,
            error       VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS gate_results (
            record_id         VARCHAR PRIMARY KEY,
            source            VARCHAR NOT NULL,
            checked_at        TIMESTAMPTZ NOT NULL,
            passed            BOOLEAN NOT NULL,
            failure_code      VARCHAR,
            gemini_explanation VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_status (
            source      VARCHAR PRIMARY KEY,
            updated_at  TIMESTAMPTZ NOT NULL,
            trust_score DOUBLE,
            is_blocked  BOOLEAN DEFAULT FALSE,
            block_reason VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_stocks (
            ticker      VARCHAR,
            date        DATE,
            open        DOUBLE,
            high        DOUBLE,
            low         DOUBLE,
            close       DOUBLE,
            volume      BIGINT,
            ingested_at TIMESTAMPTZ,
            source_file VARCHAR,
            PRIMARY KEY (ticker, date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_news (
            article_id   VARCHAR PRIMARY KEY,
            title        VARCHAR,
            description  VARCHAR,
            url          VARCHAR,
            published_at TIMESTAMPTZ,
            source_name  VARCHAR,
            ingested_at  TIMESTAMPTZ
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_macro (
            series_id   VARCHAR,
            date        DATE,
            value       DOUBLE,
            ingested_at TIMESTAMPTZ
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_reddit (
            post_id      VARCHAR PRIMARY KEY,
            subreddit    VARCHAR,
            title        VARCHAR,
            score        INTEGER,
            num_comments INTEGER,
            upvote_ratio DOUBLE,
            created_utc  TIMESTAMPTZ,
            body_snippet VARCHAR,
            ingested_at  TIMESTAMPTZ
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS quarantine_log (
            record_id          VARCHAR PRIMARY KEY,
            source             VARCHAR NOT NULL,
            quarantined_at     TIMESTAMPTZ NOT NULL,
            failure_code       VARCHAR NOT NULL,
            failure_detail     VARCHAR,
            gemini_explanation VARCHAR,
            raw_record         JSON
        )
    """)

    logger.debug("DuckDB schema verified/created")


def log_pipeline_run(
    run_id: str,
    source: str,
    records: int,
    status: str,
    error: str = None
):
    """Logs a pipeline run to the observability table."""
    conn = get_duckdb_connection()
    conn.execute("""
        INSERT OR REPLACE INTO pipeline_runs
            (run_id, source, run_at, records, status, error)
        VALUES (?, ?, current_timestamp, ?, ?, ?)
    """, [run_id, source, records, status, error])
    conn.close()