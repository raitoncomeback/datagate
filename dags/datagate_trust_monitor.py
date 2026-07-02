# =============================================================================
# dags/datagate_trust_monitor.py
# DataGate trust score monitoring DAG
# Runs 30 minutes after the daily pipeline to check source health
# Updates source_status circuit breaker table
# =============================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'datagate',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}

with DAG(
    dag_id='datagate_trust_monitor',
    description='Monitors trust scores and updates circuit breaker per source',
    schedule_interval='30 13 * * 1-5',  # 7:00 PM IST, 30 min after pipeline
    start_date=days_ago(1),
    catchup=False,
    tags=['datagate', 'monitoring', 'trust'],
    default_args=default_args,
) as dag:

    def check_trust_scores():
        """
        Reads int_trust_score_daily and evaluates each source.
        Logs a summary of today's trust scores.
        """
        import duckdb
        from loguru import logger

        conn = duckdb.connect('data/duckdb/datagate.db', read_only=True)

        scores = conn.execute("""
            SELECT
                source,
                trust_score,
                pass_rate,
                records_checked,
                records_failed,
                is_blocked
            FROM main_silver.int_trust_score_daily
            WHERE date = current_date
            ORDER BY trust_score ASC
        """).fetchall()

        conn.close()

        if not scores:
            logger.warning("No trust scores found for today — pipeline may not have run yet")
            return

        logger.info("=" * 50)
        logger.info("DataGate Trust Score Report")
        logger.info("=" * 50)

        for source, trust, pass_rate, checked, failed, blocked in scores:
            status = "🔴 BLOCKED" if blocked else "🟢 OK"
            logger.info(
                f"{status} | {source:10} | "
                f"trust: {trust:.1%} | "
                f"pass rate: {pass_rate:.1%} | "
                f"{checked} checked, {failed} failed"
            )

        logger.info("=" * 50)

    def update_circuit_breaker():
        """
        Writes current source status to source_status table.
        This is what the advisor checks before answering.
        """
        import duckdb
        from datetime import datetime, timezone
        from loguru import logger

        conn = duckdb.connect('data/duckdb/datagate.db')

        scores = conn.execute("""
            SELECT source, trust_score, is_blocked
            FROM main_silver.int_trust_score_daily
            WHERE date = current_date
        """).fetchall()

        for source, trust_score, is_blocked in scores:
            block_reason = None
            if is_blocked:
                block_reason = f"Trust score {trust_score:.1%} below 85% threshold"

            conn.execute("""
                INSERT OR REPLACE INTO source_status
                    (source, updated_at, trust_score, is_blocked, block_reason)
                VALUES (?, ?, ?, ?, ?)
            """, [
                source,
                datetime.now(timezone.utc).isoformat(),
                trust_score,
                is_blocked,
                block_reason,
            ])

            logger.info(
                f"Updated source_status: {source} → "
                f"trust={trust_score:.1%}, blocked={is_blocked}"
            )

        conn.close()
        logger.info("Circuit breaker table updated")

    def alert_if_blocked():
        """
        Checks if any source is currently blocked.
        In production this would send a Slack/email alert.
        For portfolio: logs a clear warning.
        """
        import duckdb
        from loguru import logger

        conn = duckdb.connect('data/duckdb/datagate.db', read_only=True)

        blocked = conn.execute("""
            SELECT source, trust_score, block_reason
            FROM source_status
            WHERE is_blocked = true
        """).fetchall()

        conn.close()

        if blocked:
            logger.warning("⚠️  CIRCUIT BREAKER ACTIVE — following sources are blocked:")
            for source, trust, reason in blocked:
                logger.warning(f"   {source}: {reason}")
            logger.warning("AI advisor is running in degraded mode")
        else:
            logger.info("✅ All sources healthy — advisor running at full capacity")

    t_check = PythonOperator(
        task_id='check_trust_scores',
        python_callable=check_trust_scores,
    )

    t_update = PythonOperator(
        task_id='update_circuit_breaker',
        python_callable=update_circuit_breaker,
    )

    t_alert = PythonOperator(
        task_id='alert_if_blocked',
        python_callable=alert_if_blocked,
    )

    t_check >> t_update >> t_alert