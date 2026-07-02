# =============================================================================
# dags/datagate_daily_pipeline.py
# DataGate daily batch pipeline DAG
# Runs every weekday at 6:30 PM IST after market close
# Tasks: ingest → gate → enrich → dbt run → dbt test
# =============================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'datagate',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='datagate_daily_pipeline',
    description='Daily batch pipeline: ingest → gate → enrich → dbt',
    schedule_interval='0 13 * * 1-5',  # 6:30 PM IST weekdays
    start_date=days_ago(1),
    catchup=False,
    tags=['datagate', 'batch', 'daily'],
    default_args=default_args,
) as dag:

    def ingest_stocks():
        from src.ingestion.stocks import run
        run()

    def ingest_news():
        from src.ingestion.news import run
        run()

    def ingest_macro():
        from src.ingestion.macro import run
        run()

    def run_gate():
        from src.gate.gate import run
        run()

    def run_enrichment():
        from src.enrichment.news_enricher import run
        run()

    t_stocks = PythonOperator(
        task_id='ingest_stocks',
        python_callable=ingest_stocks,
    )

    t_news = PythonOperator(
        task_id='ingest_news',
        python_callable=ingest_news,
    )

    t_macro = PythonOperator(
        task_id='ingest_macro',
        python_callable=ingest_macro,
    )

    t_gate = PythonOperator(
        task_id='run_quality_gate',
        python_callable=run_gate,
    )

    t_enrich = PythonOperator(
        task_id='run_enrichment',
        python_callable=run_enrichment,
    )

    t_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            'cd /opt/airflow/dbt/finpulse && '
            'dbt run --profiles-dir /opt/airflow/dbt/finpulse && '
            'dbt test --profiles-dir /opt/airflow/dbt/finpulse'
        ),
    )

    # Task dependencies — sequential pipeline
    t_stocks >> t_news >> t_macro >> t_gate >> t_enrich >> t_dbt