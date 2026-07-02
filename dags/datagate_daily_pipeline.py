from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'datagate',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# Base command — runs Python from the project directory
BASE = 'cd /opt/airflow && python -m'

with DAG(
    dag_id='datagate_daily_pipeline',
    description='Daily batch pipeline: ingest → gate → enrich → dbt',
    schedule_interval='0 13 * * 1-5',
    start_date=days_ago(1),
    catchup=False,
    tags=['datagate', 'batch', 'daily'],
    default_args=default_args,
) as dag:

    t_stocks = BashOperator(
        task_id='ingest_stocks',
        bash_command=f'{BASE} src.ingestion.stocks',
    )

    t_news = BashOperator(
        task_id='ingest_news',
        bash_command=f'{BASE} src.ingestion.news',
    )

    t_macro = BashOperator(
        task_id='ingest_macro',
        bash_command=f'{BASE} src.ingestion.macro',
    )

    t_gate = BashOperator(
        task_id='run_quality_gate',
        bash_command=f'{BASE} src.gate.gate',
    )

    t_enrich = BashOperator(
        task_id='run_enrichment',
        bash_command=f'{BASE} src.enrichment.news_enricher',
    )

    t_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            'cd /opt/airflow/dbt/finpulse && '
            'dbt run --profiles-dir /opt/airflow/dbt/finpulse && '
            'dbt test --profiles-dir /opt/airflow/dbt/finpulse'
        ),
    )

    # Stocks and news run sequentially, macro runs in parallel
[t_stocks >> t_news, t_macro] >> t_gate >> t_enrich >> t_dbt