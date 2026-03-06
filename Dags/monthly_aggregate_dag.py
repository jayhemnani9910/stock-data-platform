from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 20),
}

with DAG(
    dag_id='monthly_aggregate_dag',
    default_args=default_args,
    schedule_interval='@monthly',
    template_searchpath=['/opt/airflow/sql'],
    catchup=False,
    tags=['aggregate', 'monthly']
) as dag:
    ensure_schema = PostgresOperator(
        task_id='ensure_schema',
        postgres_conn_id='timescaledb_conn',
        sql='schema.sql'
    )

    aggregate_task = PostgresOperator(
        task_id='aggregate_monthly_data',
        postgres_conn_id='timescaledb_conn',
        sql='aggregate_monthly.sql'
    )

    ensure_schema >> aggregate_task
