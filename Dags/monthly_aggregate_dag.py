from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from dag_config import DEFAULT_ARGS

with DAG(
    dag_id='monthly_aggregate_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval='@monthly',
    template_searchpath=['/opt/airflow/sql'],
    catchup=False,
    tags=['aggregate', 'monthly']
) as dag:
    aggregate_task = PostgresOperator(
        task_id='aggregate_monthly_data',
        postgres_conn_id='timescaledb_conn',
        sql='aggregate_monthly.sql'
    )
