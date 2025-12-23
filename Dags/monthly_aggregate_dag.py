from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 20),
}

with DAG(
    dag_id='monthly_aggregate_dag',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    tags=['aggregate', 'monthly']
) as dag:

    aggregate_task = PostgresOperator(
        task_id='aggregate_monthly_data',
        postgres_conn_id='timescaledb_conn',
        sql='sql/aggregate_monthly.sql'
    )
