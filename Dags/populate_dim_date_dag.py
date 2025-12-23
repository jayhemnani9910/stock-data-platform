# dags/populate_dim_date_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add scripts directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from populate_dim_date import generate_dates

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 20),
}

with DAG(
    dag_id='populate_dim_date',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['init', 'dimension']
) as dag:

    populate = PythonOperator(
        task_id='populate_dim_date_table',
        python_callable=generate_dates
    )
