from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys, os

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from populate_dim_company import populate_dim_company

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 20),
}

with DAG(
    dag_id='populate_dim_company',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['init', 'dimension']
) as dag:

    populate = PythonOperator(
        task_id='populate_dim_company_table',
        python_callable=populate_dim_company
    )
