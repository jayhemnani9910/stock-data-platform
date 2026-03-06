from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from populate_fact_stock_price import populate_fact_stock_price

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 20),
}

with DAG(
    dag_id='populate_fact_stock_price',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['fact', 'stock']
) as dag:

    populate = PythonOperator(
        task_id='populate_fact_stock_price_table',
        python_callable=populate_fact_stock_price
    )
