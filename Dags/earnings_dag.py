from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_config import ETL_DEFAULT_ARGS  # noqa: F401 — side-effect: adds scripts/ to sys.path
from populate_earnings import populate_earnings  # noqa: E402

with DAG(
    dag_id='earnings_weekly',
    default_args=ETL_DEFAULT_ARGS,
    schedule_interval='@weekly',
    catchup=False,
    tags=['earnings', 'weekly']
) as dag:
    PythonOperator(
        task_id='populate_earnings',
        python_callable=populate_earnings
    )
