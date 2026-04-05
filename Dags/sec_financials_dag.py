from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_config import ETL_DEFAULT_ARGS  # noqa: F401 — side-effect: adds scripts/ to sys.path
from populate_sec_financials import populate_sec_financials  # noqa: E402

with DAG(
    dag_id="sec_financials_quarterly",
    default_args=ETL_DEFAULT_ARGS,
    schedule_interval="0 6 15 1,4,7,10 *",
    catchup=False,
    tags=["sec", "quarterly"],
) as dag:
    PythonOperator(
        task_id="populate_sec_financials", python_callable=populate_sec_financials
    )
