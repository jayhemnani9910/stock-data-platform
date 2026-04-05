from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_config import ETL_DEFAULT_ARGS  # noqa: F401 — side-effect: adds scripts/ to sys.path
from populate_macro_data import populate_macro_data  # noqa: E402

with DAG(
    dag_id="macro_daily",
    default_args=ETL_DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    tags=["macro", "daily"],
) as dag:
    PythonOperator(task_id="populate_macro_data", python_callable=populate_macro_data)
