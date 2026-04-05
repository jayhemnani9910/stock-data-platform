from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_config import ETL_DEFAULT_ARGS  # noqa: F401 — side-effect: adds scripts/ to sys.path
from populate_company_fundamentals import populate_company_fundamentals  # noqa: E402

with DAG(
    dag_id="fundamentals_daily",
    default_args=ETL_DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    tags=["fundamentals", "daily"],
) as dag:
    PythonOperator(
        task_id="populate_company_fundamentals",
        python_callable=populate_company_fundamentals,
    )
