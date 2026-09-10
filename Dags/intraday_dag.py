from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_config import ETL_DEFAULT_ARGS  # noqa: F401 — side-effect: adds scripts/ to sys.path
from populate_stock_price_intraday import populate_stock_price_intraday  # noqa: E402

with DAG(
    dag_id="intraday_prices_daily",
    default_args=ETL_DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    tags=["stock", "intraday", "daily"],
) as dag:
    PythonOperator(
        task_id="populate_stock_price_intraday",
        python_callable=populate_stock_price_intraday,
    )
