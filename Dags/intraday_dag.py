from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from dag_config import ETL_DEFAULT_ARGS  # noqa: F401 — side-effect: adds scripts/ to sys.path
from populate_stock_price_intraday import populate_stock_price_intraday  # noqa: E402


def _refresh_hourly_bars():
    # Without Alpaca keys the loader writes nothing. Mark the run skipped, not
    # successful, so a stack running without keys shows it in the UI instead of
    # reporting green while the hourly data quietly stops updating.
    if not populate_stock_price_intraday():
        raise AirflowSkipException("ALPACA_API_KEY / ALPACA_API_SECRET not set; hourly bars not refreshed")


with DAG(
    dag_id="intraday_prices_daily",
    default_args=ETL_DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    tags=["stock", "intraday", "daily"],
) as dag:
    PythonOperator(
        task_id="populate_stock_price_intraday",
        python_callable=_refresh_hourly_bars,
    )
