from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_config import DEFAULT_ARGS  # noqa: F401 — side-effect: adds scripts/ to sys.path
from populate_company_fundamentals import populate_company_fundamentals  # noqa: E402
from populate_dim_company import populate_dim_company  # noqa: E402
from populate_dim_date import generate_dates  # noqa: E402
from populate_earnings import populate_earnings  # noqa: E402
from populate_fact_stock_price import populate_fact_stock_price  # noqa: E402
from populate_macro_data import populate_macro_data  # noqa: E402
from populate_sec_financials import populate_sec_financials  # noqa: E402

_POPULATE_DAGS = [
    (
        "populate_dim_company",
        populate_dim_company,
        "populate_dim_company_table",
        ["init", "dimension"],
    ),
    (
        "populate_dim_date",
        generate_dates,
        "populate_dim_date_table",
        ["init", "dimension"],
    ),
    (
        "populate_fact_stock_price",
        populate_fact_stock_price,
        "populate_fact_stock_price_table",
        ["fact", "stock"],
    ),
    (
        "populate_fundamentals",
        populate_company_fundamentals,
        "populate_fundamentals_table",
        ["init", "fundamentals"],
    ),
    (
        "populate_earnings",
        populate_earnings,
        "populate_earnings_table",
        ["init", "earnings"],
    ),
    (
        "populate_sec_financials",
        populate_sec_financials,
        "populate_sec_financials_table",
        ["init", "sec"],
    ),
    (
        "populate_macro_data",
        populate_macro_data,
        "populate_macro_data_table",
        ["init", "macro"],
    ),
]

for dag_id, callable_fn, task_id, tags in _POPULATE_DAGS:
    with DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        schedule_interval=None,
        catchup=False,
        tags=tags,
    ) as dag:
        PythonOperator(task_id=task_id, python_callable=callable_fn)

    globals()[dag_id] = dag
