from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dag_config import DEFAULT_ARGS

with DAG(
    dag_id="monthly_aggregate_dag",
    default_args=DEFAULT_ARGS,
    # Not monthly: on a monthly schedule the current month's row was always
    # stale -- every September average disagreed with the daily table until
    # October. Not @daily either: beside the ETL it could run before the
    # night's closes landed. csv_export_dag triggers it after each load; the
    # rollup is an idempotent upsert that takes about a second. The dag_id
    # keeps its name so its run history stays attached.
    schedule_interval=None,
    template_searchpath=["/opt/airflow/sql"],
    catchup=False,
    tags=["aggregate", "monthly"],
) as dag:
    aggregate_task = PostgresOperator(
        task_id="aggregate_monthly_data",
        postgres_conn_id="timescaledb_conn",
        sql="aggregate_monthly.sql",
    )
