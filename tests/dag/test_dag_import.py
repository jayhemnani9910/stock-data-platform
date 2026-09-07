"""Import every DAG file and fail if any of them is broken.

These tests need apache-airflow, which is not in requirements.txt because the
project only ever runs Airflow inside the image built from Dockerfile.airflow.
So they skip on a plain checkout and run for real in the `dag-import` CI job,
which executes pytest inside that image.

Without this, nothing in CI ever loads the files under Dags/. That is how an
AttributeError in etl_stock_data_dag.py survived from March to September 2026
with ruff and 62 unit tests green the whole time.
"""

import os

import pytest

airflow_models = pytest.importorskip("airflow.models", reason="apache-airflow not installed")

DAGS_FOLDER = os.environ.get(
    "AIRFLOW__CORE__DAGS_FOLDER",
    os.path.join(os.path.dirname(__file__), "..", "..", "Dags"),
)

EXPECTED_TICKERS = ["aapl", "amzn", "dis", "goog", "jpm", "meta", "msft", "nflx", "nvda", "tsla"]


@pytest.fixture(scope="module")
def dagbag():
    return airflow_models.DagBag(DAGS_FOLDER, include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG files failed to import: {dagbag.import_errors}"


def test_dags_were_found(dagbag):
    assert len(dagbag.dags) > 0, f"no DAGs discovered in {DAGS_FOLDER}"


def test_every_ticker_has_an_etl_dag(dagbag):
    missing = [t for t in EXPECTED_TICKERS if f"etl_stock_data_{t}" not in dagbag.dags]
    assert not missing, f"no ETL DAG for: {missing}"


def test_no_fake_price_seeder_is_registered(dagbag):
    """populate_fact_stock_price wrote hardcoded 2024 prices over real rows."""
    assert "populate_fact_stock_price" not in dagbag.dags


def test_every_task_has_a_callable_or_sql(dagbag):
    for dag_id, dag in dagbag.dags.items():
        for task in dag.tasks:
            has_work = (
                getattr(task, "python_callable", None) is not None
                or getattr(task, "sql", None) is not None
                or getattr(task, "trigger_dag_id", None) is not None
            )
            assert has_work, f"{dag_id}.{task.task_id} does nothing"
