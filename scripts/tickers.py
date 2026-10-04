import os


def load_tickers(path=None):
    """The ticker list, one per line. TICKERS_FILE is read at call time, and
    the same variable drives the ETL DAG factory through dag_config."""
    with open(path or os.environ.get("TICKERS_FILE", "/opt/airflow/dags/tickers.txt")) as f:
        return [line.strip() for line in f if line.strip()]
