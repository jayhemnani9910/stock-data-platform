import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

ETL_DEFAULT_ARGS = {
    **DEFAULT_ARGS,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Honour the same env var the populate_*.py scripts read, so overriding it
# changes both which tickers are loaded and which ETL DAGs are created.
TICKERS_FILE = os.environ.get("TICKERS_FILE", "/opt/airflow/dags/tickers.txt")


def load_tickers():
    with open(TICKERS_FILE) as f:
        return [line.strip() for line in f if line.strip()]
