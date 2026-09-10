"""Make the project importable from the tests without per-file sys.path edits.

Nothing here is installed as a package: Dags/ and scripts/ are bind-mounted flat
into the Airflow image, and the root-level Kafka scripts import `db_utils` with
no package prefix. Every test module used to repeat the same sys.path.insert to
compensate. Doing it once here keeps the tests importing the real code instead
of copies of it.
"""

import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))

# Dags/ is on the path so dag_config can be imported directly. Importing any
# of the DAG modules beside it still pulls in Airflow -- only dag_config is
# safe to import on a plain checkout.
for path in (ROOT, os.path.join(ROOT, "scripts"), os.path.join(ROOT, "Dags")):
    if path not in sys.path:
        sys.path.insert(0, path)

# Dags/dag_config.py and the populate scripts read this to find the ticker list.
# In the image it is /opt/airflow/dags/tickers.txt; under test it is the repo's.
os.environ.setdefault("TICKERS_FILE", os.path.join(ROOT, "Dags", "tickers.txt"))
