from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import os
import gzip
from pathlib import Path

from dag_config import ETL_DEFAULT_ARGS, load_tickers
from db_utils import get_db_connection, get_company_key, upsert_stock_prices

TICKERS = load_tickers()
STANDARD_COLS = ["open", "high", "low", "close", "volume"]

_BASE_DIR = "/tmp/stock_data_platform"
os.makedirs(_BASE_DIR, exist_ok=True)


def _stage_path_for_run(ticker, stage, run_suffix):
    return os.path.join(_BASE_DIR, f"{ticker.lower()}_{stage}_{run_suffix}.json.gz")


def _normalize_columns(df, ticker):
    """Normalize yfinance DataFrame columns to standard names: open, high, low, close, volume."""
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(col) for col in df.columns]

    # Map yfinance column names (with or without ticker suffix) to standard names
    col_map = {}
    for std_name in ["Open", "High", "Low", "Close", "Volume"]:
        suffixed = f"{std_name}_{ticker}"
        if suffixed in df.columns:
            col_map[suffixed] = std_name.lower()
        elif std_name in df.columns:
            col_map[std_name] = std_name.lower()

    if "Adj Close" in df.columns:
        df = df.drop(columns=["Adj Close"], errors="ignore")
    if f"Adj Close_{ticker}" in df.columns:
        df = df.drop(columns=[f"Adj Close_{ticker}"], errors="ignore")

    df = df.rename(columns=col_map)
    return df


def _get_last_loaded_date(ticker):
    """Get the most recent date loaded for this ticker, or None for first run."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(f.date) FROM fact_stock_price_daily f
                    JOIN dim_company d ON f.company_key = d.company_key
                    WHERE d.ticker = %s AND d.is_current = TRUE
                """,
                    (ticker,),
                )
                row = cur.fetchone()
                return row[0] if row and row[0] else None
    except Exception:
        return None


def extract_data(ticker, ti):
    try:
        end_date = datetime.today()
        last_date = _get_last_loaded_date(ticker)
        if last_date:
            start_date = last_date - timedelta(days=1)
            print(f"Incremental extract for {ticker} from {start_date}")
        else:
            start_date = end_date - timedelta(days=365 * 25)
            print(f"Full extract for {ticker} (first run)")

        df = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if df.empty:
            raise ValueError("Downloaded dataframe is empty.")
        df = _normalize_columns(df, ticker)
        run_suffix = ti.ts_nodash or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        raw_path = _stage_path_for_run(ticker, "raw", run_suffix)
        with gzip.open(raw_path, "wt", encoding="utf-8") as f:
            f.write(df.to_json(orient="split"))
        ti.xcom_push(key="raw_path", value=raw_path)
        print(f"Extracted {len(df)} records for {ticker}")
    except Exception as e:
        raise Exception(f"Extract Error [{ticker}]: {str(e)}") from e


def transform_data(ticker, ti):
    try:
        raw_path = ti.xcom_pull(key="raw_path", task_ids=f"{ticker}_extract")
        if not raw_path:
            raise ValueError("Missing raw dataframe path.")
        df = pd.read_json(raw_path, orient="split", compression="gzip")
        missing_cols = [col for col in STANDARD_COLS if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing columns after normalization: {missing_cols}")
        df.dropna(inplace=True)
        df = df[df["volume"] > 0]
        df.index = pd.to_datetime(df.index)
        run_suffix = ti.ts_nodash or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        cleaned_path = _stage_path_for_run(ticker, "cleaned", run_suffix)
        with gzip.open(cleaned_path, "wt", encoding="utf-8") as f:
            f.write(df.to_json(orient="split"))
        ti.xcom_push(key="cleaned_path", value=cleaned_path)
        print(f"Transformed {ticker}")
    except Exception as e:
        raise Exception(f"Transform Error [{ticker}]: {str(e)}") from e


def load_data(ticker, ti):
    raw_path = ti.xcom_pull(key="raw_path", task_ids=f"{ticker}_extract")
    cleaned_path = ti.xcom_pull(key="cleaned_path", task_ids=f"{ticker}_transform")
    try:
        if not cleaned_path:
            raise ValueError("Missing cleaned dataframe path.")
        df = pd.read_json(cleaned_path, orient="split", compression="gzip")

        with get_db_connection() as conn:
            company_key = get_company_key(conn, ticker)
            if not company_key:
                raise ValueError(
                    f"Ticker {ticker} not found in dim_company. Run populate_dim_company first."
                )

            rows = [
                (
                    row.Index.date(),
                    company_key,
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    int(row.volume),
                )
                for row in df.itertuples()
            ]
            upsert_stock_prices(conn, rows)

        print(f"Loaded {len(df)} rows for {ticker}")
    except Exception as e:
        raise Exception(f"Load Error [{ticker}]: {str(e)}") from e
    finally:
        for path in (raw_path, cleaned_path):
            if path:
                Path(path).unlink(missing_ok=True)


def export_30_day_csvs():
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=30)
    output_dir = "/opt/airflow/dags/stock_csvs"
    os.makedirs(output_dir, exist_ok=True)

    with get_db_connection() as conn:
        query = """
            SELECT f.date, d.ticker, f.open, f.high, f.low, f.close, f.volume
            FROM fact_stock_price_daily f
            JOIN dim_company d ON f.company_key = d.company_key
            WHERE d.ticker = ANY(%s) AND f.date BETWEEN %s AND %s
            ORDER BY d.ticker, f.date DESC
        """
        df_all = pd.read_sql(query, conn, params=(list(TICKERS), start_date, end_date))

    for ticker, df in df_all.groupby("ticker"):
        csv_path = os.path.join(output_dir, f"{ticker}_last_30_days.csv")
        df.to_csv(csv_path, index=False)

    print("CSVs updated: last 30 days for all stocks.")


for ticker in TICKERS:
    with DAG(
        dag_id=f"etl_stock_data_{ticker.lower()}",
        default_args=ETL_DEFAULT_ARGS,
        schedule_interval="@daily",
        catchup=False,
        tags=["stock", "ETL"],
    ) as dag:
        extract = PythonOperator(
            task_id=f"{ticker}_extract",
            python_callable=extract_data,
            op_kwargs={"ticker": ticker},
        )

        transform = PythonOperator(
            task_id=f"{ticker}_transform",
            python_callable=transform_data,
            op_kwargs={"ticker": ticker},
        )

        load = PythonOperator(
            task_id=f"{ticker}_load",
            python_callable=load_data,
            op_kwargs={"ticker": ticker},
        )

        trigger_export = TriggerDagRunOperator(
            task_id=f"{ticker}_trigger_export",
            trigger_dag_id="csv_export_dag",
            wait_for_completion=False,
            reset_dag_run=True,
        )

        extract >> transform >> load >> trigger_export

    globals()[f"etl_stock_data_{ticker.lower()}"] = dag

with DAG(
    dag_id="csv_export_dag",
    default_args=ETL_DEFAULT_ARGS,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=["stock", "CSV"],
) as export_dag:
    export_csvs = PythonOperator(
        task_id="export_30_day_csvs", python_callable=export_30_day_csvs
    )

globals()["csv_export_dag"] = export_dag
