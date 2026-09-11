import gzip
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dag_config import ETL_DEFAULT_ARGS, load_tickers
from db_utils import get_company_key, get_db_connection, upsert_stock_prices

TICKERS = load_tickers()
STANDARD_COLS = ["open", "high", "low", "close", "volume"]

_BASE_DIR = "/tmp/stock_data_platform"
os.makedirs(_BASE_DIR, exist_ok=True)
_STAGE_MAX_AGE_SECONDS = 24 * 3600


def _prune_stale_stage_files():
    """Drop staged files older than a day.

    load_data removes the pair it consumed, but when extract succeeds and
    transform fails nothing ever runs load, so the raw file is left behind.
    """
    cutoff = time.time() - _STAGE_MAX_AGE_SECONDS
    for path in Path(_BASE_DIR).glob("*.json.gz"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
        except OSError:
            pass


def _stage_path_for_run(ticker, stage, run_suffix):
    return os.path.join(_BASE_DIR, f"{ticker.lower()}_{stage}_{run_suffix}.json.gz")


def _read_staged(path):
    """Read a staged frame back, restoring its DatetimeIndex.

    to_json(orient="split") writes a DatetimeIndex as epoch milliseconds, and
    read_json's convert_axes heuristic declines to convert negative values --
    so every date before 1970 comes back as a plain int64 and row.Index.date()
    dies with "'int' object has no attribute 'date'". A 25-year extract window
    hid this for years; DIS trades back to 1962 and broke the moment the window
    was lifted.
    """
    df = pd.read_json(path, orient="split", compression="gzip")
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, unit="ms")
    return df


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
    """Most recent date loaded for this ticker, or None when nothing is loaded yet.

    Database errors are deliberately allowed to propagate. Swallowing them here
    returns None, which extract_data cannot tell apart from a genuine first run,
    so a brief outage silently turns an incremental pull into a full-history refetch
    and hides the outage itself.
    """
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


# How far back each incremental run re-reads. Long enough to still hold prices
# from before an ex-date when the provider publishes a re-adjustment a few days
# late, short enough to stay one cheap request.
OVERLAP_DAYS = 10

# Median |stored/fresh - 1| above which the adjustment basis has changed.
# Measured on all ten tickers with nothing changed: 3e-14 to 4e-13, pure
# floating-point noise. The smallest real adjustment worth catching -- a $0.01
# dividend on a $220 stock -- is 4.5e-5. 1e-6 sits seven orders of magnitude
# above the noise and 45x below that.
ADJUSTMENT_TOLERANCE = 1e-6


def _get_stored_closes(ticker, since):
    """{date: close} for this ticker from `since` onwards."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT f.date, f.close FROM fact_stock_price_daily f
                JOIN dim_company d ON f.company_key = d.company_key
                WHERE d.ticker = %s AND d.is_current = TRUE AND f.date >= %s
            """,
                (ticker, since),
            )
            return dict(cur.fetchall())


def _adjustment_changed(stored, fresh):
    """True if freshly downloaded prices sit on a different split/dividend basis.

    yf.download returns prices adjusted as of the moment of the call, so every
    dividend or split rescales the entire history before its ex-date. This
    compares the overlap the incremental run re-reads anyway, rather than
    trusting a corporate-actions list, because that list is only as good as
    the date it is checked against. It used to be checked against the last
    loaded date, and the Kafka consumer writes the current session's row
    during market hours -- so when NVDA went ex-dividend on 2026-09-10, the
    nightly run found that date already loaded, asked whether any action fell
    *after* it, got no, and left 6,949 NVDA closes 0.11% high.

    An adjustment moves every older row by the same factor, so the median
    ratio shifts. A single late correction to one day's close -- which the
    overlap upsert fixes by itself -- does not move the median, and must not
    trigger a 60-year refetch. The newest stored day is excluded: it may be a
    streaming row or a close taken before the provider finalised it.

    stored: {date: close}. fresh: pandas Series of closes indexed by date.
    """
    if not stored or fresh is None or fresh.empty:
        return False
    newest = max(stored)
    fresh_by_day = {pd.Timestamp(k).date(): float(v) for k, v in fresh.items()}
    ratios = [
        stored[day] / fresh_by_day[day] for day in stored if day != newest and day in fresh_by_day and fresh_by_day[day]
    ]
    if not ratios:
        return False
    return abs(float(pd.Series(ratios).median()) - 1) > ADJUSTMENT_TOLERANCE


def extract_data(ticker, ti, ts_nodash):
    try:
        _prune_stale_stage_files()
        end_date = datetime.today()
        last_date = _get_last_loaded_date(ticker)
        df = None
        if last_date:
            # Re-read OVERLAP_DAYS, not one day: the overlap corrects any close
            # taken before the provider finalised it, and it is what
            # _adjustment_changed compares against.
            start_date = last_date - timedelta(days=OVERLAP_DAYS)
            print(f"Incremental extract for {ticker} from {start_date}")
            df = _normalize_columns(yf.download(ticker, start=start_date, end=end_date, progress=False), ticker)
            if not df.empty and _adjustment_changed(_get_stored_closes(ticker, start_date), df["close"]):
                print(f"Adjustment basis changed for {ticker}: refetching full history to re-adjust")
                df = None
        else:
            print(f"Full extract for {ticker} (first run)")

        if df is None:
            # period="max" reaches each ticker's first traded day. A fixed
            # 25-year window instead cut every listing older than that at the
            # same arbitrary date: DIS lost 9,995 bars back to 1962, JPM 5,430,
            # AAPL 5,242 -- 26,338 across the ten, a third of the available
            # history, including the dot-com crash for AMZN and NVDA.
            # dim_date starts in 1945, so the date foreign key covers this.
            df = _normalize_columns(yf.download(ticker, period="max", progress=False), ticker)
        if df.empty:
            raise ValueError("Downloaded dataframe is empty.")
        run_suffix = ts_nodash
        raw_path = _stage_path_for_run(ticker, "raw", run_suffix)
        with gzip.open(raw_path, "wt", encoding="utf-8") as f:
            f.write(df.to_json(orient="split"))
        ti.xcom_push(key="raw_path", value=raw_path)
        print(f"Extracted {len(df)} records for {ticker}")
    except Exception as e:
        raise Exception(f"Extract Error [{ticker}]: {str(e)}") from e


def transform_data(ticker, ti, ts_nodash):
    try:
        raw_path = ti.xcom_pull(key="raw_path", task_ids=f"{ticker}_extract")
        if not raw_path:
            raise ValueError("Missing raw dataframe path.")
        df = _read_staged(raw_path)
        missing_cols = [col for col in STANDARD_COLS if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing columns after normalization: {missing_cols}")
        df.dropna(inplace=True)
        df = df[df["volume"] > 0]
        df.index = pd.to_datetime(df.index)
        run_suffix = ts_nodash
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
        df = _read_staged(cleaned_path)

        with get_db_connection() as conn:
            company_key = get_company_key(conn, ticker)
            if not company_key:
                raise ValueError(f"Ticker {ticker} not found in dim_company. Run populate_dim_company first.")

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
    export_csvs = PythonOperator(task_id="export_30_day_csvs", python_callable=export_30_day_csvs)

globals()["csv_export_dag"] = export_dag
