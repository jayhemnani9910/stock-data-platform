from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import json
import os
import psycopg2
import gzip

# Load tickers
TICKERS_FILE = '/opt/airflow/dags/tickers.txt'
with open(TICKERS_FILE, 'r') as f:
    TICKERS = [line.strip() for line in f if line.strip()]

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# === ETL TASKS ===
def _stage_path_for_run(ticker, stage, run_suffix):
    safe_ticker = ticker.lower()
    base_dir = "/tmp/stock_data_platform"
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, f"{safe_ticker}_{stage}_{run_suffix}.json.gz")

def _normalize_columns(df, ticker):
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col) for col in df.columns]

    expected = [
        f'Open_{ticker}',
        f'High_{ticker}',
        f'Low_{ticker}',
        f'Close_{ticker}',
        f'Volume_{ticker}',
    ]
    if not all(col in df.columns for col in expected):
        rename_map = {}
        for col in ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']:
            if col in df.columns:
                rename_map[col] = f"{col}_{ticker}"
        if rename_map:
            df = df.rename(columns=rename_map)

    return df

def extract_data(ticker, ti):
    try:
        end_date = datetime.today()
        start_date = end_date - timedelta(days=365 * 25)
        df = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if df.empty:
            raise ValueError("Downloaded dataframe is empty.")
        run_suffix = ti.ts_nodash or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        raw_path = _stage_path_for_run(ticker, "raw", run_suffix)
        with gzip.open(raw_path, "wt", encoding="utf-8") as f:
            f.write(df.to_json(orient="split"))
        ti.xcom_push(key='raw_path', value=raw_path)
        print(f"✅ Extracted {len(df)} records for {ticker}")
    except Exception as e:
        raise Exception(f"❌ Extract Error [{ticker}]: {str(e)}")

def transform_data(ticker, ti):
    try:
        raw_path = ti.xcom_pull(key='raw_path', task_ids=f'{ticker}_extract')
        if not raw_path or not os.path.exists(raw_path):
            raise ValueError("Missing raw dataframe path.")
        df = pd.read_json(raw_path, orient='split', compression='gzip')
        df = _normalize_columns(df, ticker)
        required_cols = [
            f'Open_{ticker}', f'High_{ticker}', f'Low_{ticker}',
            f'Close_{ticker}', f'Volume_{ticker}'
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing columns after normalization: {missing_cols}")
        df.dropna(inplace=True)
        df = df[df[f'Volume_{ticker}'] > 0]
        df.index = pd.to_datetime(df.index)
        run_suffix = ti.ts_nodash or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        cleaned_path = _stage_path_for_run(ticker, "cleaned", run_suffix)
        with gzip.open(cleaned_path, "wt", encoding="utf-8") as f:
            f.write(df.to_json(orient="split"))
        ti.xcom_push(key='cleaned_path', value=cleaned_path)
        print(f"✅ Transformed {ticker}")
    except Exception as e:
        raise Exception(f"❌ Transform Error [{ticker}]: {str(e)}")

def load_data(ticker, ti):
    try:
        cleaned_path = ti.xcom_pull(key='cleaned_path', task_ids=f'{ticker}_transform')
        if not cleaned_path or not os.path.exists(cleaned_path):
            raise ValueError("Missing cleaned dataframe path.")
        df = pd.read_json(cleaned_path, orient='split', compression='gzip')

        conn = psycopg2.connect(
            host="timescaledb", dbname="stockdw",
            user="data226", password="12345678", port=5432
        )
        cur = conn.cursor()

        cur.execute("SELECT company_key FROM dim_company WHERE ticker = %s AND is_current=TRUE", (ticker,))
        result = cur.fetchone()
        if result:
            company_key = result[0]
        else:
            cur.execute(
                "INSERT INTO dim_company (ticker, company_name, is_current, effective_date) VALUES (%s, %s, TRUE, CURRENT_DATE) RETURNING company_key",
                (ticker, ticker)
            )
            company_key = cur.fetchone()[0]
            conn.commit()

        for index, row in df.iterrows():
            price_date = index.date()
            cur.execute("""
                INSERT INTO fact_stock_price_daily (date, company_key, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, company_key) DO UPDATE 
                SET open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
            """, (
                price_date, company_key,
                row[f'Open_{ticker}'],
                row[f'High_{ticker}'],
                row[f'Low_{ticker}'],
                row[f'Close_{ticker}'],
                int(row[f'Volume_{ticker}'])
            ))

        conn.commit()
        cur.close()
        conn.close()
        raw_path = ti.xcom_pull(key='raw_path', task_ids=f'{ticker}_extract')
        for path in (raw_path, cleaned_path):
            if path and os.path.exists(path):
                os.remove(path)
        print(f"✅ Loaded {len(df)} rows for {ticker}")
    except Exception as e:
        raise Exception(f"❌ Load Error [{ticker}]: {str(e)}")

# CSV Export task — pulls 30-day data from TimescaleDB
def export_30_day_csvs():
    import psycopg2
    import pandas as pd
    import os
    from datetime import datetime, timedelta

    conn = psycopg2.connect(
        host="timescaledb", dbname="stockdw",
        user="data226", password="12345678", port=5432
    )

    with open('/opt/airflow/dags/tickers.txt', 'r') as f:
        tickers = [line.strip() for line in f if line.strip()]

    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=30)

    output_dir = "/opt/airflow/dags/stock_csvs"
    os.makedirs(output_dir, exist_ok=True)

    for ticker in tickers:
        query = """
            SELECT f.date, d.ticker, f.open, f.high, f.low, f.close, f.volume
            FROM fact_stock_price_daily f
            JOIN dim_company d ON f.company_key = d.company_key
            WHERE d.ticker = %s AND f.date BETWEEN %s AND %s
            ORDER BY f.date DESC;
        """
        df = pd.read_sql(query, conn, params=(ticker, start_date, end_date))
        csv_path = os.path.join(output_dir, f"{ticker}_last_30_days.csv")
        df.to_csv(csv_path, index=False)

    conn.close()
    print("✅ CSVs updated: last 30 days for all stocks.")

# === Dynamic DAGs for each stock ===
for ticker in TICKERS:
    with DAG(
        dag_id=f'etl_stock_data_{ticker.lower()}',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2025, 4, 20),
        catchup=False,
        tags=['stock', 'ETL']
    ) as dag:

        extract = PythonOperator(
            task_id=f'{ticker}_extract',
            python_callable=extract_data,
            op_kwargs={'ticker': ticker}
        )

        transform = PythonOperator(
            task_id=f'{ticker}_transform',
            python_callable=transform_data,
            op_kwargs={'ticker': ticker}
        )

        load = PythonOperator(
            task_id=f'{ticker}_load',
            python_callable=load_data,
            op_kwargs={'ticker': ticker}
        )

        trigger_export = TriggerDagRunOperator(
            task_id=f'{ticker}_trigger_export',
            trigger_dag_id='csv_export_dag',
            wait_for_completion=False,
            reset_dag_run=False
        )

        extract >> transform >> load >> trigger_export

    globals()[f'etl_stock_data_{ticker.lower()}'] = dag

# === Global Export DAG ===
with DAG(
    dag_id='csv_export_dag',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    start_date=datetime(2025, 4, 20),
    catchup=False,
    tags=['stock', 'CSV']
) as export_dag:

    export_csvs = PythonOperator(
        task_id='export_30_day_csvs',
        python_callable=export_30_day_csvs
    )

globals()['csv_export_dag'] = export_dag
