import os
from fredapi import Fred
from db_utils import get_db_connection, get_indicator_key, batch_insert, UPSERT_MACRO_DATA_SQL

MACRO_SERIES = {
    'FEDFUNDS': ('Federal Funds Effective Rate', 'monthly', 'Percent'),
    'CPIAUCSL': ('Consumer Price Index (All Urban)', 'monthly', 'Index 1982-1984=100'),
    'UNRATE': ('Unemployment Rate', 'monthly', 'Percent'),
    'GDP': ('Gross Domestic Product', 'quarterly', 'Billions of Dollars'),
}


def _seed_indicators(conn):
    """Ensure all macro indicators exist in dim_macro_indicator."""
    with conn.cursor() as cur:
        for series_id, (name, frequency, units) in MACRO_SERIES.items():
            cur.execute("""
                INSERT INTO dim_macro_indicator (series_id, name, frequency, units)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (series_id) DO NOTHING
            """, (series_id, name, frequency, units))
    conn.commit()


def populate_macro_data():
    api_key = os.environ.get('FRED_API_KEY', '')
    if not api_key or api_key == 'your_fred_api_key_here':
        print("FRED_API_KEY not set. Register free at https://fred.stlouisfed.org/docs/api/api_key.html")
        return

    fred = Fred(api_key=api_key)

    with get_db_connection() as conn:
        _seed_indicators(conn)

        all_rows = []
        for series_id in MACRO_SERIES:
            indicator_key = get_indicator_key(conn, series_id)
            if not indicator_key:
                print(f"Skipping {series_id}: not in dim_macro_indicator")
                continue
            try:
                series = fred.get_series(series_id)
                for date, value in series.items():
                    if value is not None and str(value) != 'NaN':
                        all_rows.append((date.date(), indicator_key, float(value)))
                print(f"  {series_id}: {len(series)} data points")
            except Exception as e:
                print(f"Error fetching {series_id}: {e}")

        if all_rows:
            batch_insert(conn, UPSERT_MACRO_DATA_SQL, all_rows)

    print(f"Macro data updated: {len(all_rows)} data points across {len(MACRO_SERIES)} series")
