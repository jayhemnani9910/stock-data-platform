import os

import pandas as pd
from db_utils import (
    UPSERT_MACRO_DATA_SQL,
    batch_insert,
    get_db_connection,
    get_indicator_key,
)
from fredapi import Fred

MACRO_SERIES = {
    "FEDFUNDS": ("Federal Funds Effective Rate", "monthly", "Percent"),
    "CPIAUCSL": ("Consumer Price Index (All Urban)", "monthly", "Index 1982-1984=100"),
    "UNRATE": ("Unemployment Rate", "monthly", "Percent"),
    "GDP": ("Gross Domestic Product", "quarterly", "Billions of Dollars"),
}


def _seed_indicators(conn):
    """Ensure all macro indicators exist in dim_macro_indicator."""
    with conn.cursor() as cur:
        for series_id, (name, frequency, units) in MACRO_SERIES.items():
            cur.execute(
                """
                INSERT INTO dim_macro_indicator (series_id, name, frequency, units)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (series_id) DO NOTHING
            """,
                (series_id, name, frequency, units),
            )
    conn.commit()


def _series_to_rows(series, indicator_key):
    """Convert a FRED series into upsert rows, dropping periods with no observation.

    FRED returns NaN for periods it has no reading for (1946 quarters in GDP, the
    October 2025 BLS gap). NaN satisfies the NOT NULL constraint and Postgres sorts
    it above every real value, so one leaked row makes MAX() and AVG() return NaN
    for the whole series. pd.isna covers None, NaN and NaT in one check.
    """
    return [(date.date(), indicator_key, float(value)) for date, value in series.items() if not pd.isna(value)]


def populate_macro_data():
    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key or api_key == "your_fred_api_key_here":
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
                all_rows.extend(_series_to_rows(series, indicator_key))
                print(f"  {series_id}: {len(series)} data points")
            except Exception as e:
                print(f"Error fetching {series_id}: {e}")

        if all_rows:
            batch_insert(conn, UPSERT_MACRO_DATA_SQL, all_rows)

    print(f"Macro data updated: {len(all_rows)} data points across {len(MACRO_SERIES)} series")
