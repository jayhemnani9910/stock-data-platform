"""Export data from TimescaleDB to static JSON files for the GitHub Pages dashboard."""

import json
import math
import os
from datetime import UTC, date, datetime, timedelta

from db_utils import get_db_connection

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "site", "data")


def _clean(val):
    """Replace NaN/Inf with None for valid JSON."""
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _serialize(rows, columns):
    return [{k: _clean(v) for k, v in zip(columns, row)} for row in rows]


def export_price_summary(conn):
    end = date.today()
    start = end - timedelta(days=90)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.ticker, f.date, f.open, f.high, f.low, f.close, f.volume
            FROM fact_stock_price_daily f
            JOIN dim_company d ON f.company_key = d.company_key
            WHERE d.is_current AND f.date >= %s ORDER BY d.ticker, f.date
        """,
            (start,),
        )
        rows = cur.fetchall()
    data = _serialize(rows, ["ticker", "date", "open", "high", "low", "close", "volume"])
    for r in data:
        r["date"] = str(r["date"])
    return data


def export_fundamentals(conn):
    with conn.cursor() as cur:
        # DISTINCT ON gives each ticker its own latest row. A single global
        # MAX(date) drops any ticker whose refresh failed that day, even though
        # it still has perfectly good fundamentals from an earlier date.
        cur.execute("""
            SELECT DISTINCT ON (d.ticker)
                   d.ticker, f.date, f.market_cap, f.trailing_pe, f.forward_pe,
                   f.price_to_book, f.dividend_yield, f.beta, f.week_52_high, f.week_52_low
            FROM fact_company_fundamentals f
            JOIN dim_company d ON f.company_key = d.company_key
            WHERE d.is_current
            ORDER BY d.ticker, f.date DESC
        """)
        rows = cur.fetchall()
    data = _serialize(
        rows,
        [
            "ticker",
            "date",
            "market_cap",
            "trailing_pe",
            "forward_pe",
            "price_to_book",
            "dividend_yield",
            "beta",
            "week_52_high",
            "week_52_low",
        ],
    )
    for r in data:
        r["date"] = str(r["date"])
    return data


def export_earnings(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.ticker, f.report_date, f.eps_estimate, f.eps_actual, f.surprise_pct
            FROM fact_earnings f
            JOIN dim_company d ON f.company_key = d.company_key
            WHERE d.is_current
              AND f.report_date >= (CURRENT_DATE - INTERVAL '2 years')
              -- yfinance publishes scheduled reports with a NULL eps_actual.
              -- The dashboard picks each ticker's latest report_date, so an
              -- unreported future quarter would become its "Latest Quarter"
              -- and render as blank EPS.
              AND f.report_date <= CURRENT_DATE
            ORDER BY d.ticker, f.report_date DESC
        """)
        rows = cur.fetchall()
    data = _serialize(rows, ["ticker", "report_date", "eps_estimate", "eps_actual", "surprise_pct"])
    for r in data:
        r["report_date"] = str(r["report_date"])
    return data


def export_macro(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.series_id, m.name, f.date, f.value
            FROM fact_macro_data f
            JOIN dim_macro_indicator m ON f.indicator_key = m.indicator_key
            WHERE f.date >= (CURRENT_DATE - INTERVAL '5 years')
            ORDER BY m.series_id, f.date
        """)
        rows = cur.fetchall()
    data = _serialize(rows, ["series_id", "name", "date", "value"])
    for r in data:
        r["date"] = str(r["date"])
    return data


# Which field carries each dataset's own timestamp, for the snapshot stamp.
_DATE_FIELD = {
    "price_summary.json": "date",
    "fundamentals.json": "date",
    "earnings.json": "report_date",
    "macro.json": "date",
}


def build_meta(exports, generated_at=None):
    """Describe when the export ran and how current each dataset is.

    The dashboard used to stamp `new Date()` on itself, so it read "today" over
    data that was days old. It can only tell the truth if the truth ships with
    the data.
    """
    generated_at = generated_at or datetime.now(UTC)
    datasets = {}
    for filename, data in exports.items():
        field = _DATE_FIELD.get(filename)
        dates = [r[field] for r in data if field and r.get(field)] if data else []
        datasets[filename] = {
            "records": len(data),
            "data_through": max(dates) if dates else None,
        }
    return {"generated_at": generated_at.isoformat(timespec="seconds"), "datasets": datasets}


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with get_db_connection() as conn:
        exports = {
            "price_summary.json": export_price_summary(conn),
            "fundamentals.json": export_fundamentals(conn),
            "earnings.json": export_earnings(conn),
            "macro.json": export_macro(conn),
        }

    # An empty result means the query or the warehouse is broken, not that the
    # data went away. Overwriting a good file with [] would publish a blank
    # dashboard and destroy the only copy of what it replaced.
    empty = [name for name, data in exports.items() if not data]
    if empty:
        raise SystemExit(f"Refusing to export: no rows returned for {', '.join(sorted(empty))}")

    exports["meta.json"] = build_meta(exports)

    for filename, data in exports.items():
        path = os.path.join(OUTPUT_DIR, filename)
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        count = len(data) if isinstance(data, list) else len(data.get("datasets", {}))
        print(f"Exported {filename}: {count} records")


if __name__ == "__main__":
    main()
