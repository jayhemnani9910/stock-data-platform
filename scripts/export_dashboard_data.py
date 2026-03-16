"""Export data from TimescaleDB to static JSON files for the GitHub Pages dashboard."""
import os
import json
from datetime import date, timedelta
from db_utils import get_db_connection

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'site', 'data')


def _serialize(rows, columns):
    return [dict(zip(columns, row)) for row in rows]


def export_price_summary(conn):
    end = date.today()
    start = end - timedelta(days=90)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.ticker, f.date, f.open, f.high, f.low, f.close, f.volume
            FROM fact_stock_price_daily f
            JOIN dim_company d ON f.company_key = d.company_key
            WHERE f.date >= %s ORDER BY d.ticker, f.date
        """, (start,))
        rows = cur.fetchall()
    data = _serialize(rows, ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'])
    for r in data:
        r['date'] = str(r['date'])
    return data


def export_fundamentals(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.ticker, f.market_cap, f.trailing_pe, f.forward_pe,
                   f.price_to_book, f.dividend_yield, f.beta, f.week_52_high, f.week_52_low
            FROM fact_company_fundamentals f
            JOIN dim_company d ON f.company_key = d.company_key
            WHERE f.date = (SELECT MAX(date) FROM fact_company_fundamentals)
            ORDER BY d.ticker
        """)
        rows = cur.fetchall()
    return _serialize(rows, [
        'ticker', 'market_cap', 'trailing_pe', 'forward_pe',
        'price_to_book', 'dividend_yield', 'beta', 'week_52_high', 'week_52_low'
    ])


def export_earnings(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.ticker, f.report_date, f.eps_estimate, f.eps_actual, f.surprise_pct
            FROM fact_earnings f
            JOIN dim_company d ON f.company_key = d.company_key
            WHERE f.report_date >= (CURRENT_DATE - INTERVAL '2 years')
            ORDER BY d.ticker, f.report_date DESC
        """)
        rows = cur.fetchall()
    data = _serialize(rows, ['ticker', 'report_date', 'eps_estimate', 'eps_actual', 'surprise_pct'])
    for r in data:
        r['report_date'] = str(r['report_date'])
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
    data = _serialize(rows, ['series_id', 'name', 'date', 'value'])
    for r in data:
        r['date'] = str(r['date'])
    return data


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with get_db_connection() as conn:
        exports = {
            'price_summary.json': export_price_summary(conn),
            'fundamentals.json': export_fundamentals(conn),
            'earnings.json': export_earnings(conn),
            'macro.json': export_macro(conn),
        }

    for filename, data in exports.items():
        path = os.path.join(OUTPUT_DIR, filename)
        with open(path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"Exported {filename}: {len(data)} records")


if __name__ == '__main__':
    main()
