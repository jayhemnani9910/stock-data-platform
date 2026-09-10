import os
import re
from datetime import date

from db_utils import (
    UPSERT_SEC_FINANCIALS_SQL,
    batch_insert,
    get_company_key,
    get_db_connection,
)
from edgar import Company, set_identity

TICKERS_FILE = os.environ.get("TICKERS_FILE", "/opt/airflow/dags/tickers.txt")

STATEMENT_TYPES = {
    "IncomeStatement": "income",
    "BalanceSheet": "balance_sheet",
    "CashFlowStatement": "cash_flow",
}

_DURATION_RE = re.compile(r"^duration_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})$")
_INSTANT_RE = re.compile(r"^instant_(\d{4}-\d{2}-\d{2})$")

INSTANT = "instant"


def _parse_period(period_key):
    """Split an XBRL period key into (start, end, period_type).

    Keys look like 'duration_2026-03-29_2026-06-27' or 'instant_2025-09-27'.
    Both dates matter: a 10-Q reports the same line item over the quarter and
    over the year to date, and those two windows share an end date. Keeping
    only the end -- which this function used to do -- collapsed them onto one
    row, and the caller's dedup then kept whichever came last. Apple's Q3
    FY2026 quarter (109,417M of Net sales) was discarded in favour of the
    nine-month figure (364,357M), which was then stored as if it were the
    quarter.

    Instant facts (the balance sheet) have no duration; start equals end so
    the column can stay NOT NULL and join like any other.

    Returns None for a key in neither shape.
    """
    m = _DURATION_RE.match(period_key)
    if m:
        return date.fromisoformat(m.group(1)), date.fromisoformat(m.group(2)), None

    m = _INSTANT_RE.match(period_key)
    if m:
        instant = date.fromisoformat(m.group(1))
        return instant, instant, INSTANT

    return None


def _period_index(xbrl):
    """Map each period key to the filing's own description of that window.

    reporting_periods carries period_type ('Quarterly', 'Nine Months',
    'Annual'), fiscal_year and, when the filing declares one, fiscal_period
    ('Q3', 'FY'). Reading it here beats inferring the window from a day count,
    which gets 52/53-week fiscal calendars wrong.
    """
    index = {}
    try:
        for period in xbrl.reporting_periods:
            key = period.get("key")
            if key:
                index[key] = period
    except Exception as e:  # a filing with no usable period metadata
        print(f"  Could not read reporting periods: {e}")
    return index


def _extract_statement(xbrl, stmt_type_key, stmt_label, company_key, filing_date, filing_type):
    rows = []
    try:
        s_info = xbrl.get_statement_by_type(stmt_type_key)
        if not s_info or not s_info.get("role"):
            return rows
        periods = _period_index(xbrl)
        # The period this filing is actually reporting on. Every other column in
        # it is a comparative carried over from an earlier filing, so stamping
        # this filing's date on all of them dated Apple's FY2023 income to
        # 2025-10-31. Comparatives get NULL filing_date/filing_type instead.
        period_of_report = None
        try:
            if xbrl.period_of_report:
                period_of_report = date.fromisoformat(str(xbrl.period_of_report)[:10])
        except (ValueError, TypeError):
            pass

        stmt_list = xbrl.get_statement(s_info["role"])
        for item in stmt_list:
            label = item.get("label", "")
            values = item.get("values", {})
            if not values or label.endswith("[Abstract]") or label.endswith("[Table]") or label.endswith("[Axis]"):
                continue
            for period_key, value in values.items():
                parsed = _parse_period(period_key)
                if parsed is None or value is None:
                    continue
                period_start, period_end, forced_type = parsed
                meta = periods.get(period_key, {})
                period_type = forced_type or meta.get("period_type") or "duration"
                own_period = period_of_report is not None and period_end == period_of_report
                try:
                    rows.append(
                        (
                            company_key,
                            stmt_label,
                            label,
                            period_start,
                            period_end,
                            period_type,
                            meta.get("fiscal_year"),
                            meta.get("fiscal_period"),
                            filing_date if own_period else None,
                            filing_type if own_period else None,
                            filing_date,
                            filing_type,
                            float(value),
                        )
                    )
                except (ValueError, TypeError):
                    pass
    except Exception as e:
        print(f"  Error extracting {stmt_label}: {e}")
    return rows


def populate_sec_financials():
    identity = os.environ.get("EDGAR_IDENTITY", "StockDataPlatform user@example.com")
    set_identity(identity)

    with open(TICKERS_FILE) as f:
        tickers = [line.strip() for line in f if line.strip()]

    all_rows = []

    with get_db_connection() as conn:
        for ticker in tickers:
            company_key = get_company_key(conn, ticker)
            if not company_key:
                print(f"Skipping {ticker}: not in dim_company")
                continue
            try:
                company = Company(ticker)
                for filing_type in ["10-K", "10-Q"]:
                    filing = company.get_filings(form=filing_type, amendments=False).latest(1)
                    if filing is None:
                        continue
                    filing_date = filing.filing_date
                    xbrl = filing.xbrl()
                    if xbrl is None:
                        continue
                    for stmt_key, stmt_label in STATEMENT_TYPES.items():
                        rows = _extract_statement(
                            xbrl,
                            stmt_key,
                            stmt_label,
                            company_key,
                            filing_date,
                            filing_type,
                        )
                        all_rows.extend(rows)
                ticker_count = len([r for r in all_rows if r[0] == company_key])
                print(f"  {ticker}: extracted {ticker_count} line items")
            except Exception as e:
                print(f"Error processing {ticker}: {e}")

        if all_rows:
            all_rows = _dedupe(all_rows)
            batch_insert(conn, UPSERT_SEC_FINANCIALS_SQL, all_rows)

    print(f"SEC financials updated: {len(all_rows)} line items across {len(tickers)} tickers")


def _dedupe(rows):
    """Collapse rows that would collide on the primary key.

    execute_values sends the batch as one INSERT, and Postgres rejects a
    statement whose rows hit the conflict target twice. The key must match the
    table's: dropping period_start from it is what let a quarter and a
    year-to-date figure overwrite each other. The 10-K is read before the 10-Q,
    so on a genuine tie the more recent filing wins.
    """
    seen = {}
    for row in rows:
        # (company_key, statement_type, line_item, period_start, period_end)
        seen[(row[0], row[1], row[2], row[3], row[4])] = row
    return list(seen.values())
