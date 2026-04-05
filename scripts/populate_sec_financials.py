import os
import re

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


def _parse_period_end(period_key):
    """Extract end date from period key like 'duration_2024-09-29_2025-09-27' or 'instant_2025-09-27'."""
    match = re.search(r"(\d{4}-\d{2}-\d{2})$", period_key)
    return match.group(1) if match else None


def _extract_statement(xbrl, stmt_type_key, stmt_label, company_key, filing_date, filing_type):
    rows = []
    try:
        s_info = xbrl.get_statement_by_type(stmt_type_key)
        if not s_info or not s_info.get("role"):
            return rows
        stmt_list = xbrl.get_statement(s_info["role"])
        for item in stmt_list:
            label = item.get("label", "")
            values = item.get("values", {})
            if not values or label.endswith("[Abstract]") or label.endswith("[Table]") or label.endswith("[Axis]"):
                continue
            for period_key, value in values.items():
                period_end = _parse_period_end(period_key)
                if period_end is None or value is None:
                    continue
                try:
                    rows.append(
                        (
                            company_key,
                            period_end,
                            stmt_label,
                            label,
                            str(filing_date),
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
                    filing_date = str(filing.filing_date)
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
            # Deduplicate: keep last value per (company_key, period_end, statement_type, line_item)
            seen = {}
            for row in all_rows:
                key = (row[0], row[1], row[2], row[3])
                seen[key] = row
            all_rows = list(seen.values())
            batch_insert(conn, UPSERT_SEC_FINANCIALS_SQL, all_rows)

    print(f"SEC financials updated: {len(all_rows)} line items across {len(tickers)} tickers")
