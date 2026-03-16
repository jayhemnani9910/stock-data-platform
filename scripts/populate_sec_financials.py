import os
from edgar import set_identity, Company
from db_utils import get_db_connection, get_company_key, batch_insert, UPSERT_SEC_FINANCIALS_SQL

TICKERS_FILE = os.environ.get("TICKERS_FILE", "/opt/airflow/dags/tickers.txt")

STATEMENT_METHODS = {
    'income': 'income_statement',
    'balance_sheet': 'balance_sheet',
    'cash_flow': 'cash_flow_statement',
}


def _extract_statement(financials, method_name, statement_type, company_key, filing_date, filing_type):
    rows = []
    try:
        statement = getattr(financials, method_name, None)
        if statement is None:
            return rows
        df = statement.to_dataframe() if hasattr(statement, 'to_dataframe') else None
        if df is None or df.empty:
            return rows
        for _, row in df.iterrows():
            label = row.get('label') or row.get('concept') or str(row.name)
            value = row.get('value')
            period_end = row.get('end_date') or row.get('period')
            if value is not None and period_end is not None:
                try:
                    rows.append((
                        company_key, str(period_end), statement_type,
                        label, str(filing_date), filing_type, float(value)
                    ))
                except (ValueError, TypeError):
                    pass
    except Exception as e:
        print(f"  Error extracting {statement_type}: {e}")
    return rows


def populate_sec_financials():
    identity = os.environ.get("EDGAR_IDENTITY", "StockDataPlatform user@example.com")
    set_identity(identity)

    with open(TICKERS_FILE, 'r') as f:
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
                for filing_type in ['10-K', '10-Q']:
                    filings = company.get_filings(form=filing_type).latest(2)
                    for filing in filings:
                        filing_date = str(filing.filing_date)
                        xbrl = filing.xbrl()
                        if xbrl is None:
                            continue
                        for stmt_type, method in STATEMENT_METHODS.items():
                            rows = _extract_statement(
                                xbrl, method, stmt_type,
                                company_key, filing_date, filing_type
                            )
                            all_rows.extend(rows)
                print(f"  {ticker}: extracted {len([r for r in all_rows if r[0] == company_key])} line items")
            except Exception as e:
                print(f"Error processing {ticker}: {e}")

        if all_rows:
            batch_insert(conn, UPSERT_SEC_FINANCIALS_SQL, all_rows)

    print(f"SEC financials updated: {len(all_rows)} line items across {len(tickers)} tickers")
