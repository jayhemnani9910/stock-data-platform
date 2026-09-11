CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS dim_company (
    company_key SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    company_name TEXT NOT NULL,
    sector TEXT,
    industry TEXT,
    exchange TEXT,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE
);

-- SCD Type 2: many rows may share a ticker, but only one may be current.
CREATE UNIQUE INDEX IF NOT EXISTS dim_company_ticker_current_idx
    ON dim_company (ticker) WHERE is_current;

CREATE TABLE IF NOT EXISTS dim_date (
    date DATE PRIMARY KEY,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_stock_price_daily (
    date DATE NOT NULL,
    company_key INT NOT NULL REFERENCES dim_company(company_key),
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    PRIMARY KEY (date, company_key)
);

CREATE TABLE IF NOT EXISTS fact_stock_price_monthly (
    company_key INT NOT NULL REFERENCES dim_company(company_key),
    month DATE NOT NULL,
    avg_open DOUBLE PRECISION NOT NULL,
    avg_close DOUBLE PRECISION NOT NULL,
    avg_high DOUBLE PRECISION NOT NULL,
    avg_low DOUBLE PRECISION NOT NULL,
    total_volume BIGINT NOT NULL,
    PRIMARY KEY (company_key, month)
);

CREATE TABLE IF NOT EXISTS fact_company_fundamentals (
    date DATE NOT NULL,
    company_key INT NOT NULL REFERENCES dim_company(company_key),
    market_cap BIGINT,
    trailing_pe DOUBLE PRECISION,
    forward_pe DOUBLE PRECISION,
    price_to_book DOUBLE PRECISION,
    dividend_rate DOUBLE PRECISION,
    dividend_yield DOUBLE PRECISION,
    beta DOUBLE PRECISION,
    week_52_high DOUBLE PRECISION,
    week_52_low DOUBLE PRECISION,
    employees INT,
    business_summary TEXT,
    PRIMARY KEY (date, company_key)
);

CREATE TABLE IF NOT EXISTS fact_earnings (
    report_date DATE NOT NULL,
    company_key INT NOT NULL REFERENCES dim_company(company_key),
    eps_estimate DOUBLE PRECISION,
    eps_actual DOUBLE PRECISION,
    surprise_pct DOUBLE PRECISION,
    PRIMARY KEY (report_date, company_key)
);

-- A filing reports the same line item over several windows that share an end
-- date: a 10-Q carries both the quarter and the year to date. period_start is
-- therefore part of the key, and period_type says which window a row is, so
-- quarterly and cumulative figures are never mistaken for each other.
-- period_start equals period_end for instant facts (the balance sheet).
--
-- filing_date/filing_type describe the period only when it is the filing's own
-- reporting period; for the comparative columns they are NULL. source_filing_*
-- always records the filing the row was read from.
CREATE TABLE IF NOT EXISTS fact_sec_financials (
    company_key INT NOT NULL REFERENCES dim_company(company_key),
    statement_type TEXT NOT NULL,
    line_item TEXT NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    period_type TEXT NOT NULL,
    fiscal_year INT,
    fiscal_period TEXT,
    filing_date DATE,
    filing_type TEXT,
    source_filing_date DATE NOT NULL,
    source_filing_type TEXT NOT NULL,
    value DOUBLE PRECISION,
    PRIMARY KEY (company_key, statement_type, line_item, period_start, period_end)
);

CREATE INDEX IF NOT EXISTS fact_sec_financials_period_idx
    ON fact_sec_financials (company_key, period_type, period_end DESC);

CREATE TABLE IF NOT EXISTS dim_macro_indicator (
    indicator_key SERIAL PRIMARY KEY,
    series_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    frequency TEXT,
    units TEXT
);

CREATE TABLE IF NOT EXISTS fact_macro_data (
    date DATE NOT NULL,
    indicator_key INT NOT NULL REFERENCES dim_macro_indicator(indicator_key),
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (date, indicator_key)
);

-- Intraday bars, beside the daily series rather than inside it.
-- bar_interval is in the key because Yahoo serves several grains with very
-- different reach (1h back ~730 trading days, 1m about a week), and a table
-- that mixed them without saying which was which would repeat the
-- fact_sec_financials mistake. trade_date is the US Eastern session date, so
-- this joins dim_date like every other fact.
CREATE TABLE IF NOT EXISTS fact_stock_price_intraday (
    ts TIMESTAMPTZ NOT NULL,
    company_key INT NOT NULL REFERENCES dim_company(company_key),
    bar_interval TEXT NOT NULL,
    trade_date DATE NOT NULL REFERENCES dim_date(date),
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    -- Which provider produced the bar ('alpaca', 'yahoo'). Provenance, not
    -- key: one load writes one provider over the whole window.
    source TEXT NOT NULL,
    PRIMARY KEY (ts, company_key, bar_interval)
);

CREATE INDEX IF NOT EXISTS fact_stock_price_intraday_company_idx
    ON fact_stock_price_intraday (company_key, bar_interval, ts DESC);

-- Time-series optimisation. Both tables are keyed on date and grow forever, so
-- they become hypertables and get chunking and partition pruning. Every other
-- fact table is small and bounded, so plain tables are the right shape for them.
-- migrate_data handles an existing non-empty table; if_not_exists makes reruns safe.
SELECT create_hypertable('fact_stock_price_daily', 'date',
                         chunk_time_interval => INTERVAL '1 year',
                         migrate_data => TRUE, if_not_exists => TRUE);
SELECT create_hypertable('fact_macro_data', 'date',
                         chunk_time_interval => INTERVAL '5 years',
                         migrate_data => TRUE, if_not_exists => TRUE);
SELECT create_hypertable('fact_stock_price_intraday', 'ts',
                         chunk_time_interval => INTERVAL '1 month',
                         migrate_data => TRUE, if_not_exists => TRUE);

-- Every fact keyed on a calendar date references the date dimension. Without
-- these, fact_macro_data silently accumulated 1,618 rows (53% of the table)
-- whose dates predated dim_date's range and joined to nothing.
ALTER TABLE fact_stock_price_daily
    DROP CONSTRAINT IF EXISTS fact_stock_price_daily_date_fkey;
ALTER TABLE fact_stock_price_daily
    ADD CONSTRAINT fact_stock_price_daily_date_fkey
    FOREIGN KEY (date) REFERENCES dim_date(date);

ALTER TABLE fact_macro_data
    DROP CONSTRAINT IF EXISTS fact_macro_data_date_fkey;
ALTER TABLE fact_macro_data
    ADD CONSTRAINT fact_macro_data_date_fkey
    FOREIGN KEY (date) REFERENCES dim_date(date);
