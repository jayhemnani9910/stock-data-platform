CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS dim_company (
    company_key SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    company_name TEXT NOT NULL,
    sector TEXT,
    industry TEXT,
    exchange TEXT,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE
);

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

CREATE TABLE IF NOT EXISTS fact_sec_financials (
    company_key INT NOT NULL REFERENCES dim_company(company_key),
    period_end DATE NOT NULL,
    statement_type TEXT NOT NULL,
    line_item TEXT NOT NULL,
    filing_date DATE,
    filing_type TEXT,
    value DOUBLE PRECISION,
    PRIMARY KEY (company_key, period_end, statement_type, line_item)
);

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
