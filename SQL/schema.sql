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
