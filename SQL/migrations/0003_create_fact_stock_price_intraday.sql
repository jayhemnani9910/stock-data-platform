-- Intraday bars. fact_stock_price_daily is one row per trading day and stays
-- the authoritative daily series; this sits beside it, not inside it.
--
-- bar_interval is part of the key on purpose. Yahoo serves several intraday
-- grains with very different reach (1h goes back ~730 trading days, 1m only
-- about a week), and a table that mixed them without saying which was which
-- would repeat the fact_sec_financials mistake, where quarterly and
-- year-to-date figures shared a column and nothing could tell them apart.
-- Adding '1m' later needs no migration -- just rows with a different label.
--
-- trade_date carries the session date in US Eastern so the table joins
-- dim_date like every other fact, and the foreign key keeps it honest.

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
    PRIMARY KEY (ts, company_key, bar_interval)
);

-- Ten tickers at seven bars a session is ~18k rows a year per interval, so a
-- month per chunk keeps them small enough to prune well.
SELECT create_hypertable('fact_stock_price_intraday', 'ts',
                         chunk_time_interval => INTERVAL '1 month',
                         migrate_data => TRUE, if_not_exists => TRUE);

-- The common query is one ticker's recent bars at one grain.
CREATE INDEX IF NOT EXISTS fact_stock_price_intraday_company_idx
    ON fact_stock_price_intraday (company_key, bar_interval, ts DESC);
