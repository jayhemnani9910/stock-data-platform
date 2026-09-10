-- fact_sec_financials was keyed on period_end alone. A 10-Q reports the same
-- line item over several windows that share an end date -- Apple's Q3 FY2026
-- filing carries both duration_2026-03-29_2026-06-27 (the quarter, 109,417M)
-- and duration_2025-09-28_2026-06-27 (nine months, 364,357M) for "Net sales".
-- Both collapsed onto period_end 2026-06-27 and the loader's dedup kept the
-- last one, so the warehouse stored year-to-date figures as if they were
-- quarterly and dropped half of every income statement.
--
-- period_start goes into the key, and period_type says what the window is.
-- The discriminator was never stored, so existing rows cannot be repaired --
-- the table is recreated and repopulated from EDGAR.
--
-- filing_date/filing_type used to be stamped from the filing being read onto
-- every period in it, which dated Apple's FY2023 figures to 2025-10-31. They
-- are now NULL unless the period is the filing's own reporting period;
-- source_filing_* always records where the row was actually read from.

-- Idempotent, and deliberately not a bare DROP: `make migrate` reapplies every
-- file, and an unguarded DROP would wipe the table on each run. This fires only
-- while the old shape is still in place -- once period_start exists, it is a
-- no-op.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'fact_sec_financials')
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_schema = 'public'
                         AND table_name = 'fact_sec_financials'
                         AND column_name = 'period_start')
    THEN
        DROP TABLE fact_sec_financials;
        RAISE NOTICE 'dropped fact_sec_financials: old period_end-only key, rows are unrecoverable';
    END IF;
END $$;

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

-- The common query is "this company's quarterly revenue over time".
CREATE INDEX IF NOT EXISTS fact_sec_financials_period_idx
    ON fact_sec_financials (company_key, period_type, period_end DESC);
