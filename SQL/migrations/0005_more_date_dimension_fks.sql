-- 0002 tied fact_stock_price_daily and fact_macro_data to dim_date, and the
-- intraday table was created with its FK. These three date-keyed facts were
-- missed, so a date outside dim_date's range would have joined to nothing.
--
-- Idempotent: each constraint is added only if it is not already present. The
-- names match what an inline REFERENCES in SQL/schema.sql generates.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fact_stock_price_monthly_month_fkey'
    ) THEN
        ALTER TABLE fact_stock_price_monthly
            ADD CONSTRAINT fact_stock_price_monthly_month_fkey
            FOREIGN KEY (month) REFERENCES dim_date(date);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fact_company_fundamentals_date_fkey'
    ) THEN
        ALTER TABLE fact_company_fundamentals
            ADD CONSTRAINT fact_company_fundamentals_date_fkey
            FOREIGN KEY (date) REFERENCES dim_date(date);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fact_earnings_report_date_fkey'
    ) THEN
        ALTER TABLE fact_earnings
            ADD CONSTRAINT fact_earnings_report_date_fkey
            FOREIGN KEY (report_date) REFERENCES dim_date(date);
    END IF;
END $$;
