-- dim_date ran 1990-2035 while FRED series start in 1947, so 1,618 of 3,081
-- fact_macro_data rows (53%) had no matching dimension row. Nothing caught it
-- because no fact table referenced dim_date at all -- a star schema whose date
-- dimension was joinable only by luck.
--
-- populate_dim_date now generates 1945-2050 (scripts/populate_dim_date.py).
-- Run that DAG before this migration, or the constraints below will be
-- rejected by the rows they are meant to protect.
--
-- Idempotent: each constraint is added only if it is not already present.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fact_stock_price_daily_date_fkey'
    ) THEN
        ALTER TABLE fact_stock_price_daily
            ADD CONSTRAINT fact_stock_price_daily_date_fkey
            FOREIGN KEY (date) REFERENCES dim_date(date);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fact_macro_data_date_fkey'
    ) THEN
        ALTER TABLE fact_macro_data
            ADD CONSTRAINT fact_macro_data_date_fkey
            FOREIGN KEY (date) REFERENCES dim_date(date);
    END IF;
END $$;
