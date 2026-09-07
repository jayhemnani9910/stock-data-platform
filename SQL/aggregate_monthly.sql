INSERT INTO fact_stock_price_monthly (company_key, month, avg_open, avg_close, avg_high, avg_low, total_volume)
SELECT
    company_key,
    DATE_TRUNC('month', date) AS month,
    AVG(open) AS avg_open,
    AVG(close) AS avg_close,
    AVG(high) AS avg_high,
    AVG(low) AS avg_low,
    SUM(volume) AS total_volume
FROM fact_stock_price_daily
GROUP BY company_key, DATE_TRUNC('month', date)
ON CONFLICT (company_key, month) DO UPDATE
SET avg_open = EXCLUDED.avg_open,
    avg_close = EXCLUDED.avg_close,
    avg_high = EXCLUDED.avg_high,
    avg_low = EXCLUDED.avg_low,
    total_volume = EXCLUDED.total_volume;
