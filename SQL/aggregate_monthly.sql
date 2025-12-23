INSERT INTO fact_stock_price_monthly (company_key, month, avg_open, avg_close, avg_high, avg_low, total_volume)
SELECT
    company_key,
    DATE_TRUNC('month', date) AS month,
    AVG(open),
    AVG(close),
    AVG(high),
    AVG(low),
    SUM(volume)
FROM fact_stock_price_daily
GROUP BY company_key, DATE_TRUNC('month', date);
