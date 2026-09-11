-- Record where every intraday bar came from.
--
-- Hourly bars now come from Alpaca, which reaches back to 2016, instead of
-- Yahoo, which stops ~730 trading days back. The two providers do not cut an
-- hour the same way -- Alpaca's native hours start on the clock and include
-- pre- and post-market trading, Yahoo's start at :30 and cover the regular
-- session only -- so the loader rebuilds :30-anchored regular-session hours
-- from Alpaca's 30-minute bars. The results agree to the cent on closes, but
-- they are still different measurements, and a bar that does not say which
-- provider produced it is the same trap as an SEC row that does not say
-- which reporting window it covers.
--
-- source is provenance, not part of the key: one load writes one provider
-- over the whole window, so two providers never hold the same hour.
--
-- Idempotent: existing rows predate Alpaca and came from Yahoo.

ALTER TABLE fact_stock_price_intraday ADD COLUMN IF NOT EXISTS source TEXT;
UPDATE fact_stock_price_intraday SET source = 'yahoo' WHERE source IS NULL;
ALTER TABLE fact_stock_price_intraday ALTER COLUMN source SET NOT NULL;
