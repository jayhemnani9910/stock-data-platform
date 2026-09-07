"""Tests for scripts/db_utils.py — SQL template constant validation."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))

import db_utils
from db_utils import (
    UPSERT_EARNINGS_SQL,
    UPSERT_FUNDAMENTALS_SQL,
    UPSERT_MACRO_DATA_SQL,
    UPSERT_SEC_FINANCIALS_SQL,
    UPSERT_STOCK_PRICE_SQL,
    UPSERT_STREAMING_PRICE_SQL,
)


def _all_templates():
    """Every UPSERT_*_SQL constant, discovered rather than listed.

    The old hardcoded list silently stopped covering everything the moment a
    sixth template was added, so discover them instead.
    """
    return {n: getattr(db_utils, n) for n in dir(db_utils) if n.startswith("UPSERT_") and n.endswith("_SQL")}


class TestSQLTemplates:
    """Validate SQL templates are well-formed and contain expected clauses."""

    def test_stock_price_sql_has_upsert(self):
        assert "INSERT INTO fact_stock_price_daily" in UPSERT_STOCK_PRICE_SQL
        assert "ON CONFLICT" in UPSERT_STOCK_PRICE_SQL
        assert "DO UPDATE" in UPSERT_STOCK_PRICE_SQL

    def test_stock_price_sql_columns(self):
        for col in ["date", "company_key", "open", "high", "low", "close", "volume"]:
            assert col in UPSERT_STOCK_PRICE_SQL

    def test_fundamentals_sql_has_upsert(self):
        assert "INSERT INTO fact_company_fundamentals" in UPSERT_FUNDAMENTALS_SQL
        assert "ON CONFLICT" in UPSERT_FUNDAMENTALS_SQL

    def test_fundamentals_sql_columns(self):
        for col in ["market_cap", "trailing_pe", "forward_pe", "dividend_yield", "beta"]:
            assert col in UPSERT_FUNDAMENTALS_SQL

    def test_earnings_sql_has_upsert(self):
        assert "INSERT INTO fact_earnings" in UPSERT_EARNINGS_SQL
        assert "ON CONFLICT" in UPSERT_EARNINGS_SQL

    def test_earnings_sql_columns(self):
        for col in ["report_date", "company_key", "eps_estimate", "eps_actual", "surprise_pct"]:
            assert col in UPSERT_EARNINGS_SQL

    def test_sec_financials_sql_has_upsert(self):
        assert "INSERT INTO fact_sec_financials" in UPSERT_SEC_FINANCIALS_SQL
        assert "ON CONFLICT" in UPSERT_SEC_FINANCIALS_SQL

    def test_sec_financials_sql_columns(self):
        for col in ["company_key", "period_end", "statement_type", "line_item", "value"]:
            assert col in UPSERT_SEC_FINANCIALS_SQL

    def test_macro_data_sql_has_upsert(self):
        assert "INSERT INTO fact_macro_data" in UPSERT_MACRO_DATA_SQL
        assert "ON CONFLICT" in UPSERT_MACRO_DATA_SQL

    def test_macro_data_sql_columns(self):
        for col in ["date", "indicator_key", "value"]:
            assert col in UPSERT_MACRO_DATA_SQL

    def test_all_templates_use_values_placeholder(self):
        templates = _all_templates()
        assert len(templates) >= 6, f"expected every UPSERT template to be discovered, got {sorted(templates)}"
        for name, sql in templates.items():
            assert "VALUES %s" in sql, f"{name} is missing the VALUES %s placeholder"

    def test_all_templates_handle_conflicts(self):
        for name, sql in _all_templates().items():
            assert "ON CONFLICT" in sql, f"{name} would raise on a duplicate key instead of upserting"


class TestStreamingPriceTemplate:
    """The kafka-consumer write path must never shrink a real daily bar.

    live_from_kafka sends a one-minute bar labelled with the day's date. Under
    the plain overwrite template that replaced the whole row, so NVDA's real
    volume of 134,946,800 became a single minute's 118,400.
    """

    def test_targets_the_daily_price_table(self):
        assert "INSERT INTO fact_stock_price_daily" in UPSERT_STREAMING_PRICE_SQL

    def test_conflict_target_matches_the_primary_key(self):
        assert "ON CONFLICT (date, company_key)" in UPSERT_STREAMING_PRICE_SQL

    def test_high_only_widens(self):
        assert "high = GREATEST(fact_stock_price_daily.high, EXCLUDED.high)" in UPSERT_STREAMING_PRICE_SQL

    def test_low_only_widens(self):
        assert "low = LEAST(fact_stock_price_daily.low, EXCLUDED.low)" in UPSERT_STREAMING_PRICE_SQL

    def test_volume_never_shrinks(self):
        assert "volume = GREATEST(fact_stock_price_daily.volume, EXCLUDED.volume)" in UPSERT_STREAMING_PRICE_SQL

    def test_open_is_never_overwritten(self):
        assert "open = fact_stock_price_daily.open" in UPSERT_STREAMING_PRICE_SQL

    def test_close_follows_the_latest_tick(self):
        assert "close = EXCLUDED.close" in UPSERT_STREAMING_PRICE_SQL

    def test_differs_from_the_authoritative_etl_template(self):
        """The ETL must keep overwriting; only the streaming path merges."""
        assert UPSERT_STREAMING_PRICE_SQL != UPSERT_STOCK_PRICE_SQL
        assert "GREATEST" not in UPSERT_STOCK_PRICE_SQL
