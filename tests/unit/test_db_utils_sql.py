"""Tests for scripts/db_utils.py — SQL template constant validation."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))

from db_utils import (
    UPSERT_EARNINGS_SQL,
    UPSERT_FUNDAMENTALS_SQL,
    UPSERT_MACRO_DATA_SQL,
    UPSERT_SEC_FINANCIALS_SQL,
    UPSERT_STOCK_PRICE_SQL,
)


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
        for sql in [
            UPSERT_STOCK_PRICE_SQL,
            UPSERT_FUNDAMENTALS_SQL,
            UPSERT_EARNINGS_SQL,
            UPSERT_SEC_FINANCIALS_SQL,
            UPSERT_MACRO_DATA_SQL,
        ]:
            assert "VALUES %s" in sql
