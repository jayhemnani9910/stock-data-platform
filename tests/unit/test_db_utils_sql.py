"""Tests for scripts/db_utils.py — SQL template constant validation."""

import db_utils
from db_utils import (
    UPSERT_COMPANY_SQL,
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
        for col in ["company_key", "statement_type", "line_item", "period_start", "period_end", "value"]:
            assert col in UPSERT_SEC_FINANCIALS_SQL

    def test_macro_data_sql_has_upsert(self):
        assert "INSERT INTO fact_macro_data" in UPSERT_MACRO_DATA_SQL
        assert "ON CONFLICT" in UPSERT_MACRO_DATA_SQL

    def test_macro_data_sql_columns(self):
        for col in ["date", "indicator_key", "value"]:
            assert col in UPSERT_MACRO_DATA_SQL

    def test_all_templates_use_values_placeholder(self):
        templates = _all_templates()
        assert len(templates) >= 7, f"expected every UPSERT template to be discovered, got {sorted(templates)}"
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


class TestSecFinancialsTemplate:
    """A filing reports one line item over several windows sharing an end date.
    Keying on period_end alone let a 10-Q's nine-month figure overwrite its own
    quarter — Apple's Q3 FY2026 Net sales was stored as 364,357M (nine months)
    instead of 109,417M (the quarter)."""

    def test_period_start_is_in_the_conflict_target(self):
        assert (
            "ON CONFLICT (company_key, statement_type, line_item, period_start, period_end)"
            in UPSERT_SEC_FINANCIALS_SQL
        )

    def test_period_type_is_stored(self):
        """Without it, nothing downstream can tell a quarter from a year."""
        assert "period_type" in UPSERT_SEC_FINANCIALS_SQL

    def test_source_filing_is_recorded_separately(self):
        """filing_date describes the period; source_filing_date describes where
        the row was read. Conflating them dated Apple's FY2023 income to 2025."""
        assert "source_filing_date" in UPSERT_SEC_FINANCIALS_SQL
        assert "source_filing_type" in UPSERT_SEC_FINANCIALS_SQL

    def test_updates_value_on_conflict(self):
        assert "value = EXCLUDED.value" in UPSERT_SEC_FINANCIALS_SQL


class TestCompanyTemplate:
    """This template lived in populate_dim_company.py — the only UPSERT_*_SQL
    outside db_utils — so _all_templates() never saw it, despite being the one
    with a partial-index conflict target that has to match SQL/schema.sql."""

    def test_is_discovered(self):
        assert "UPSERT_COMPANY_SQL" in _all_templates()

    def test_conflict_target_matches_the_partial_index(self):
        # SQL/schema.sql: CREATE UNIQUE INDEX ... ON dim_company (ticker) WHERE is_current
        assert "ON CONFLICT (ticker) WHERE is_current" in UPSERT_COMPANY_SQL

    def test_updates_metadata_but_never_the_ticker(self):
        for col in ["company_name", "sector", "industry", "exchange"]:
            assert f"{col} = EXCLUDED.{col}" in UPSERT_COMPANY_SQL
        assert "ticker = EXCLUDED.ticker" not in UPSERT_COMPANY_SQL

    def test_does_not_touch_company_key(self):
        """Minting a new key would strand the ticker's existing facts on the
        retired one, since every loader resolves through get_company_key."""
        assert "company_key" not in UPSERT_COMPANY_SQL
