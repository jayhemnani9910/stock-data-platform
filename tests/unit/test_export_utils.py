"""Tests for scripts/export_dashboard_data.py — _clean() and _serialize()."""

from datetime import UTC, date, datetime

from export_dashboard_data import (
    _clean,
    _serialize,
    build_meta,
    export_earnings,
    export_fundamentals,
    export_price_summary,
)


class TestClean:
    def test_none_stays_none(self):
        assert _clean(None) is None

    def test_nan_becomes_none(self):
        assert _clean(float("nan")) is None

    def test_inf_becomes_none(self):
        assert _clean(float("inf")) is None

    def test_neg_inf_becomes_none(self):
        assert _clean(float("-inf")) is None

    def test_normal_float_unchanged(self):
        assert _clean(3.14) == 3.14

    def test_zero_unchanged(self):
        assert _clean(0.0) == 0.0

    def test_integer_unchanged(self):
        assert _clean(42) == 42

    def test_string_unchanged(self):
        assert _clean("hello") == "hello"


class TestSerialize:
    def test_single_row(self):
        rows = [(1, "AAPL", 150.0)]
        columns = ["id", "ticker", "price"]
        result = _serialize(rows, columns)
        assert result == [{"id": 1, "ticker": "AAPL", "price": 150.0}]

    def test_multiple_rows(self):
        rows = [(1, "AAPL"), (2, "GOOG")]
        columns = ["id", "ticker"]
        result = _serialize(rows, columns)
        assert len(result) == 2
        assert result[0]["ticker"] == "AAPL"
        assert result[1]["ticker"] == "GOOG"

    def test_nan_in_row_becomes_none(self):
        rows = [(1, float("nan"))]
        columns = ["id", "value"]
        result = _serialize(rows, columns)
        assert result[0]["value"] is None

    def test_empty_rows(self):
        result = _serialize([], ["a", "b"])
        assert result == []


class _FakeCursor:
    """Records the SQL it is handed and returns canned rows."""

    def __init__(self, rows):
        self._rows = rows
        self.sql = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self.cursor_obj = _FakeCursor(rows)

    def cursor(self):
        return self.cursor_obj


def _normalise(sql):
    return " ".join(sql.split())


class TestCurrentCompanyFilter:
    """dim_company is SCD Type 2 and a retired row keeps its facts. Joining
    without is_current double-counts every price point and earnings row for any
    ticker whose sector or industry has ever been corrected. export_fundamentals
    filtered; the other two did not."""

    def test_price_summary_filters_current_companies(self):
        conn = _FakeConn([])
        export_price_summary(conn)
        assert "d.is_current" in _normalise(conn.cursor_obj.sql)

    def test_earnings_filters_current_companies(self):
        conn = _FakeConn([])
        export_earnings(conn)
        assert "d.is_current" in _normalise(conn.cursor_obj.sql)

    def test_fundamentals_filters_current_companies(self):
        conn = _FakeConn([])
        export_fundamentals(conn)
        assert "d.is_current" in _normalise(conn.cursor_obj.sql)


class TestEarningsWindow:
    def test_excludes_scheduled_future_reports(self):
        """yfinance publishes upcoming reports with a NULL eps_actual. The
        dashboard shows each ticker's latest report_date, so one of those would
        become its 'Latest Quarter' and render blank EPS."""
        conn = _FakeConn([])
        export_earnings(conn)
        assert "f.report_date <= CURRENT_DATE" in _normalise(conn.cursor_obj.sql)


class TestFundamentalsSnapshotDate:
    def test_date_is_exported(self):
        """Without it the payload carries no evidence of how stale it is."""
        conn = _FakeConn([("AAPL", date(2026, 9, 10), 1, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0)])
        (row,) = export_fundamentals(conn)
        assert row["date"] == "2026-09-10"
        assert row["ticker"] == "AAPL"


class TestBuildMeta:
    """The dashboard used to stamp new Date() on itself, so it read 'today'
    over data that was days old."""

    def test_data_through_is_the_newest_row(self):
        meta = build_meta(
            {"price_summary.json": [{"date": "2026-09-04"}, {"date": "2026-09-09"}]},
            generated_at=datetime(2026, 9, 10, 12, 0, tzinfo=UTC),
        )
        assert meta["datasets"]["price_summary.json"]["data_through"] == "2026-09-09"

    def test_records_are_counted(self):
        meta = build_meta({"macro.json": [{"date": "2026-01-01"}] * 3})
        assert meta["datasets"]["macro.json"]["records"] == 3

    def test_earnings_uses_report_date(self):
        meta = build_meta({"earnings.json": [{"report_date": "2026-07-30"}]})
        assert meta["datasets"]["earnings.json"]["data_through"] == "2026-07-30"

    def test_generated_at_is_recorded(self):
        meta = build_meta({}, generated_at=datetime(2026, 9, 10, 12, 0, tzinfo=UTC))
        assert meta["generated_at"].startswith("2026-09-10T12:00:00")

    def test_empty_dataset_has_no_data_through(self):
        meta = build_meta({"macro.json": []})
        assert meta["datasets"]["macro.json"]["data_through"] is None
