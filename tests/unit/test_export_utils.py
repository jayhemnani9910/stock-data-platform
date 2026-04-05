"""Tests for scripts/export_dashboard_data.py — _clean() and _serialize()."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))

from export_dashboard_data import _clean, _serialize


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
