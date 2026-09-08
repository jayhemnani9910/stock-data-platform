"""Tests for scripts/populate_macro_data.py — _series_to_rows()."""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))

from populate_macro_data import _series_to_rows


def _series(pairs):
    dates, values = zip(*pairs)
    return pd.Series(list(values), index=pd.to_datetime(list(dates)))


class TestSeriesToRows:
    def test_converts_dates_and_values(self):
        rows = _series_to_rows(_series([("2020-01-01", 1.5), ("2020-02-01", 2.5)]), 7)
        assert rows == [
            (pd.Timestamp("2020-01-01").date(), 7, 1.5),
            (pd.Timestamp("2020-02-01").date(), 7, 2.5),
        ]

    def test_drops_nan_observations(self):
        rows = _series_to_rows(_series([("2020-01-01", 1.5), ("2020-02-01", np.nan), ("2020-03-01", 2.5)]), 3)
        assert [r[0].isoformat() for r in rows] == ["2020-01-01", "2020-03-01"]

    def test_no_nan_survives(self):
        """The old guard compared str(value) to 'NaN'; numpy renders it 'nan'."""
        rows = _series_to_rows(_series([("1946-01-01", np.nan), ("1947-01-01", 243.1)]), 4)
        assert all(not pd.isna(value) for _, _, value in rows)

    def test_drops_none(self):
        rows = _series_to_rows(_series([("2020-01-01", None), ("2020-02-01", 4.0)]), 1)
        assert len(rows) == 1

    def test_all_nan_yields_no_rows(self):
        rows = _series_to_rows(_series([("2020-01-01", np.nan), ("2020-02-01", np.nan)]), 1)
        assert rows == []

    def test_values_are_floats(self):
        rows = _series_to_rows(_series([("2020-01-01", np.float64(5.25))]), 2)
        assert isinstance(rows[0][2], float)

    def test_empty_series(self):
        assert _series_to_rows(pd.Series(dtype="float64"), 1) == []
