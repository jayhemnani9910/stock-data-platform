"""Tests for Dags/etl_stock_data_dag.py — the pure helpers.

This file used to carry copies of _stage_path_for_run and _normalize_columns
because importing the DAG module pulls in Airflow, which is not in
requirements.txt. Copies drift: replacing _stage_path_for_run's body with a
constant left all four of its tests passing.

Loading the real module instead, with the Airflow names it touches at import
time stubbed out. tests/unit/test_kafka_batch.py uses the same
spec_from_file_location approach for the root-level Kafka scripts.
"""

import importlib.util
import os
import sys
import types
from datetime import date, timedelta

import pandas as pd
import pytest


def _stub_airflow():
    """Minimal stand-ins for the Airflow symbols the DAG module touches on
    import. Only the DAG context manager and the operator constructors run at
    module scope; none of their behaviour is under test here."""

    class _Anything:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __rshift__(self, other):
            return other

    modules = {
        "airflow": types.ModuleType("airflow"),
        "airflow.operators": types.ModuleType("airflow.operators"),
        "airflow.operators.python": types.ModuleType("airflow.operators.python"),
        "airflow.operators.trigger_dagrun": types.ModuleType("airflow.operators.trigger_dagrun"),
    }
    modules["airflow"].DAG = _Anything
    modules["airflow.operators.python"].PythonOperator = _Anything
    modules["airflow.operators.trigger_dagrun"].TriggerDagRunOperator = _Anything
    return modules


def _load_dag_module():
    root = os.path.join(os.path.dirname(__file__), "..", "..")
    path = os.path.abspath(os.path.join(root, "Dags", "etl_stock_data_dag.py"))
    stubs = _stub_airflow()
    saved = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        spec = importlib.util.spec_from_file_location("etl_stock_data_dag_undertest", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for name, previous in saved.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous


pytest.importorskip("yfinance", reason="yfinance not installed")
_etl = _load_dag_module()
_stage_path_for_run = _etl._stage_path_for_run
_normalize_columns = _etl._normalize_columns


class TestStagePathForRun:
    def test_basic_path(self):
        assert _stage_path_for_run("AAPL", "raw", "20260910T000000") == os.path.join(
            _etl._BASE_DIR, "aapl_raw_20260910T000000.json.gz"
        )

    def test_ticker_lowercased(self):
        assert "/aapl_" in _stage_path_for_run("AAPL", "raw", "1")

    def test_stage_in_path(self):
        assert "_cleaned_" in _stage_path_for_run("AAPL", "cleaned", "1")

    def test_ends_with_gz(self):
        assert _stage_path_for_run("AAPL", "raw", "1").endswith(".json.gz")

    def test_uses_the_modules_base_dir(self):
        """The copy hardcoded /tmp/stock_data_platform, so moving _BASE_DIR
        would not have failed a single test."""
        assert _stage_path_for_run("AAPL", "raw", "1").startswith(_etl._BASE_DIR + os.sep)

    def test_runs_do_not_collide(self):
        a = _stage_path_for_run("AAPL", "raw", "run1")
        b = _stage_path_for_run("AAPL", "raw", "run2")
        assert a != b


STANDARD = ["open", "high", "low", "close", "volume"]


class TestNormalizeColumns:
    def test_standard_columns(self):
        df = pd.DataFrame({c: [1] for c in ["Open", "High", "Low", "Close", "Volume"]})
        assert list(_normalize_columns(df, "AAPL").columns) == STANDARD

    def test_suffixed_columns(self):
        df = pd.DataFrame({f"{c}_AAPL": [1] for c in ["Open", "High", "Low", "Close", "Volume"]})
        assert list(_normalize_columns(df, "AAPL").columns) == STANDARD

    def test_multiindex_is_flattened(self):
        df = pd.DataFrame(
            [[1, 2, 3, 4, 5]],
            columns=pd.MultiIndex.from_tuples([(c, "AAPL") for c in ["Open", "High", "Low", "Close", "Volume"]]),
        )
        assert list(_normalize_columns(df, "AAPL").columns) == STANDARD

    def test_adj_close_dropped_and_the_rest_survive(self):
        """The old assertion was `"adj close" not in result.columns`, which no
        input could ever violate — the rename only ever emits the five standard
        names. Assert what actually matters: Adj Close goes, the five stay."""
        df = pd.DataFrame({c: [1] for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]})
        result = _normalize_columns(df, "AAPL")
        assert list(result.columns) == STANDARD
        assert "Adj Close" not in result.columns

    def test_adj_close_suffixed_dropped_and_the_rest_survive(self):
        cols = [f"{c}_AAPL" for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]]
        df = pd.DataFrame({c: [1] for c in cols})
        result = _normalize_columns(df, "AAPL")
        assert list(result.columns) == STANDARD
        assert "Adj Close_AAPL" not in result.columns

    def test_values_are_preserved(self):
        df = pd.DataFrame({"Open": [1.5], "High": [2.5], "Low": [0.5], "Close": [2.0], "Volume": [99]})
        result = _normalize_columns(df, "AAPL")
        assert result["close"].iloc[0] == 2.0
        assert result["volume"].iloc[0] == 99

    def test_does_not_modify_original(self):
        df = pd.DataFrame({c: [1] for c in ["Open", "High", "Low", "Close", "Volume"]})
        _normalize_columns(df, "AAPL")
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]


# NVDA as stored and as Yahoo served it after going ex-dividend on 2026-09-10:
# every close before the ex-date re-adjusted by 1/1.001119, and the two newest
# days already refreshed by the previous incremental run.
_NVDA_STORED = {
    date(2026, 9, 1): 217.440002,
    date(2026, 9, 2): 224.410004,
    date(2026, 9, 3): 228.449997,
    date(2026, 9, 4): 230.360001,
    date(2026, 9, 8): 225.729996,
    date(2026, 9, 9): 223.419998,
    date(2026, 9, 10): 218.360001,
}
_NVDA_FRESH = pd.Series(
    [217.196976, 224.159180, 228.194656, 230.102524, 225.477692, 223.419998, 218.360001],
    index=pd.to_datetime(list(_NVDA_STORED)),
)


def _series(stored, scale=1.0, overrides=None):
    s = pd.Series({pd.Timestamp(d): v * scale for d, v in stored.items()})
    for d, v in (overrides or {}).items():
        s[pd.Timestamp(d)] = v
    return s


class TestAdjustmentChanged:
    """Compare the re-read overlap to what is stored, instead of trusting a
    corporate-actions list checked against a date the Kafka consumer may
    already have written."""

    def test_the_nvda_regression(self):
        """Ex-dividend on the newest loaded date: the old check asked whether
        an action fell *after* 2026-09-10, got no, and left 6,949 closes 0.11%
        high. The overlap sees the shift."""
        assert _etl._adjustment_changed(_NVDA_STORED, _NVDA_FRESH) is True

    def test_nothing_changed(self):
        assert _etl._adjustment_changed(_NVDA_STORED, _series(_NVDA_STORED)) is False

    def test_floating_point_noise_is_not_a_change(self):
        """Measured noise between stored and fresh closes is ~1e-13."""
        assert _etl._adjustment_changed(_NVDA_STORED, _series(_NVDA_STORED, scale=1 + 1e-12)) is False

    def test_the_smallest_real_dividend_is_caught(self):
        """$0.01 on a $220 stock rescales history by ~4.5e-5."""
        assert _etl._adjustment_changed(_NVDA_STORED, _series(_NVDA_STORED, scale=1 / (1 + 4.5e-5))) is True

    def test_a_split_is_caught(self):
        assert _etl._adjustment_changed(_NVDA_STORED, _series(_NVDA_STORED, scale=0.1)) is True

    def test_one_late_close_correction_is_not_an_adjustment(self):
        """DIS 2026-09-10 was stored at 104.99 and finalised at 105.82. The
        overlap upsert fixes one bad day; it must not trigger a 60-year
        refetch."""
        fresh = _series(_NVDA_STORED, overrides={date(2026, 9, 8): 230.0})
        assert _etl._adjustment_changed(_NVDA_STORED, fresh) is False

    def test_the_newest_stored_day_is_ignored(self):
        """It may be a streaming row or a close taken before finalisation.
        With a long overlap the median would absorb it anyway; the exclusion
        matters when the overlap is short -- two stored days, where a bad
        newest row is half the evidence."""
        stored = {date(2026, 9, 9): 223.42, date(2026, 9, 10): 218.36}
        fresh = pd.Series([223.42, 300.0], index=pd.to_datetime([date(2026, 9, 9), date(2026, 9, 10)]))
        assert _etl._adjustment_changed(stored, fresh) is False

    def test_no_overlap(self):
        assert _etl._adjustment_changed(_NVDA_STORED, pd.Series(dtype=float)) is False
        assert _etl._adjustment_changed({}, _NVDA_FRESH) is False

    def test_tolerance_sits_between_noise_and_the_smallest_dividend(self):
        assert 1e-12 < _etl.ADJUSTMENT_TOLERANCE < 4.5e-5


class _Spy:
    """Captures every way extract_data asked yfinance for data."""

    def __init__(self, fresh_close=1.5):
        self.calls = []
        self.fresh_close = fresh_close

    def __call__(self, ticker, **kwargs):
        self.calls.append(kwargs)
        idx = pd.to_datetime(["2026-09-08", "2026-09-09"])
        c = self.fresh_close
        return pd.DataFrame(
            {"Open": [c, c], "High": [c * 2, c * 2], "Low": [c / 2, c / 2], "Close": [c, c], "Volume": [10, 10]},
            index=idx,
        )


class _Ti:
    def __init__(self):
        self.pushed = {}

    def xcom_push(self, key, value):
        self.pushed[key] = value


class TestExtractWindow:
    """A first load reaches each ticker's first traded day; a routine run
    re-reads OVERLAP_DAYS; an adjustment change escalates to the full history."""

    def _run(self, monkeypatch, last_date, stored=None, fresh_close=1.5):
        spy = _Spy(fresh_close)
        monkeypatch.setattr(_etl.yf, "download", spy)
        monkeypatch.setattr(_etl, "_get_last_loaded_date", lambda t: last_date)
        monkeypatch.setattr(_etl, "_get_stored_closes", lambda t, since: stored or {})
        monkeypatch.setattr(_etl, "_prune_stale_stage_files", lambda: None)
        _etl.extract_data("AAPL", _Ti(), "20260910T000000")
        return spy.calls

    def test_first_load_asks_for_the_whole_history(self, monkeypatch):
        (call,) = self._run(monkeypatch, last_date=None)
        assert call.get("period") == "max"
        assert "start" not in call, "a start date would re-impose a window"

    def test_a_routine_run_rereads_the_overlap_only(self, monkeypatch):
        stored = {date(2026, 9, 8): 1.5, date(2026, 9, 9): 1.5}
        (call,) = self._run(monkeypatch, last_date=date(2026, 9, 9), stored=stored)
        assert "period" not in call
        assert call["start"] == date(2026, 9, 9) - timedelta(days=_etl.OVERLAP_DAYS)

    def test_the_overlap_is_longer_than_one_day(self):
        """One day of overlap could not see an adjustment published late, and
        left a late close correction for the next night."""
        assert _etl.OVERLAP_DAYS >= 5

    def test_a_changed_basis_escalates_to_the_full_history(self, monkeypatch):
        stored = {date(2026, 9, 8): 1.5, date(2026, 9, 9): 1.5}
        calls = self._run(monkeypatch, last_date=date(2026, 9, 9), stored=stored, fresh_close=1.5 / 1.001119)
        assert len(calls) == 2, "incremental read, then the full refetch"
        assert "start" in calls[0] and calls[1].get("period") == "max"


class TestReadStaged:
    """to_json(orient="split") writes a DatetimeIndex as epoch milliseconds and
    read_json declines to convert negative ones, so every pre-1970 date came
    back as int64 and the load died on row.Index.date(). The 25-year extract
    window hid it; DIS trades back to 1962."""

    def _stage(self, tmp_path, dates):
        import gzip

        df = pd.DataFrame(
            {
                "open": [1.0] * len(dates),
                "high": [2.0] * len(dates),
                "low": [0.5] * len(dates),
                "close": [1.5] * len(dates),
                "volume": [10] * len(dates),
            },
            index=pd.to_datetime(dates),
        )
        path = tmp_path / "staged.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as f:
            f.write(df.to_json(orient="split"))
        return str(path)

    def test_modern_dates_round_trip(self, tmp_path):
        df = _etl._read_staged(self._stage(tmp_path, ["2026-09-08", "2026-09-09"]))
        assert isinstance(df.index, pd.DatetimeIndex)
        assert df.index[0].date() == date(2026, 9, 8)

    def test_pre_1970_dates_round_trip(self, tmp_path):
        """The regression. DIS's first bar is 1962-01-02."""
        df = _etl._read_staged(self._stage(tmp_path, ["1962-01-02", "1962-01-03"]))
        assert isinstance(df.index, pd.DatetimeIndex)
        assert df.index[0].date() == date(1962, 1, 2)

    def test_dates_straddling_the_epoch_round_trip(self, tmp_path):
        df = _etl._read_staged(self._stage(tmp_path, ["1969-12-31", "1970-01-02"]))
        assert [d.date() for d in df.index] == [date(1969, 12, 31), date(1970, 1, 2)]

    def test_the_index_supports_date_which_is_what_load_calls(self, tmp_path):
        df = _etl._read_staged(self._stage(tmp_path, ["1962-01-02"]))
        for row in df.itertuples():
            assert row.Index.date() == date(1962, 1, 2)

    def test_values_survive(self, tmp_path):
        df = _etl._read_staged(self._stage(tmp_path, ["1962-01-02"]))
        assert df["close"].iloc[0] == 1.5 and df["volume"].iloc[0] == 10
