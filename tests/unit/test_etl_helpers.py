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
from datetime import date

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


class _FakeTicker:
    def __init__(self, actions):
        self.actions = actions


def _actions(dates):
    if dates is None:
        return None
    idx = pd.to_datetime(dates).tz_localize("America/New_York")
    return pd.DataFrame({"Dividends": [0.26] * len(idx), "Stock Splits": [0.0] * len(idx)}, index=idx)


class TestHasCorporateActionSince:
    """yf.download returns prices adjusted as of the moment of the call, and an
    incremental load only refetches two days. Every dividend therefore left a
    step in the stored series: AAPL measured 1.001785x too high for every row
    before 2026-03-12 and exactly right after it."""

    def _patch(self, monkeypatch, actions):
        monkeypatch.setattr(_etl.yf, "Ticker", lambda t: _FakeTicker(actions))

    def test_dividend_after_last_load_triggers_a_refetch(self, monkeypatch):
        self._patch(monkeypatch, _actions(["2026-08-10"]))
        assert _etl._has_corporate_action_since("AAPL", date(2026, 3, 12)) is True

    def test_action_before_last_load_does_not(self, monkeypatch):
        self._patch(monkeypatch, _actions(["2026-02-09"]))
        assert _etl._has_corporate_action_since("AAPL", date(2026, 3, 12)) is False

    def test_action_on_the_boundary_does_not(self, monkeypatch):
        """The boundary date is already loaded, so its adjustment is applied."""
        self._patch(monkeypatch, _actions(["2026-03-12"]))
        assert _etl._has_corporate_action_since("AAPL", date(2026, 3, 12)) is False

    def test_only_the_newest_action_needs_to_be_recent(self, monkeypatch):
        self._patch(monkeypatch, _actions(["2020-01-01", "2026-08-10"]))
        assert _etl._has_corporate_action_since("AAPL", date(2026, 3, 12)) is True

    def test_no_actions(self, monkeypatch):
        self._patch(monkeypatch, _actions([]))
        assert _etl._has_corporate_action_since("AAPL", date(2026, 3, 12)) is False

    def test_none_actions(self, monkeypatch):
        self._patch(monkeypatch, None)
        assert _etl._has_corporate_action_since("AAPL", date(2026, 3, 12)) is False

    def test_a_lookup_failure_stays_incremental(self, monkeypatch):
        """Refetching the whole history on every transient yfinance error would be worse
        than the drift it prevents."""

        def boom(_):
            raise RuntimeError("yfinance is down")

        monkeypatch.setattr(_etl.yf, "Ticker", boom)
        assert _etl._has_corporate_action_since("AAPL", date(2026, 3, 12)) is False


class _Spy:
    """Captures how extract_data asked yfinance for data."""

    def __init__(self):
        self.kwargs = None

    def __call__(self, ticker, **kwargs):
        self.kwargs = kwargs
        idx = pd.to_datetime(["2026-09-08", "2026-09-09"])
        return pd.DataFrame(
            {"Open": [1.0, 1.0], "High": [2.0, 2.0], "Low": [0.5, 0.5], "Close": [1.5, 1.5], "Volume": [10, 10]},
            index=idx,
        )


class _Ti:
    def __init__(self):
        self.pushed = {}

    def xcom_push(self, key, value):
        self.pushed[key] = value


class TestExtractWindow:
    """A fixed 25-year window truncated every listing older than it at the same
    arbitrary date -- DIS lost 9,995 bars back to 1962, 26,338 across the ten
    tickers. A first load must reach each ticker's own first traded day."""

    def _run(self, monkeypatch, last_date, action=False):
        spy = _Spy()
        monkeypatch.setattr(_etl.yf, "download", spy)
        monkeypatch.setattr(_etl, "_get_last_loaded_date", lambda t: last_date)
        monkeypatch.setattr(_etl, "_has_corporate_action_since", lambda t, d: action)
        monkeypatch.setattr(_etl, "_prune_stale_stage_files", lambda: None)
        _etl.extract_data("AAPL", _Ti(), "20260910T000000")
        return spy.kwargs

    def test_first_load_asks_for_the_whole_history(self, monkeypatch):
        kwargs = self._run(monkeypatch, last_date=None)
        assert kwargs.get("period") == "max"
        assert "start" not in kwargs, "a start date would re-impose a window"

    def test_a_corporate_action_also_refetches_everything(self, monkeypatch):
        kwargs = self._run(monkeypatch, last_date=date(2026, 3, 12), action=True)
        assert kwargs.get("period") == "max"

    def test_a_routine_run_stays_incremental(self, monkeypatch):
        kwargs = self._run(monkeypatch, last_date=date(2026, 9, 9), action=False)
        assert "period" not in kwargs, "an incremental run must not refetch everything"
        assert kwargs["start"] == date(2026, 9, 8), "one day of overlap"


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
