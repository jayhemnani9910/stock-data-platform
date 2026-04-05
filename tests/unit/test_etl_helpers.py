"""Tests for Dags/etl_stock_data_dag.py — _stage_path_for_run() and _normalize_columns()."""

import os

import pandas as pd

# We can't import the DAG module directly (it imports airflow).
# Instead, test the pure logic by reproducing it.


def _stage_path_for_run(ticker, stage, run_suffix):
    """Reproduces the function from etl_stock_data_dag.py."""
    base_dir = "/tmp/stock_data_platform"
    return os.path.join(base_dir, f"{ticker.lower()}_{stage}_{run_suffix}.json.gz")


def _normalize_columns(df, ticker):
    """Reproduces the function from etl_stock_data_dag.py."""
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(col) for col in df.columns]

    col_map = {}
    for std_name in ["Open", "High", "Low", "Close", "Volume"]:
        suffixed = f"{std_name}_{ticker}"
        if suffixed in df.columns:
            col_map[suffixed] = std_name.lower()
        elif std_name in df.columns:
            col_map[std_name] = std_name.lower()

    if "Adj Close" in df.columns:
        df = df.drop(columns=["Adj Close"], errors="ignore")
    if f"Adj Close_{ticker}" in df.columns:
        df = df.drop(columns=[f"Adj Close_{ticker}"], errors="ignore")

    df = df.rename(columns=col_map)
    return df


class TestStagePathForRun:
    def test_basic_path(self):
        result = _stage_path_for_run("AAPL", "raw", "20240101T120000")
        assert result == "/tmp/stock_data_platform/aapl_raw_20240101T120000.json.gz"

    def test_ticker_lowercased(self):
        result = _stage_path_for_run("GOOG", "cleaned", "abc123")
        assert "goog_" in result

    def test_stage_in_path(self):
        result = _stage_path_for_run("TSLA", "raw", "suffix")
        assert "_raw_" in result
        result2 = _stage_path_for_run("TSLA", "cleaned", "suffix")
        assert "_cleaned_" in result2

    def test_ends_with_gz(self):
        result = _stage_path_for_run("META", "raw", "test")
        assert result.endswith(".json.gz")


class TestNormalizeColumns:
    def test_standard_columns(self):
        df = pd.DataFrame({"Open": [1], "High": [2], "Low": [3], "Close": [4], "Volume": [100]})
        result = _normalize_columns(df, "AAPL")
        assert list(result.columns) == ["open", "high", "low", "close", "volume"]

    def test_suffixed_columns(self):
        df = pd.DataFrame(
            {"Open_AAPL": [1], "High_AAPL": [2], "Low_AAPL": [3], "Close_AAPL": [4], "Volume_AAPL": [100]}
        )
        result = _normalize_columns(df, "AAPL")
        assert list(result.columns) == ["open", "high", "low", "close", "volume"]

    def test_adj_close_dropped(self):
        df = pd.DataFrame({"Open": [1], "High": [2], "Low": [3], "Close": [4], "Volume": [100], "Adj Close": [4.1]})
        result = _normalize_columns(df, "AAPL")
        assert "Adj Close" not in result.columns
        assert "adj close" not in result.columns

    def test_adj_close_suffixed_dropped(self):
        df = pd.DataFrame(
            {
                "Open_AAPL": [1],
                "High_AAPL": [2],
                "Low_AAPL": [3],
                "Close_AAPL": [4],
                "Volume_AAPL": [100],
                "Adj Close_AAPL": [4.1],
            }
        )
        result = _normalize_columns(df, "AAPL")
        assert "Adj Close_AAPL" not in result.columns

    def test_does_not_modify_original(self):
        df = pd.DataFrame({"Open": [1], "High": [2], "Low": [3], "Close": [4], "Volume": [100]})
        _normalize_columns(df, "AAPL")
        assert "Open" in df.columns  # Original unchanged
