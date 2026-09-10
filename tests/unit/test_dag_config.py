"""Tests for Dags/dag_config.py — load_tickers().

This used to test a local reimplementation that took a path argument, so
TICKERS_FILE resolution went uncovered and gutting the real function left the
suite green. dag_config imports only os/sys/datetime, so it loads fine on a
plain checkout without Airflow.
"""

import importlib
import os

import dag_config


def _reload_with(monkeypatch, path):
    monkeypatch.setenv("TICKERS_FILE", str(path))
    return importlib.reload(dag_config)


class TestLoadTickers:
    def test_reads_tickers(self, tmp_path, monkeypatch):
        f = tmp_path / "tickers.txt"
        f.write_text("AAPL\nMSFT\nGOOG\n")
        assert _reload_with(monkeypatch, f).load_tickers() == ["AAPL", "MSFT", "GOOG"]

    def test_strips_whitespace(self, tmp_path, monkeypatch):
        f = tmp_path / "tickers.txt"
        f.write_text("  AAPL  \n\tMSFT\t\n")
        assert _reload_with(monkeypatch, f).load_tickers() == ["AAPL", "MSFT"]

    def test_skips_empty_lines(self, tmp_path, monkeypatch):
        f = tmp_path / "tickers.txt"
        f.write_text("AAPL\n\n\nMSFT\n   \n")
        assert _reload_with(monkeypatch, f).load_tickers() == ["AAPL", "MSFT"]

    def test_empty_file(self, tmp_path, monkeypatch):
        f = tmp_path / "tickers.txt"
        f.write_text("")
        assert _reload_with(monkeypatch, f).load_tickers() == []

    def test_single_ticker(self, tmp_path, monkeypatch):
        f = tmp_path / "tickers.txt"
        f.write_text("AAPL\n")
        assert _reload_with(monkeypatch, f).load_tickers() == ["AAPL"]

    def test_honours_the_tickers_file_env_var(self, tmp_path, monkeypatch):
        """The env var is the one knob that drives both the populate scripts
        and the per-ticker DAG factory. The old copy took a path argument, so
        this path was never exercised."""
        f = tmp_path / "elsewhere.txt"
        f.write_text("TSLA\n")
        mod = _reload_with(monkeypatch, f)
        assert mod.TICKERS_FILE == str(f)
        assert mod.load_tickers() == ["TSLA"]

    def test_missing_file_raises(self, tmp_path, monkeypatch):
        """A missing ticker list must fail loudly — every DAG id depends on it."""
        mod = _reload_with(monkeypatch, tmp_path / "nope.txt")
        try:
            mod.load_tickers()
        except FileNotFoundError:
            return
        raise AssertionError("expected FileNotFoundError")


class TestDefaultArgs:
    def test_etl_args_extend_the_base(self):
        for key, value in dag_config.DEFAULT_ARGS.items():
            assert dag_config.ETL_DEFAULT_ARGS[key] == value

    def test_etl_args_add_a_retry(self):
        assert dag_config.ETL_DEFAULT_ARGS["retries"] >= 1
        assert dag_config.ETL_DEFAULT_ARGS["retry_delay"].total_seconds() > 0


def teardown_module():
    """Leave the module holding the repo's real ticker file for other tests."""
    os.environ.pop("TICKERS_FILE", None)
    importlib.reload(dag_config)
