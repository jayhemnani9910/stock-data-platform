"""Tests for Dags/dag_config.py — load_tickers() logic."""


def _load_tickers_from_file(filepath):
    """Reproduces load_tickers() logic without hardcoded path."""
    with open(filepath) as f:
        return [line.strip() for line in f if line.strip()]


class TestLoadTickers:
    def test_reads_tickers(self, tmp_path):
        f = tmp_path / "tickers.txt"
        f.write_text("AAPL\nGOOG\nMSFT\n")
        result = _load_tickers_from_file(str(f))
        assert result == ["AAPL", "GOOG", "MSFT"]

    def test_strips_whitespace(self, tmp_path):
        f = tmp_path / "tickers.txt"
        f.write_text("  AAPL  \n  GOOG  \n")
        result = _load_tickers_from_file(str(f))
        assert result == ["AAPL", "GOOG"]

    def test_skips_empty_lines(self, tmp_path):
        f = tmp_path / "tickers.txt"
        f.write_text("AAPL\n\n\nGOOG\n\n")
        result = _load_tickers_from_file(str(f))
        assert result == ["AAPL", "GOOG"]

    def test_empty_file(self, tmp_path):
        f = tmp_path / "tickers.txt"
        f.write_text("")
        result = _load_tickers_from_file(str(f))
        assert result == []

    def test_single_ticker(self, tmp_path):
        f = tmp_path / "tickers.txt"
        f.write_text("NVDA\n")
        result = _load_tickers_from_file(str(f))
        assert result == ["NVDA"]
