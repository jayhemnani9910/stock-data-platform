"""Tests for scripts/populate_earnings.py."""

import contextlib

import numpy as np
import pandas as pd
import populate_earnings as pe


class TestPopulateEarnings:
    """yfinance hands back numpy scalars. psycopg2 writes a float by its repr,
    and under numpy 2 that is `np.float64(1.89)`, which Postgres reads as a
    schema-qualified name: the whole insert failed with `schema "np" does not
    exist`. Every number has to reach the driver as a plain float."""

    def test_rows_hold_plain_floats(self, tmp_path, monkeypatch):
        tickers = tmp_path / "tickers.txt"
        tickers.write_text("AAPL\n")
        frame = pd.DataFrame(
            {
                "EPS Estimate": [np.float64(1.89), np.nan],
                "Reported EPS": [np.float64(2.02), np.float64(1.5)],
                "Surprise(%)": [np.float64(6.74), np.nan],
            },
            index=pd.DatetimeIndex(["2026-07-30 16:00", "2026-04-30 16:00"], tz="America/New_York"),
        )

        class _Ticker:
            def __init__(self, ticker):
                pass

            def get_earnings_dates(self, limit):
                return frame

        captured = []
        monkeypatch.setenv("TICKERS_FILE", str(tickers))
        monkeypatch.setattr(pe, "get_db_connection", lambda: contextlib.nullcontext(object()))
        monkeypatch.setattr(pe, "get_company_key", lambda conn, ticker: 1)
        monkeypatch.setattr(pe.yf, "Ticker", _Ticker)
        monkeypatch.setattr(pe, "batch_insert", lambda conn, sql, rows: captured.extend(rows))

        pe.populate_earnings()

        assert len(captured) == 2
        for row in captured:
            for value in row[2:]:
                assert value is None or type(value) is float
        assert captured[0][2:] == (1.89, 2.02, 6.74)
        assert captured[1][2] is None and captured[1][4] is None
