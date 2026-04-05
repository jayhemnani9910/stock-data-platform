"""Tests for scripts/populate_sec_financials.py — _parse_period_end()."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))

from populate_sec_financials import _parse_period_end


class TestParsePeriodEnd:
    def test_duration_format(self):
        result = _parse_period_end("duration_2024-09-29_2025-09-27")
        assert result == "2025-09-27"

    def test_instant_format(self):
        result = _parse_period_end("instant_2025-09-27")
        assert result == "2025-09-27"

    def test_no_date_returns_none(self):
        result = _parse_period_end("no_date_here")
        assert result is None

    def test_empty_string(self):
        result = _parse_period_end("")
        assert result is None

    def test_single_date(self):
        result = _parse_period_end("2024-01-15")
        assert result == "2024-01-15"

    def test_extracts_last_date(self):
        result = _parse_period_end("prefix_2023-01-01_2024-06-30")
        assert result == "2024-06-30"
