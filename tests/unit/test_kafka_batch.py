"""Tests for the Kafka consumer's batch collapsing.

execute_values sends a whole batch as one INSERT. Postgres rejects a statement
whose rows collide on the ON CONFLICT target with "ON CONFLICT DO UPDATE command
cannot affect row a second time". The producer emits one tick per ticker per
cycle and stamps them all with the same calendar date, so any batch that spans
more than one cycle collided and the consumer discarded every message in it.
Observed live: "Discarding 20 messages", exactly two cycles of ten tickers.
"""

import importlib.util
import os
import sys

import pytest

_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
sys.path.insert(0, os.path.join(_ROOT, "scripts"))

pytest.importorskip("kafka", reason="kafka-python not installed")

_spec = importlib.util.spec_from_file_location("kafka_to_postgres", os.path.join(_ROOT, "kafka_to_postgres.py"))
_kp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_kp)
_collapse_batch = _kp._collapse_batch

DATE = "2026-09-04"


class TestCollapseBatch:
    def test_empty_batch(self):
        assert _collapse_batch([]) == []

    def test_distinct_keys_are_untouched(self):
        batch = [
            (DATE, 1, 100.0, 101.0, 99.0, 100.5, 1000),
            (DATE, 2, 200.0, 201.0, 199.0, 200.5, 2000),
        ]
        assert len(_collapse_batch(batch)) == 2

    def test_conflict_keys_become_unique(self):
        """This is the property that stops Postgres rejecting the statement."""
        batch = [(DATE, 1, 100.0, 101.0, 99.0, 100.5, 1000)] * 5
        out = _collapse_batch(batch)
        keys = [(r[0], r[1]) for r in out]
        assert len(keys) == len(set(keys)) == 1

    def test_two_cycles_of_ten_tickers_collapse_to_ten(self):
        """The exact shape of the batch that was being discarded live."""
        batch = [(DATE, k, 10.0, 11.0, 9.0, 10.5, 100) for k in range(1, 11)]
        batch += [(DATE, k, 10.1, 12.0, 8.0, 11.0, 300) for k in range(1, 11)]
        out = _collapse_batch(batch)
        assert len(out) == 10
        keys = [(r[0], r[1]) for r in out]
        assert len(keys) == len(set(keys))

    def test_merge_matches_the_sql_semantics(self):
        """First open, widest high and low, latest close, largest volume.

        The last row deliberately carries the SMALLEST high, the LARGEST low and
        a SMALLER volume, so "take the last value" and "take the extreme" give
        different answers and the assertions can tell them apart.
        """
        batch = [
            (DATE, 1, 100.0, 101.0, 99.0, 100.5, 1000),
            (DATE, 1, 100.4, 103.0, 98.0, 102.0, 5000),
            (DATE, 1, 100.9, 100.95, 100.9, 100.92, 400),
        ]
        (_, _, open_, high, low, close, volume) = _collapse_batch(batch)[0]
        assert open_ == 100.0, "open must be the first seen, not the latest"
        assert high == 103.0, "high must be the widest, not the latest"
        assert low == 98.0, "low must be the widest, not the latest"
        assert close == 100.92, "close must be the latest tick"
        assert volume == 5000, "volume must be the largest, not the latest"

    def test_different_dates_stay_separate(self):
        batch = [
            (DATE, 1, 100.0, 101.0, 99.0, 100.5, 1000),
            ("2026-09-05", 1, 102.0, 104.0, 101.0, 103.0, 2000),
        ]
        assert len(_collapse_batch(batch)) == 2
