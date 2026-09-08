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
_flush_batch = _kp._flush_batch
_commit_offsets = _kp._commit_offsets

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


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _RecordingConn:
    """Captures the rows handed to execute_values, so a test can inspect them."""

    def __init__(self):
        self.rows = None

    def cursor(self):
        return _FakeCursor()

    def commit(self):
        pass

    def close(self):
        pass


class TestFlushBatchCollapses:
    """_flush_batch is the only path to the database, including on shutdown.

    The shutdown path used to call upsert_streaming_prices directly, skipping
    the collapse, so a final batch spanning two produce cycles was rejected by
    Postgres and discarded. Every exit route must collapse.
    """

    def test_flush_collapses_before_the_upsert(self, monkeypatch):
        seen = {}

        def fake_upsert(conn, rows, page_size=500):
            seen["rows"] = rows

        monkeypatch.setattr(_kp, "upsert_streaming_prices", fake_upsert)
        batch = [(DATE, k, 10.0, 11.0, 9.0, 10.5, 100) for k in range(1, 11)]
        batch += [(DATE, k, 10.1, 12.0, 8.0, 11.0, 300) for k in range(1, 11)]

        _conn, ok = _flush_batch(_RecordingConn(), batch)

        assert ok is True
        keys = [(r[0], r[1]) for r in seen["rows"]]
        assert len(keys) == 10, "20 rows over two cycles must collapse to 10"
        assert len(keys) == len(set(keys)), "no duplicate ON CONFLICT targets may reach Postgres"


class TestCommitOffsets:
    """A broker restart expires group membership and the next commit raises.

    That exception used to propagate out of main() and kill the container,
    which never came back because the service does not restart.
    """

    class _OkConsumer:
        def __init__(self):
            self.commits = 0

        def commit(self):
            self.commits += 1

    class _RebalancedConsumer:
        def commit(self):
            raise Exception("CommitFailedError: [Error 25] UnknownMemberIdError")

    def test_successful_commit_reports_true(self):
        consumer = self._OkConsumer()
        assert _commit_offsets(consumer) is True
        assert consumer.commits == 1

    def test_rebalance_is_swallowed_not_raised(self):
        assert _commit_offsets(self._RebalancedConsumer()) is False


class _FakeMessage:
    def __init__(self, value):
        self.value = value


class _ShutdownConsumer:
    """Delivers two produce cycles, then stops the loop without a flush.

    20 messages is under BATCH_SIZE and arrives inside FLUSH_INTERVAL, so the
    loop buffers them and never flushes. The second poll ends the run, which
    leaves main() to drain the batch in its finally block.
    """

    def __init__(self):
        self.polls = 0
        self.commits = 0
        self.closed = False

    def poll(self, timeout_ms=None):
        self.polls += 1
        if self.polls > 1:
            raise KeyboardInterrupt
        rows = [
            {"date": DATE, "company_key": k, "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 100}
            for k in range(1, 11)
        ]
        rows += [
            {"date": DATE, "company_key": k, "open": 10.1, "high": 12.0, "low": 8.0, "close": 11.0, "volume": 300}
            for k in range(1, 11)
        ]
        return {"tp": [_FakeMessage(r) for r in rows]}

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


class TestShutdownDrain:
    """The regression: main()'s finally block bypassed the collapse.

    It called upsert_streaming_prices directly, so a shutdown batch spanning
    two produce cycles reached Postgres with duplicate ON CONFLICT targets and
    was rejected with "cannot affect row a second time", then discarded.
    A test on _flush_batch alone cannot see this — only the exit path can.
    """

    def test_final_batch_is_collapsed_before_it_reaches_postgres(self, monkeypatch):
        seen = {}

        def fake_upsert(conn, rows, page_size=500):
            keys = [(r[0], r[1]) for r in rows]
            if len(keys) != len(set(keys)):
                raise Exception("ON CONFLICT DO UPDATE command cannot affect row a second time")
            seen["rows"] = rows

        consumer = _ShutdownConsumer()
        monkeypatch.setattr(_kp, "upsert_streaming_prices", fake_upsert)
        monkeypatch.setattr(_kp, "_connect_kafka", lambda: consumer)
        monkeypatch.setattr(_kp, "connect_db", lambda *a, **k: _RecordingConn())

        with pytest.raises(KeyboardInterrupt):
            _kp.main()

        assert "rows" in seen, "the shutdown batch never reached the database"
        assert len(seen["rows"]) == 10, "20 buffered rows must collapse to 10 on shutdown"
        assert consumer.closed is True
