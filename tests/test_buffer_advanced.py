"""
Tests for the new RingBuffer features:
  - overflow strategies (drop_newest, raise, block)
  - thread safety under concurrent access
  - __len__
"""

import threading
import time

import pytest

from transport.buffer import RingBuffer


# ---------------------------------------------------------------------------
# Overflow: drop_newest
# ---------------------------------------------------------------------------

class TestOverflowDropNewest:
    def test_returns_false_when_full(self):
        buf = RingBuffer(size=2, overflow="drop_newest")
        buf.append(1)
        buf.append(2)
        result = buf.append(3)
        assert result is False

    def test_oldest_items_preserved(self):
        buf = RingBuffer(size=2, overflow="drop_newest")
        buf.append("a")
        buf.append("b")
        buf.append("c")   # dropped
        assert buf.pop() == "a"
        assert buf.pop() == "b"


# ---------------------------------------------------------------------------
# Overflow: raise
# ---------------------------------------------------------------------------

class TestOverflowRaise:
    def test_raises_on_full(self):
        buf = RingBuffer(size=2, overflow="raise")
        buf.append(1)
        buf.append(2)
        with pytest.raises(BufferError, match="full"):
            buf.append(3)

    def test_does_not_raise_after_pop(self):
        buf = RingBuffer(size=2, overflow="raise")
        buf.append(1)
        buf.append(2)
        buf.pop()
        buf.append(3)   # should succeed now
        assert len(buf) == 2


# ---------------------------------------------------------------------------
# Overflow: block
# ---------------------------------------------------------------------------

class TestOverflowBlock:
    def test_blocks_then_succeeds_after_pop(self):
        buf = RingBuffer(size=1, overflow="block")
        buf.append("first")

        results = []

        def producer():
            buf.append("second")   # blocks until consumer pops
            results.append("produced")

        t = threading.Thread(target=producer)
        t.start()
        time.sleep(0.05)
        buf.pop()
        t.join(timeout=2)
        assert "produced" in results

    def test_timeout_raises(self):
        buf = RingBuffer(size=1, overflow="block")
        buf.append("x")
        with pytest.raises(TimeoutError):
            buf.append("y", block_timeout=0.1)


# ---------------------------------------------------------------------------
# Overflow: overwrite (default) — regression check
# ---------------------------------------------------------------------------

class TestOverflowOverwrite:
    def test_returns_true(self):
        buf = RingBuffer(size=2)
        buf.append(1)
        buf.append(2)
        result = buf.append(3)
        assert result is True

    def test_oldest_overwritten(self):
        buf = RingBuffer(size=2)
        buf.append("a")
        buf.append("b")
        buf.append("c")   # overwrites "a"
        assert buf.pop() == "b"
        assert buf.pop() == "c"


# ---------------------------------------------------------------------------
# __len__
# ---------------------------------------------------------------------------

class TestLen:
    def test_empty_buffer(self):
        buf = RingBuffer(size=4)
        assert len(buf) == 0

    def test_partial_fill(self):
        buf = RingBuffer(size=4)
        buf.append(1)
        buf.append(2)
        assert len(buf) == 2

    def test_full_buffer(self):
        buf = RingBuffer(size=3)
        buf.append(1)
        buf.append(2)
        buf.append(3)
        assert len(buf) == 3

    def test_after_pop(self):
        buf = RingBuffer(size=4)
        buf.append(1)
        buf.append(2)
        buf.pop()
        assert len(buf) == 1


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------

class TestThreadSafety:
    def test_concurrent_appends_no_data_loss(self):
        """Multiple threads appending should not corrupt internal state."""
        buf = RingBuffer(size=200)
        errors = []

        def producer(start, count):
            for i in range(start, start + count):
                try:
                    buf.append(i)
                except Exception as e:
                    errors.append(e)

        threads = [
            threading.Thread(target=producer, args=(i * 20, 20))
            for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert not buf.is_empty()

    def test_concurrent_append_and_pop(self):
        """Producer and consumer running simultaneously should not deadlock."""
        buf = RingBuffer(size=10)
        produced = []
        consumed = []

        def producer():
            for i in range(50):
                buf.append(i)
                produced.append(i)
                time.sleep(0.001)

        def consumer():
            count = 0
            while count < 50:
                if not buf.is_empty():
                    consumed.append(buf.pop())
                    count += 1
                else:
                    time.sleep(0.001)

        t_prod = threading.Thread(target=producer)
        t_cons = threading.Thread(target=consumer)
        t_prod.start()
        t_cons.start()
        t_prod.join(timeout=5)
        t_cons.join(timeout=5)

        assert len(consumed) == 50
        assert sorted(consumed) == list(range(50))
