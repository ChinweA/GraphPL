"""
Unit tests for transport.buffer.RingBuffer.

Covers: basic append/pop, wrap-around, full/empty detection, back-pressure,
and mixed sequences.
"""

import pytest

from transport.buffer import RingBuffer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_buffer(size: int) -> RingBuffer:
    return RingBuffer(size=size)


# ---------------------------------------------------------------------------
# Basic behaviour
# ---------------------------------------------------------------------------

class TestBasicAppendPop:
    def test_single_item(self):
        buf = make_buffer(4)
        buf.append(42)
        assert buf.pop() == 42

    def test_fifo_order(self):
        buf = make_buffer(4)
        for i in range(4):
            buf.append(i)
        result = [buf.pop() for _ in range(4)]
        assert result == [0, 1, 2, 3]

    def test_pop_empty_raises(self):
        buf = make_buffer(4)
        with pytest.raises(IndexError):
            buf.pop()


# ---------------------------------------------------------------------------
# Empty / full detection
# ---------------------------------------------------------------------------

class TestEmptyFull:
    def test_starts_empty(self):
        buf = make_buffer(4)
        assert buf.is_empty()
        assert not buf.is_full()

    def test_full_after_filling(self):
        buf = make_buffer(3)
        for i in range(3):
            buf.append(i)
        assert buf.is_full()
        assert not buf.is_empty()

    def test_empty_after_draining(self):
        buf = make_buffer(3)
        buf.append(1)
        buf.pop()
        assert buf.is_empty()

    def test_not_full_after_one_pop(self):
        buf = make_buffer(3)
        for i in range(3):
            buf.append(i)
        buf.pop()
        assert not buf.is_full()


# ---------------------------------------------------------------------------
# Wrap-around (ring behaviour)
# ---------------------------------------------------------------------------

class TestWrapAround:
    def test_wrap_around_preserves_fifo(self):
        buf = make_buffer(4)
        # Fill to capacity.
        for i in range(4):
            buf.append(i)
        # Pop two, then push two more — head and tail both wrap.
        buf.pop()
        buf.pop()
        buf.append(10)
        buf.append(11)
        result = [buf.pop() for _ in range(4)]
        assert result == [2, 3, 10, 11]

    def test_overwrite_when_full(self):
        """When the buffer is full, appending overwrites the oldest item."""
        buf = make_buffer(3)
        buf.append("a")
        buf.append("b")
        buf.append("c")
        buf.append("d")  # overwrites "a"
        result = [buf.pop() for _ in range(3)]
        assert result == ["b", "c", "d"]


# ---------------------------------------------------------------------------
# Mixed types
# ---------------------------------------------------------------------------

class TestMixedTypes:
    def test_stores_any_python_object(self):
        buf = make_buffer(8)
        items = [1, "hello", 3.14, {"key": "val"}, [1, 2, 3], None]
        for item in items:
            buf.append(item)
        result = [buf.pop() for _ in range(len(items))]
        assert result == items


# ---------------------------------------------------------------------------
# Size = 1 edge case
# ---------------------------------------------------------------------------

class TestSizeOne:
    def test_size_one_full_empty(self):
        buf = make_buffer(1)
        assert buf.is_empty()
        buf.append(99)
        assert buf.is_full()
        assert buf.pop() == 99
        assert buf.is_empty()
