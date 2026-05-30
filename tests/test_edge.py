"""
Unit tests for transport.edge.Edge.

Covers: basic transfer, partial transfer (back-pressure), transfer count,
and repr.
"""

import pytest

from transport.node import Node
from transport.edge import Edge


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def noop(in_buf, out_buf):
    pass


def make_node(node_id, size=8):
    return Node(id=node_id, in_size=size, out_size=size, action=noop)


# ---------------------------------------------------------------------------
# Basic transfer
# ---------------------------------------------------------------------------

class TestEdgeTransfer:
    def test_transfers_single_item(self):
        src = make_node("src")
        tgt = make_node("tgt")
        edge = Edge(src, tgt)

        src._out_buffer.append("hello")
        edge.transfer()

        assert tgt._in_buffer.pop() == "hello"
        assert src._out_buffer.is_empty()

    def test_transfers_multiple_items_in_fifo_order(self):
        src = make_node("src")
        tgt = make_node("tgt")
        edge = Edge(src, tgt)

        for i in range(5):
            src._out_buffer.append(i)

        transferred = edge.transfer()

        assert transferred == 5
        result = [tgt._in_buffer.pop() for _ in range(5)]
        assert result == [0, 1, 2, 3, 4]

    def test_nothing_transferred_when_source_empty(self):
        src = make_node("src")
        tgt = make_node("tgt")
        edge = Edge(src, tgt)

        count = edge.transfer()
        assert count == 0
        assert tgt._in_buffer.is_empty()

    def test_returns_transfer_count(self):
        src = make_node("src")
        tgt = make_node("tgt")
        edge = Edge(src, tgt)

        for i in range(3):
            src._out_buffer.append(i)

        assert edge.transfer() == 3


# ---------------------------------------------------------------------------
# Back-pressure (target buffer full)
# ---------------------------------------------------------------------------

class TestBackPressure:
    def test_stops_when_target_full(self):
        src = make_node("src", size=8)
        tgt = make_node("tgt", size=3)  # small target
        edge = Edge(src, tgt)

        # Push 6 items into source out-buffer.
        for i in range(6):
            src._out_buffer.append(i)

        # Only 3 can make it across before the target is full.
        transferred = edge.transfer()
        assert transferred == 3
        assert tgt._in_buffer.is_full()
        # 3 items remain in source out-buffer.
        assert not src._out_buffer.is_empty()


# ---------------------------------------------------------------------------
# Repr
# ---------------------------------------------------------------------------

class TestEdgeRepr:
    def test_repr_contains_node_ids(self):
        src = make_node("alpha")
        tgt = make_node("beta")
        edge = Edge(src, tgt)
        r = repr(edge)
        assert "alpha" in r
        assert "beta" in r
