"""
Unit tests for transport.node.Node.

Covers: construction, write/read_all helpers, run_action delegation,
peek_out, and node_type labelling.
"""

import pytest

from transport.node import Node


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def noop(in_buf, out_buf):
    """Action that does nothing — useful for construction tests."""
    pass


def echo_action(in_buf, out_buf):
    """Drain in-buffer and copy every item to out-buffer unchanged."""
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop())


def doubler_action(in_buf, out_buf):
    """Multiply every incoming value by 2."""
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop() * 2)


def make_node(action=None, in_size=8, out_size=8, node_type="any") -> Node:
    return Node(
        id="test",
        in_size=in_size,
        out_size=out_size,
        action=action or noop,
        node_type=node_type,
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestConstruction:
    def test_default_node_type(self):
        node = make_node()
        assert node.node_type == "any"

    def test_custom_node_type(self):
        node = make_node(node_type="source")
        assert node.node_type == "source"

    def test_buffers_start_empty(self):
        node = make_node()
        assert node._in_buffer.is_empty()
        assert node._out_buffer.is_empty()

    def test_repr_contains_id(self):
        node = make_node()
        assert "test" in repr(node)


# ---------------------------------------------------------------------------
# write() / read_all() helpers
# ---------------------------------------------------------------------------

class TestWriteReadAll:
    def test_write_puts_item_in_in_buffer(self):
        node = make_node()
        node.write(7)
        assert not node._in_buffer.is_empty()

    def test_read_all_empty_returns_empty_list(self):
        node = make_node()
        assert node.read_all() == []

    def test_read_all_drains_out_buffer(self):
        node = make_node(action=echo_action)
        for v in [1, 2, 3]:
            node.write(v)
        node.run_action()
        result = node.read_all()
        assert result == [1, 2, 3]
        assert node._out_buffer.is_empty()

    def test_write_multiple_items(self):
        node = make_node(action=echo_action)
        values = list(range(5))
        for v in values:
            node.write(v)
        node.run_action()
        assert node.read_all() == values


# ---------------------------------------------------------------------------
# run_action()
# ---------------------------------------------------------------------------

class TestRunAction:
    def test_action_called_with_buffers(self):
        received = []

        def capture(in_buf, out_buf):
            while not in_buf.is_empty():
                received.append(in_buf.pop())

        node = make_node(action=capture)
        node.write("hello")
        node.run_action()
        assert received == ["hello"]

    def test_doubler_action(self):
        node = make_node(action=doubler_action)
        for v in [1, 2, 3, 4]:
            node.write(v)
        node.run_action()
        assert node.read_all() == [2, 4, 6, 8]

    def test_action_with_empty_buffer_does_nothing(self):
        side_effects = []

        def track(in_buf, out_buf):
            while not in_buf.is_empty():
                side_effects.append(in_buf.pop())

        node = make_node(action=track)
        node.run_action()
        assert side_effects == []


# ---------------------------------------------------------------------------
# peek_out()
# ---------------------------------------------------------------------------

class TestPeekOut:
    def test_peek_empty_returns_none(self):
        node = make_node()
        assert node.peek_out() is None

    def test_peek_does_not_remove_item(self):
        node = make_node(action=echo_action)
        node.write(99)
        node.run_action()
        assert node.peek_out() == 99
        # Item is still there.
        assert node.read_all() == [99]
