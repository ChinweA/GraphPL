"""
Tests for Graph v2 features:
  - validate() — cycles, orphan nodes, disconnected components
  - metrics()
  - visualize() — ASCII and Mermaid
  - priority-ordered execution
  - trace flag (smoke test, no output assertions)
  - edge max_items batch limit
"""

import pytest

from transport import Graph, Node
from transport.graph import ValidationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def noop(in_buf, out_buf):
    pass


def echo_action(in_buf, out_buf):
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop())


def always_fail(in_buf, out_buf):
    raise RuntimeError("boom")


def make_node(node_id, action=None, priority=0, error_policy="fail_fast") -> Node:
    return Node(
        id=node_id,
        in_size=16,
        out_size=16,
        action=action or noop,
        priority=priority,
        error_policy=error_policy,
    )


# ---------------------------------------------------------------------------
# validate() — valid graph
# ---------------------------------------------------------------------------

class TestValidateValid:
    def test_linear_pipeline_is_valid(self):
        g = Graph()
        g.add_node(make_node("a")).add_node(make_node("b")).add_node(make_node("c"))
        g.add_edge("a", "b").add_edge("b", "c")
        result = g.validate()
        assert result.is_valid
        assert result.issues == []

    def test_single_node_is_valid(self):
        g = Graph()
        g.add_node(make_node("solo"))
        result = g.validate()
        assert result.is_valid   # solo node: no edges issue only fires when >1 node

    def test_str_on_valid(self):
        g = Graph()
        g.add_node(make_node("a"))
        result = g.validate()
        assert "valid" in str(result).lower()


# ---------------------------------------------------------------------------
# validate() — cycles
# ---------------------------------------------------------------------------

class TestValidateCycles:
    def test_simple_cycle_detected(self):
        g = Graph()
        g.add_node(make_node("a")).add_node(make_node("b"))
        g.add_edge("a", "b").add_edge("b", "a")
        result = g.validate()
        assert not result.is_valid
        assert any("cycle" in issue.lower() for issue in result.issues)

    def test_three_node_cycle_detected(self):
        g = Graph()
        g.add_node(make_node("a")).add_node(make_node("b")).add_node(make_node("c"))
        g.add_edge("a", "b").add_edge("b", "c").add_edge("c", "a")
        result = g.validate()
        assert not result.is_valid

    def test_strict_raises_on_cycle(self):
        g = Graph()
        g.add_node(make_node("x")).add_node(make_node("y"))
        g.add_edge("x", "y").add_edge("y", "x")
        with pytest.raises(ValueError):
            g.validate(strict=True)


# ---------------------------------------------------------------------------
# validate() — orphan nodes
# ---------------------------------------------------------------------------

class TestValidateOrphan:
    def test_orphan_node_detected(self):
        g = Graph()
        g.add_node(make_node("a")).add_node(make_node("b")).add_node(make_node("orphan"))
        g.add_edge("a", "b")
        result = g.validate()
        assert not result.is_valid
        assert any("orphan" in issue.lower() or "isolated" in issue.lower()
                   for issue in result.issues)


# ---------------------------------------------------------------------------
# validate() — disconnected components
# ---------------------------------------------------------------------------

class TestValidateDisconnected:
    def test_two_components_detected(self):
        g = Graph()
        # Component 1: a → b
        g.add_node(make_node("a")).add_node(make_node("b"))
        g.add_edge("a", "b")
        # Component 2: c → d (separate, no connection to a/b)
        g.add_node(make_node("c")).add_node(make_node("d"))
        g.add_edge("c", "d")
        result = g.validate()
        assert not result.is_valid
        assert any("disconnected" in issue.lower() for issue in result.issues)


# ---------------------------------------------------------------------------
# Priority-ordered execution
# ---------------------------------------------------------------------------

class TestPriorityExecution:
    def test_higher_priority_runs_first(self):
        """
        Three nodes write their id to a shared list.
        We verify the list is ordered by descending priority.
        """
        execution_order = []

        def make_tracker(node_id):
            def action(in_buf, out_buf):
                execution_order.append(node_id)
            return action

        g = Graph()
        g.add_node(Node(id="low",  in_size=4, out_size=4,
                        action=make_tracker("low"),  priority=0))
        g.add_node(Node(id="high", in_size=4, out_size=4,
                        action=make_tracker("high"), priority=10))
        g.add_node(Node(id="mid",  in_size=4, out_size=4,
                        action=make_tracker("mid"),  priority=5))

        g.run(steps=1)

        assert execution_order == ["high", "mid", "low"]


# ---------------------------------------------------------------------------
# metrics()
# ---------------------------------------------------------------------------

class TestMetrics:
    def test_metrics_returns_dict_per_node(self):
        g = Graph()
        g.add_node(make_node("a", action=echo_action))
        g.add_node(make_node("b", action=echo_action))
        g.add_edge("a", "b")
        g.run(steps=1)
        m = g.metrics()
        assert "a" in m
        assert "b" in m

    def test_processed_count_increments(self):
        g = Graph()
        g.add_node(make_node("a", action=echo_action))
        g.run(steps=3)
        assert g.metrics()["a"]["processed"] == 3

    def test_failed_count_increments(self):
        g = Graph()
        g.add_node(make_node("a", action=always_fail, error_policy="skip"))
        g.run(steps=2)
        assert g.metrics()["a"]["failed"] == 2


# ---------------------------------------------------------------------------
# visualize()
# ---------------------------------------------------------------------------

class TestVisualize:
    def test_ascii_contains_node_ids(self):
        g = Graph(name="test-graph")
        g.add_node(make_node("src")).add_node(make_node("dst"))
        g.add_edge("src", "dst")
        out = g.visualize()
        assert "src" in out
        assert "dst" in out
        assert "test-graph" in out

    def test_mermaid_starts_with_fenced_block(self):
        g = Graph()
        g.add_node(make_node("a")).add_node(make_node("b"))
        g.add_edge("a", "b")
        out = g.visualize(as_mermaid=True)
        assert out.startswith("```mermaid")
        assert "a --> b" in out

    def test_terminal_node_labelled(self):
        g = Graph()
        g.add_node(make_node("src")).add_node(make_node("sink"))
        g.add_edge("src", "sink")
        out = g.visualize()
        assert "terminal" in out


# ---------------------------------------------------------------------------
# Trace flag (smoke — just checks it doesn't crash)
# ---------------------------------------------------------------------------

class TestTrace:
    def test_trace_does_not_raise(self, capsys):
        g = Graph()
        g.add_node(make_node("n", action=echo_action))
        g.run(steps=2, trace=True)
        captured = capsys.readouterr()
        assert "Step" in captured.out


# ---------------------------------------------------------------------------
# Edge max_items batch limit
# ---------------------------------------------------------------------------

class TestEdgeBatchTransfer:
    def test_max_items_limits_transfer(self):
        from transport.edge import Edge

        src = make_node("src", action=noop)
        tgt = make_node("tgt", action=noop)
        edge = Edge(src, tgt)

        for i in range(10):
            src._out_buffer.append(i)

        transferred = edge.transfer(max_items=3)
        assert transferred == 3
        assert len(tgt._in_buffer) == 3
        assert not src._out_buffer.is_empty()   # 7 still waiting

    def test_unlimited_transfer_by_default(self):
        from transport.edge import Edge

        src = make_node("src", action=noop)
        tgt = make_node("tgt", action=noop)
        edge = Edge(src, tgt)

        for i in range(8):
            src._out_buffer.append(i)

        transferred = edge.transfer()
        assert transferred == 8
        assert src._out_buffer.is_empty()
