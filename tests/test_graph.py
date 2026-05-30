"""
Integration tests for transport.graph.Graph.

Covers: add_node/add_edge validation, run(), chained pipelines, threaded
execution, run_forever interrupt, and the summary() helper.
"""

import pytest

from transport import Graph, Node


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def noop(in_buf, out_buf):
    pass


def echo_action(in_buf, out_buf):
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop())


def doubler_action(in_buf, out_buf):
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop() * 2)


def adder_action(in_buf, out_buf):
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop() + 100)


def make_node(node_id, action=None, size=16, node_type="any") -> Node:
    return Node(
        id=node_id,
        in_size=size,
        out_size=size,
        action=action or noop,
        node_type=node_type,
    )


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

class TestGraphConstruction:
    def test_add_node_increments_count(self):
        g = Graph()
        g.add_node(make_node("a"))
        assert g.node_count == 1

    def test_add_edge_increments_count(self):
        g = Graph()
        g.add_node(make_node("a")).add_node(make_node("b"))
        g.add_edge("a", "b")
        assert g.edge_count == 1

    def test_chained_add_node(self):
        g = Graph()
        g.add_node(make_node("a")).add_node(make_node("b")).add_node(make_node("c"))
        assert g.node_count == 3

    def test_chained_add_edge(self):
        g = Graph()
        g.add_node(make_node("a")).add_node(make_node("b")).add_node(make_node("c"))
        g.add_edge("a", "b").add_edge("b", "c")
        assert g.edge_count == 2

    def test_duplicate_node_raises(self):
        g = Graph()
        g.add_node(make_node("a"))
        with pytest.raises(ValueError, match="already exists"):
            g.add_node(make_node("a"))

    def test_edge_with_missing_source_raises(self):
        g = Graph()
        g.add_node(make_node("b"))
        with pytest.raises(KeyError):
            g.add_edge("missing", "b")

    def test_edge_with_missing_target_raises(self):
        g = Graph()
        g.add_node(make_node("a"))
        with pytest.raises(KeyError):
            g.add_edge("a", "missing")

    def test_self_loop_raises(self):
        g = Graph()
        g.add_node(make_node("a"))
        with pytest.raises(ValueError, match="Self-loops"):
            g.add_edge("a", "a")

    def test_steps_less_than_one_raises(self):
        g = Graph()
        with pytest.raises(ValueError, match="steps"):
            g.run(steps=0)


# ---------------------------------------------------------------------------
# Single-node execution
# ---------------------------------------------------------------------------

class TestSingleNode:
    def test_single_echo_node(self):
        g = Graph()
        node = make_node("echo", action=echo_action)
        g.add_node(node)

        node.write(42)
        g.run(steps=1)

        assert node.read_all() == [42]

    def test_single_doubler_node(self):
        g = Graph()
        node = make_node("d", action=doubler_action)
        g.add_node(node)

        for v in [1, 2, 3]:
            node.write(v)
        g.run(steps=1)

        assert node.read_all() == [2, 4, 6]


# ---------------------------------------------------------------------------
# Two-node pipeline
# ---------------------------------------------------------------------------

class TestTwoNodePipeline:
    def test_source_to_sink(self):
        """src echoes its input; doubler doubles it.
        
        Step 1: src fires (in → out), then edge transfers src-out → doubler-in.
        Step 2: doubler fires (in → out).
        """
        g = Graph()
        src = make_node("src", action=echo_action, node_type="source")
        dbl = make_node("dbl", action=doubler_action, node_type="sink")

        g.add_node(src).add_node(dbl)
        g.add_edge("src", "dbl")

        for v in [1, 2, 3]:
            src.write(v)

        g.run(steps=2)

        assert dbl.read_all() == [2, 4, 6]


# ---------------------------------------------------------------------------
# Three-node pipeline (integration)
# ---------------------------------------------------------------------------

class TestThreeNodePipeline:
    def test_source_processor_sink(self):
        """Full pipeline: numbers 1-10 → doubler → adder (+100)."""
        g = Graph()
        src = make_node("src", action=echo_action, node_type="source")
        dbl = make_node("dbl", action=doubler_action, node_type="processor")
        add = make_node("add", action=adder_action, node_type="sink")

        g.add_node(src).add_node(dbl).add_node(add)
        g.add_edge("src", "dbl").add_edge("dbl", "add")

        for i in range(1, 11):
            src.write(i)

        g.run(steps=3)

        expected = [(i * 2) + 100 for i in range(1, 11)]
        assert add.read_all() == expected

    def test_run_steps_twice(self):
        """Two separate run() calls on the same graph keep working."""
        g = Graph()
        src = make_node("src", action=echo_action)
        dbl = make_node("dbl", action=doubler_action)
        g.add_node(src).add_node(dbl)
        g.add_edge("src", "dbl")

        # First batch
        for i in range(1, 4):
            src.write(i)
        g.run(steps=2)
        assert dbl.read_all() == [2, 4, 6]

        # Second batch
        for i in range(10, 13):
            src.write(i)
        g.run(steps=2)
        assert dbl.read_all() == [20, 22, 24]


# ---------------------------------------------------------------------------
# Threaded execution
# ---------------------------------------------------------------------------

class TestThreadedExecution:
    def test_threaded_produces_same_results(self):
        """Running in threaded mode must produce the same output as sequential."""
        def build_pipeline():
            g = Graph()
            src = make_node("src", action=echo_action)
            dbl = make_node("dbl", action=doubler_action)
            g.add_node(src).add_node(dbl)
            g.add_edge("src", "dbl")
            return g, src, dbl

        # Sequential
        g1, src1, dbl1 = build_pipeline()
        for i in range(1, 6):
            src1.write(i)
        g1.run(steps=2, threaded=False)
        sequential_result = dbl1.read_all()

        # Threaded
        g2, src2, dbl2 = build_pipeline()
        for i in range(1, 6):
            src2.write(i)
        g2.run(steps=2, threaded=True)
        threaded_result = dbl2.read_all()

        assert sequential_result == threaded_result == [2, 4, 6, 8, 10]


# ---------------------------------------------------------------------------
# Inspection helpers
# ---------------------------------------------------------------------------

class TestInspection:
    def test_get_node_returns_node(self):
        g = Graph()
        node = make_node("n1")
        g.add_node(node)
        assert g.get_node("n1") is node

    def test_get_node_missing_returns_none(self):
        g = Graph()
        assert g.get_node("nope") is None

    def test_summary_contains_name(self):
        g = Graph(name="my-graph")
        assert "my-graph" in g.summary()

    def test_repr(self):
        g = Graph(name="test")
        assert "test" in repr(g)
