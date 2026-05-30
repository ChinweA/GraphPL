"""
Graph — the top-level orchestrator for a GraphPL pipeline.

A Graph holds a set of nodes and a set of directed edges.  Calling run()
drives data through the pipeline: each step first fires every node's action
(optionally in parallel threads) and then flushes all edges so data moves
from out-buffers to in-buffers, ready for the next step.

Typical usage
-------------
    from transport import Graph, Node

    def double(in_buf, out_buf):
        while not in_buf.is_empty():
            out_buf.append(in_buf.pop() * 2)

    source = Node(id="src", in_size=32, out_size=32, action=lambda i, o: None)
    worker = Node(id="double", in_size=32, out_size=32, action=double)

    g = Graph()
    g.add_node(source)
    g.add_node(worker)
    g.add_edge("src", "double")

    for value in range(10):
        source.write(value)

    g.run(steps=1)
    print(worker.read_all())   # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
"""

from __future__ import annotations

import threading
from typing import Dict, List, Optional

from transport.edge import Edge
from transport.node import Node


class Graph:
    """A directed graph of processing nodes connected by data edges.

    Args:
        name: An optional label for the graph, used in ``__repr__``.
    """

    def __init__(self, name: str = "graph") -> None:
        self.name = name
        self._nodes: Dict[str, Node] = {}
        self._edges: List[Edge] = []

    # ------------------------------------------------------------------
    # Building the graph
    # ------------------------------------------------------------------

    def add_node(self, node: Node) -> "Graph":
        """Register a node with the graph.

        Args:
            node: The Node instance to add.

        Returns:
            ``self``, so calls can be chained.

        Raises:
            ValueError: If a node with the same id is already registered.
        """
        if node.id in self._nodes:
            raise ValueError(
                f"A node with id {node.id!r} already exists in this graph. "
                "Each node must have a unique id."
            )
        self._nodes[node.id] = node
        return self

    def add_edge(self, source_id: str, target_id: str) -> "Graph":
        """Connect two nodes with a directed edge (source → target).

        Data written to *source*'s out-buffer will be transferred to
        *target*'s in-buffer at the end of every step.

        Args:
            source_id: The id of the node whose output feeds the edge.
            target_id: The id of the node whose input receives from the edge.

        Returns:
            ``self``, so calls can be chained.

        Raises:
            KeyError:   If either id is not registered in this graph.
            ValueError: If source and target are the same node (self-loop).
        """
        if source_id not in self._nodes:
            raise KeyError(f"No node with id {source_id!r} found in the graph.")
        if target_id not in self._nodes:
            raise KeyError(f"No node with id {target_id!r} found in the graph.")
        if source_id == target_id:
            raise ValueError("Self-loops are not allowed (source and target are the same node).")

        edge = Edge(self._nodes[source_id], self._nodes[target_id])
        self._edges.append(edge)
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self, steps: int = 1, threaded: bool = False) -> None:
        """Execute the graph for a given number of steps.

        Each step consists of two phases:

        1. **Action phase** — every node's action function is called.
           With ``threaded=True`` each node runs in its own thread so they
           all fire concurrently, which is useful for I/O-bound workloads.

        2. **Transfer phase** — every edge drains its source node's
           out-buffer and pushes items into the target node's in-buffer,
           so the next step starts with fresh input data ready to go.

        Args:
            steps:    How many times to cycle through action + transfer.
                      Defaults to 1.
            threaded: If ``True``, each node's action runs in its own
                      ``threading.Thread`` during the action phase.
                      Defaults to ``False`` (sequential, deterministic).

        Raises:
            ValueError: If *steps* is less than 1.
        """
        if steps < 1:
            raise ValueError(f"steps must be at least 1, got {steps}.")

        for _ in range(steps):
            self._run_actions(threaded=threaded)
            self._transfer_all()

    def run_forever(self, threaded: bool = False) -> None:
        """Run the graph in an infinite loop until interrupted (Ctrl-C).

        This is the streaming execution mode — useful when source nodes
        continuously produce data (e.g. reading from a socket or sensor).
        Press Ctrl-C to stop gracefully.

        Args:
            threaded: Same as in :meth:`run`.
        """
        try:
            while True:
                self._run_actions(threaded=threaded)
                self._transfer_all()
        except KeyboardInterrupt:
            pass

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_actions(self, threaded: bool) -> None:
        """Fire every node's action, either sequentially or in threads."""
        if not threaded:
            for node in self._nodes.values():
                node.run_action()
            return

        # Threaded: launch all nodes at once and wait for them all to finish
        # before moving on to the transfer phase.
        threads: List[threading.Thread] = [
            threading.Thread(target=node.run_action, name=f"graphpl-{node.id}", daemon=True)
            for node in self._nodes.values()
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def _transfer_all(self) -> None:
        """Flush every edge — move data from out-buffers to in-buffers."""
        for edge in self._edges:
            edge.transfer()

    # ------------------------------------------------------------------
    # Inspection helpers
    # ------------------------------------------------------------------

    def get_node(self, node_id: str) -> Optional[Node]:
        """Return the node with the given id, or ``None`` if not found."""
        return self._nodes.get(node_id)

    @property
    def node_count(self) -> int:
        """Number of nodes currently registered."""
        return len(self._nodes)

    @property
    def edge_count(self) -> int:
        """Number of directed edges currently registered."""
        return len(self._edges)

    def summary(self) -> str:
        """Return a human-readable summary of the graph topology."""
        lines = [f"Graph '{self.name}'  ({self.node_count} nodes, {self.edge_count} edges)"]
        lines.append("  Nodes:")
        for node in self._nodes.values():
            lines.append(f"    • {node}")
        lines.append("  Edges:")
        for edge in self._edges:
            lines.append(f"    • {edge}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"Graph(name={self.name!r}, nodes={self.node_count}, edges={self.edge_count})"
        )
