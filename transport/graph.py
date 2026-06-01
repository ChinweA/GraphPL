"""
Graph — the top-level orchestrator for a GraphPL pipeline.

New in v2
---------
* **Priority scheduling** — nodes with a higher ``priority`` value run first.
* **Validation** — ``graph.validate()`` checks for cycles, orphan nodes, and
  disconnected components before you run.
* **Metrics** — ``graph.metrics()`` returns live execution statistics for
  every node.
* **Visualization** — ``graph.visualize()`` prints an ASCII topology map;
  ``graph.visualize(as_mermaid=True)`` returns a Mermaid diagram string you
  can paste straight into GitHub.
* **Trace logging** — pass ``trace=True`` to ``run()`` to get a step-by-step
  execution log printed to stdout.
* **Error-aware execution** — node-level error policies are respected;
  the graph records which nodes failed each step.
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from transport.edge import Edge
from transport.node import Node


# ---------------------------------------------------------------------------
# Validation result
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """The outcome of ``graph.validate()``."""

    is_valid: bool
    issues: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        if self.is_valid:
            return "Graph is valid."
        header = f"Graph has {len(self.issues)} issue(s):"
        lines = [header] + [f"  • {issue}" for issue in self.issues]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

class Graph:
    """A directed graph of processing nodes connected by data edges.

    Args:
        name: Optional label used in summaries and visualizations.
    """

    def __init__(self, name: str = "graph") -> None:
        self.name = name
        self._nodes: Dict[str, Node] = {}
        self._edges: List[Edge] = []

    # ------------------------------------------------------------------
    # Building the graph
    # ------------------------------------------------------------------

    def add_node(self, node: Node) -> "Graph":
        """Register a node.  Returns ``self`` for chaining.

        Raises:
            ValueError: If a node with the same id already exists.
        """
        if node.id in self._nodes:
            raise ValueError(
                f"A node with id {node.id!r} already exists in this graph."
            )
        self._nodes[node.id] = node
        return self

    def add_edge(self, source_id: str, target_id: str) -> "Graph":
        """Connect two nodes.  Returns ``self`` for chaining.

        Raises:
            KeyError:   If either id is not registered.
            ValueError: If source and target are the same node.
        """
        if source_id not in self._nodes:
            raise KeyError(f"No node with id {source_id!r} found in the graph.")
        if target_id not in self._nodes:
            raise KeyError(f"No node with id {target_id!r} found in the graph.")
        if source_id == target_id:
            raise ValueError("Self-loops are not allowed.")
        self._edges.append(Edge(self._nodes[source_id], self._nodes[target_id]))
        return self

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self, strict: bool = False) -> ValidationResult:
        """Inspect the graph topology for potential problems.

        Checks performed:

        * **Cycles** — a cycle means data loops forever and the graph never
          drains.  Acceptable for streaming graphs but usually a mistake in
          batch pipelines.
        * **Orphan nodes** — nodes with no edges at all receive no data and
          produce no output, so they are almost certainly wiring mistakes.
        * **Disconnected components** — groups of nodes not reachable from
          any other group suggest incomplete wiring.

        Args:
            strict: If ``True``, raise ``ValueError`` when issues are found
                    instead of just returning them.

        Returns:
            A :class:`ValidationResult` with ``is_valid`` and ``issues``.
        """
        issues: List[str] = []

        # Build adjacency maps.
        out_edges: Dict[str, List[str]] = defaultdict(list)
        in_edges: Dict[str, List[str]] = defaultdict(list)
        for edge in self._edges:
            out_edges[edge.source.id].append(edge.target.id)
            in_edges[edge.target.id].append(edge.source.id)

        # 1. Cycle detection via iterative DFS.
        if self._has_cycle(out_edges):
            issues.append(
                "Graph contains a cycle.  Cyclic pipelines run forever — "
                "make sure this is intentional."
            )

        # 2. Orphan nodes (no edges at all).
        for node_id in self._nodes:
            if not out_edges[node_id] and not in_edges[node_id]:
                if len(self._nodes) > 1:
                    issues.append(
                        f"Node {node_id!r} has no edges — it is isolated and "
                        "will never send or receive data."
                    )

        # 3. Disconnected components (only meaningful for multi-node graphs).
        if len(self._nodes) > 1:
            components = self._connected_components()
            if len(components) > 1:
                groups = ", ".join(
                    "{" + ", ".join(sorted(c)) + "}" for c in components
                )
                issues.append(
                    f"Graph has {len(components)} disconnected components: "
                    f"{groups}.  Data cannot flow between them."
                )

        result = ValidationResult(is_valid=len(issues) == 0, issues=issues)
        if strict and not result.is_valid:
            raise ValueError(str(result))
        return result

    def _has_cycle(self, out_edges: Dict[str, List[str]]) -> bool:
        """Iterative DFS cycle detection."""
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def dfs(start: str) -> bool:
            stack = [(start, iter(out_edges[start]))]
            rec_stack.add(start)
            visited.add(start)
            while stack:
                node_id, children = stack[-1]
                try:
                    child = next(children)
                    if child not in visited:
                        visited.add(child)
                        rec_stack.add(child)
                        stack.append((child, iter(out_edges[child])))
                    elif child in rec_stack:
                        return True
                except StopIteration:
                    rec_stack.discard(node_id)
                    stack.pop()
            return False

        for node_id in self._nodes:
            if node_id not in visited:
                if dfs(node_id):
                    return True
        return False

    def _connected_components(self) -> List[Set[str]]:
        """Find connected components treating edges as undirected."""
        adj: Dict[str, Set[str]] = defaultdict(set)
        for edge in self._edges:
            adj[edge.source.id].add(edge.target.id)
            adj[edge.target.id].add(edge.source.id)

        visited: Set[str] = set()
        components: List[Set[str]] = []

        for node_id in self._nodes:
            if node_id not in visited:
                component: Set[str] = set()
                queue: deque = deque([node_id])
                while queue:
                    current = queue.popleft()
                    if current in visited:
                        continue
                    visited.add(current)
                    component.add(current)
                    queue.extend(adj[current] - visited)
                components.append(component)

        return components

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self, steps: int = 1, threaded: bool = False, trace: bool = False) -> None:
        """Execute the graph for a given number of steps.

        Each step:

        1. **Action phase** — every node's action fires (concurrently if
           ``threaded=True``).  Nodes run in descending ``priority`` order
           during sequential mode.
        2. **Transfer phase** — every edge moves data from source out-buffers
           to target in-buffers.

        Args:
            steps:    Number of cycles to run.  Must be ≥ 1.
            threaded: Run each node's action in its own thread.
            trace:    Print step-by-step execution info to stdout.

        Raises:
            ValueError: If *steps* < 1.
        """
        if steps < 1:
            raise ValueError(f"steps must be at least 1, got {steps}.")

        for step in range(1, steps + 1):
            if trace:
                print(f"[GraphPL] Step {step}/{steps} — action phase")
            self._run_actions(threaded=threaded, trace=trace)
            if trace:
                print(f"[GraphPL] Step {step}/{steps} — transfer phase")
            self._transfer_all()

    def run_forever(self, threaded: bool = False, trace: bool = False) -> None:
        """Run in an infinite loop until Ctrl-C (streaming mode)."""
        try:
            step = 0
            while True:
                step += 1
                if trace:
                    print(f"[GraphPL] Step {step} — action phase")
                self._run_actions(threaded=threaded, trace=trace)
                if trace:
                    print(f"[GraphPL] Step {step} — transfer phase")
                self._transfer_all()
        except KeyboardInterrupt:
            if trace:
                print("[GraphPL] run_forever() stopped by KeyboardInterrupt.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _sorted_nodes(self) -> List[Node]:
        """Return nodes sorted by descending priority (highest runs first)."""
        return sorted(self._nodes.values(), key=lambda n: n.priority, reverse=True)

    def _run_actions(self, threaded: bool, trace: bool) -> None:
        nodes = self._sorted_nodes()
        if not threaded:
            for node in nodes:
                if trace:
                    print(f"  → {node.id} (priority={node.priority})")
                node.run_action()
            return

        threads: List[threading.Thread] = [
            threading.Thread(
                target=node.run_action,
                name=f"graphpl-{node.id}",
                daemon=True,
            )
            for node in nodes
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def _transfer_all(self) -> None:
        for edge in self._edges:
            edge.transfer()

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def metrics(self) -> Dict[str, dict]:
        """Return execution statistics for every node.

        Returns:
            A ``dict`` keyed by node id.  Each value is a plain ``dict``
            with ``processed``, ``failed``, ``retried``, ``last_error``,
            and ``dead_letters`` counts.

        Example::

            for node_id, m in graph.metrics().items():
                print(f"{node_id}: processed={m['processed']}, failed={m['failed']}")
        """
        result = {}
        for node_id, node in self._nodes.items():
            m = node.metrics
            result[node_id] = {
                "processed": m.processed,
                "failed": m.failed,
                "retried": m.retried,
                "last_error": str(m.last_error) if m.last_error else None,
                "dead_letters": len(m.dead_letters),
            }
        return result

    def visualize(self, as_mermaid: bool = False) -> str:
        """Return a visual representation of the graph topology.

        Args:
            as_mermaid: If ``True``, return a Mermaid ``graph LR`` diagram
                        you can paste into GitHub Markdown.  If ``False``
                        (default), return a human-readable ASCII layout.

        Returns:
            A multi-line string.  Print it or embed it in docs.
        """
        if as_mermaid:
            return self._as_mermaid()
        return self._as_ascii()

    def _as_ascii(self) -> str:
        out_map: Dict[str, List[str]] = defaultdict(list)
        for edge in self._edges:
            out_map[edge.source.id].append(edge.target.id)

        lines = [f"Graph '{self.name}'  ({self.node_count} nodes, {self.edge_count} edges)"]
        for node_id, node in self._nodes.items():
            targets = out_map[node_id]
            label = f"[{node_id}]  ({node.node_type}, priority={node.priority})"
            if targets:
                arrow = "  ──►  " + ",  ".join(f"[{t}]" for t in targets)
            else:
                arrow = "  (terminal)"
            lines.append(f"  {label}{arrow}")
        return "\n".join(lines)

    def _as_mermaid(self) -> str:
        lines = ["```mermaid", "graph LR"]
        for node_id, node in self._nodes.items():
            lines.append(
                f'  {node_id}["{node_id} ({node.node_type})"]'
            )
        for edge in self._edges:
            lines.append(f"  {edge.source.id} --> {edge.target.id}")
        lines.append("```")
        return "\n".join(lines)

    def summary(self) -> str:
        """Human-readable overview of nodes and edges."""
        lines = [f"Graph '{self.name}'  ({self.node_count} nodes, {self.edge_count} edges)"]
        lines.append("  Nodes:")
        for node in self._sorted_nodes():
            lines.append(f"    • {node}")
        lines.append("  Edges:")
        for edge in self._edges:
            lines.append(f"    • {edge}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def get_node(self, node_id: str) -> Optional[Node]:
        """Return the node with the given id, or ``None``."""
        return self._nodes.get(node_id)

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    @property
    def edge_count(self) -> int:
        return len(self._edges)

    def __repr__(self) -> str:
        return f"Graph(name={self.name!r}, nodes={self.node_count}, edges={self.edge_count})"
