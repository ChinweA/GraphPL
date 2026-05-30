"""
Node — the fundamental processing unit in a GraphPL graph.

A node owns two ring buffers (in and out) and a single user-supplied action
function.  When the graph ticks, it calls run_action(), which hands both
buffers to the action so it can read incoming items, do whatever work it
likes, and write results to the out-buffer.

Convenience helpers make it easy to push data in (for source nodes) and pull
results out (for sink nodes) without touching the buffer internals directly.
"""

from __future__ import annotations

from typing import Any, Callable, List, Literal, Optional

from pydantic import BaseModel, PrivateAttr

from transport.buffer import RingBuffer

# A node's role in the graph — purely informational, not enforced at runtime.
NodeType = Literal["source", "processor", "sink", "any"]


class Node(BaseModel):
    """A single processing node in the graph.

    Args:
        id:       A unique string identifier for this node.
        in_size:  Capacity of the input ring buffer.
        out_size: Capacity of the output ring buffer.
        action:   A callable with signature ``(in_buf, out_buf) -> None``.
                  It should read from *in_buf* and write results to *out_buf*.
        node_type: Optional label — ``"source"``, ``"processor"``, ``"sink"``,
                   or ``"any"`` (default).  Has no effect on execution, but
                   makes pipelines easier to reason about when printed.
    """

    id: str
    in_size: int
    out_size: int
    action: Callable[["RingBuffer", "RingBuffer"], None]
    node_type: NodeType = "any"

    # Private — not part of the public Pydantic model schema.
    _in_buffer: RingBuffer = PrivateAttr()
    _out_buffer: RingBuffer = PrivateAttr()

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._in_buffer = RingBuffer(size=self.in_size)
        self._out_buffer = RingBuffer(size=self.out_size)

    # ------------------------------------------------------------------
    # Core execution
    # ------------------------------------------------------------------

    def run_action(self) -> None:
        """Execute the node's action with its current buffer state."""
        self.action(self._in_buffer, self._out_buffer)

    # ------------------------------------------------------------------
    # Convenience helpers for source / sink nodes
    # ------------------------------------------------------------------

    def write(self, item: Any) -> None:
        """Push an item directly into this node's in-buffer.

        Handy for **source** nodes where there is no upstream edge feeding
        them — you just call ``node.write(value)`` before running the graph.

        Args:
            item: Any Python value to enqueue.
        """
        self._in_buffer.append(item)

    def read_all(self) -> List[Any]:
        """Drain and return every item from this node's out-buffer.

        Handy for **sink** nodes where you want to collect results after
        the graph has finished running.

        Returns:
            A list of all items that were in the out-buffer, in FIFO order.
        """
        results: List[Any] = []
        while not self._out_buffer.is_empty():
            results.append(self._out_buffer.pop())
        return results

    def peek_out(self) -> Optional[Any]:
        """Return the next item from the out-buffer without removing it.

        Returns ``None`` if the out-buffer is empty.
        """
        if self._out_buffer.is_empty():
            return None
        # Pop and re-append to peek without side effects.
        item = self._out_buffer.pop()
        self._out_buffer.append(item)
        return item

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"Node(id={self.id!r}, type={self.node_type!r}, "
            f"in_size={self.in_size}, out_size={self.out_size})"
        )
