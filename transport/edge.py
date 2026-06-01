"""
Edge — a directed data channel between two nodes.

An edge transfers items from the out-buffer of a source node into the
in-buffer of a target node.  Transfer respects back-pressure: if the
target's in-buffer is full, remaining items stay in the source's
out-buffer so nothing is silently dropped.

You can limit how many items move per transfer call with the
``max_items`` parameter — useful for flow control on hot edges.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from transport.node import Node


class Edge:
    """A directed data channel from one node's output to another's input.

    Args:
        source:    Node whose out-buffer is drained.
        target:    Node whose in-buffer receives the data.
    """

    def __init__(self, source: "Node", target: "Node") -> None:
        self.source = source
        self.target = target

    # ------------------------------------------------------------------
    # Data movement
    # ------------------------------------------------------------------

    def transfer(self, max_items: int = -1) -> int:
        """Move pending items from source → target.

        Args:
            max_items: Maximum number of items to transfer in one call.
                       ``-1`` (default) means unlimited — drain everything
                       that fits.  Use a positive value for flow control
                       (e.g. ``max_items=100`` for batch processing).

        Returns:
            The number of items actually transferred.
        """
        moved = 0
        while not self.source._out_buffer.is_empty():
            if self.target._in_buffer.is_full():
                break   # back-pressure: leave items in source for next tick
            if max_items != -1 and moved >= max_items:
                break   # batch limit reached
            item = self.source._out_buffer.pop()
            self.target._in_buffer.append(item)
            moved += 1
        return moved

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"Edge({self.source.id!r} → {self.target.id!r})"
