"""
Edge — a directed connection between two nodes.

An edge sits between the out-buffer of a source node and the in-buffer of a
target node.  Calling transfer() drains every item currently sitting in the
source's out-buffer and pushes each one into the target's in-buffer, so that
the target can consume it on its next action cycle.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from transport.node import Node


class Edge:
    """A directed data channel from one node's output to another node's input.

    Args:
        source: The node whose out-buffer will be drained.
        target: The node whose in-buffer will receive the drained items.
    """

    def __init__(self, source: "Node", target: "Node") -> None:
        self.source = source
        self.target = target

    # ------------------------------------------------------------------
    # Data movement
    # ------------------------------------------------------------------

    def transfer(self) -> int:
        """Move all pending items from source → target.

        Drains the source node's out-buffer one item at a time and appends
        each item to the target node's in-buffer.  If the target's in-buffer
        is full, remaining items are left in the source's out-buffer so
        nothing is silently dropped.

        Returns:
            The number of items that were actually transferred.
        """
        moved = 0
        while not self.source._out_buffer.is_empty():
            if self.target._in_buffer.is_full():
                # Back-pressure: target can't accept more right now.
                break
            item = self.source._out_buffer.pop()
            self.target._in_buffer.append(item)
            moved += 1
        return moved

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"Edge({self.source.id!r} → {self.target.id!r})"
