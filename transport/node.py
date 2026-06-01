"""
Node — the fundamental processing unit in a GraphPL graph.

Each node owns two ring buffers and a user-supplied action function.
On every graph tick, ``run_action()`` is called.  Error handling,
retry logic, and per-node execution metrics are all built in.

Error policies
--------------
``"fail_fast"``   Re-raise the exception immediately (default).
``"skip"``        Catch the exception, record it, and move on.
``"retry"``       Retry up to *max_retries* times, then apply the
                  fallback policy (``"skip"`` after retries exhausted).
``"dead_letter"`` Like ``"skip"`` but also stores the exception in the
                  node's dead-letter log for later inspection.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, List, Literal, Optional

from pydantic import BaseModel, PrivateAttr

from transport.buffer import RingBuffer

NodeType = Literal["source", "processor", "sink", "any"]
ErrorPolicy = Literal["fail_fast", "skip", "retry", "dead_letter"]


@dataclass
class NodeMetrics:
    """Execution statistics for a single node."""

    processed: int = 0
    """Number of successful ``run_action()`` calls."""

    failed: int = 0
    """Number of calls that raised an exception (after retries)."""

    retried: int = 0
    """Total retry attempts across all ``run_action()`` calls."""

    last_error: Optional[Exception] = None
    """The most recent exception, if any."""

    dead_letters: List[Exception] = field(default_factory=list)
    """Exceptions captured when ``error_policy="dead_letter"``."""


class Node(BaseModel):
    """A single processing node in the graph.

    Args:
        id:           Unique string identifier.
        in_size:      Capacity of the input ring buffer.
        out_size:     Capacity of the output ring buffer.
        action:       ``(in_buf, out_buf) -> None``.
        node_type:    Informational label — ``"source"``, ``"processor"``,
                      ``"sink"``, or ``"any"`` (default).
        priority:     Execution priority.  Higher values run first within a
                      graph step.  Defaults to ``0``.
        error_policy: What to do when ``action`` raises an exception.
        max_retries:  Maximum retry attempts when ``error_policy="retry"``.
    """

    id: str
    in_size: int
    out_size: int
    action: Callable[["RingBuffer", "RingBuffer"], None]
    node_type: NodeType = "any"
    priority: int = 0
    error_policy: ErrorPolicy = "fail_fast"
    max_retries: int = 3

    _in_buffer: RingBuffer = PrivateAttr()
    _out_buffer: RingBuffer = PrivateAttr()
    _metrics: NodeMetrics = PrivateAttr()

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._in_buffer = RingBuffer(size=self.in_size)
        self._out_buffer = RingBuffer(size=self.out_size)
        self._metrics = NodeMetrics()

    # ------------------------------------------------------------------
    # Core execution
    # ------------------------------------------------------------------

    def run_action(self) -> None:
        """Execute the node's action, applying the configured error policy."""
        attempts = 0
        max_attempts = (self.max_retries + 1) if self.error_policy == "retry" else 1

        while attempts < max_attempts:
            try:
                self.action(self._in_buffer, self._out_buffer)
                self._metrics.processed += 1
                return
            except Exception as exc:
                attempts += 1
                self._metrics.last_error = exc

                if self.error_policy == "retry" and attempts < max_attempts:
                    self._metrics.retried += 1
                    time.sleep(0.01 * attempts)   # brief back-off between retries
                    continue

                # All attempts exhausted or non-retry policy.
                self._metrics.failed += 1

                if self.error_policy == "fail_fast":
                    raise

                if self.error_policy == "dead_letter":
                    self._metrics.dead_letters.append(exc)

                # "skip" and "dead_letter" both swallow the exception here.
                return

    # ------------------------------------------------------------------
    # Convenience helpers for source / sink nodes
    # ------------------------------------------------------------------

    def write(self, item: Any) -> None:
        """Push an item directly into this node's in-buffer (source helper)."""
        self._in_buffer.append(item)

    def read_all(self) -> List[Any]:
        """Drain and return every item from this node's out-buffer (sink helper)."""
        results: List[Any] = []
        while not self._out_buffer.is_empty():
            results.append(self._out_buffer.pop())
        return results

    def peek_out(self) -> Optional[Any]:
        """Return the next out-buffer item without removing it, or ``None``."""
        if self._out_buffer.is_empty():
            return None
        item = self._out_buffer.pop()
        self._out_buffer.append(item)
        return item

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    @property
    def metrics(self) -> NodeMetrics:
        """Live execution statistics for this node."""
        return self._metrics

    def reset_metrics(self) -> None:
        """Reset all counters and error records back to zero."""
        self._metrics = NodeMetrics()

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"Node(id={self.id!r}, type={self.node_type!r}, "
            f"priority={self.priority}, "
            f"in_size={self.in_size}, out_size={self.out_size})"
        )
