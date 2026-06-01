"""
RingBuffer — a thread-safe, fixed-capacity circular queue.

Thread safety is guaranteed via an internal reentrant lock.  Every public
method is safe to call from multiple threads simultaneously.

Overflow strategies
-------------------
``"overwrite"``   Silently drop the oldest item (classic ring-buffer, default).
``"drop_newest"`` Silently discard the incoming item.
``"block"``       Block the caller until space is available.
``"raise"``       Raise ``BufferError`` immediately.
"""

from __future__ import annotations

import threading
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field, PrivateAttr

OverflowStrategy = Literal["overwrite", "drop_newest", "block", "raise"]


class RingBuffer(BaseModel):
    """Fixed-capacity FIFO ring buffer with configurable overflow behaviour.

    Args:
        size:     Maximum number of items the buffer can hold.
        overflow: What to do when :meth:`append` is called on a full buffer.
    """

    size: int
    overflow: OverflowStrategy = "overwrite"
    buffer: List[Optional[Any]] = Field(default_factory=list)

    _head: int = PrivateAttr(default=0)
    _tail: int = PrivateAttr(default=0)
    _full: bool = PrivateAttr(default=False)
    _lock: Any = PrivateAttr()           # threading.RLock
    _not_full: Any = PrivateAttr()       # threading.Condition

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.buffer = [None] * self.size
        self._lock = threading.RLock()
        self._not_full = threading.Condition(self._lock)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def append(self, item: Any, block_timeout: float = 5.0) -> bool:
        """Add an item to the buffer.

        Args:
            item:          The value to enqueue.
            block_timeout: Seconds to wait when ``overflow="block"``.

        Returns:
            ``True`` if the item was added; ``False`` if dropped
            (only with ``overflow="drop_newest"``).

        Raises:
            BufferError:  When ``overflow="raise"`` and the buffer is full.
            TimeoutError: When ``overflow="block"`` and no space opens in time.
        """
        with self._not_full:
            if self._full:
                if self.overflow == "overwrite":
                    # Drop the oldest item to make room.
                    self._tail = (self._tail + 1) % self.size
                elif self.overflow == "drop_newest":
                    return False
                elif self.overflow == "raise":
                    raise BufferError(
                        f"RingBuffer is full (capacity={self.size}). "
                        "Increase size or choose a different overflow strategy."
                    )
                elif self.overflow == "block":
                    acquired = self._not_full.wait_for(
                        lambda: not self._full, timeout=block_timeout
                    )
                    if not acquired:
                        raise TimeoutError(
                            f"RingBuffer blocked for {block_timeout}s "
                            "waiting for free space."
                        )

            self.buffer[self._head] = item
            self._head = (self._head + 1) % self.size
            self._full = self._head == self._tail
            self._not_full.notify_all()
            return True

    def pop(self) -> Any:
        """Remove and return the oldest item.

        Raises:
            IndexError: If the buffer is empty.
        """
        with self._not_full:
            if self._head == self._tail and not self._full:
                raise IndexError("Buffer is empty")
            item = self.buffer[self._tail]
            self.buffer[self._tail] = None
            self._tail = (self._tail + 1) % self.size
            self._full = False
            self._not_full.notify_all()
            return item

    def is_empty(self) -> bool:
        """``True`` when there are no items in the buffer."""
        with self._lock:
            return not self._full and self._head == self._tail

    def is_full(self) -> bool:
        """``True`` when the buffer has reached its capacity."""
        with self._lock:
            return self._full

    def __len__(self) -> int:
        """Number of items currently stored."""
        with self._lock:
            if self._full:
                return self.size
            return (self._head - self._tail) % self.size

    def __repr__(self) -> str:
        return (
            f"RingBuffer(size={self.size}, len={len(self)}, "
            f"overflow={self.overflow!r})"
        )
