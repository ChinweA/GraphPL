from typing import Any, List, Optional
from pydantic import BaseModel, Field, PrivateAttr


class RingBuffer(BaseModel):
    size: int
    buffer: List[Optional[Any]] = Field(default_factory=list)

    _head: int = PrivateAttr(0)
    _tail: int = PrivateAttr(0)
    _full: bool = PrivateAttr(False)

    def __init__(self, **data):
        super().__init__(**data)
        self.buffer = [None] * self.size

    def append(self, item: Any):
        self.buffer[self._head] = item
        if self._full:
            self._tail = (self._tail + 1) % self.size
        self._head = (self._head + 1) % self.size
        self._full = self._head == self._tail

    def pop(self):
        if self.is_empty():
            raise IndexError("Buffer is empty")
        item = self.buffer[self._tail]
        self.buffer[self._tail] = None
        self._tail = (self._tail + 1) % self.size
        self._full = False
        return item

    def is_empty(self):
        return not self._full and self._head == self._tail

    def is_full(self):
        return self._full
