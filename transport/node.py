from typing import Callable
from pydantic import BaseModel, PrivateAttr
from transport.buffer import RingBuffer


class Node(BaseModel):
    id: str
    in_size: int
    out_size: int
    action: Callable[[RingBuffer, RingBuffer], None]

    _in_buffer: RingBuffer = PrivateAttr()
    _out_buffer: RingBuffer = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)
        self._in_buffer = RingBuffer(size=self.in_size)
        self._out_buffer = RingBuffer(size=self.out_size)

    def run_action(self):
        return self.action(self._in_buffer, self._out_buffer)
