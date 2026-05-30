# GraphPL

A generic, high-throughput runtime framework for graph processing in Python.

GraphPL lets you build **data pipelines as directed graphs**. You wire up
nodes with edges and the framework takes care of moving data between them,
running every node's processing logic, and optionally parallelising execution
across threads.

---

## Core concepts

```
[Source] ──► [Processor] ──► [Sink]
```

| Concept | What it is |
|---------|-----------|
| **Node** | An independent processing unit with an input buffer, an output buffer, and a user-defined action function. |
| **Edge** | A directed channel from one node's output to another node's input. After each step it drains data across the connection. |
| **Graph** | The orchestrator — holds all nodes and edges, drives execution step by step. |
| **RingBuffer** | A fixed-size circular buffer used for both the in and out channels of every node. It never allocates beyond its initial capacity. |

---

## How it works

Each call to `graph.run(steps=N)` does the following `N` times:

1. **Action phase** — every node's action function fires. It reads from the
   node's in-buffer, does its work, and writes results to the node's out-buffer.
2. **Transfer phase** — every edge drains its source node's out-buffer and
   pushes those items into the target node's in-buffer, making them available
   for the next step.

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Quick start

```python
from transport import Graph, Node

# 1. Define action functions
def source_action(in_buf, out_buf):
    # Pass whatever arrived in the in-buffer straight through.
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop())

def doubler_action(in_buf, out_buf):
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop() * 2)

def sink_action(in_buf, out_buf):
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop())

# 2. Create nodes
source = Node(id="source", in_size=16, out_size=16, action=source_action, node_type="source")
worker = Node(id="worker", in_size=16, out_size=16, action=doubler_action, node_type="processor")
sink   = Node(id="sink",   in_size=16, out_size=16, action=sink_action,   node_type="sink")

# 3. Build the graph
g = Graph(name="my-pipeline")
g.add_node(source).add_node(worker).add_node(sink)
g.add_edge("source", "worker").add_edge("worker", "sink")

print(g.summary())

# 4. Feed data into the source
for i in range(1, 11):
    source.write(i)

# 5. Run for enough steps for data to flow through all three nodes
g.run(steps=2)

# 6. Collect results from the sink
print(sink.read_all())  # [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
```

Run the bundled demo:

```bash
python main.py
```

---

## API reference

### `Node`

```python
Node(
    id: str,                          # unique identifier
    in_size: int,                     # in-buffer capacity
    out_size: int,                    # out-buffer capacity
    action: Callable[[RingBuffer, RingBuffer], None],
    node_type: str = "any",           # "source" | "processor" | "sink" | "any"
)
```

| Method | Description |
|--------|-------------|
| `node.run_action()` | Fire the action with the node's current buffers. |
| `node.write(item)` | Push an item directly into the in-buffer (handy for source nodes). |
| `node.read_all()` | Drain and return every item from the out-buffer (handy for sink nodes). |
| `node.peek_out()` | Return the next item from the out-buffer without removing it. |

### `Graph`

```python
Graph(name: str = "graph")
```

| Method | Description |
|--------|-------------|
| `g.add_node(node)` | Register a node. Returns `self` for chaining. |
| `g.add_edge(source_id, target_id)` | Connect two nodes. Returns `self` for chaining. |
| `g.run(steps=1, threaded=False)` | Run the graph for N steps. |
| `g.run_forever(threaded=False)` | Run indefinitely until Ctrl-C (streaming mode). |
| `g.get_node(node_id)` | Look up a node by id. |
| `g.summary()` | Human-readable overview of nodes and edges. |

### `Edge`

Created automatically by `g.add_edge()`. You rarely need to instantiate one
directly.

| Method | Description |
|--------|-------------|
| `edge.transfer()` | Move items from source out-buffer to target in-buffer. Returns the count transferred. |

### `RingBuffer`

A fixed-capacity FIFO. When the buffer is full, `append()` overwrites the
oldest item (like a circular log).

| Method | Description |
|--------|-------------|
| `buf.append(item)` | Add an item (overwrites oldest if full). |
| `buf.pop()` | Remove and return the oldest item. Raises `IndexError` if empty. |
| `buf.is_empty()` | `True` when there are no items. |
| `buf.is_full()` | `True` when the buffer has reached capacity. |

---

## Threading

Pass `threaded=True` to `run()` to execute all node actions concurrently in
separate threads during the action phase. The transfer phase always runs on
the main thread after all node threads have finished.

```python
g.run(steps=10, threaded=True)
```

This is useful when node actions involve I/O or other blocking work. For
purely CPU-bound numeric pipelines the sequential mode is simpler and
completely deterministic.

---

## Running tests

```bash
pytest tests/ -v
```

---

## Project layout

```
GraphPL/
├── transport/
│   ├── __init__.py   # Public exports: Graph, Node, Edge, RingBuffer
│   ├── buffer.py     # RingBuffer — fixed-capacity circular buffer
│   ├── node.py       # Node — processing unit with in/out buffers
│   ├── edge.py       # Edge — directed data channel between nodes
│   └── graph.py      # Graph — orchestrates nodes and edges
├── tests/
│   ├── test_buffer.py
│   ├── test_node.py
│   ├── test_edge.py
│   └── test_graph.py
├── main.py           # Demo pipeline (source → doubler → sink)
└── requirements.txt
```

---

## License

MIT
