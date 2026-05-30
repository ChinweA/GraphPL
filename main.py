"""
GraphPL demo — a three-node pipeline.

    [source] ──► [doubler] ──► [printer]

- source:  pushes the integers 1 through 10 into the pipeline
- doubler: reads each integer and writes back double the value
- printer: collects the final results and prints them

Run with:
    python main.py
"""

from transport import Graph, Node


# ---------------------------------------------------------------------------
# Node action functions
# ---------------------------------------------------------------------------

def source_action(in_buf, out_buf):
    """Source node: drain whatever was written into in_buf and pass it on."""
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop())


def doubler_action(in_buf, out_buf):
    """Processor node: multiply every incoming value by 2."""
    while not in_buf.is_empty():
        value = in_buf.pop()
        out_buf.append(value * 2)


def printer_action(in_buf, out_buf):
    """Sink node: collect results in the out_buf so we can read them later."""
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop())


# ---------------------------------------------------------------------------
# Build the graph
# ---------------------------------------------------------------------------

source  = Node(id="source",  in_size=16, out_size=16, action=source_action,  node_type="source")
doubler = Node(id="doubler", in_size=16, out_size=16, action=doubler_action, node_type="processor")
printer = Node(id="printer", in_size=16, out_size=16, action=printer_action, node_type="sink")

g = Graph(name="demo-pipeline")
g.add_node(source).add_node(doubler).add_node(printer)
g.add_edge("source", "doubler").add_edge("doubler", "printer")

print(g.summary())
print()

# ---------------------------------------------------------------------------
# Feed data and run
# ---------------------------------------------------------------------------

# Push the numbers 1-10 into the source node's in-buffer.
for i in range(1, 11):
    source.write(i)

# Three steps are needed — one per hop in the chain:
#   Step 1 — source fires (pushes to out-buf).  Edges transfer: source-out → doubler-in.
#   Step 2 — doubler fires (doubles, pushes to out-buf).  Edges transfer: doubler-out → printer-in.
#   Step 3 — printer fires (collects into its own out-buf, ready to read_all).
g.run(steps=3)

# ---------------------------------------------------------------------------
# Collect and display results
# ---------------------------------------------------------------------------

results = printer.read_all()
print(f"Input : {list(range(1, 11))}")
print(f"Output: {results}")

assert results == [i * 2 for i in range(1, 11)], "Pipeline output mismatch!"
print("\nAll good — pipeline works correctly.")
