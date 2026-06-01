"""
Tests for new Node features:
  - priority field
  - error_policy: skip, retry, dead_letter, fail_fast
  - NodeMetrics
  - reset_metrics()
"""

import pytest

from transport.node import Node


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def noop(in_buf, out_buf):
    pass


def echo_action(in_buf, out_buf):
    while not in_buf.is_empty():
        out_buf.append(in_buf.pop())


def always_fail(in_buf, out_buf):
    raise RuntimeError("action intentionally failed")


def fail_once_then_succeed(counter):
    """Returns an action that fails on first call, succeeds after."""
    calls = {"n": 0}

    def action(in_buf, out_buf):
        calls["n"] += 1
        if calls["n"] <= counter:
            raise RuntimeError(f"simulated failure #{calls['n']}")
        while not in_buf.is_empty():
            out_buf.append(in_buf.pop())

    return action


def make_node(action=None, error_policy="fail_fast", max_retries=3,
              priority=0, node_type="any") -> Node:
    return Node(
        id="test",
        in_size=8,
        out_size=8,
        action=action or noop,
        error_policy=error_policy,
        max_retries=max_retries,
        priority=priority,
        node_type=node_type,
    )


# ---------------------------------------------------------------------------
# Priority
# ---------------------------------------------------------------------------

class TestPriority:
    def test_default_priority_is_zero(self):
        node = make_node()
        assert node.priority == 0

    def test_custom_priority(self):
        node = make_node(priority=10)
        assert node.priority == 10


# ---------------------------------------------------------------------------
# Error policy: fail_fast
# ---------------------------------------------------------------------------

class TestErrorPolicyFailFast:
    def test_reraises_exception(self):
        node = make_node(action=always_fail, error_policy="fail_fast")
        with pytest.raises(RuntimeError, match="intentionally failed"):
            node.run_action()

    def test_metrics_failed_incremented(self):
        node = make_node(action=always_fail, error_policy="fail_fast")
        try:
            node.run_action()
        except RuntimeError:
            pass
        assert node.metrics.failed == 1
        assert node.metrics.processed == 0


# ---------------------------------------------------------------------------
# Error policy: skip
# ---------------------------------------------------------------------------

class TestErrorPolicySkip:
    def test_does_not_raise(self):
        node = make_node(action=always_fail, error_policy="skip")
        node.run_action()   # must not raise

    def test_metrics_failed_incremented(self):
        node = make_node(action=always_fail, error_policy="skip")
        node.run_action()
        assert node.metrics.failed == 1
        assert node.metrics.processed == 0

    def test_last_error_recorded(self):
        node = make_node(action=always_fail, error_policy="skip")
        node.run_action()
        assert isinstance(node.metrics.last_error, RuntimeError)

    def test_subsequent_calls_work(self):
        call_count = {"n": 0}

        def flaky(in_buf, out_buf):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("first call fails")
            out_buf.append("ok")

        node = make_node(action=flaky, error_policy="skip")
        node.run_action()   # fails, skipped
        node.run_action()   # succeeds
        assert node.read_all() == ["ok"]
        assert node.metrics.processed == 1
        assert node.metrics.failed == 1


# ---------------------------------------------------------------------------
# Error policy: retry
# ---------------------------------------------------------------------------

class TestErrorPolicyRetry:
    def test_retries_and_eventually_succeeds(self):
        action = fail_once_then_succeed(counter=2)
        node = make_node(action=action, error_policy="retry", max_retries=3)
        node.write("data")
        node.run_action()   # fails twice, succeeds on 3rd attempt
        assert node.metrics.processed == 1
        assert node.metrics.retried == 2

    def test_exhausted_retries_records_failure(self):
        node = make_node(action=always_fail, error_policy="retry", max_retries=2)
        node.run_action()   # retries 2 times, then skips
        assert node.metrics.failed == 1
        assert node.metrics.retried == 2

    def test_retried_count_accumulates(self):
        action = fail_once_then_succeed(counter=1)
        node = make_node(action=action, error_policy="retry", max_retries=3)
        node.write("x")
        node.run_action()
        assert node.metrics.retried == 1


# ---------------------------------------------------------------------------
# Error policy: dead_letter
# ---------------------------------------------------------------------------

class TestErrorPolicyDeadLetter:
    def test_stores_exception_in_dead_letters(self):
        node = make_node(action=always_fail, error_policy="dead_letter")
        node.run_action()
        assert len(node.metrics.dead_letters) == 1
        assert isinstance(node.metrics.dead_letters[0], RuntimeError)

    def test_does_not_raise(self):
        node = make_node(action=always_fail, error_policy="dead_letter")
        node.run_action()   # must not raise

    def test_dead_letters_accumulate(self):
        node = make_node(action=always_fail, error_policy="dead_letter")
        node.run_action()
        node.run_action()
        assert len(node.metrics.dead_letters) == 2


# ---------------------------------------------------------------------------
# reset_metrics
# ---------------------------------------------------------------------------

class TestResetMetrics:
    def test_reset_clears_all_fields(self):
        node = make_node(action=always_fail, error_policy="skip")
        node.run_action()
        node.run_action()
        node.reset_metrics()
        assert node.metrics.processed == 0
        assert node.metrics.failed == 0
        assert node.metrics.last_error is None
