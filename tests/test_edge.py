"""Tests for flowlet.edge (Thread + Ray EdgeNode)."""

from __future__ import annotations

import threading

import pytest
from flowlet.compute_graph import InputSlot, InputVar, OutputSpec, TaskNode
from flowlet.edge import (
    DeadNodeError,
    EdgeConfig,
    RayEdgeNode,
    ThreadEdgeNode,
    UnsupportedTargetError,
)
from flowlet.edge._test_helpers import (
    AddKernel,
    DoubleWorkflow,
    add_value,
    lock_and_report,
    make_lock_state,
    multiply,
    raise_error,
    set_value,
    slow_increment,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------
@pytest.fixture(params=["thread", "ray"])
def backend(request):
    return request.param


@pytest.fixture
def make_node(backend):
    def _make(initializer=None, **kwargs):
        cls = ThreadEdgeNode if backend == "thread" else RayEdgeNode
        return cls(initializer=initializer, **kwargs)

    return _make


# ------------------------------------------------------------------
# Basic push / apply / pull
# ------------------------------------------------------------------
class TestBasicIO:
    def test_push_apply_pull(self, make_node):
        node = make_node(initializer=lambda: {"value": 10})
        node.apply(add_value, output="value", value=5)
        node.join()
        assert node.pull("value") == 15
        node.kill()

    def test_apply_returns_none(self, make_node):
        node = make_node(initializer=lambda: {"value": 1})
        result = node.apply(add_value, output="value", value=2)
        assert result is None
        node.join()
        assert node.pull("value") == 3
        node.kill()

    def test_pull_waits_pending(self, make_node):
        node = make_node(initializer=lambda: {"value": 0})
        node.apply(add_value, output="value", value=1)
        # pull should implicitly join pending writes.
        assert node.pull("value") == 1
        node.kill()

    def test_output_none_for_side_effect(self, make_node):
        node = make_node(initializer=lambda: {"value": 0})
        node.apply(set_value, output=None, value=42)
        node.join()
        assert node.pull("value") == 42
        node.kill()


# ------------------------------------------------------------------
# bind + run
# ------------------------------------------------------------------
class TestBindRun:
    def test_bind_run_function(self, make_node):
        node = make_node(initializer=lambda: {"value": 3})
        node.bind(multiply, output="value")
        node.run(factor=4)
        node.join()
        assert node.pull("value") == 12
        node.kill()

    def test_bind_run_kernel(self, make_node):
        node = make_node(initializer=lambda: {"value": 5})
        node.bind(AddKernel(), output="value")
        node.run(value=7)
        node.join()
        assert node.pull("value") == 12
        node.kill()

    def test_bind_run_workflow(self, make_node):
        node = make_node(initializer=lambda: {"value": 6})
        node.bind(DoubleWorkflow(), output="value")
        node.run()
        node.join()
        assert node.pull("value") == 12
        node.kill()


# ------------------------------------------------------------------
# TaskNode graph inside EdgeNode
# ------------------------------------------------------------------
class TestTaskNodeTarget:
    def test_apply_task_graph(self, make_node):
        load = TaskNode(
            func=lambda path: f"loaded:{path}",
            inputs=[InputSlot("path")],
            outputs=OutputSpec("single"),
            name="load",
        )
        process = TaskNode(
            func=lambda data: f"processed:{data}",
            inputs=[InputSlot("data")],
            outputs=OutputSpec("single"),
            name="process",
        )
        graph = process.bind(data=load.bind(path=InputVar("dataset_path")))

        node = make_node()
        node.push("dataset_path", "data.csv")
        node.apply(graph, output="result")
        node.join()
        assert node.pull("result") == "processed:loaded:data.csv"
        node.kill()

    def test_bind_run_task_graph(self, make_node):
        step = TaskNode(
            func=lambda x: x + 1,
            inputs=[InputSlot("x")],
            outputs=OutputSpec("single"),
            name="inc",
        )
        graph = step.bind(x=InputVar("x"))

        node = make_node()
        node.bind(graph, output="y")
        node.run(x=10)
        node.join()
        assert node.pull("y") == 11
        node.kill()

    def test_task_graph_reusable(self, make_node):
        step = TaskNode(
            func=lambda x: x * 2,
            inputs=[InputSlot("x")],
            outputs=OutputSpec("single"),
            name="double",
        )
        graph = step.bind(x=InputVar("x"))

        node = make_node()
        node.bind(graph, output="y")
        node.run(x=3)
        node.run(x=5)
        node.join()
        # last run wins
        assert node.pull("y") == 10
        node.kill()


# ------------------------------------------------------------------
# Unserializable objects
# ------------------------------------------------------------------
class TestUnserializableState:
    def test_thread_can_pull_unserializable(self):
        node = ThreadEdgeNode(initializer=make_lock_state)
        node.apply(lock_and_report, output="status")
        node.join()
        # Thread backend shares process, so pull returns the actual Lock object.
        lock = node.pull("lock")
        assert isinstance(lock, type(threading.Lock()))
        assert node.pull("status") == "acquired"
        node.kill()

    def test_ray_cannot_pull_unserializable(self):
        node = RayEdgeNode(initializer=make_lock_state)
        node.apply(lock_and_report, output="status")
        node.join()
        assert node.pull("status") == "acquired"
        with pytest.raises(Exception):
            node.pull("lock")
        node.kill()


# ------------------------------------------------------------------
# Batch IO
# ------------------------------------------------------------------
class TestBatchIO:
    def test_push_many_pull_many(self, make_node):
        node = make_node()
        node.push_many({"a": 1, "b": 2, "c": 3})
        node.join()
        assert node.pull_many(["a", "c"]) == {"a": 1, "c": 3}
        node.kill()


# ------------------------------------------------------------------
# Lifecycle
# ------------------------------------------------------------------
class TestLifecycle:
    def test_context_manager_closes(self, make_node):
        with make_node(initializer=lambda: {"value": 0}) as node:
            node.apply(add_value, output="value", value=1)
            node.join()
            assert node.pull("value") == 1
        assert not node.alive

    def test_context_manager_exception_kills(self, make_node):
        node = None
        with pytest.raises(RuntimeError):
            with make_node() as node:
                raise RuntimeError("fail")
        assert node is not None
        assert not node.alive

    def test_join_close(self, make_node):
        node = make_node(initializer=lambda: {"value": 0})
        node.apply(slow_increment, output="value")
        node.join()
        assert node.pull("value") == 1
        node.close()
        assert not node.alive

    def test_kill_drops_pending(self, make_node):
        node = make_node(initializer=lambda: {"value": 0})
        node.apply(slow_increment, output="value")
        node.kill()
        assert not node.alive

    def test_operation_after_close_raises(self, make_node):
        node = make_node()
        node.close()
        with pytest.raises(DeadNodeError):
            node.push("x", 1)


# ------------------------------------------------------------------
# Exceptions
# ------------------------------------------------------------------
class TestExceptions:
    def test_apply_exception_propagates_on_join(self, make_node):
        node = make_node()
        node.apply(raise_error, output="x")
        with pytest.raises(ValueError, match="boom"):
            node.join()
        node.kill()

    def test_pull_after_exception_reads_old_state(self, make_node):
        node = make_node(initializer=lambda: {"x": 42})
        node.apply(raise_error, output="x")
        with pytest.raises(ValueError):
            node.join()
        assert node.pull("x") == 42
        node.kill()


# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
class TestConfig:
    def test_ray_resource_config(self):
        config = EdgeConfig(num_cpus=0.5, num_gpus=0)
        node = RayEdgeNode(initializer=lambda: {"value": 1}, config=config)
        assert node._backend.config.num_cpus == 0.5
        node.kill()

    def test_thread_ignores_ray_fields(self):
        config = EdgeConfig(num_cpus=2.0)
        node = ThreadEdgeNode(initializer=lambda: {"value": 1}, config=config)
        assert node._backend.config.num_cpus == 2.0
        node.kill()


# ------------------------------------------------------------------
# Unsupported target
# ------------------------------------------------------------------
class TestUnsupportedTarget:
    def test_apply_unsupported_type(self, make_node):
        node = make_node()
        node.apply("not-a-target", output="x")
        with pytest.raises(UnsupportedTargetError):
            node.join()
        node.kill()
