from __future__ import annotations

import asyncio
import os
import time

import pytest
from flowlet import (
    Kernel,
    Scheduler,
    SignalPool,
    StateMachineSpec,
    TransitionSpec,
)
from flowlet.compute_graph import InputSlot, InputVar, OutputSpec, TaskNode
from flowlet.config import BaseConfigContainer
from flowlet.fsm import FSMEdgeNode, FSMStatus
from flowlet.scheduler_helpers import add_values, fail_target, write_signal


def _wait_until(predicate, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


@pytest.fixture
def ray_cluster(monkeypatch):
    ray = pytest.importorskip("ray")
    monkeypatch.setenv("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_cpus=2, include_dashboard=False, log_to_driver=False, ignore_reinit_error=True)
    try:
        yield ray
    finally:
        ray.shutdown()


class TestSignalPool:
    def test_update_snapshot_and_wait(self):
        pool = SignalPool()
        assert pool.ensure("ready").status == "pending"
        version = pool.version

        pool.mark_completed("ready", value=42, metadata={"source": "test"})

        signal = pool["ready"]
        assert signal.completed
        assert signal.value == 42
        assert signal.metadata == {"source": "test"}
        assert pool.version > version
        assert pool.wait_for(lambda snapshot: snapshot["ready"].completed, timeout=0.01)

    def test_ray_proxy_roundtrip(self, ray_cluster):
        ray = ray_cluster
        pool = SignalPool()
        proxy = pool.get_ray_proxy()

        @ray.remote
        def worker(signals):
            signals.mark_completed("ray_ready", value=7)
            return "ok"

        assert ray.get(worker.remote(proxy)) == "ok"
        assert _wait_until(lambda: pool.get("ray_ready") is not None)
        assert pool["ray_ready"].completed
        assert pool["ray_ready"].value == 7
        pool.close()


class TestScheduler:
    def test_thread_action_lifecycle_and_trigger(self):
        pool = SignalPool()
        scheduler = Scheduler(signal_pool=pool, name="sched")
        scheduler.schedule(
            add_values,
            name="add",
            args=(2,),
            kwargs={"y": 3},
            trigger=lambda signals: signals.get("go") is not None and signals["go"].completed,
            lifecycle_signal="add_status",
            result_signal="add_result",
        )

        scheduler.run_until_idle(max_ticks=3)
        assert pool["add_status"].pending

        pool.mark_completed("go")
        scheduler.run_until_idle()

        assert pool["add_status"].completed
        assert pool["add_status"].value == 5
        assert pool["add_result"].value == 5
        scheduler.close()

    def test_failed_action_updates_lifecycle_signal(self):
        pool = SignalPool()
        scheduler = Scheduler(signal_pool=pool)
        scheduler.schedule(fail_target, name="fail", lifecycle_signal="fail_status")

        scheduler.run_until_idle()

        assert pool["fail_status"].failed
        assert "scheduled failure" in pool["fail_status"].error
        scheduler.close()

    def test_coroutine_backend_awaits_async_target(self):
        async def async_add(x: int) -> int:
            await asyncio.sleep(0.01)
            return x + 1

        pool = SignalPool()
        scheduler = Scheduler(signal_pool=pool)
        scheduler.schedule(
            async_add,
            name="async_add",
            args=(4,),
            backend="coroutine",
            lifecycle_signal="async_status",
        )

        scheduler.run_until_idle()

        assert pool["async_status"].completed
        assert pool["async_status"].value == 5
        scheduler.close()

    def test_callable_receives_local_signal_pool(self):
        def target(signals):
            signals.mark_completed("inside", value="ok")
            return 1

        pool = SignalPool()
        scheduler = Scheduler(signal_pool=pool)
        scheduler.schedule(target, name="local_signal", pass_signals=True, lifecycle_signal="local_status")

        scheduler.run_until_idle()

        assert pool["inside"].value == "ok"
        assert pool["local_status"].completed
        scheduler.close()

    def test_task_node_and_executable_unit_targets(self):
        class AddConfig(BaseConfigContainer):
            inc: int = 1

        class AddKernel(Kernel[AddConfig, int]):
            config: AddConfig = AddConfig()

            def __call__(self, value: int) -> int:
                return value + self.config.inc

        graph = TaskNode(
            func=lambda x: x * 2,
            inputs=[InputSlot("x")],
            outputs=OutputSpec("single"),
            name="double",
        ).bind(x=InputVar("x"))

        pool = SignalPool()
        scheduler = Scheduler(signal_pool=pool)
        scheduler.schedule(graph, name="graph", kwargs={"x": 3}, lifecycle_signal="graph_status")
        scheduler.schedule(AddKernel(inc=4), name="kernel", args=(3,), lifecycle_signal="kernel_status")

        scheduler.run_until_idle()

        assert pool["graph_status"].value == 6
        assert pool["kernel_status"].value == 7
        scheduler.close()

    def test_fsm_target(self):
        def add_action(state, runtime, event):
            state["value"] = state.get("value", 0) + 1

        spec = StateMachineSpec.create(
            initial="new",
            terminal={"done"},
            transitions=[TransitionSpec("add", "new", "done", action=add_action)],
        )
        fsm = FSMEdgeNode(spec, backend="thread", initializer=lambda: {"value": 0})
        pool = SignalPool()
        scheduler = Scheduler(signal_pool=pool)
        scheduler.schedule(fsm, name="fsm", lifecycle_signal="fsm_status")

        scheduler.run_until_idle()

        runtime = pool["fsm_status"].value
        assert runtime.status == FSMStatus.TERMINAL
        assert fsm.pull("value") == 1
        scheduler.close()

    def test_ray_backend_and_signal_proxy(self, ray_cluster):
        pool = SignalPool()
        scheduler = Scheduler(signal_pool=pool)
        scheduler.schedule(
            write_signal,
            name="ray_action",
            args=(6,),
            backend="ray",
            pass_ray_proxy=True,
            lifecycle_signal="ray_status",
        )

        scheduler.run_until_idle(max_ticks=200)

        assert pool["ray_status"].completed
        assert pool["ray_status"].value == 12
        assert _wait_until(lambda: pool.get("worker_signal") is not None)
        assert pool["worker_signal"].value == 6
        scheduler.close()
