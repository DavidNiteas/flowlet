from __future__ import annotations

import os

import pytest
from flowlet.fsm import FSMEdgeNode, FSMStatus, StateMachineSpec, TransitionSpec
from flowlet.fsm._test_helpers import (
    add_action,
    async_add_action,
    fail_action,
    load_action,
    require_loaded,
)


def make_linear_spec() -> StateMachineSpec:
    return StateMachineSpec.create(
        initial="new",
        terminal={"done"},
        transitions=[
            TransitionSpec("load", "new", "loaded", action=load_action),
            TransitionSpec("add", "loaded", "done", action=add_action, guard=require_loaded),
        ],
    )


def make_event_spec() -> StateMachineSpec:
    return StateMachineSpec.create(
        initial="idle",
        terminal={"done"},
        transitions=[
            TransitionSpec("start", "idle", "running", event="start"),
            TransitionSpec("finish", "running", "done", event="finish", action=add_action),
        ],
    )


@pytest.fixture(params=["thread", "asyncio"])
def local_backend(request):
    return request.param


class TestFSMEdgeNode:
    def test_run_until_terminal(self, local_backend):
        with FSMEdgeNode(make_linear_spec(), backend=local_backend, initializer=lambda: {"value": 0}) as fsm:
            runtime = fsm.run_until_terminal()
            assert runtime.state == "done"
            assert runtime.status == FSMStatus.TERMINAL
            assert fsm.pull("value") == 1
            assert [record.transition for record in runtime.history] == ["load", "add"]

    def test_event_driven_transitions(self, local_backend):
        with FSMEdgeNode(make_event_spec(), backend=local_backend, initializer=lambda: {"value": 0}) as fsm:
            runtime = fsm.run_until_terminal(["start", "finish"])
            assert runtime.state == "done"
            assert runtime.status == FSMStatus.TERMINAL
            assert fsm.pull("value") == 1

    def test_missing_transition_fails(self, local_backend):
        with FSMEdgeNode(make_event_spec(), backend=local_backend) as fsm:
            runtime = fsm.run_until_terminal(["unknown"])
            assert runtime.status == FSMStatus.FAILED
            assert "No transition" in runtime.last_error

    def test_guard_rejection_fails(self, local_backend):
        spec = StateMachineSpec.create(
            initial="loaded",
            terminal={"done"},
            transitions=[
                TransitionSpec("add", "loaded", "done", action=add_action, guard=require_loaded),
            ],
        )
        with FSMEdgeNode(spec, backend=local_backend, initializer=lambda: {"value": 0}) as fsm:
            runtime = fsm.run_until_terminal()
            assert runtime.status == FSMStatus.FAILED
            assert "guard rejected" in runtime.last_error
            assert runtime.history[-1].target == "loaded"

    def test_error_transition(self, local_backend):
        spec = StateMachineSpec.create(
            initial="new",
            terminal={"failed"},
            transitions=[
                TransitionSpec("fail", "new", "done", action=fail_action, on_error="failed"),
            ],
        )
        with FSMEdgeNode(spec, backend=local_backend) as fsm:
            runtime = fsm.run_until_terminal()
            assert runtime.state == "failed"
            assert runtime.status == FSMStatus.TERMINAL
            assert runtime.last_error == "fsm action failed"
            assert runtime.history[-1].error == "fsm action failed"

    def test_async_action_requires_asyncio_backend(self):
        spec = StateMachineSpec.create(
            initial="new",
            terminal={"done"},
            transitions=[
                TransitionSpec("async_add", "new", "done", action=async_add_action),
            ],
        )
        with FSMEdgeNode(spec, backend="asyncio", initializer=lambda: {"value": 0}) as fsm:
            runtime = fsm.run_until_terminal()
            assert runtime.state == "done"
            assert runtime.status == FSMStatus.TERMINAL
            assert fsm.pull("value") == 1

    def test_sync_backend_rejects_async_action(self):
        spec = StateMachineSpec.create(
            initial="new",
            terminal={"done", "failed"},
            transitions=[
                TransitionSpec("async_add", "new", "done", action=async_add_action, on_error="failed"),
            ],
        )
        with FSMEdgeNode(spec, backend="thread", initializer=lambda: {"value": 0}) as fsm:
            runtime = fsm.run_until_terminal()
            assert runtime.state == "failed"
            assert "awaitable action" in runtime.last_error


def test_ray_backend_smoke(monkeypatch):
    ray = pytest.importorskip("ray")
    monkeypatch.setenv("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_cpus=2, include_dashboard=False, log_to_driver=False, ignore_reinit_error=True)
    try:
        with FSMEdgeNode(make_linear_spec(), backend="ray", initializer=lambda: {"value": 0}) as fsm:
            runtime = fsm.run_until_terminal()
            assert runtime.state == "done"
            assert runtime.status == FSMStatus.TERMINAL
            assert fsm.pull("value") == 1
    finally:
        ray.shutdown()
