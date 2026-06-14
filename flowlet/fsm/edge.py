"""FSM EdgeNode wrapper."""

from __future__ import annotations

from typing import Any, Literal

from ..edge import AsyncioEdgeNode, EdgeConfig, EdgeNode, RayEdgeNode, ThreadEdgeNode
from .machine import (
    FSMRuntime,
    FSMStatus,
    StateMachineSpec,
    fsm_start_target,
    fsm_step_target,
    fsm_step_target_async,
)

BackendName = Literal["thread", "asyncio", "ray"]


class FSMEdgeNode:
    """Programmable EdgeNode controlled by a finite state machine spec."""

    def __init__(
        self,
        spec: StateMachineSpec,
        *,
        backend: BackendName | EdgeNode = "thread",
        initializer=None,
        name: str | None = None,
        config: EdgeConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self.spec = spec
        self._backend_name: BackendName | None = backend if isinstance(backend, str) else None
        self._node = self._create_node(backend, initializer=initializer, name=name, config=config, **kwargs)

    @property
    def node(self) -> EdgeNode:
        """Underlying EdgeNode."""
        return self._node

    @property
    def alive(self) -> bool:
        return self._node.alive

    def _create_node(
        self,
        backend: BackendName | EdgeNode,
        *,
        initializer,
        name: str | None,
        config: EdgeConfig | None,
        **kwargs: Any,
    ) -> EdgeNode:
        if isinstance(backend, EdgeNode):
            return backend
        if backend == "thread":
            return ThreadEdgeNode(initializer=initializer, name=name, config=config)
        if backend == "asyncio":
            return AsyncioEdgeNode(initializer=initializer, name=name, config=config)
        if backend == "ray":
            return RayEdgeNode(initializer=initializer, name=name, config=config, **kwargs)
        raise ValueError(f"Unknown FSM backend: {backend!r}")

    def start(self, *, reset: bool = False) -> None:
        """Initialize or resume FSM runtime."""
        self._node.apply(fsm_start_target, output=self.spec.runtime_key, spec=self.spec, reset=reset)

    def step(self, event: str | None = None) -> None:
        """Execute one transition."""
        if self._backend_name == "asyncio":
            target = fsm_step_target_async
        else:
            target = fsm_step_target
        self._node.apply(target, output=self.spec.runtime_key, spec=self.spec, event=event)

    def run_until_terminal(
        self,
        events: list[str | None] | tuple[str | None, ...] | None = None,
        *,
        max_steps: int = 100,
    ) -> FSMRuntime:
        """Run transitions until terminal/failed status or until events are exhausted."""
        if max_steps < 1:
            raise ValueError("max_steps must be >= 1")
        self.start()
        self.join()

        if events is None:
            for _ in range(max_steps):
                runtime = self.runtime()
                if runtime.status in {FSMStatus.TERMINAL, FSMStatus.FAILED}:
                    return runtime
                self.step()
                self.join()
            return self.runtime()

        for event in events:
            runtime = self.runtime()
            if runtime.status in {FSMStatus.TERMINAL, FSMStatus.FAILED}:
                return runtime
            self.step(event)
            self.join()
        return self.runtime()

    def runtime(self) -> FSMRuntime:
        """Return current FSM runtime."""
        return self._node.pull(self.spec.runtime_key)

    def current_state(self) -> str:
        return self.runtime().state

    def status(self) -> FSMStatus:
        return self.runtime().status

    def push(self, name: str, value: Any) -> None:
        self._node.push(name, value)

    def push_many(self, mapping: dict[str, Any]) -> None:
        self._node.push_many(mapping)

    def pull(self, name: str) -> Any:
        return self._node.pull(name)

    def pull_many(self, names: list[str]) -> dict[str, Any]:
        return self._node.pull_many(names)

    def join(self) -> None:
        self._node.join()

    def close(self) -> None:
        self._node.close()

    def kill(self) -> None:
        self._node.kill()

    def __enter__(self) -> FSMEdgeNode:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.close()
        else:
            self.kill()
