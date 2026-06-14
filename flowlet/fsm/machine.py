"""Finite state machine primitives."""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class FSMStatus(StrEnum):
    """Runtime status of a finite state machine."""

    NOT_STARTED = "not_started"
    RUNNING = "running"
    TERMINAL = "terminal"
    FAILED = "failed"


@dataclass(slots=True)
class TransitionRecord:
    """A single transition execution record."""

    transition: str
    source: str
    target: str
    event: str | None
    timestamp: float
    elapsed: float
    error: str | None = None


@dataclass(slots=True)
class FSMRuntime:
    """Serializable runtime state stored inside Edge state."""

    state: str
    status: FSMStatus = FSMStatus.NOT_STARTED
    history: list[TransitionRecord] = field(default_factory=list)
    last_error: str | None = None

    @property
    def terminal(self) -> bool:
        return self.status == FSMStatus.TERMINAL

    @property
    def failed(self) -> bool:
        return self.status == FSMStatus.FAILED


GuardCallable = Callable[[dict[str, Any], FSMRuntime, str | None], bool]
ActionCallable = Callable[[dict[str, Any], FSMRuntime, str | None], Any]


@dataclass(frozen=True, slots=True)
class TransitionSpec:
    """Static transition definition."""

    name: str
    source: str | set[str]
    target: str
    event: str | None = None
    action: ActionCallable | None = None
    guard: GuardCallable | None = None
    on_error: str | None = None

    def matches(self, state: str, event: str | None) -> bool:
        sources = self.source if isinstance(self.source, set) else {self.source}
        if state not in sources:
            return False
        return self.event is None or self.event == event


@dataclass(frozen=True, slots=True)
class StateMachineSpec:
    """Static finite state machine definition."""

    initial: str
    transitions: tuple[TransitionSpec, ...]
    terminal: set[str] = field(default_factory=set)
    name: str = "state_machine"
    runtime_key: str = "_fsm"

    def __post_init__(self) -> None:
        if not self.initial:
            raise ValueError("initial state must be non-empty")
        if not self.transitions:
            raise ValueError("state machine requires at least one transition")
        names = [transition.name for transition in self.transitions]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate transition names are not allowed: {names}")

    @classmethod
    def create(
        cls,
        *,
        initial: str,
        transitions: list[TransitionSpec] | tuple[TransitionSpec, ...],
        terminal: set[str] | None = None,
        name: str = "state_machine",
        runtime_key: str = "_fsm",
    ) -> StateMachineSpec:
        return cls(
            initial=initial,
            transitions=tuple(transitions),
            terminal=terminal or set(),
            name=name,
            runtime_key=runtime_key,
        )

    def get_runtime(self, state: dict[str, Any]) -> FSMRuntime:
        runtime = state.get(self.runtime_key)
        if runtime is None:
            runtime = FSMRuntime(state=self.initial)
            state[self.runtime_key] = runtime
        if isinstance(runtime, dict):
            runtime = _runtime_from_dict(runtime)
            state[self.runtime_key] = runtime
        return runtime

    def get_transition(self, runtime: FSMRuntime, event: str | None = None) -> TransitionSpec | None:
        for transition in self.transitions:
            if transition.matches(runtime.state, event):
                return transition
        return None


def _runtime_from_dict(value: dict[str, Any]) -> FSMRuntime:
    history = [
        item
        if isinstance(item, TransitionRecord)
        else TransitionRecord(
            transition=item["transition"],
            source=item["source"],
            target=item["target"],
            event=item.get("event"),
            timestamp=item["timestamp"],
            elapsed=item["elapsed"],
            error=item.get("error"),
        )
        for item in value.get("history", [])
    ]
    return FSMRuntime(
        state=value["state"],
        status=FSMStatus(value.get("status", FSMStatus.NOT_STARTED)),
        history=history,
        last_error=value.get("last_error"),
    )


def _maybe_await(value: Any) -> Any:
    return value


async def _maybe_await_async(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _record_transition(
    runtime: FSMRuntime,
    transition: TransitionSpec,
    source: str,
    target: str,
    event: str | None,
    start: float,
    error: str | None = None,
) -> None:
    runtime.history.append(
        TransitionRecord(
            transition=transition.name,
            source=source,
            target=target,
            event=event,
            timestamp=start,
            elapsed=time.perf_counter() - start,
            error=error,
        )
    )


def fsm_start_target(state: dict[str, Any], spec: StateMachineSpec, reset: bool = False) -> FSMRuntime:
    """Edge target: initialize or resume FSM runtime."""
    if reset or spec.runtime_key not in state:
        runtime = FSMRuntime(state=spec.initial, status=FSMStatus.RUNNING)
        state[spec.runtime_key] = runtime
    else:
        runtime = spec.get_runtime(state)
        if runtime.state in spec.terminal:
            runtime.status = FSMStatus.TERMINAL
        elif runtime.status != FSMStatus.FAILED:
            runtime.status = FSMStatus.RUNNING
    return runtime


def fsm_step_target(state: dict[str, Any], spec: StateMachineSpec, event: str | None = None) -> FSMRuntime:
    """Edge target: execute one synchronous transition."""
    runtime = spec.get_runtime(state)
    if runtime.status == FSMStatus.NOT_STARTED:
        runtime.status = FSMStatus.RUNNING
    if runtime.status in {FSMStatus.TERMINAL, FSMStatus.FAILED}:
        return runtime

    transition = spec.get_transition(runtime, event)
    if transition is None:
        runtime.status = FSMStatus.FAILED
        runtime.last_error = f"No transition from state {runtime.state!r} for event {event!r}"
        return runtime

    start = time.perf_counter()
    source = runtime.state
    try:
        if transition.guard is not None and not transition.guard(state, runtime, event):
            runtime.status = FSMStatus.FAILED
            runtime.last_error = f"Transition guard rejected {transition.name!r}"
            _record_transition(runtime, transition, source, source, event, start, runtime.last_error)
            return runtime

        if transition.action is not None:
            result = transition.action(state, runtime, event)
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise RuntimeError("Synchronous FSM step cannot run awaitable action")
        runtime.state = transition.target
        runtime.last_error = None
        runtime.status = FSMStatus.TERMINAL if runtime.state in spec.terminal else FSMStatus.RUNNING
        _record_transition(runtime, transition, source, runtime.state, event, start)
    except Exception as exc:  # noqa: BLE001
        runtime.last_error = str(exc)
        if transition.on_error is not None:
            runtime.state = transition.on_error
            runtime.status = FSMStatus.TERMINAL if runtime.state in spec.terminal else FSMStatus.RUNNING
            _record_transition(runtime, transition, source, runtime.state, event, start, str(exc))
        else:
            runtime.status = FSMStatus.FAILED
            _record_transition(runtime, transition, source, source, event, start, str(exc))
    return runtime


async def fsm_step_target_async(
    state: dict[str, Any],
    spec: StateMachineSpec,
    event: str | None = None,
) -> FSMRuntime:
    """Edge target: execute one async-compatible transition."""
    runtime = spec.get_runtime(state)
    if runtime.status == FSMStatus.NOT_STARTED:
        runtime.status = FSMStatus.RUNNING
    if runtime.status in {FSMStatus.TERMINAL, FSMStatus.FAILED}:
        return runtime

    transition = spec.get_transition(runtime, event)
    if transition is None:
        runtime.status = FSMStatus.FAILED
        runtime.last_error = f"No transition from state {runtime.state!r} for event {event!r}"
        return runtime

    start = time.perf_counter()
    source = runtime.state
    try:
        if transition.guard is not None:
            accepted = await _maybe_await_async(transition.guard(state, runtime, event))
            if not accepted:
                runtime.status = FSMStatus.FAILED
                runtime.last_error = f"Transition guard rejected {transition.name!r}"
                _record_transition(runtime, transition, source, source, event, start, runtime.last_error)
                return runtime

        if transition.action is not None:
            await _maybe_await_async(transition.action(state, runtime, event))
        runtime.state = transition.target
        runtime.last_error = None
        runtime.status = FSMStatus.TERMINAL if runtime.state in spec.terminal else FSMStatus.RUNNING
        _record_transition(runtime, transition, source, runtime.state, event, start)
    except Exception as exc:  # noqa: BLE001
        runtime.last_error = str(exc)
        if transition.on_error is not None:
            runtime.state = transition.on_error
            runtime.status = FSMStatus.TERMINAL if runtime.state in spec.terminal else FSMStatus.RUNNING
            _record_transition(runtime, transition, source, runtime.state, event, start, str(exc))
        else:
            runtime.status = FSMStatus.FAILED
            _record_transition(runtime, transition, source, source, event, start, str(exc))
    return runtime
