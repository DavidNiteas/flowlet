"""Signal-driven scheduler."""

from __future__ import annotations

import inspect
import threading
import traceback
import uuid
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, Literal

from .base.coroutine import CoroutinePool
from .base.signal import RaySignalProxy, SignalPool, SignalState
from .compute_graph import TaskNode
from .edge import EdgeNode
from .executable_unit import ExecutableUnit
from .fsm import FSMEdgeNode

Trigger = Callable[[dict[str, SignalState]], bool]
ActionBackend = Literal["thread", "coroutine", "ray", "sync"]
ActionMode = Literal["once", "repeat", "on_change"]
ActionStatus = Literal["pending", "running", "completed", "failed", "skipped"]


@dataclass(slots=True)
class ScheduledAction:
    """One signal-triggered action."""

    name: str
    action: Any
    trigger: Trigger | None = None
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    lifecycle_signal: str | None = None
    backend: ActionBackend = "thread"
    mode: ActionMode = "once"
    pass_signals: bool = False
    pass_ray_proxy: bool = False
    max_runs: int | None = 1
    result_signal: str | None = None


@dataclass(slots=True)
class ActionRuntime:
    """Runtime state for a scheduled action."""

    status: ActionStatus = "pending"
    runs: int = 0
    last_error: str | None = None
    last_result: Any = None
    last_trigger_version: int = -1
    future: Future | Any | None = None


class Scheduler:
    """Poll a SignalPool and execute matching actions."""

    def __init__(
        self,
        *,
        signal_pool: SignalPool | None = None,
        poll_interval: float = 0.05,
        name: str | None = None,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be > 0")
        self.name = name or f"scheduler-{uuid.uuid4().hex[:6]}"
        self.signal_pool = signal_pool or SignalPool()
        self.poll_interval = poll_interval
        self._actions: dict[str, ScheduledAction] = {}
        self._runtime: dict[str, ActionRuntime] = {}
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._pool = CoroutinePool(name=f"{self.name}-coroutine")
        self._ray_proxy: RaySignalProxy | None = None
        self.signal_pool.ensure(f"{self.name}.control", status="pending")
        self.signal_pool.ensure(f"{self.name}.status", status="pending")

    def add_action(self, action: ScheduledAction) -> ScheduledAction:
        with self._lock:
            if action.name in self._actions:
                raise ValueError(f"duplicate scheduled action name: {action.name}")
            if action.max_runs is not None and action.max_runs < 1:
                raise ValueError("max_runs must be >= 1 or None")
            self._actions[action.name] = action
            self._runtime[action.name] = ActionRuntime()
            if action.lifecycle_signal is not None:
                self.signal_pool.ensure(action.lifecycle_signal)
            if action.result_signal is not None:
                self.signal_pool.ensure(action.result_signal)
        return action

    def schedule(
        self,
        action: Any,
        *,
        name: str | None = None,
        trigger: Trigger | None = None,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        lifecycle_signal: str | None = None,
        backend: ActionBackend = "thread",
        mode: ActionMode = "once",
        pass_signals: bool = False,
        pass_ray_proxy: bool = False,
        max_runs: int | None = 1,
        result_signal: str | None = None,
    ) -> ScheduledAction:
        scheduled = ScheduledAction(
            name=name or getattr(action, "__name__", action.__class__.__name__),
            action=action,
            trigger=trigger,
            args=args,
            kwargs=kwargs or {},
            lifecycle_signal=lifecycle_signal,
            backend=backend,
            mode=mode,
            pass_signals=pass_signals,
            pass_ray_proxy=pass_ray_proxy,
            max_runs=max_runs,
            result_signal=result_signal,
        )
        return self.add_action(scheduled)

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self.signal_pool.mark_running(f"{self.name}.status")
            self._thread = threading.Thread(target=self._run_loop, name=self.name, daemon=True)
            self._thread.start()

    def run_until_idle(self, *, max_ticks: int = 1000) -> None:
        if max_ticks < 1:
            raise ValueError("max_ticks must be >= 1")
        self.signal_pool.mark_running(f"{self.name}.status")
        try:
            for _ in range(max_ticks):
                self.tick()
                if self.idle:
                    break
                self.signal_pool.wait_for_change(timeout=self.poll_interval)
        finally:
            if self.idle:
                self.signal_pool.mark_completed(f"{self.name}.status")

    def tick(self) -> None:
        self._collect_finished()
        snapshot = self.signal_pool.snapshot()
        with self._lock:
            actions = list(self._actions.values())
        for action in actions:
            if self._should_start(action, snapshot):
                self._start_action(action)

    def request_stop(self) -> None:
        self.signal_pool.update(f"{self.name}.control", status="stopped")
        self._stop_event.set()

    def close(self) -> None:
        self.request_stop()
        if self._thread is not None and threading.get_ident() != self._thread.ident:
            self._thread.join(timeout=5.0)
        self._collect_finished()
        self._pool.close(wait_tasks=True)
        self.signal_pool.mark_completed(f"{self.name}.status")
        self.signal_pool.close()

    def kill(self) -> None:
        self._stop_event.set()
        self._pool.kill()
        self.signal_pool.update(f"{self.name}.status", status="stopped")
        self.signal_pool.close()

    @property
    def idle(self) -> bool:
        self._collect_finished()
        snapshot = self.signal_pool.snapshot()
        with self._lock:
            if any(runtime.future is not None for runtime in self._runtime.values()):
                return False
            return not any(
                self._can_run_more(action, self._runtime[action.name])
                and (True if action.trigger is None else action.trigger(snapshot))
                for action in self._actions.values()
            )

    def runtime(self, name: str) -> ActionRuntime:
        with self._lock:
            return self._runtime[name]

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            control = self.signal_pool.get(f"{self.name}.control")
            if control is not None and control.status == "stopped":
                break
            self.tick()
            self.signal_pool.wait_for_change(timeout=self.poll_interval)
        self._collect_finished()
        self.signal_pool.mark_completed(f"{self.name}.status")

    def _should_start(self, action: ScheduledAction, snapshot: dict[str, SignalState]) -> bool:
        with self._lock:
            runtime = self._runtime[action.name]
            if runtime.future is not None or runtime.status == "running":
                return False
            if not self._can_run_more(action, runtime):
                return False

            triggered = True if action.trigger is None else action.trigger(snapshot)
            if not triggered:
                return False

            current_version = self.signal_pool.version
            if action.mode == "once" and runtime.runs > 0:
                return False
            if action.mode == "on_change" and runtime.last_trigger_version == current_version:
                return False
            runtime.last_trigger_version = current_version
            return True

    def _can_run_more(self, action: ScheduledAction, runtime: ActionRuntime) -> bool:
        if action.max_runs is None:
            return True
        return runtime.runs < action.max_runs

    def _start_action(self, action: ScheduledAction) -> None:
        runtime = self._runtime[action.name]
        runtime.status = "running"
        runtime.runs += 1
        runtime.last_error = None
        if action.lifecycle_signal is not None:
            self.signal_pool.mark_running(
                action.lifecycle_signal, metadata={"action": action.name, "run": runtime.runs}
            )

        if action.backend == "sync":
            try:
                result = self._execute_action(action)
            except Exception as exc:  # noqa: BLE001
                self._finish_failed(action, exc)
            else:
                self._finish_success(action, result)
            return

        if action.backend == "coroutine":
            future = self._pool.submit(self._execute_action, action)
        elif action.backend == "ray":
            future = self._submit_ray(action)
        elif action.backend == "thread":
            future = _ThreadFuture(self._execute_action, action)
        else:
            raise ValueError(f"unknown scheduler backend: {action.backend}")
        runtime.future = future

    def _collect_finished(self) -> None:
        with self._lock:
            items = [(action, self._runtime[action.name]) for action in self._actions.values()]
        for action, runtime in items:
            future = runtime.future
            if future is None or not _future_done(future, action.backend):
                continue
            runtime.future = None
            try:
                result = _future_result(future, action.backend)
            except Exception as exc:  # noqa: BLE001
                self._finish_failed(action, exc)
            else:
                self._finish_success(action, result)

    def _finish_success(self, action: ScheduledAction, result: Any) -> None:
        runtime = self._runtime[action.name]
        runtime.status = "completed"
        runtime.last_result = result
        if action.lifecycle_signal is not None:
            self.signal_pool.mark_completed(
                action.lifecycle_signal,
                value=result,
                metadata={"action": action.name, "run": runtime.runs},
            )
        if action.result_signal is not None:
            self.signal_pool.mark_completed(action.result_signal, value=result)

    def _finish_failed(self, action: ScheduledAction, exc: BaseException) -> None:
        runtime = self._runtime[action.name]
        runtime.status = "failed"
        runtime.last_error = str(exc)
        error_text = "".join(traceback.format_exception_only(type(exc), exc)).strip()
        if action.lifecycle_signal is not None:
            self.signal_pool.mark_failed(
                action.lifecycle_signal,
                error=error_text,
                metadata={"action": action.name, "run": runtime.runs},
            )

    def _execute_action(self, action: ScheduledAction) -> Any:
        kwargs = dict(action.kwargs)
        if action.pass_signals:
            kwargs.setdefault("signals", self.signal_pool)
        if action.pass_ray_proxy:
            kwargs.setdefault("signals", self._get_ray_proxy())
        return execute_scheduled_target(
            action.action, *action.args, allow_awaitable=action.backend == "coroutine", **kwargs
        )

    def _submit_ray(self, action: ScheduledAction) -> Any:
        try:
            import ray
        except ImportError as exc:
            raise RuntimeError("Ray is not installed. Install ray to use scheduler backend='ray'.") from exc
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        kwargs = dict(action.kwargs)
        if action.pass_ray_proxy or action.pass_signals:
            kwargs.setdefault("signals", self._get_ray_proxy())
        return _ray_execute_scheduled_target.remote(action.action, action.args, kwargs)

    def _get_ray_proxy(self) -> RaySignalProxy:
        if self._ray_proxy is None:
            self._ray_proxy = self.signal_pool.get_ray_proxy()
        return self._ray_proxy

    def __enter__(self) -> Scheduler:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.close()
        else:
            self.kill()


def execute_scheduled_target(target: Any, *args: Any, allow_awaitable: bool = False, **kwargs: Any) -> Any:
    """Execute any supported scheduler target."""

    if isinstance(target, TaskNode):
        if args:
            raise TypeError("TaskNode scheduled targets only accept keyword inputs")
        return target.execute(kwargs)
    if isinstance(target, ExecutableUnit):
        return target.bind_input(*args, **kwargs).execute()
    if isinstance(target, EdgeNode):
        target.run(**kwargs)
        target.join()
        return None
    if isinstance(target, FSMEdgeNode):
        events = kwargs.pop("events", None)
        max_steps = kwargs.pop("max_steps", 100)
        if kwargs:
            raise TypeError(f"unexpected FSM scheduler kwargs: {sorted(kwargs)}")
        return target.run_until_terminal(events=events, max_steps=max_steps)
    if callable(target):
        result = target(*args, **kwargs)
        if inspect.isawaitable(result):
            if allow_awaitable:
                return result
            if inspect.iscoroutine(result):
                result.close()
            raise RuntimeError("awaitable result requires scheduler backend='coroutine'")
        return result
    raise TypeError(f"unsupported scheduled target type: {type(target).__name__}")


class _ThreadFuture:
    def __init__(self, func: Callable[..., Any], *args: Any) -> None:
        self._future: Future = Future()
        self._thread = threading.Thread(target=self._run, args=(func, args), daemon=True)
        self._thread.start()

    def _run(self, func: Callable[..., Any], args: tuple[Any, ...]) -> None:
        try:
            self._future.set_result(func(*args))
        except Exception as exc:  # noqa: BLE001
            self._future.set_exception(exc)

    def done(self) -> bool:
        return self._future.done()

    def result(self) -> Any:
        return self._future.result()


def _future_done(future: Any, backend: ActionBackend) -> bool:
    if backend == "ray":
        import ray

        ready, _ = ray.wait([future], timeout=0)
        return bool(ready)
    return future.done()


def _future_result(future: Any, backend: ActionBackend) -> Any:
    if backend == "ray":
        import ray

        return ray.get(future)
    return future.result()


try:
    import ray

    @ray.remote
    def _ray_execute_scheduled_target(target: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        return execute_scheduled_target(target, *args, **kwargs)

except ImportError:
    _ray_execute_scheduled_target = None
