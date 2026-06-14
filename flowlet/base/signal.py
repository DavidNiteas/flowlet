"""Signal primitives used by schedulers and cross-process workers."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from copy import deepcopy
from queue import Empty
from typing import Any, Literal

from pydantic import Field

from ..config.base_config import BaseConfig

SignalStatus = Literal["pending", "running", "completed", "failed", "cancelled", "stopped"]


class SignalState(BaseConfig):
    """Serializable signal state."""

    name: str = ""
    status: SignalStatus = "pending"
    value: Any = None
    error: str | None = None
    version: int = 0
    timestamp: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def pending(self) -> bool:
        return self.status == "pending"

    @property
    def running(self) -> bool:
        return self.status == "running"

    @property
    def completed(self) -> bool:
        return self.status == "completed"

    @property
    def failed(self) -> bool:
        return self.status == "failed"

    @property
    def terminal(self) -> bool:
        return self.status in {"completed", "failed", "cancelled", "stopped"}


class SignalUpdateMessage(BaseConfig):
    """Serializable update message sent by Ray workers."""

    name: str
    status: SignalStatus | None = None
    value: Any = None
    error: str | None = None
    metadata: dict[str, Any] | None = None
    replace_metadata: bool = False

    def apply(self, pool: SignalPool) -> SignalState:
        return pool.update(
            self.name,
            status=self.status,
            value=self.value,
            error=self.error,
            metadata=self.metadata,
            replace_metadata=self.replace_metadata,
        )


class SignalPool:
    """Thread-safe local signal registry."""

    def __init__(self, initial: dict[str, SignalState | Any] | None = None) -> None:
        self._signals: dict[str, SignalState] = {}
        self._lock = threading.RLock()
        self._changed = threading.Condition(self._lock)
        self._ray_queue: Any | None = None
        self._consumer_threads: list[threading.Thread] = []
        if initial:
            for name, value in initial.items():
                if isinstance(value, SignalState):
                    self._signals[name] = deepcopy(value)
                else:
                    self._signals[name] = SignalState(name=name, value=value, timestamp=time.time())

    def ensure(self, name: str, *, initial: Any = None, status: SignalStatus = "pending") -> SignalState:
        with self._changed:
            signal = self._signals.get(name)
            if signal is None:
                signal = SignalState(name=name, status=status, value=initial, timestamp=time.time())
                self._signals[name] = signal
                self._changed.notify_all()
            return deepcopy(signal)

    def update(
        self,
        name: str,
        *,
        status: SignalStatus | None = None,
        value: Any = None,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
        replace_metadata: bool = False,
    ) -> SignalState:
        with self._changed:
            current = self._signals.get(name)
            if current is None:
                current = SignalState(name=name)
                self._signals[name] = current

            if status is not None:
                current.status = status
            current.value = value
            current.error = error
            if metadata is not None:
                current.metadata = dict(metadata) if replace_metadata else {**current.metadata, **metadata}
            current.version += 1
            current.timestamp = time.time()
            self._changed.notify_all()
            return deepcopy(current)

    def mark_running(self, name: str, *, metadata: dict[str, Any] | None = None) -> SignalState:
        return self.update(name, status="running", metadata=metadata)

    def mark_completed(
        self,
        name: str,
        *,
        value: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> SignalState:
        return self.update(name, status="completed", value=value, metadata=metadata)

    def mark_failed(
        self,
        name: str,
        *,
        error: str,
        metadata: dict[str, Any] | None = None,
    ) -> SignalState:
        return self.update(name, status="failed", error=error, metadata=metadata)

    def get(self, name: str, default: SignalState | None = None) -> SignalState | None:
        with self._lock:
            signal = self._signals.get(name)
            if signal is None:
                return deepcopy(default)
            return deepcopy(signal)

    def __getitem__(self, name: str) -> SignalState:
        signal = self.get(name)
        if signal is None:
            raise KeyError(name)
        return signal

    def snapshot(self) -> dict[str, SignalState]:
        with self._lock:
            return deepcopy(self._signals)

    def clear(self) -> None:
        with self._changed:
            self._signals.clear()
            self._changed.notify_all()

    def wait_for_change(self, version: int | None = None, timeout: float | None = None) -> bool:
        with self._changed:
            if version is None:
                return self._changed.wait(timeout=timeout)
            return self._changed.wait_for(lambda: self.version > version, timeout=timeout)

    def wait_for(
        self,
        predicate: Callable[[dict[str, SignalState]], bool],
        timeout: float | None = None,
    ) -> bool:
        with self._changed:
            return self._changed.wait_for(lambda: predicate(deepcopy(self._signals)), timeout=timeout)

    @property
    def version(self) -> int:
        with self._lock:
            return sum(signal.version for signal in self._signals.values())

    def get_ray_proxy(self) -> RaySignalProxy:
        if self._ray_queue is None:
            try:
                from ray.util.queue import Queue as RayQueue
            except ImportError as exc:
                raise RuntimeError("Ray is not installed. Install ray to use get_ray_proxy().") from exc
            self._ray_queue = RayQueue()
            thread = threading.Thread(target=self._consume_ray_loop, daemon=True)
            thread.start()
            self._consumer_threads.append(thread)
        return RaySignalProxy(self._ray_queue, self.snapshot())

    def close(self) -> None:
        if self._ray_queue is not None:
            self._ray_queue.put(None)
            self._ray_queue = None
        for thread in self._consumer_threads:
            thread.join(timeout=2.0)
        self._consumer_threads.clear()

    def _consume_ray_loop(self) -> None:
        while True:
            try:
                msg = self._ray_queue.get(block=True, timeout=0.5)
            except Empty:
                continue
            except Exception:
                continue
            if msg is None:
                break
            if isinstance(msg, SignalUpdateMessage):
                msg.apply(self)


class RaySignalProxy:
    """Ray-serializable signal pool proxy."""

    def __init__(
        self,
        queue: Any | None = None,
        snapshot: dict[str, SignalState] | None = None,
    ) -> None:
        self._queue = queue
        self._snapshot = deepcopy(snapshot) if snapshot is not None else {}

    def update(
        self,
        name: str,
        *,
        status: SignalStatus | None = None,
        value: Any = None,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
        replace_metadata: bool = False,
    ) -> None:
        msg = SignalUpdateMessage(
            name=name,
            status=status,
            value=value,
            error=error,
            metadata=metadata,
            replace_metadata=replace_metadata,
        )
        self._apply_local(msg)
        if self._queue is not None:
            self._queue.put(msg)

    def mark_running(self, name: str, *, metadata: dict[str, Any] | None = None) -> None:
        self.update(name, status="running", metadata=metadata)

    def mark_completed(
        self,
        name: str,
        *,
        value: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.update(name, status="completed", value=value, metadata=metadata)

    def mark_failed(
        self,
        name: str,
        *,
        error: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.update(name, status="failed", error=error, metadata=metadata)

    def get(self, name: str, default: SignalState | None = None) -> SignalState | None:
        signal = self._snapshot.get(name)
        if signal is None:
            return deepcopy(default)
        return deepcopy(signal)

    def snapshot(self) -> dict[str, SignalState]:
        return deepcopy(self._snapshot)

    def _apply_local(self, msg: SignalUpdateMessage) -> None:
        current = self._snapshot.get(msg.name)
        if current is None:
            current = SignalState(name=msg.name)
            self._snapshot[msg.name] = current
        if msg.status is not None:
            current.status = msg.status
        current.value = msg.value
        current.error = msg.error
        if msg.metadata is not None:
            current.metadata = dict(msg.metadata) if msg.replace_metadata else {**current.metadata, **msg.metadata}
        current.version += 1
        current.timestamp = time.time()

    def __getstate__(self) -> dict[str, Any]:
        return {
            "queue": self._queue,
            "snapshot": {name: signal.to_dict() for name, signal in self._snapshot.items()},
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self._queue = state.get("queue")
        self._snapshot = {name: SignalState.from_dict(signal) for name, signal in state.get("snapshot", {}).items()}
