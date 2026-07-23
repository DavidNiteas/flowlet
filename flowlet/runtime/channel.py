"""Worker-safe channels for standard RuntimeEvent transport."""

from __future__ import annotations

import time
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any

from .event_store import RuntimeEventStore
from .process import RuntimeProcessContext
from .schema import RuntimeEvent


class RuntimeCancellationState:
    """Small cancellation flag that can be mirrored to worker proxies."""

    def __init__(self) -> None:
        self._cancelled = False

    def request_cancel(self) -> None:
        self._cancelled = True

    def is_cancelled(self) -> bool:
        return self._cancelled


@dataclass
class RuntimeEventChannel:
    """Driver-side queue for RuntimeEvent objects emitted by workers."""

    queue: Any
    cancellation_state: Any | None = None

    @classmethod
    def local(cls) -> RuntimeEventChannel:
        """Create a thread/process-local channel backed by queue.Queue."""
        return cls(queue=Queue(), cancellation_state=RuntimeCancellationState())

    @classmethod
    def ray(cls) -> RuntimeEventChannel:
        """Create a Ray-serializable channel backed by ray.util.queue.Queue."""
        try:
            import ray
            from ray.util.queue import Queue as RayQueue
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("Ray is not installed. Install ray to use RuntimeEventChannel.ray().") from exc
        cancellation_state = ray.remote(RuntimeCancellationState).remote()
        return cls(queue=RayQueue(), cancellation_state=cancellation_state)

    def proxy(self, context: RuntimeProcessContext | None = None) -> RuntimeEventProxy:
        """Return a worker-safe proxy. Optionally seed it from a process context."""
        return RuntimeEventProxy(self.queue, self.cancellation_state, context=context)

    def request_cancel(self) -> None:
        """Request cancellation for workers using this channel."""
        if self.cancellation_state is None:
            return
        if hasattr(self.cancellation_state, "request_cancel") and hasattr(
            self.cancellation_state.request_cancel, "remote"
        ):
            self.cancellation_state.request_cancel.remote()
            return
        self.cancellation_state.request_cancel()

    def drain_into(self, target: RuntimeEventStore | RuntimeProcessContext) -> int:
        """Drain queued events into a RuntimeEventStore or process context event store."""
        store = target.event_store if isinstance(target, RuntimeProcessContext) else target
        count = 0
        while True:
            try:
                event = self.queue.get(block=False)
            except Empty:
                return count
            if event is None:
                return count
            if not isinstance(event, RuntimeEvent):
                raise TypeError(f"RuntimeEventChannel received unsupported payload: {type(event)!r}")
            store.append(event)
            count += 1


class RuntimeEventProxy:
    """Worker-side proxy that emits standard RuntimeEvent objects to a driver."""

    def __init__(
        self,
        queue: Any,
        cancellation_state: Any | None = None,
        *,
        context: RuntimeProcessContext | None = None,
    ) -> None:
        self.queue = queue
        self.cancellation_state = cancellation_state
        self.context = context
        self._next_event_id = 1

    def emit(self, event: RuntimeEvent) -> RuntimeEvent:
        """Send one event to the channel."""
        self.queue.put(event)
        return event

    def append(self, event: RuntimeEvent) -> RuntimeEvent:
        """RuntimeEventStore compatibility alias for emit()."""
        return self.emit(event)

    def context_for(
        self,
        *,
        runtime_id: str,
        process_id: str,
        parent_process_id: str | None = None,
        execution_id: str | None = None,
        attempt_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeProcessContext:
        """Build a RuntimeProcessContext that writes to this proxy."""
        return RuntimeProcessContext(
            runtime_id=runtime_id,
            process_id=process_id,
            parent_process_id=parent_process_id,
            execution_id=execution_id,
            attempt_id=attempt_id,
            event_store=self,
            metadata=metadata,
            event_id_start=self._next_event_id,
        )

    def list(self, *, since: int | str | None = None) -> list[RuntimeEvent]:
        """RuntimeEventStore compatibility: worker proxies are write-only."""
        del since
        return []

    def wait_for_next(self, *, since: int | str | None = None, timeout: float | None = None) -> list[RuntimeEvent]:
        """RuntimeEventStore compatibility: worker proxies are write-only."""
        del since, timeout
        return []

    def load(self) -> None:
        """RuntimeEventStore compatibility no-op."""

    def is_cancel_requested(self) -> bool:
        """Return whether the driver requested cancellation."""
        state = self.cancellation_state
        if state is None:
            return False
        if hasattr(state, "is_cancelled") and hasattr(state.is_cancelled, "remote"):
            import ray

            return bool(ray.get(state.is_cancelled.remote()))
        return bool(state.is_cancelled())

    def raise_if_cancelled(self) -> None:
        """Raise when cancellation has been requested."""
        if self.is_cancel_requested():
            raise RuntimeError("Runtime worker cancellation requested.")


def runtime_event_now(
    *,
    runtime_id: str,
    event_id: int | str,
    event_type: str,
    process_id: str | None = None,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> RuntimeEvent:
    """Convenience constructor for simple channel tests and adapters."""
    return RuntimeEvent(
        runtime_id=runtime_id,
        event_id=event_id,
        event_type=event_type,
        process_id=process_id,
        timestamp=time.time(),
        payload=payload or {},
        metadata=metadata or {},
    )
