"""Minimal framework runtime executor for registered processes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .event_store import RuntimeEventJsonlStore, RuntimeEventStore
from .process import (
    RuntimeProcess,
    RuntimeProcessContext,
    RuntimeProcessOperation,
    RuntimeProcessRunner,
    RuntimeUnsupportedOperationError,
)
from .projection import RuntimeFrameworkReducer, RuntimeProjection, RuntimeProjectionPolicy
from .schema import RuntimeEventStatus
from .store import RuntimeStore


class RuntimeBackendExecutor:
    """Business-neutral executor that runs registered runtime processes."""

    def __init__(
        self,
        *,
        runtime_id: str,
        runtime_dir: str | Path | None = None,
        event_store: RuntimeEventStore | None = None,
        projection_policy: RuntimeProjectionPolicy | None = None,
    ) -> None:
        self.runtime_id = runtime_id
        self.runtime_dir = Path(runtime_dir) if runtime_dir is not None else None
        self.event_store = event_store or RuntimeEventJsonlStore(
            RuntimeStore(self.runtime_dir).path(RuntimeStore(self.runtime_dir).layout.runtime_events)
            if self.runtime_dir is not None
            else None
        )
        self.projection_policy = projection_policy or RuntimeProjectionPolicy()
        self._processes: dict[str, RuntimeProcess] = {}

    def register(self, process: RuntimeProcess) -> RuntimeProcess:
        """Register a process implementation."""
        process_id = process.spec.resolved_process_id()
        self._processes[process_id] = process
        return process

    def run_process(self, process_id: str) -> Any:
        """Run one registered process and persist the current projection."""
        process = self._get_process(process_id)
        context = self._context_for(process)
        try:
            return RuntimeProcessRunner(context).run(process)
        finally:
            self.write_projection()

    def run_all(self) -> dict[str, Any]:
        """Run all registered processes in registration order."""
        results: dict[str, Any] = {}
        for process_id in list(self._processes):
            results[process_id] = self.run_process(process_id)
        return results

    def cancel_process(self, process_id: str) -> None:
        """Dispatch a cancel hook when supported."""
        process = self._get_process(process_id)
        context = self._context_for(process)
        if not process.spec.capabilities.supports(RuntimeProcessOperation.CANCEL):
            error = RuntimeUnsupportedOperationError(process_id, RuntimeProcessOperation.CANCEL)
            context.emit_error(error.to_error_info(), event_type="process.cancel.unsupported")
            self.write_projection()
            raise error
        context.request_cancel()
        try:
            process.cancel(context)
        except RuntimeUnsupportedOperationError as exc:
            context.emit_error(exc.to_error_info(), event_type=f"process.{exc.operation.value}.unsupported")
            self.write_projection()
            raise
        context.emit_status(RuntimeEventStatus.CANCELLED, message="process cancelled")
        self.write_projection()

    def projection(self) -> RuntimeProjection:
        """Return the current framework projection."""
        return RuntimeFrameworkReducer(policy=self.projection_policy).reduce(self.event_store.list())

    def write_projection(self) -> RuntimeProjection:
        """Persist and return the current framework projection."""
        projection = self.projection()
        if self.runtime_dir is not None:
            RuntimeStore(self.runtime_dir).write_projection(projection.model_dump(mode="json"))
        return projection

    def _context_for(self, process: RuntimeProcess) -> RuntimeProcessContext:
        process_id = process.spec.resolved_process_id()
        return RuntimeProcessContext(
            runtime_id=self.runtime_id,
            process_id=process_id,
            parent_process_id=process.spec.parent_process_id,
            event_store=self.event_store,
            runtime_dir=self.runtime_dir,
            metadata=process.spec.metadata,
            event_id_start=_next_event_id(self.event_store.list()),
        )

    def _get_process(self, process_id: str) -> RuntimeProcess:
        try:
            return self._processes[process_id]
        except KeyError as exc:
            raise KeyError(f"Runtime process {process_id!r} is not registered.") from exc


def _next_event_id(events: list[Any]) -> int:
    next_id = 1
    for event in events:
        numeric_id = _numeric_event_id(event)
        if numeric_id is not None:
            next_id = max(next_id, numeric_id + 1)
    return next_id


def _numeric_event_id(event: Any) -> int | None:
    try:
        return int(event.event_id)
    except (AttributeError, TypeError, ValueError):
        return None
