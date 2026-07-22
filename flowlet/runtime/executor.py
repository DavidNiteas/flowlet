"""Minimal framework runtime executor for registered processes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .event_store import RuntimeEventJsonlStore, RuntimeEventStore
from .manager_bridge import RuntimeManagerEventBridge
from .process import (
    RuntimeProcess,
    RuntimeProcessContext,
    RuntimeProcessOperation,
    RuntimeProcessRunner,
    RuntimeResourceUsage,
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
        manager_bridge: RuntimeManagerEventBridge | None = None,
        projection_policy: RuntimeProjectionPolicy | None = None,
    ) -> None:
        self.runtime_id = runtime_id
        self.runtime_dir = Path(runtime_dir) if runtime_dir is not None else None
        self.event_store = event_store or RuntimeEventJsonlStore(
            RuntimeStore(self.runtime_dir).path(RuntimeStore(self.runtime_dir).layout.runtime_events)
            if self.runtime_dir is not None
            else None
        )
        if manager_bridge is not None and manager_bridge.event_store is not self.event_store:
            raise ValueError("manager_bridge must write to the executor event_store")
        self.manager_bridge = manager_bridge
        self.projection_policy = projection_policy or RuntimeProjectionPolicy()
        self._processes: dict[str, RuntimeProcess] = {}

    def register(self, process: RuntimeProcess) -> RuntimeProcess:
        """Register a process implementation."""
        process_id = process.spec.resolved_process_id()
        self._processes[process_id] = process
        self._write_process_specs()
        self._context_for(process).emit_process_created(
            process.spec.process_type,
            display_name=process.spec.display_name,
            metadata=process.spec.metadata,
        )
        self.write_projection()
        return process

    def run_process(self, process_id: str) -> Any:
        """Run one registered process and persist the current projection."""
        process = self._get_process(process_id)
        context = self._context_for(process)
        self.sync_manager_events()
        try:
            return RuntimeProcessRunner(context).run(process)
        finally:
            self.sync_manager_events()
            self.write_projection()

    def run_all(self) -> dict[str, Any]:
        """Run all registered processes in registration order."""
        results: dict[str, Any] = {}
        for process_id in list(self._processes):
            results[process_id] = self.run_process(process_id)
        return results

    def cancel_process(self, process_id: str) -> None:
        """Dispatch a cancel hook when supported."""
        self._dispatch_hook(
            process_id,
            RuntimeProcessOperation.CANCEL,
            terminal_status=RuntimeEventStatus.CANCELLED,
            terminal_message="process cancelled",
            request_cancel=True,
        )

    def pause_process(self, process_id: str) -> None:
        """Dispatch a pause hook when supported."""
        self._dispatch_hook(
            process_id,
            RuntimeProcessOperation.PAUSE,
            terminal_status="paused",
            terminal_message="process paused",
        )

    def resume_process(self, process_id: str) -> None:
        """Dispatch a resume hook when supported."""
        self._dispatch_hook(
            process_id,
            RuntimeProcessOperation.RESUME,
            terminal_status=RuntimeEventStatus.RUNNING,
            terminal_message="process resumed",
        )

    def retry_process(self, process_id: str) -> Any:
        """Dispatch a retry hook when supported."""
        return self._dispatch_hook(
            process_id,
            RuntimeProcessOperation.RETRY,
            terminal_status=RuntimeEventStatus.SUCCEEDED,
            terminal_message="process retried",
        )

    def cleanup_process(self, process_id: str) -> None:
        """Dispatch a cleanup hook when supported."""
        self._dispatch_hook(
            process_id,
            RuntimeProcessOperation.CLEANUP,
            terminal_status=RuntimeEventStatus.SUCCEEDED,
            terminal_message="process cleaned up",
        )

    def sample_process_resources(self, process_id: str) -> RuntimeResourceUsage:
        """Collect a process resource observation and append a standard event."""
        process = self._get_process(process_id)
        context = self._context_for(process)
        self.sync_manager_events()
        try:
            usage = process.resources(context)
            if not isinstance(usage, RuntimeResourceUsage):
                usage = RuntimeResourceUsage.model_validate(usage)
            context.emit_resource_usage(usage)
            return usage
        finally:
            self.sync_manager_events()
            self.write_projection()

    def projection(self) -> RuntimeProjection:
        """Return the current framework projection."""
        return RuntimeFrameworkReducer(policy=self.projection_policy).reduce(self.event_store.list())

    def sync_manager_events(self) -> list[Any]:
        """Synchronize the optional manager bridge without owning its lifecycle."""
        if self.manager_bridge is None:
            return []
        return self.manager_bridge.sync()

    def write_projection(self) -> RuntimeProjection:
        """Persist and return the current framework projection."""
        projection = self.projection()
        if self.runtime_dir is not None:
            RuntimeStore(self.runtime_dir).write_projection(projection.model_dump(mode="json"))
        return projection

    def _write_process_specs(self) -> None:
        if self.runtime_dir is not None:
            RuntimeStore(self.runtime_dir).write_process_specs([process.spec for process in self._processes.values()])

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

    def _dispatch_hook(
        self,
        process_id: str,
        operation: RuntimeProcessOperation,
        *,
        terminal_status: RuntimeEventStatus | str,
        terminal_message: str,
        request_cancel: bool = False,
    ) -> Any:
        process = self._get_process(process_id)
        context = self._context_for(process)
        self.sync_manager_events()
        if not process.spec.capabilities.supports(operation):
            error = RuntimeUnsupportedOperationError(process_id, operation)
            context.emit_error(error.to_error_info(), event_type=f"process.{operation.value}.unsupported")
            self.write_projection()
            raise error
        if request_cancel:
            context.request_cancel()
        try:
            result = getattr(process, operation.value)(context)
        except RuntimeUnsupportedOperationError as exc:
            context.emit_error(exc.to_error_info(), event_type=f"process.{exc.operation.value}.unsupported")
            raise
        else:
            context.emit_status(
                terminal_status,
                message=terminal_message,
                payload={"result": result if isinstance(result, dict) else {"value": result}},
            )
            return result
        finally:
            self.sync_manager_events()
            self.write_projection()


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
