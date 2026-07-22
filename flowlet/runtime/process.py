"""Standard runtime process contracts."""

from __future__ import annotations

import time
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel, Field, field_validator

from .event_store import RuntimeEventStore
from .schema import (
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeEventStatus,
    RuntimeEventType,
    RuntimeProgress,
    RuntimeStatusClass,
)

if TYPE_CHECKING:
    from .recovery import RuntimeCheckpointRef


class RuntimeProcessOperation(StrEnum):
    """Standard process operations known by Flowlet runtime."""

    START = "start"
    CANCEL = "cancel"
    PAUSE = "pause"
    RESUME = "resume"
    RETRY = "retry"
    CLEANUP = "cleanup"
    CHECKPOINT = "checkpoint"
    STATUS = "status"
    RESOURCES = "resources"


class RuntimeProcessCapabilities(BaseModel):
    """Declarative process operation capabilities."""

    can_start: bool = True
    can_cancel: bool = False
    can_pause: bool = False
    can_resume: bool = False
    can_retry: bool = False
    can_cleanup: bool = False
    can_checkpoint: bool = False

    def supports(self, operation: RuntimeProcessOperation | str) -> bool:
        """Return whether this process declares support for an operation."""
        operation = RuntimeProcessOperation(operation)
        if operation == RuntimeProcessOperation.START:
            return self.can_start
        if operation == RuntimeProcessOperation.CANCEL:
            return self.can_cancel
        if operation == RuntimeProcessOperation.PAUSE:
            return self.can_pause
        if operation == RuntimeProcessOperation.RESUME:
            return self.can_resume
        if operation == RuntimeProcessOperation.RETRY:
            return self.can_retry
        if operation == RuntimeProcessOperation.CLEANUP:
            return self.can_cleanup
        if operation == RuntimeProcessOperation.CHECKPOINT:
            return self.can_checkpoint
        return True


class RuntimeResourceRequest(BaseModel):
    """Business-neutral resource request for a runtime process."""

    cpu: float | int | None = None
    memory_bytes: int | None = None
    gpu: float | int | None = None
    disk_bytes: int | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RuntimeResourceUsage(BaseModel):
    """Business-neutral resource observation reported by one process."""

    cpu_percent: float | None = None
    memory_bytes: int | None = None
    gpu_percent: float | None = None
    gpu_memory_bytes: int | None = None
    disk_bytes: int | None = None
    network_sent_bytes: int | None = None
    network_received_bytes: int | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RuntimeRetryPolicy(BaseModel):
    """Framework retry policy metadata for a runtime process."""

    max_attempts: int = 1
    backoff_seconds: float | None = None
    retryable_error_types: list[str] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("max_attempts")
    @classmethod
    def _validate_max_attempts(cls, value: int) -> int:
        if value < 1:
            raise ValueError("max_attempts must be >= 1")
        return value


class RuntimeIdempotency(StrEnum):
    """Declared replay safety for one logical process."""

    UNKNOWN = "unknown"
    IDEMPOTENT = "idempotent"
    REQUIRES_CLEANUP = "requires_cleanup"
    NON_IDEMPOTENT = "non_idempotent"


class RuntimeCheckpointMode(StrEnum):
    """Declared checkpoint support for one logical process."""

    NONE = "none"
    OPTIONAL = "optional"
    REQUIRED = "required"


class RuntimeCheckpointPolicy(BaseModel):
    """Business-neutral declaration of checkpoint requirements."""

    mode: RuntimeCheckpointMode = RuntimeCheckpointMode.NONE
    format: str | None = None
    cursor_semantics: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RuntimeProcessSpec(BaseModel):
    """Business-neutral process declaration."""

    process_id: str | None = None
    process_type: str
    display_name: str | None = None
    parent_process_id: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    execution_key: str | None = None
    input_fingerprint: str | None = None
    implementation_version: str | None = None
    idempotency: RuntimeIdempotency = RuntimeIdempotency.UNKNOWN
    checkpoint_policy: RuntimeCheckpointPolicy = Field(default_factory=RuntimeCheckpointPolicy)
    output_contract: list[str] = Field(default_factory=list)
    inputs: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    capabilities: RuntimeProcessCapabilities = Field(default_factory=RuntimeProcessCapabilities)
    resource_request: RuntimeResourceRequest | None = None
    retry_policy: RuntimeRetryPolicy | None = None

    @field_validator("process_type")
    @classmethod
    def _validate_process_type(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("process_type must not be empty")
        return value

    @field_validator("depends_on")
    @classmethod
    def _validate_dependencies(cls, value: list[str]) -> list[str]:
        if any(not dependency.strip() for dependency in value):
            raise ValueError("depends_on entries must not be empty")
        if len(value) != len(set(value)):
            raise ValueError("depends_on entries must be unique")
        return value

    def resolved_process_id(self) -> str:
        """Return the explicit process id or a stable fallback from type."""
        return self.process_id or self.process_type


class RuntimeProcessState(BaseModel):
    """Framework-level state snapshot for one runtime process."""

    process_id: str
    process_type: str | None = None
    parent_process_id: str | None = None
    current_attempt_id: str | None = None
    status: RuntimeEventStatus | str = RuntimeEventStatus.PENDING
    status_class: RuntimeStatusClass = RuntimeStatusClass.NOT_STARTED
    started_at: float | None = None
    updated_at: float | None = None
    finished_at: float | None = None
    progress: RuntimeProgress | None = None
    result: dict[str, Any] | None = None
    error: RuntimeErrorInfo | None = None
    resource_usage: RuntimeResourceUsage | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RuntimeUnsupportedOperationError(RuntimeError):
    """Raised when a process operation is not supported by its contract."""

    def __init__(self, process_id: str, operation: RuntimeProcessOperation | str) -> None:
        self.process_id = process_id
        self.operation = RuntimeProcessOperation(operation)
        super().__init__(f"Process {process_id!r} does not support operation {self.operation.value!r}.")

    def to_error_info(self) -> RuntimeErrorInfo:
        """Return a standard RuntimeErrorInfo payload."""
        return RuntimeErrorInfo(
            type=type(self).__name__,
            message=str(self),
            retryable=False,
            context={"process_id": self.process_id, "operation": self.operation.value},
        )


class RuntimeProcessContext:
    """Framework context used by process implementations to emit events."""

    def __init__(
        self,
        *,
        runtime_id: str,
        process_id: str,
        event_store: RuntimeEventStore,
        parent_process_id: str | None = None,
        execution_id: str | None = None,
        attempt_id: str | None = None,
        checkpoint_id: str | None = None,
        runtime_dir: str | Path | None = None,
        metadata: dict[str, Any] | None = None,
        event_id_start: int = 1,
    ) -> None:
        self.runtime_id = runtime_id
        self.process_id = process_id
        self.parent_process_id = parent_process_id
        self.execution_id = execution_id
        self.attempt_id = attempt_id
        self.checkpoint_id = checkpoint_id
        self.event_store = event_store
        self.runtime_dir = Path(runtime_dir) if runtime_dir is not None else None
        self.metadata = metadata or {}
        self._next_event_id = event_id_start
        self._cancel_requested = False

    def emit(self, event: RuntimeEvent) -> RuntimeEvent:
        """Append a standard runtime event."""
        self._next_event_id = max(self._next_event_id, _next_numeric_event_id(event.event_id))
        return self.event_store.append(event)

    def emit_status(
        self,
        status: RuntimeEventStatus | str,
        *,
        status_class: RuntimeStatusClass | None = None,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Emit a process status change event."""
        status_class = status_class or _status_class(status)
        return self._emit(
            "process.status.changed",
            status=status,
            status_class=status_class,
            message=message,
            payload=payload,
            metadata=metadata,
        )

    def emit_process_created(
        self,
        process_type: str,
        *,
        display_name: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Emit the declaration event for this independently addressable process."""
        payload = {"process_type": process_type}
        if display_name is not None:
            payload["display_name"] = display_name
        return self._emit(
            RuntimeEventType.PROCESS_CREATED,
            status=RuntimeEventStatus.PENDING,
            status_class=RuntimeStatusClass.NOT_STARTED,
            payload=payload,
            metadata=metadata,
        )

    def emit_progress(
        self,
        current: float | int,
        *,
        total: float | int | None = None,
        unit: str | None = None,
        description: str | None = None,
        status: RuntimeEventStatus | str = RuntimeEventStatus.RUNNING,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Emit a process progress event."""
        progress = RuntimeProgress(current=current, total=total, unit=unit, description=description)
        return self._emit(
            "process.progressed",
            status=status,
            status_class=_status_class(status),
            progress=progress,
            metadata=metadata,
        )

    def emit_error(
        self,
        error: RuntimeErrorInfo | Exception,
        *,
        event_type: str = "process.failed",
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Emit a standard process error event."""
        error_info = (
            error
            if isinstance(error, RuntimeErrorInfo)
            else RuntimeErrorInfo(type=type(error).__name__, message=str(error))
        )
        return self._emit(
            event_type,
            status=RuntimeEventStatus.FAILED,
            status_class=RuntimeStatusClass.TERMINAL_FAILURE,
            error=error_info,
            metadata=metadata,
        )

    def emit_artifact(
        self,
        path: str | Path,
        *,
        metadata: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Emit a standard artifact event."""
        artifact_payload = {"path": str(path), **(payload or {})}
        return self._emit("artifact.produced", payload=artifact_payload, metadata=metadata)

    def emit_log(
        self,
        message: str,
        *,
        level: str = "info",
        metadata: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Emit a standard log event."""
        log_payload = {"level": level, **(payload or {})}
        return self._emit("log.emitted", message=message, payload=log_payload, metadata=metadata)

    def emit_metric(
        self,
        name: str,
        value: Any,
        *,
        unit: str | None = None,
        metadata: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Emit a standard metric event."""
        metric_payload = {"name": name, "value": value, "unit": unit, **(payload or {})}
        return self._emit("metric.sampled", payload=metric_payload, metadata=metadata)

    def emit_resource_usage(
        self,
        usage: RuntimeResourceUsage,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Report the process's latest resource observation."""
        return self._emit(
            RuntimeEventType.RESOURCE_SAMPLED,
            payload={"resource_usage": usage.model_dump(mode="json")},
            metadata=metadata,
        )

    def emit_signal(
        self,
        name: str,
        *,
        value: Any = None,
        status: RuntimeEventStatus | str | None = None,
        metadata: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Emit a standard signal event."""
        signal_payload = {"name": name, "value": value, **(payload or {})}
        return self._emit(
            "signal.changed",
            status=status,
            status_class=_status_class(status) if status is not None else None,
            payload=signal_payload,
            metadata=metadata,
        )

    def checkpoint(
        self,
        name: str,
        *,
        metadata: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Emit a standard checkpoint event."""
        checkpoint_payload = {"name": name, **(payload or {})}
        return self._emit("process.checkpointed", payload=checkpoint_payload, metadata=metadata)

    def commit_checkpoint(
        self,
        checkpoint: RuntimeCheckpointRef,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Commit a package-created recoverable checkpoint reference."""
        if checkpoint.process_id != self.process_id:
            raise ValueError("checkpoint process_id must match the runtime process context")
        if self.attempt_id is not None and checkpoint.attempt_id != self.attempt_id:
            raise ValueError("checkpoint attempt_id must match the runtime process context")
        return self._emit(
            RuntimeEventType.CHECKPOINT_COMMITTED,
            checkpoint_id=checkpoint.checkpoint_id,
            payload={"checkpoint": checkpoint.model_dump(mode="json")},
            metadata=metadata,
        )

    def invalidate_checkpoint(
        self,
        checkpoint_id: str,
        *,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Invalidate one previously committed recoverable checkpoint."""
        if not checkpoint_id.strip():
            raise ValueError("checkpoint_id must not be empty")
        return self._emit(
            RuntimeEventType.CHECKPOINT_INVALIDATED,
            checkpoint_id=checkpoint_id,
            payload={"reason": reason},
            metadata=metadata,
        )

    def request_cancel(self) -> None:
        """Mark this context as cancelled."""
        self._cancel_requested = True

    def is_cancel_requested(self) -> bool:
        """Return whether cancellation has been requested."""
        return self._cancel_requested

    def raise_if_cancelled(self) -> None:
        """Raise a standard cancellation error if cancellation was requested."""
        if self._cancel_requested:
            raise RuntimeError("Runtime process cancellation requested.")

    def artifact_path(self, relative_path: str | Path) -> Path:
        """Return a runtime-relative artifact path."""
        if self.runtime_dir is None:
            raise ValueError("runtime_dir is required for artifact_path")
        return self.runtime_dir / relative_path

    def unsupported_operation(self, operation: RuntimeProcessOperation | str) -> RuntimeUnsupportedOperationError:
        """Build a standard unsupported-operation error."""
        return RuntimeUnsupportedOperationError(self.process_id, operation)

    def _emit(
        self,
        event_type: str,
        *,
        status: RuntimeEventStatus | str | None = None,
        status_class: RuntimeStatusClass | None = None,
        progress: RuntimeProgress | None = None,
        message: str | None = None,
        error: RuntimeErrorInfo | None = None,
        checkpoint_id: str | None = None,
        payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        event = RuntimeEvent(
            event_id=self._take_event_id(),
            runtime_id=self.runtime_id,
            process_id=self.process_id,
            parent_process_id=self.parent_process_id,
            execution_id=self.execution_id,
            attempt_id=self.attempt_id,
            checkpoint_id=checkpoint_id or self.checkpoint_id,
            event_type=event_type,
            timestamp=time.time(),
            subject_type="process",
            subject_id=self.process_id,
            status=status,
            status_class=status_class,
            progress=progress,
            message=message,
            error=error,
            payload=payload or {},
            metadata={**self.metadata, **(metadata or {})},
        )
        return self.event_store.append(event)

    def _take_event_id(self) -> int:
        event_id = self._next_event_id
        self._next_event_id += 1
        return event_id


class RuntimeProcess(Protocol):
    """Protocol implemented by concrete runtime processes."""

    spec: RuntimeProcessSpec

    def start(self, context: RuntimeProcessContext) -> Any:
        """Start the process."""
        ...

    def cancel(self, context: RuntimeProcessContext) -> None:
        """Cancel the process when supported."""
        raise context.unsupported_operation(RuntimeProcessOperation.CANCEL)

    def pause(self, context: RuntimeProcessContext) -> None:
        """Pause the process when supported."""
        raise context.unsupported_operation(RuntimeProcessOperation.PAUSE)

    def resume(self, context: RuntimeProcessContext) -> None:
        """Resume the process when supported."""
        raise context.unsupported_operation(RuntimeProcessOperation.RESUME)

    def retry(self, context: RuntimeProcessContext) -> Any:
        """Retry the process when supported."""
        raise context.unsupported_operation(RuntimeProcessOperation.RETRY)

    def cleanup(self, context: RuntimeProcessContext) -> None:
        """Clean up the process when supported."""
        raise context.unsupported_operation(RuntimeProcessOperation.CLEANUP)

    def status(self, context: RuntimeProcessContext) -> RuntimeProcessState:
        """Return current process state."""
        del context
        return RuntimeProcessState(
            process_id=self.spec.resolved_process_id(),
            process_type=self.spec.process_type,
            parent_process_id=self.spec.parent_process_id,
        )

    def resources(self, context: RuntimeProcessContext) -> RuntimeResourceUsage:
        """Return the current resource observation when available."""
        del context
        return RuntimeResourceUsage()


class RuntimeProcessBase:
    """Base class with standard unsupported-operation hook behavior."""

    spec: RuntimeProcessSpec

    def __init__(self, spec: RuntimeProcessSpec) -> None:
        self.spec = spec

    def start(self, context: RuntimeProcessContext) -> Any:
        """Start the process."""
        raise context.unsupported_operation(RuntimeProcessOperation.START)

    def cancel(self, context: RuntimeProcessContext) -> None:
        """Cancel the process when supported."""
        raise context.unsupported_operation(RuntimeProcessOperation.CANCEL)

    def pause(self, context: RuntimeProcessContext) -> None:
        """Pause the process when supported."""
        raise context.unsupported_operation(RuntimeProcessOperation.PAUSE)

    def resume(self, context: RuntimeProcessContext) -> None:
        """Resume the process when supported."""
        raise context.unsupported_operation(RuntimeProcessOperation.RESUME)

    def retry(self, context: RuntimeProcessContext) -> Any:
        """Retry the process when supported."""
        raise context.unsupported_operation(RuntimeProcessOperation.RETRY)

    def cleanup(self, context: RuntimeProcessContext) -> None:
        """Clean up the process when supported."""
        raise context.unsupported_operation(RuntimeProcessOperation.CLEANUP)

    def status(self, context: RuntimeProcessContext) -> RuntimeProcessState:
        """Return current process state."""
        del context
        return RuntimeProcessState(
            process_id=self.spec.resolved_process_id(),
            process_type=self.spec.process_type,
            parent_process_id=self.spec.parent_process_id,
        )

    def resources(self, context: RuntimeProcessContext) -> RuntimeResourceUsage:
        """Return an empty observation when the process does not expose usage."""
        del context
        return RuntimeResourceUsage()


class RuntimeProcessRunner:
    """Minimal runner that wraps process execution with standard events."""

    def __init__(self, context: RuntimeProcessContext) -> None:
        self.context = context

    def run(self, process: RuntimeProcess) -> Any:
        """Run a process and emit start/completed/failed events."""
        process_id = process.spec.resolved_process_id()
        if process_id != self.context.process_id:
            raise ValueError(
                f"Context process_id {self.context.process_id!r} does not match process spec {process_id!r}."
            )
        if not process.spec.capabilities.supports(RuntimeProcessOperation.START):
            error = RuntimeUnsupportedOperationError(process_id, RuntimeProcessOperation.START)
            self.context.emit_error(error.to_error_info(), event_type="process.start.unsupported")
            raise error
        self.context.emit_status(
            RuntimeEventStatus.RUNNING,
            message="process started",
            payload={"process_type": process.spec.process_type},
            metadata=process.spec.metadata,
        )
        try:
            result = process.start(self.context)
        except RuntimeUnsupportedOperationError as exc:
            self.context.emit_error(exc.to_error_info(), event_type=f"process.{exc.operation.value}.unsupported")
            raise
        except Exception as exc:
            self.context.emit_error(exc)
            raise
        self.context.emit_status(
            RuntimeEventStatus.SUCCEEDED,
            message="process completed",
            payload={"result": result if isinstance(result, dict) else {"value": result}},
            metadata=process.spec.metadata,
        )
        return result


def runtime_process_spec_payload(**kwargs: Any) -> dict[str, Any]:
    """Build a JSON-safe runtime process spec payload."""
    return RuntimeProcessSpec(**kwargs).model_dump(mode="json")


def _next_numeric_event_id(event_id: int | str) -> int:
    try:
        return int(event_id) + 1
    except (TypeError, ValueError):
        return 1


def _status_class(status: RuntimeEventStatus | str) -> RuntimeStatusClass:
    if status in {RuntimeEventStatus.PENDING, "created", "queued", "pending"}:
        return RuntimeStatusClass.NOT_STARTED
    if status in {RuntimeEventStatus.RUNNING, "running", "paused"}:
        return RuntimeStatusClass.ACTIVE
    if status in {RuntimeEventStatus.SUCCEEDED, "completed", "succeeded"}:
        return RuntimeStatusClass.TERMINAL_SUCCESS
    if status in {RuntimeEventStatus.FAILED, "failed"}:
        return RuntimeStatusClass.TERMINAL_FAILURE
    if status in {RuntimeEventStatus.CANCELLED, "cancelled"}:
        return RuntimeStatusClass.TERMINAL_CANCELLED
    if status in {RuntimeEventStatus.BLOCKED, "blocked"}:
        return RuntimeStatusClass.BLOCKED
    return RuntimeStatusClass.UNKNOWN
