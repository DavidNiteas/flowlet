"""Framework-level runtime projections derived from RuntimeEvent streams."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, Field

from .process import RuntimeProcessOperation, RuntimeProcessState, RuntimeResourceUsage
from .recovery import RuntimeCheckpointRef, RuntimeProcessAttempt
from .schema import (
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeEventStatus,
    RuntimeEventType,
    RuntimeProgress,
    RuntimeStatusClass,
)


class RuntimeProjection(BaseModel):
    """Framework-level runtime state derived from standard events."""

    runtime_id: str
    status: RuntimeEventStatus | str = RuntimeEventStatus.PENDING
    status_class: RuntimeStatusClass = RuntimeStatusClass.NOT_STARTED
    updated_at: float | None = None
    processes: dict[str, RuntimeProcessState] = Field(default_factory=dict)
    attempts: dict[str, RuntimeProcessAttempt] = Field(default_factory=dict)
    process_attempt_ids: dict[str, list[str]] = Field(default_factory=dict)
    checkpoints: dict[str, RuntimeCheckpointRef] = Field(default_factory=dict)
    latest_checkpoint_by_process: dict[str, str] = Field(default_factory=dict)
    active_process_ids: list[str] = Field(default_factory=list)
    terminal_success_count: int = 0
    terminal_failure_count: int = 0
    terminal_cancelled_count: int = 0
    error_summary: list[RuntimeErrorInfo] = Field(default_factory=list)
    artifact_index: list[dict[str, Any]] = Field(default_factory=list)
    progress_summary: dict[str, RuntimeProgress] = Field(default_factory=dict)
    resource_usage_summary: dict[str, RuntimeResourceUsage] = Field(default_factory=dict)
    event_count: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class RuntimeProjectionPolicy(BaseModel):
    """Framework projection behavior switches."""

    propagate_child_failure: bool = True
    propagate_child_cancelled: bool = True


class RuntimeReducer(Protocol):
    """Protocol for reducers that derive projections from RuntimeEvent streams."""

    def reduce(self, events: list[RuntimeEvent]) -> RuntimeProjection:
        """Reduce a list of events into a deterministic projection."""
        ...


class RuntimeFrameworkReducer:
    """Business-neutral reducer for standard Flowlet runtime events."""

    def __init__(self, policy: RuntimeProjectionPolicy | None = None) -> None:
        self.policy = policy or RuntimeProjectionPolicy()

    def reduce(self, events: list[RuntimeEvent]) -> RuntimeProjection:
        """Reduce a list of standard events into framework state."""
        ordered_events = sorted(events, key=_event_sort_key)
        runtime_id = ordered_events[0].runtime_id if ordered_events else "runtime"
        projection = RuntimeProjection(runtime_id=runtime_id, event_count=len(ordered_events))
        for event in ordered_events:
            projection.runtime_id = event.runtime_id
            projection.updated_at = event.timestamp
            _apply_attempt_event(projection, event)
            _apply_checkpoint_event(projection, event)
            _apply_process_event(projection, event)
            _apply_runtime_event(projection, event)
            _apply_indexes(projection, event)
        _finalize_projection(projection, self.policy)
        return projection


def runtime_projection_payload(
    events: list[RuntimeEvent],
    *,
    policy: RuntimeProjectionPolicy | None = None,
) -> dict[str, Any]:
    """Build a JSON-safe runtime projection payload from standard events."""
    return RuntimeFrameworkReducer(policy=policy).reduce(events).model_dump(mode="json")


def load_runtime_projection(path: str | Path) -> RuntimeProjection | None:
    """Load a persisted runtime projection when it exists."""
    projection_path = Path(path)
    if projection_path.is_dir():
        projection_path = projection_path / "runtime" / "projection.json"
    if not projection_path.exists():
        return None
    return RuntimeProjection.model_validate_json(projection_path.read_text(encoding="utf-8"))


def _apply_process_event(projection: RuntimeProjection, event: RuntimeEvent) -> None:
    if event.process_id is None:
        return
    state = projection.processes.get(event.process_id)
    if state is None:
        state = RuntimeProcessState(
            process_id=event.process_id,
            parent_process_id=event.parent_process_id,
            updated_at=event.timestamp,
        )
    update: dict[str, Any] = {
        "updated_at": event.timestamp,
        "parent_process_id": event.parent_process_id or state.parent_process_id,
    }
    if event.attempt_id is not None:
        update["current_attempt_id"] = event.attempt_id
    if event.payload.get("process_type") is not None:
        update["process_type"] = str(event.payload["process_type"])
    attempt_state = _ATTEMPT_EVENT_STATE.get(event.event_type)
    if event.status is not None:
        update["status"] = event.status
    elif attempt_state is not None:
        update["status"] = attempt_state[0]
    if event.status_class is not None:
        update["status_class"] = event.status_class
    elif attempt_state is not None:
        update["status_class"] = attempt_state[1]
    if event.progress is not None:
        update["progress"] = event.progress
        projection.progress_summary[event.process_id] = event.progress
    if event.error is not None:
        update["error"] = event.error
    resource_usage = _resource_usage_from_event(event)
    if resource_usage is not None:
        update["resource_usage"] = resource_usage
        projection.resource_usage_summary[event.process_id] = resource_usage
    effective_status_class = event.status_class or (attempt_state[1] if attempt_state is not None else None)
    if effective_status_class == RuntimeStatusClass.ACTIVE:
        update["started_at"] = state.started_at or event.timestamp
    if effective_status_class in {
        RuntimeStatusClass.TERMINAL_SUCCESS,
        RuntimeStatusClass.TERMINAL_FAILURE,
        RuntimeStatusClass.TERMINAL_CANCELLED,
    }:
        update["finished_at"] = event.timestamp
    if effective_status_class == RuntimeStatusClass.TERMINAL_SUCCESS and "result" in event.payload:
        result = event.payload["result"]
        update["result"] = result if isinstance(result, dict) else {"value": result}
    state = state.model_copy(update=update)
    projection.processes[event.process_id] = state


_ATTEMPT_EVENT_STATE: dict[str, tuple[RuntimeEventStatus, RuntimeStatusClass]] = {
    RuntimeEventType.PROCESS_ATTEMPT_CREATED: (
        RuntimeEventStatus.PENDING,
        RuntimeStatusClass.NOT_STARTED,
    ),
    RuntimeEventType.PROCESS_ATTEMPT_STARTED: (
        RuntimeEventStatus.RUNNING,
        RuntimeStatusClass.ACTIVE,
    ),
    RuntimeEventType.PROCESS_ATTEMPT_COMPLETED: (
        RuntimeEventStatus.SUCCEEDED,
        RuntimeStatusClass.TERMINAL_SUCCESS,
    ),
    RuntimeEventType.PROCESS_ATTEMPT_FAILED: (
        RuntimeEventStatus.FAILED,
        RuntimeStatusClass.TERMINAL_FAILURE,
    ),
    RuntimeEventType.PROCESS_ATTEMPT_CANCELLED: (
        RuntimeEventStatus.CANCELLED,
        RuntimeStatusClass.TERMINAL_CANCELLED,
    ),
}


def _apply_attempt_event(projection: RuntimeProjection, event: RuntimeEvent) -> None:
    if event.event_type not in _ATTEMPT_EVENT_STATE or event.process_id is None or event.attempt_id is None:
        return
    default_status, default_status_class = _ATTEMPT_EVENT_STATE[event.event_type]
    attempt = projection.attempts.get(event.attempt_id)
    if attempt is None:
        attempt_ids = projection.process_attempt_ids.setdefault(event.process_id, [])
        attempt_ids.append(event.attempt_id)
        attempt = RuntimeProcessAttempt(
            attempt_id=event.attempt_id,
            process_id=event.process_id,
            runtime_id=event.runtime_id,
            execution_id=event.execution_id,
            ordinal=int(event.payload.get("ordinal", len(attempt_ids))),
            operation=_attempt_operation(event.payload),
            execution_key=event.payload.get("execution_key"),
            input_fingerprint=event.payload.get("input_fingerprint"),
            implementation_version=event.payload.get("implementation_version"),
            first_event_sequence=_event_sequence(event),
        )
    update: dict[str, Any] = {
        "status": event.status or default_status,
        "status_class": event.status_class or default_status_class,
        "checkpoint_id": event.checkpoint_id or attempt.checkpoint_id,
        "error": event.error or attempt.error,
        "last_event_sequence": _event_sequence(event),
    }
    if event.payload.get("resumed_from_attempt_id") is not None:
        update["resumed_from_attempt_id"] = str(event.payload["resumed_from_attempt_id"])
    if event.event_type == RuntimeEventType.PROCESS_ATTEMPT_STARTED:
        update["started_at"] = attempt.started_at or event.timestamp
    if default_status_class in {
        RuntimeStatusClass.TERMINAL_SUCCESS,
        RuntimeStatusClass.TERMINAL_FAILURE,
        RuntimeStatusClass.TERMINAL_CANCELLED,
    }:
        update["finished_at"] = event.timestamp
    projection.attempts[event.attempt_id] = attempt.model_copy(update=update)


def _apply_checkpoint_event(projection: RuntimeProjection, event: RuntimeEvent) -> None:
    if event.checkpoint_id is None:
        return
    if event.event_type == RuntimeEventType.CHECKPOINT_INVALIDATED:
        removed = projection.checkpoints.pop(event.checkpoint_id, None)
        if (
            removed is not None
            and projection.latest_checkpoint_by_process.get(removed.process_id) == event.checkpoint_id
        ):
            candidates = [
                checkpoint
                for checkpoint in projection.checkpoints.values()
                if checkpoint.process_id == removed.process_id
            ]
            if candidates:
                latest = max(candidates, key=lambda checkpoint: (checkpoint.created_at, checkpoint.checkpoint_id))
                projection.latest_checkpoint_by_process[removed.process_id] = latest.checkpoint_id
            else:
                projection.latest_checkpoint_by_process.pop(removed.process_id, None)
        return
    if event.event_type != RuntimeEventType.CHECKPOINT_COMMITTED:
        return
    payload = event.payload.get("checkpoint", event.payload)
    if not isinstance(payload, dict):
        return
    try:
        checkpoint = RuntimeCheckpointRef.model_validate(payload)
    except ValueError:
        return
    if checkpoint.checkpoint_id != event.checkpoint_id:
        return
    projection.checkpoints[checkpoint.checkpoint_id] = checkpoint
    current_id = projection.latest_checkpoint_by_process.get(checkpoint.process_id)
    current = projection.checkpoints.get(current_id) if current_id is not None else None
    if current is None or (checkpoint.created_at, checkpoint.checkpoint_id) >= (
        current.created_at,
        current.checkpoint_id,
    ):
        projection.latest_checkpoint_by_process[checkpoint.process_id] = checkpoint.checkpoint_id


def _apply_runtime_event(projection: RuntimeProjection, event: RuntimeEvent) -> None:
    if event.process_id is not None:
        return
    if event.status is not None:
        projection.status = event.status
    if event.status_class is not None:
        projection.status_class = event.status_class


def _apply_indexes(projection: RuntimeProjection, event: RuntimeEvent) -> None:
    if event.error is not None:
        projection.error_summary.append(event.error)
    if event.event_type in {
        RuntimeEventType.ARTIFACT_PRODUCED,
        RuntimeEventType.ARTIFACT_UPDATED,
        RuntimeEventType.ARTIFACT_REMOVED,
    }:
        artifact = dict(event.payload)
        artifact.setdefault("event_type", event.event_type)
        artifact.setdefault("process_id", event.process_id)
        artifact.setdefault("timestamp", event.timestamp)
        projection.artifact_index.append(artifact)


def _resource_usage_from_event(event: RuntimeEvent) -> RuntimeResourceUsage | None:
    if event.event_type != RuntimeEventType.RESOURCE_SAMPLED:
        return None
    payload = event.payload.get("resource_usage")
    return RuntimeResourceUsage.model_validate(payload) if isinstance(payload, dict) else None


def _finalize_projection(projection: RuntimeProjection, policy: RuntimeProjectionPolicy) -> None:
    if policy.propagate_child_failure or policy.propagate_child_cancelled:
        _propagate_child_terminal_state(projection, policy)
    projection.active_process_ids = sorted(
        process_id
        for process_id, state in projection.processes.items()
        if state.status_class == RuntimeStatusClass.ACTIVE
    )
    projection.terminal_success_count = _count_processes(projection, RuntimeStatusClass.TERMINAL_SUCCESS)
    projection.terminal_failure_count = _count_processes(projection, RuntimeStatusClass.TERMINAL_FAILURE)
    projection.terminal_cancelled_count = _count_processes(projection, RuntimeStatusClass.TERMINAL_CANCELLED)
    if projection.terminal_failure_count:
        projection.status = RuntimeEventStatus.FAILED
        projection.status_class = RuntimeStatusClass.TERMINAL_FAILURE
    elif projection.terminal_cancelled_count:
        projection.status = RuntimeEventStatus.CANCELLED
        projection.status_class = RuntimeStatusClass.TERMINAL_CANCELLED
    elif projection.active_process_ids:
        projection.status = RuntimeEventStatus.RUNNING
        projection.status_class = RuntimeStatusClass.ACTIVE
    elif projection.processes and projection.terminal_success_count == len(projection.processes):
        projection.status = RuntimeEventStatus.SUCCEEDED
        projection.status_class = RuntimeStatusClass.TERMINAL_SUCCESS


def _propagate_child_terminal_state(projection: RuntimeProjection, policy: RuntimeProjectionPolicy) -> None:
    changed = True
    while changed:
        changed = False
        for child in list(projection.processes.values()):
            if child.parent_process_id is None:
                continue
            parent = projection.processes.get(child.parent_process_id)
            if parent is None:
                continue
            if policy.propagate_child_failure and child.status_class == RuntimeStatusClass.TERMINAL_FAILURE:
                changed = _update_parent_terminal_state(
                    projection,
                    parent,
                    status=RuntimeEventStatus.FAILED,
                    status_class=RuntimeStatusClass.TERMINAL_FAILURE,
                    error=child.error,
                ) or changed
            if policy.propagate_child_cancelled and child.status_class == RuntimeStatusClass.TERMINAL_CANCELLED:
                changed = _update_parent_terminal_state(
                    projection,
                    parent,
                    status=RuntimeEventStatus.CANCELLED,
                    status_class=RuntimeStatusClass.TERMINAL_CANCELLED,
                    error=child.error,
                ) or changed


def _update_parent_terminal_state(
    projection: RuntimeProjection,
    parent: RuntimeProcessState,
    *,
    status: RuntimeEventStatus,
    status_class: RuntimeStatusClass,
    error: RuntimeErrorInfo | None,
) -> bool:
    if parent.status_class == status_class:
        return False
    if parent.status_class in {
        RuntimeStatusClass.TERMINAL_FAILURE,
        RuntimeStatusClass.TERMINAL_CANCELLED,
    }:
        return False
    projection.processes[parent.process_id] = parent.model_copy(
        update={
            "status": status,
            "status_class": status_class,
            "error": parent.error or error,
        }
    )
    return True


def _count_processes(projection: RuntimeProjection, status_class: RuntimeStatusClass) -> int:
    return sum(1 for state in projection.processes.values() if state.status_class == status_class)


def _event_sort_key(event: RuntimeEvent) -> tuple[int, float, int, str]:
    numeric_id = -1
    try:
        numeric_id = int(event.event_id)
    except (TypeError, ValueError):
        pass
    if event.sequence is not None:
        return (0, float(event.sequence), numeric_id, str(event.event_id))
    return (1, event.timestamp, numeric_id, str(event.event_id))


def _event_sequence(event: RuntimeEvent) -> int | None:
    value = event.sequence if event.sequence is not None else event.event_id
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _attempt_operation(payload: dict[str, Any]) -> RuntimeProcessOperation | None:
    mapping = {
        "start": RuntimeProcessOperation.START,
        "resume": RuntimeProcessOperation.RESUME,
        "retry": RuntimeProcessOperation.RETRY,
        "restart": RuntimeProcessOperation.START,
    }
    value = payload.get("operation", payload.get("recovery_action"))
    return mapping.get(str(value))
