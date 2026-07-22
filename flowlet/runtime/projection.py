"""Framework-level runtime projections derived from RuntimeEvent streams."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, Field

from .process import RuntimeProcessState
from .schema import RuntimeErrorInfo, RuntimeEvent, RuntimeEventStatus, RuntimeProgress, RuntimeStatusClass


class RuntimeProjection(BaseModel):
    """Framework-level runtime state derived from standard events."""

    runtime_id: str
    status: RuntimeEventStatus | str = RuntimeEventStatus.PENDING
    status_class: RuntimeStatusClass = RuntimeStatusClass.NOT_STARTED
    updated_at: float | None = None
    processes: dict[str, RuntimeProcessState] = Field(default_factory=dict)
    active_process_ids: list[str] = Field(default_factory=list)
    terminal_success_count: int = 0
    terminal_failure_count: int = 0
    terminal_cancelled_count: int = 0
    error_summary: list[RuntimeErrorInfo] = Field(default_factory=list)
    artifact_index: list[dict[str, Any]] = Field(default_factory=list)
    progress_summary: dict[str, RuntimeProgress] = Field(default_factory=dict)
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
    if event.payload.get("process_type") is not None:
        update["process_type"] = str(event.payload["process_type"])
    if event.status is not None:
        update["status"] = event.status
    if event.status_class is not None:
        update["status_class"] = event.status_class
    if event.progress is not None:
        update["progress"] = event.progress
        projection.progress_summary[event.process_id] = event.progress
    if event.error is not None:
        update["error"] = event.error
    if (
        event.event_type in {"process.started", "process.status.changed"}
        and event.status_class == RuntimeStatusClass.ACTIVE
    ):
        update["started_at"] = state.started_at or event.timestamp
    if event.status_class in {
        RuntimeStatusClass.TERMINAL_SUCCESS,
        RuntimeStatusClass.TERMINAL_FAILURE,
        RuntimeStatusClass.TERMINAL_CANCELLED,
    }:
        update["finished_at"] = event.timestamp
    if event.status_class == RuntimeStatusClass.TERMINAL_SUCCESS and "result" in event.payload:
        result = event.payload["result"]
        update["result"] = result if isinstance(result, dict) else {"value": result}
    state = state.model_copy(update=update)
    projection.processes[event.process_id] = state


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
    if event.event_type in {"artifact.produced", "artifact.updated", "artifact.removed"}:
        artifact = dict(event.payload)
        artifact.setdefault("event_type", event.event_type)
        artifact.setdefault("process_id", event.process_id)
        artifact.setdefault("timestamp", event.timestamp)
        projection.artifact_index.append(artifact)


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


def _event_sort_key(event: RuntimeEvent) -> tuple[float, int, str]:
    numeric_id = -1
    try:
        numeric_id = int(event.event_id)
    except (TypeError, ValueError):
        pass
    return (event.timestamp, numeric_id, str(event.event_id))
