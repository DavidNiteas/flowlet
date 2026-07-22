"""Standard runtime information schema for Flowlet-backed jobs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class RuntimeFileLayout(BaseModel):
    """Relative file paths used by a Flowlet runtime directory."""

    status: str = "status.json"
    runtime_info: str = "runtime_info.json"
    job_spec: str = "job_spec.json"
    events: str = "events.jsonl"
    runtime_events: str = "runtime/events.runtime.jsonl"
    processes: str = "runtime/processes.json"
    snapshot: str = "snapshot.json"
    monitor_snapshot: str = "monitor_snapshot.json"
    progress: str = "runtime/progress.json"
    projection: str = "runtime/projection.json"
    signals: str = "runtime/signals.json"
    logs: str = "runtime/logs.jsonl"
    telemetry: str = "runtime/telemetry.jsonl"


class RuntimeInfo(BaseModel):
    """Portable runtime metadata shared by Flowlet-backed applications."""

    schema_version: int = 1
    engine: str
    engine_version: str = "0.0.0.dev"
    job_type: str
    job_id: str
    status: str
    runtime_dir: str
    workspace_path: str | None = None
    output_path: str | None = None
    created_at: float | None = None
    updated_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    cancel_requested: bool | None = None
    main_pid: int | None = None
    planned_unit_count: int | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    runtime_files: RuntimeFileLayout = Field(default_factory=RuntimeFileLayout)


def runtime_info_payload(
    *,
    engine: str,
    job_type: str,
    job_id: str,
    status: str,
    runtime_dir: str | Path,
    engine_version: str = "0.0.0.dev",
    workspace_path: str | Path | None = None,
    output_path: str | Path | None = None,
    created_at: float | None = None,
    updated_at: float | None = None,
    started_at: float | None = None,
    finished_at: float | None = None,
    cancel_requested: bool | None = None,
    main_pid: int | None = None,
    planned_unit_count: int | None = None,
    result: dict[str, Any] | None = None,
    error: dict[str, Any] | None = None,
    runtime_files: RuntimeFileLayout | None = None,
) -> dict[str, Any]:
    """Build a JSON-safe runtime info payload."""
    return RuntimeInfo(
        engine=engine,
        engine_version=engine_version,
        job_type=job_type,
        job_id=job_id,
        status=status,
        runtime_dir=str(runtime_dir),
        workspace_path=str(workspace_path) if workspace_path is not None else None,
        output_path=str(output_path) if output_path is not None else None,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        finished_at=finished_at,
        cancel_requested=cancel_requested,
        main_pid=main_pid,
        planned_unit_count=planned_unit_count,
        result=result,
        error=error,
        runtime_files=runtime_files or RuntimeFileLayout(),
    ).model_dump(mode="json")
