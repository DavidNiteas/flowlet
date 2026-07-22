"""Filesystem store helpers for Flowlet runtime directories."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifacts import list_runtime_artifacts
from .event_store import RuntimeEventJsonlStore
from .info import RuntimeFileLayout
from .process import RuntimeProcessSpec
from .projection import RuntimeProjection
from .schema import RuntimeEvent


def write_json(path: str | Path, payload: Any) -> None:
    """Write JSON with Flowlet's stable runtime formatting."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


class RuntimeStore:
    """Small helper around the standard Flowlet runtime file layout."""

    def __init__(self, runtime_dir: str | Path, *, layout: RuntimeFileLayout | None = None) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.layout = layout or RuntimeFileLayout()

    def path(self, relative_path: str) -> Path:
        return self.runtime_dir / relative_path

    def write_status(self, payload: Any) -> None:
        write_json(self.path(self.layout.status), payload)

    def write_runtime_info(self, payload: Any) -> None:
        write_json(self.path(self.layout.runtime_info), payload)

    def write_job_spec(self, payload: Any) -> None:
        write_json(self.path(self.layout.job_spec), payload)

    def write_process_specs(self, specs: list[RuntimeProcessSpec]) -> None:
        """Persist serializable process declarations, not process implementations."""
        write_json(self.path(self.layout.processes), [spec.model_dump(mode="json") for spec in specs])

    def load_process_specs(self) -> list[RuntimeProcessSpec]:
        """Load persisted process declarations, returning an empty list when absent."""
        path = self.path(self.layout.processes)
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError(f"Runtime process manifest must be a JSON list: {path}")
        return [RuntimeProcessSpec.model_validate(item) for item in payload]

    def write_snapshot(self, payload: Any) -> None:
        write_json(self.path(self.layout.snapshot), payload)

    def write_monitor_snapshot(self, payload: Any) -> None:
        write_json(self.path(self.layout.monitor_snapshot), payload)

    def write_progress(self, payload: Any) -> None:
        write_json(self.path(self.layout.progress), payload)

    def write_projection(self, payload: Any) -> None:
        write_json(self.path(self.layout.projection), payload)

    def load_projection(self) -> RuntimeProjection | None:
        path = self.path(self.layout.projection)
        if not path.exists():
            return None
        return RuntimeProjection.model_validate_json(path.read_text(encoding="utf-8"))

    def write_signals(self, payload: Any) -> None:
        write_json(self.path(self.layout.signals), payload)

    def runtime_event_store(self) -> RuntimeEventJsonlStore:
        """Return the standard sidecar RuntimeEvent JSONL store."""
        return RuntimeEventJsonlStore(self.path(self.layout.runtime_events))

    def append_runtime_event(self, event: RuntimeEvent) -> RuntimeEvent:
        """Append one standard RuntimeEvent to the sidecar event stream."""
        return self.runtime_event_store().append(event)

    def artifacts(self) -> list[dict[str, Any]]:
        return list_runtime_artifacts(self.runtime_dir)
