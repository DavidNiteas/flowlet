"""Snapshot reader helpers for Flowlet runtime directories."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Generic, TypeVar

from .info import RuntimeFileLayout
from .process import RuntimeProcessSpec
from .projection import RuntimeProjection, load_runtime_projection

StatusT = TypeVar("StatusT")
SnapshotT = TypeVar("SnapshotT")
MonitorT = TypeVar("MonitorT")


@dataclass
class RuntimeSnapshotView(Generic[MonitorT, SnapshotT]):
    """Typed view loaded from a runtime directory."""

    monitor: MonitorT
    snapshot: SnapshotT | None = None
    runtime_info: dict[str, Any] | None = None
    projection: RuntimeProjection | None = None
    process_specs: list[RuntimeProcessSpec] = field(default_factory=list)


class RuntimeSnapshotLoader(Generic[StatusT, SnapshotT, MonitorT]):
    """Load monitor/snapshot/status files using business-provided validators."""

    def __init__(
        self,
        *,
        status_loader: Callable[[str], StatusT],
        snapshot_loader: Callable[[str], SnapshotT],
        monitor_loader: Callable[[str], MonitorT],
        monitor_from_snapshot: Callable[[SnapshotT], MonitorT],
        monitor_from_status: Callable[[StatusT], MonitorT],
        monitor_from_projection: Callable[[RuntimeProjection], MonitorT] | None = None,
        normalize_monitor: Callable[[MonitorT], MonitorT] | None = None,
        layout: RuntimeFileLayout | None = None,
    ) -> None:
        self.status_loader = status_loader
        self.snapshot_loader = snapshot_loader
        self.monitor_loader = monitor_loader
        self.monitor_from_snapshot = monitor_from_snapshot
        self.monitor_from_status = monitor_from_status
        self.monitor_from_projection = monitor_from_projection
        self.normalize_monitor = normalize_monitor or (lambda monitor: monitor)
        self.layout = layout or RuntimeFileLayout()

    def load(self, runtime_dir: str | Path) -> RuntimeSnapshotView[MonitorT, SnapshotT] | None:
        """Load a projection-aware view with legacy monitor/snapshot/status fallback."""
        root = Path(runtime_dir)
        snapshot = self.load_snapshot(root)
        runtime_info = self.load_runtime_info(root)
        projection = self.load_projection(root)
        process_specs = self.load_process_specs(root)
        if projection is not None and self.monitor_from_projection is not None:
            return RuntimeSnapshotView(
                monitor=self.normalize_monitor(self.monitor_from_projection(projection)),
                snapshot=snapshot,
                runtime_info=runtime_info,
                projection=projection,
                process_specs=process_specs,
            )
        monitor = self.load_monitor(root)
        if monitor is not None:
            return RuntimeSnapshotView(
                monitor=self.normalize_monitor(monitor),
                snapshot=snapshot,
                runtime_info=runtime_info,
                projection=projection,
                process_specs=process_specs,
            )
        if snapshot is not None:
            return RuntimeSnapshotView(
                monitor=self.normalize_monitor(self.monitor_from_snapshot(snapshot)),
                snapshot=snapshot,
                runtime_info=runtime_info,
                projection=projection,
                process_specs=process_specs,
            )
        status = self.load_status(root)
        if status is not None:
            return RuntimeSnapshotView(
                monitor=self.normalize_monitor(self.monitor_from_status(status)),
                snapshot=None,
                runtime_info=runtime_info,
                projection=projection,
                process_specs=process_specs,
            )
        return None

    def load_monitor(self, runtime_dir: str | Path) -> MonitorT | None:
        return self._load_typed(Path(runtime_dir) / self.layout.monitor_snapshot, self.monitor_loader)

    def load_snapshot(self, runtime_dir: str | Path) -> SnapshotT | None:
        return self._load_typed(Path(runtime_dir) / self.layout.snapshot, self.snapshot_loader)

    def load_status(self, runtime_dir: str | Path) -> StatusT | None:
        return self._load_typed(Path(runtime_dir) / self.layout.status, self.status_loader)

    @staticmethod
    def load_projection(runtime_dir: str | Path) -> RuntimeProjection | None:
        """Load the standard projection when a new runtime has produced one."""
        return load_runtime_projection(runtime_dir)

    def load_runtime_info(self, runtime_dir: str | Path) -> dict[str, Any] | None:
        path = Path(runtime_dir) / self.layout.runtime_info
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def load_process_specs(self, runtime_dir: str | Path) -> list[RuntimeProcessSpec]:
        """Load an optional declaration manifest without breaking old runtimes."""
        path = Path(runtime_dir) / self.layout.processes
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, list):
                return []
            return [RuntimeProcessSpec.model_validate(item) for item in payload]
        except Exception:
            return []

    @staticmethod
    def _load_typed(path: Path, loader: Callable[[str], Any]) -> Any | None:
        if not path.exists():
            return None
        return loader(path.read_text(encoding="utf-8"))
