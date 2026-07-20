"""Snapshot reader helpers for Flowlet runtime directories."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, TypeVar

from .info import RuntimeFileLayout

StatusT = TypeVar("StatusT")
SnapshotT = TypeVar("SnapshotT")
MonitorT = TypeVar("MonitorT")


@dataclass
class RuntimeSnapshotView(Generic[MonitorT, SnapshotT]):
    """Typed view loaded from a runtime directory."""

    monitor: MonitorT
    snapshot: SnapshotT | None = None
    runtime_info: dict[str, Any] | None = None


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
        normalize_monitor: Callable[[MonitorT], MonitorT] | None = None,
        layout: RuntimeFileLayout | None = None,
    ) -> None:
        self.status_loader = status_loader
        self.snapshot_loader = snapshot_loader
        self.monitor_loader = monitor_loader
        self.monitor_from_snapshot = monitor_from_snapshot
        self.monitor_from_status = monitor_from_status
        self.normalize_monitor = normalize_monitor or (lambda monitor: monitor)
        self.layout = layout or RuntimeFileLayout()

    def load(self, runtime_dir: str | Path) -> RuntimeSnapshotView[MonitorT, SnapshotT] | None:
        """Load runtime snapshot view with monitor, snapshot, then status fallback."""
        root = Path(runtime_dir)
        snapshot = self.load_snapshot(root)
        monitor = self.load_monitor(root)
        if monitor is not None:
            return RuntimeSnapshotView(
                monitor=self.normalize_monitor(monitor),
                snapshot=snapshot,
                runtime_info=self.load_runtime_info(root),
            )
        if snapshot is not None:
            return RuntimeSnapshotView(
                monitor=self.normalize_monitor(self.monitor_from_snapshot(snapshot)),
                snapshot=snapshot,
                runtime_info=self.load_runtime_info(root),
            )
        status = self.load_status(root)
        if status is not None:
            return RuntimeSnapshotView(
                monitor=self.normalize_monitor(self.monitor_from_status(status)),
                snapshot=None,
                runtime_info=self.load_runtime_info(root),
            )
        return None

    def load_monitor(self, runtime_dir: str | Path) -> MonitorT | None:
        return self._load_typed(Path(runtime_dir) / self.layout.monitor_snapshot, self.monitor_loader)

    def load_snapshot(self, runtime_dir: str | Path) -> SnapshotT | None:
        return self._load_typed(Path(runtime_dir) / self.layout.snapshot, self.snapshot_loader)

    def load_status(self, runtime_dir: str | Path) -> StatusT | None:
        return self._load_typed(Path(runtime_dir) / self.layout.status, self.status_loader)

    def load_runtime_info(self, runtime_dir: str | Path) -> dict[str, Any] | None:
        path = Path(runtime_dir) / self.layout.runtime_info
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _load_typed(path: Path, loader: Callable[[str], Any]) -> Any | None:
        if not path.exists():
            return None
        return loader(path.read_text(encoding="utf-8"))
