"""Dashboard data aggregation."""

from __future__ import annotations

import time
from typing import Any

from ..base.logs import LogManager
from ..base.progress import ProgressManager
from ..base.signal import SignalPool
from ..base.stream import StreamManager
from ..base.telemetry import TelemetryManager
from ..scheduler import Scheduler


class DashboardState:
    """Read-only snapshot aggregator for Flowlet runtime state."""

    def __init__(
        self,
        *,
        progress: ProgressManager | None = None,
        telemetry: TelemetryManager | None = None,
        logs: LogManager | None = None,
        streams: StreamManager | None = None,
        signals: SignalPool | None = None,
        schedulers: list[Scheduler] | None = None,
        tail_limit: int = 100,
    ) -> None:
        self.progress = progress
        self.telemetry = telemetry
        self.logs = logs
        self.streams = streams
        self.signals = signals
        self.schedulers = schedulers or []
        self.tail_limit = tail_limit

    def snapshot(self) -> dict[str, Any]:
        telemetry_events = [] if self.telemetry is None else self.telemetry.tail(self.tail_limit)
        return {
            "timestamp": time.time(),
            "summary": self._summary(),
            "progress": self._progress_snapshot(),
            "signals": self._signal_snapshot(),
            "schedulers": [scheduler.snapshot() for scheduler in self.schedulers],
            "telemetry": [_model_dump(event) for event in telemetry_events],
            "resources": self._resource_snapshot(telemetry_events),
            "logs": self._log_snapshot(),
            "streams": self._stream_snapshot(),
        }

    def _summary(self) -> dict[str, int]:
        progress = self._progress_snapshot()
        signals = self._signal_snapshot()
        actions = [action for scheduler in self.schedulers for action in scheduler.snapshot()["actions"]]
        return {
            "progress_total": len(progress),
            "progress_running": sum(1 for task in progress if task.get("status") == "running"),
            "progress_failed": sum(1 for task in progress if task.get("status") == "failed"),
            "signals_total": len(signals),
            "signals_running": sum(1 for signal in signals if signal.get("status") == "running"),
            "signals_failed": sum(1 for signal in signals if signal.get("status") == "failed"),
            "actions_total": len(actions),
            "actions_running": sum(1 for action in actions if action.get("running")),
            "actions_triggered": sum(1 for action in actions if action.get("triggered")),
        }

    def _progress_snapshot(self) -> list[dict[str, Any]]:
        if self.progress is None:
            return []
        return [_model_dump(task) for task in self.progress.get_all_progress().values()]

    def _signal_snapshot(self) -> list[dict[str, Any]]:
        if self.signals is None:
            return []
        return [_model_dump(signal) for signal in self.signals.snapshot().values()]

    def _log_snapshot(self) -> list[dict[str, Any]]:
        if self.logs is None:
            return []
        return [_model_dump(record) for record in self.logs.tail(self.tail_limit)]

    def _stream_snapshot(self) -> list[dict[str, Any]]:
        if self.streams is None:
            return []
        return [_model_dump(chunk) for chunk in self.streams.tail(self.tail_limit)]

    def _resource_snapshot(self, events: list[Any]) -> dict[str, Any]:
        system = None
        processes = {}
        for event in events:
            if event.event_type == "system_resource" and event.system is not None:
                system = _model_dump(event.system)
            elif event.event_type == "process_resource" and event.process is not None:
                process = _model_dump(event.process)
                key = process.get("worker_id") or process.get("node_id") or process.get("process_id")
                processes[str(key)] = process
        return {"system": system, "processes": list(processes.values())}


def _model_dump(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return value
    return {"value": repr(value)}
