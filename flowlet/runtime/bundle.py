"""Runtime manager bundle for Flowlet-backed jobs."""

from __future__ import annotations

from dataclasses import dataclass

from ..base import LogManager, ProgressManager, SignalPool, StreamManager, TelemetryManager
from ..dashboard import DashboardServer, DashboardState


@dataclass
class RuntimeManagerBundle:
    """Flowlet runtime managers owned by one backend job."""

    progress: ProgressManager
    logs: LogManager
    streams: StreamManager
    telemetry: TelemetryManager
    signals: SignalPool
    dashboard: DashboardServer | None = None

    @classmethod
    def create(cls) -> RuntimeManagerBundle:
        return cls(
            progress=ProgressManager(),
            logs=LogManager(max_records=5000),
            streams=StreamManager(max_chunks=5000),
            telemetry=TelemetryManager(max_events=10000),
            signals=SignalPool(),
        )

    def dashboard_state(self) -> DashboardState:
        return DashboardState(
            progress=self.progress,
            telemetry=self.telemetry,
            logs=self.logs,
            streams=self.streams,
            signals=self.signals,
        )

    def close(self) -> None:
        if self.dashboard is not None:
            self.dashboard.close()
        self.progress.close()
        self.logs.close()
        self.streams.close()
        self.telemetry.close()
        self.signals.close()
