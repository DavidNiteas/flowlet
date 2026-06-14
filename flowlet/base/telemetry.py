"""Runtime telemetry collection for resources and performance events."""

from __future__ import annotations

import os
import threading
import time
from contextlib import suppress
from copy import deepcopy
from multiprocessing import Queue as MPQueue
from queue import Empty
from typing import Any, Literal

from pydantic import Field

from ..config.base_config import BaseConfig

TelemetryEventType = Literal["system_resource", "process_resource", "metric", "checkpoint"]


class SystemResourceSnapshot(BaseConfig):
    """System-level resource snapshot."""

    timestamp: float = 0.0
    cpu_percent: float | None = None
    memory_total: int | None = None
    memory_used: int | None = None
    memory_available: int | None = None
    memory_percent: float | None = None
    swap_total: int | None = None
    swap_used: int | None = None
    swap_percent: float | None = None
    disk_read_bytes: int | None = None
    disk_write_bytes: int | None = None
    net_sent_bytes: int | None = None
    net_recv_bytes: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProcessResourceSnapshot(BaseConfig):
    """Process-level resource snapshot."""

    timestamp: float = 0.0
    process_id: int | None = None
    process_name: str | None = None
    worker_id: str | None = None
    node_id: str | None = None
    run_id: str | None = None
    stage: str | None = None
    cpu_percent: float | None = None
    memory_rss: int | None = None
    memory_vms: int | None = None
    memory_percent: float | None = None
    num_threads: int | None = None
    open_files: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TelemetryEvent(BaseConfig):
    """Serializable telemetry event transported from workers to the manager."""

    event_type: TelemetryEventType
    timestamp: float = 0.0
    worker_id: str | None = None
    node_id: str | None = None
    run_id: str | None = None
    stage: str | None = None
    name: str | None = None
    value: float | int | str | bool | None = None
    unit: str | None = None
    elapsed_since_start: float | None = None
    elapsed_since_previous: float | None = None
    system: SystemResourceSnapshot | None = None
    process: ProcessResourceSnapshot | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class BaseTelemetryCollector:
    """Shared local API for telemetry managers and proxies."""

    def __init__(self, max_events: int | None = None) -> None:
        self._events: list[TelemetryEvent] = []
        self._max_events = max_events
        self._lock = threading.RLock()
        self._start_time = time.time()
        self._last_checkpoint_time: dict[str, float] = {}
        self._system_sampler_thread: threading.Thread | None = None
        self._system_sampler_running = False
        self._process_sampler_thread: threading.Thread | None = None
        self._process_sampler_running = False

    def get_events(self) -> list[TelemetryEvent]:
        with self._lock:
            return deepcopy(self._events)

    def tail(self, n: int = 100) -> list[TelemetryEvent]:
        with self._lock:
            return deepcopy(self._events[-n:])

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
            self._last_checkpoint_time.clear()

    def emit(self, event: TelemetryEvent) -> None:
        if event.timestamp == 0.0:
            event.timestamp = time.time()
        self._append_local(event)
        self._send_event(event)

    def metric(
        self,
        name: str,
        value: float | int | str | bool,
        *,
        unit: str | None = None,
        worker_id: str | None = None,
        node_id: str | None = None,
        run_id: str | None = None,
        stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.emit(
            TelemetryEvent(
                event_type="metric",
                name=name,
                value=value,
                unit=unit,
                worker_id=worker_id,
                node_id=node_id,
                run_id=run_id,
                stage=stage,
                metadata=metadata or {},
            )
        )

    def checkpoint(
        self,
        name: str,
        *,
        key: str | None = None,
        worker_id: str | None = None,
        node_id: str | None = None,
        run_id: str | None = None,
        stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = time.time()
        checkpoint_key = key or f"{worker_id or ''}:{run_id or ''}:{stage or ''}"
        previous = self._last_checkpoint_time.get(checkpoint_key, self._start_time)
        self._last_checkpoint_time[checkpoint_key] = now
        self.emit(
            TelemetryEvent(
                event_type="checkpoint",
                timestamp=now,
                name=name,
                worker_id=worker_id,
                node_id=node_id,
                run_id=run_id,
                stage=stage,
                elapsed_since_start=now - self._start_time,
                elapsed_since_previous=now - previous,
                metadata=metadata or {},
            )
        )

    def sample_system(self, *, metadata: dict[str, Any] | None = None) -> SystemResourceSnapshot:
        snapshot = _sample_system(metadata=metadata)
        self.emit(TelemetryEvent(event_type="system_resource", timestamp=snapshot.timestamp, system=snapshot))
        return snapshot

    def sample_process(
        self,
        pid: int | None = None,
        *,
        worker_id: str | None = None,
        node_id: str | None = None,
        run_id: str | None = None,
        stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ProcessResourceSnapshot:
        snapshot = _sample_process(
            pid=pid,
            worker_id=worker_id,
            node_id=node_id,
            run_id=run_id,
            stage=stage,
            metadata=metadata,
        )
        self.emit(
            TelemetryEvent(
                event_type="process_resource",
                timestamp=snapshot.timestamp,
                worker_id=worker_id,
                node_id=node_id,
                run_id=run_id,
                stage=stage,
                process=snapshot,
            )
        )
        return snapshot

    def start_system_sampler(self, interval: float = 1.0) -> None:
        if self._system_sampler_running:
            return
        self._system_sampler_running = True
        self._system_sampler_thread = threading.Thread(
            target=self._system_sampler_loop,
            args=(interval,),
            daemon=True,
        )
        self._system_sampler_thread.start()

    def stop_system_sampler(self) -> None:
        self._system_sampler_running = False
        if self._system_sampler_thread is not None:
            self._system_sampler_thread.join(timeout=2.0)
            self._system_sampler_thread = None

    def start_process_sampler(
        self,
        interval: float = 1.0,
        *,
        pid: int | None = None,
        worker_id: str | None = None,
        node_id: str | None = None,
        run_id: str | None = None,
        stage: str | None = None,
    ) -> None:
        if self._process_sampler_running:
            return
        self._process_sampler_running = True
        self._process_sampler_thread = threading.Thread(
            target=self._process_sampler_loop,
            args=(interval, pid, worker_id, node_id, run_id, stage),
            daemon=True,
        )
        self._process_sampler_thread.start()

    def stop_process_sampler(self) -> None:
        self._process_sampler_running = False
        if self._process_sampler_thread is not None:
            self._process_sampler_thread.join(timeout=2.0)
            self._process_sampler_thread = None

    def _append_local(self, event: TelemetryEvent) -> None:
        with self._lock:
            self._events.append(event)
            if self._max_events is not None and self._max_events >= 0:
                overflow = len(self._events) - self._max_events
                if overflow > 0:
                    del self._events[:overflow]

    def _send_event(self, event: TelemetryEvent) -> None:
        """Hook for proxies. Managers keep events locally only."""

    def _system_sampler_loop(self, interval: float) -> None:
        while self._system_sampler_running:
            with suppress(Exception):
                self.sample_system()
            time.sleep(interval)

    def _process_sampler_loop(
        self,
        interval: float,
        pid: int | None,
        worker_id: str | None,
        node_id: str | None,
        run_id: str | None,
        stage: str | None,
    ) -> None:
        while self._process_sampler_running:
            with suppress(Exception):
                self.sample_process(pid=pid, worker_id=worker_id, node_id=node_id, run_id=run_id, stage=stage)
            time.sleep(interval)


class BaseTelemetryProxy(BaseTelemetryCollector):
    def __init__(
        self,
        events_snapshot: list[TelemetryEvent] | None = None,
        max_events: int | None = None,
    ) -> None:
        super().__init__(max_events=max_events)
        if events_snapshot is not None:
            self._events = events_snapshot


class MPTelemetryProxy(BaseTelemetryProxy):
    def __init__(
        self,
        queue: MPQueue | None = None,
        events_snapshot: list[TelemetryEvent] | None = None,
        max_events: int | None = None,
    ) -> None:
        super().__init__(events_snapshot=events_snapshot, max_events=max_events)
        self._queue = queue

    def _send_event(self, event: TelemetryEvent) -> None:
        if self._queue is not None:
            self._queue.put(event)

    def __getstate__(self) -> dict[str, Any]:
        return {
            "events": [event.to_dict() for event in self._events],
            "max_events": self._max_events,
            "start_time": self._start_time,
            "last_checkpoint_time": dict(self._last_checkpoint_time),
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        BaseTelemetryCollector.__init__(self, max_events=state.get("max_events"))
        self._events = [TelemetryEvent.from_dict(v) for v in state.get("events", [])]
        self._start_time = state.get("start_time", time.time())
        self._last_checkpoint_time = dict(state.get("last_checkpoint_time", {}))
        self._queue = None


class RayTelemetryProxy(BaseTelemetryProxy):
    def __init__(
        self,
        queue: Any | None = None,
        events_snapshot: list[TelemetryEvent] | None = None,
        max_events: int | None = None,
    ) -> None:
        super().__init__(events_snapshot=events_snapshot, max_events=max_events)
        self._queue = queue

    def _send_event(self, event: TelemetryEvent) -> None:
        if self._queue is not None:
            self._queue.put(event)

    def __getstate__(self) -> dict[str, Any]:
        return {
            "events": [event.to_dict() for event in self._events],
            "max_events": self._max_events,
            "start_time": self._start_time,
            "last_checkpoint_time": dict(self._last_checkpoint_time),
            "queue": self._queue,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        BaseTelemetryCollector.__init__(self, max_events=state.get("max_events"))
        self._events = [TelemetryEvent.from_dict(v) for v in state.get("events", [])]
        self._start_time = state.get("start_time", time.time())
        self._last_checkpoint_time = dict(state.get("last_checkpoint_time", {}))
        self._queue = state.get("queue")


class TelemetryManager(BaseTelemetryCollector):
    """Driver-side telemetry manager."""

    def __init__(self, max_events: int | None = None) -> None:
        super().__init__(max_events=max_events)
        self._mp_queue: MPQueue | None = None
        self._ray_queue: Any | None = None
        self._consumer_threads: list[threading.Thread] = []

    def get_mp_proxy(self) -> MPTelemetryProxy:
        if self._mp_queue is None:
            self._mp_queue = MPQueue()
            thread = threading.Thread(target=self._consume_mp_loop, daemon=True)
            thread.start()
            self._consumer_threads.append(thread)
        with self._lock:
            snapshot = deepcopy(self._events)
        return MPTelemetryProxy(self._mp_queue, snapshot, self._max_events)

    def get_ray_proxy(self) -> RayTelemetryProxy:
        if self._ray_queue is None:
            try:
                from ray.util.queue import Queue as RayQueue
            except ImportError as exc:
                raise RuntimeError("Ray is not installed. Install ray to use get_ray_proxy().") from exc
            self._ray_queue = RayQueue()
            thread = threading.Thread(target=self._consume_ray_loop, daemon=True)
            thread.start()
            self._consumer_threads.append(thread)
        with self._lock:
            snapshot = deepcopy(self._events)
        return RayTelemetryProxy(self._ray_queue, snapshot, self._max_events)

    def get_proxy(self) -> MPTelemetryProxy:
        return self.get_mp_proxy()

    def to_jsonl(self, path: str | os.PathLike[str]) -> None:
        with open(path, "w", encoding="utf-8") as f:
            for event in self.get_events():
                f.write(event.model_dump_json() + "\n")

    def close(self) -> None:
        self.stop_system_sampler()
        self.stop_process_sampler()
        if self._mp_queue is not None:
            self._mp_queue.put(None)
            self._mp_queue = None
        if self._ray_queue is not None:
            with suppress(Exception):
                self._ray_queue.put(None)
            self._ray_queue = None
        for thread in self._consumer_threads:
            thread.join(timeout=2.0)
        self._consumer_threads.clear()

    def _consume_mp_loop(self) -> None:
        while True:
            try:
                msg = self._mp_queue.get(timeout=0.5)
            except Exception:
                continue
            if msg is None:
                break
            if isinstance(msg, TelemetryEvent):
                self._append_local(msg)

    def _consume_ray_loop(self) -> None:
        while True:
            try:
                msg = self._ray_queue.get(block=True, timeout=0.5)
            except Empty:
                continue
            except Exception:
                continue
            if msg is None:
                break
            if isinstance(msg, TelemetryEvent):
                self._append_local(msg)


def _sample_system(*, metadata: dict[str, Any] | None = None) -> SystemResourceSnapshot:
    try:
        import psutil
    except ImportError:
        return SystemResourceSnapshot(timestamp=time.time(), metadata={"psutil_available": False, **(metadata or {})})

    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = psutil.disk_io_counters()
    net = psutil.net_io_counters()
    return SystemResourceSnapshot(
        timestamp=time.time(),
        cpu_percent=float(psutil.cpu_percent(interval=None)),
        memory_total=int(memory.total),
        memory_used=int(memory.used),
        memory_available=int(memory.available),
        memory_percent=float(memory.percent),
        swap_total=int(swap.total),
        swap_used=int(swap.used),
        swap_percent=float(swap.percent),
        disk_read_bytes=int(disk.read_bytes) if disk is not None else None,
        disk_write_bytes=int(disk.write_bytes) if disk is not None else None,
        net_sent_bytes=int(net.bytes_sent) if net is not None else None,
        net_recv_bytes=int(net.bytes_recv) if net is not None else None,
        metadata={"psutil_available": True, **(metadata or {})},
    )


def _sample_process(
    *,
    pid: int | None = None,
    worker_id: str | None = None,
    node_id: str | None = None,
    run_id: str | None = None,
    stage: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> ProcessResourceSnapshot:
    pid = pid or os.getpid()
    try:
        import psutil
    except ImportError:
        return ProcessResourceSnapshot(
            timestamp=time.time(),
            process_id=pid,
            worker_id=worker_id,
            node_id=node_id,
            run_id=run_id,
            stage=stage,
            metadata={"psutil_available": False, **(metadata or {})},
        )

    process = psutil.Process(pid)
    memory = process.memory_info()
    with suppress(Exception):
        process.cpu_percent(interval=None)
    open_files = None
    with suppress(Exception):
        open_files = len(process.open_files())
    return ProcessResourceSnapshot(
        timestamp=time.time(),
        process_id=pid,
        process_name=process.name(),
        worker_id=worker_id,
        node_id=node_id,
        run_id=run_id,
        stage=stage,
        cpu_percent=float(process.cpu_percent(interval=None)),
        memory_rss=int(memory.rss),
        memory_vms=int(memory.vms),
        memory_percent=float(process.memory_percent()),
        num_threads=int(process.num_threads()),
        open_files=open_files,
        metadata={"psutil_available": True, **(metadata or {})},
    )


TelemetryProxy = MPTelemetryProxy
