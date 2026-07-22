"""Backend lifecycle support helpers for Flowlet runtime jobs."""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Generic, TypeVar

from .store import RuntimeStore

TERMINAL_JOB_STATUSES = frozenset({"completed", "failed", "cancelled", "interrupted"})
NON_TERMINAL_JOB_STATUSES = frozenset({"created", "queued", "running"})

JobRecordT = TypeVar("JobRecordT")
JobSpecT = TypeVar("JobSpecT")
RuntimeT = TypeVar("RuntimeT")
EventBufferT = TypeVar("EventBufferT")


class FlowletJobBackendBase(Generic[JobRecordT, JobSpecT, RuntimeT, EventBufferT]):
    """Shared state container for Flowlet-backed job backends."""

    def __init__(self, runtime_root: str | Path | None = None) -> None:
        import threading

        self.runtime_root = Path(runtime_root) if runtime_root is not None else None
        if self.runtime_root is not None:
            self.runtime_root.mkdir(parents=True, exist_ok=True)
        self._jobs: dict[str, JobRecordT] = {}
        self._specs: dict[str, JobSpecT] = {}
        self._runtimes: dict[str, RuntimeT] = {}
        self._events: dict[str, EventBufferT] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._mirror_threads: dict[str, threading.Thread] = {}
        self._mirror_offsets: dict[str, dict[str, Any]] = {}
        self._persistent_jobs: set[str] = set()
        self._lock = threading.RLock()

    def close(self) -> None:
        for runtime in list(self._runtimes.values()):
            if hasattr(runtime, "close"):
                runtime.close()

    def should_persist_job(self, job_id: str) -> bool:
        return job_id in self._persistent_jobs


def new_mirror_offsets() -> dict[str, Any]:
    """Return the standard offsets used when mirroring runtime managers to events."""
    return {"progress": {}, "signals": {}, "logs": 0, "streams": 0, "telemetry": 0}


def model_dump_json_safe(value: Any) -> Any:
    """Dump Pydantic-style values to JSON-safe objects."""
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return value
    return value


def mirror_runtime_events(runtime: Any, event_buffer: Any, offsets: dict[str, Any]) -> None:
    """Mirror Flowlet runtime manager updates into an event buffer."""
    progress_snapshot = {key: model_dump_json_safe(value) for key, value in runtime.progress.get_all_progress().items()}
    for task_id, payload in progress_snapshot.items():
        previous = offsets["progress"].get(task_id)
        if previous != payload:
            event_buffer.emit("progress", payload)
            offsets["progress"][task_id] = payload

    signal_snapshot = {key: model_dump_json_safe(value) for key, value in runtime.signals.snapshot().items()}
    for name, payload in signal_snapshot.items():
        version = payload.get("version") if isinstance(payload, dict) else None
        if offsets["signals"].get(name) != version:
            event_buffer.emit("signal", payload)
            offsets["signals"][name] = version

    logs = [model_dump_json_safe(record) for record in runtime.logs.get_records()]
    for payload in logs[offsets["logs"] :]:
        event_buffer.emit("log", payload)
    offsets["logs"] = len(logs)

    streams = [model_dump_json_safe(chunk) for chunk in runtime.streams.get_chunks()]
    for payload in streams[offsets["streams"] :]:
        event_buffer.emit("stream", payload)
    offsets["streams"] = len(streams)

    telemetry = [model_dump_json_safe(event) for event in runtime.telemetry.get_events()]
    for payload in telemetry[offsets["telemetry"] :]:
        event_buffer.emit("telemetry", payload)
    offsets["telemetry"] = len(telemetry)


def wait_runtime_events(
    *,
    mirror: Any,
    event_buffer: Any,
    since: int | None = None,
    timeout: float | None = None,
) -> list[Any]:
    """Wait for events while periodically mirroring runtime managers."""
    deadline = None if timeout is None else time.time() + timeout
    while True:
        mirror()
        events = event_buffer.list(since=since)
        if events:
            return events
        if timeout is not None and time.time() >= (deadline or 0.0):
            return []
        wait_timeout = 0.2
        if deadline is not None:
            wait_timeout = max(0.0, min(wait_timeout, deadline - time.time()))
        event_buffer.wait_for_next(since=since, timeout=wait_timeout)


def stream_runtime_events(
    *,
    wait_events: Any,
    events: Any,
    mirror: Any,
    since: int | None = None,
    heartbeat_interval: float = 1.0,
    idle_timeout: float | None = None,
) -> Iterator[Any | None]:
    """Yield events and heartbeat markers from a runtime event source."""
    last_id = since
    last_emit = time.time()
    idle_started = time.time()
    while True:
        batch = wait_events(since=last_id, timeout=heartbeat_interval)
        if batch is None:
            return
        if batch:
            idle_started = time.time()
            for event in batch:
                last_id = event.event_id
                last_emit = time.time()
                yield event
                if event.event_type == "job_state" and event.payload.get("status") in TERMINAL_JOB_STATUSES:
                    mirror()
                    for trailing in events(since=last_id) or []:
                        last_id = trailing.event_id
                        yield trailing
                    return
            continue
        now = time.time()
        if idle_timeout is not None and now - idle_started >= idle_timeout:
            return
        if now - last_emit >= heartbeat_interval:
            last_emit = now
            yield None


def write_runtime_status(runtime_dir: str | Path, status_payload: Any, runtime_info_payload: Any) -> None:
    """Write standard status and runtime info files."""
    store = RuntimeStore(runtime_dir)
    store.write_status(status_payload)
    store.write_runtime_info(runtime_info_payload)


def export_runtime_manager_files(runtime_dir: str | Path, runtime: Any) -> None:
    """Export standard runtime manager files under a runtime directory."""
    store = RuntimeStore(runtime_dir)
    manager_dir = store.path(store.layout.progress).parent
    manager_dir.mkdir(parents=True, exist_ok=True)
    store.write_progress(
        {key: model_dump_json_safe(value) for key, value in runtime.progress.get_all_progress().items()},
    )
    store.write_signals(
        {key: model_dump_json_safe(value) for key, value in runtime.signals.snapshot().items()},
    )
    runtime.logs.to_jsonl(manager_dir / "logs.jsonl")
    runtime.telemetry.to_jsonl(manager_dir / "telemetry.jsonl")
