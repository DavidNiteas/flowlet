from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from flowlet.runtime import (
    EventBuffer,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeEventJsonlStore,
    RuntimeEventStatus,
    RuntimeInfo,
    RuntimeProgress,
    RuntimeSnapshotLoader,
    RuntimeStatusClass,
    RuntimeStore,
    list_runtime_artifacts,
    runtime_event_payload,
    runtime_event_to_txn_event_payload,
    runtime_info_payload,
    txn_event_payload_to_runtime_event,
)


def test_runtime_info_payload_is_json_safe(tmp_path):
    payload = runtime_info_payload(
        engine="ExampleEngine",
        job_type="example.job",
        job_id="job1",
        status="completed",
        runtime_dir=tmp_path,
        workspace_path=tmp_path / "workspace",
        output_path=tmp_path / "workspace",
        planned_unit_count=3,
        result={"summary": {"count": 3}},
    )

    restored = RuntimeInfo.model_validate(payload)

    assert restored.schema_version == 1
    assert restored.engine == "ExampleEngine"
    assert restored.job_type == "example.job"
    assert restored.runtime_files.progress == "runtime/progress.json"


def test_runtime_store_writes_standard_files_and_lists_artifacts(tmp_path):
    store = RuntimeStore(tmp_path / "runtime")
    info = runtime_info_payload(
        engine="ExampleEngine",
        job_type="example.job",
        job_id="job1",
        status="running",
        runtime_dir=store.runtime_dir,
    )

    store.write_runtime_info(info)
    store.write_status({"job_id": "job1", "status": "running"})
    store.write_progress({"task": {"current": 1}})

    runtime_info = json.loads((store.runtime_dir / "runtime_info.json").read_text(encoding="utf-8"))
    artifacts = list_runtime_artifacts(store.runtime_dir)

    assert runtime_info["engine"] == "ExampleEngine"
    assert {item["path"] for item in artifacts} == {
        "runtime_info.json",
        "runtime/progress.json",
        "status.json",
    }


def test_runtime_event_payload_is_json_safe():
    payload = runtime_event_payload(
        event_id=1,
        runtime_id="runtime1",
        process_id="process1",
        event_type="process.progressed",
        timestamp=123.0,
        status=RuntimeEventStatus.RUNNING,
        status_class=RuntimeStatusClass.ACTIVE,
        progress=RuntimeProgress(current=1, total=4),
        payload={"domain": {"step": "prepare"}},
        metadata={"source": "test"},
    )

    restored = RuntimeEvent.model_validate(payload)

    assert restored.schema_version == 1
    assert restored.event_type == "process.progressed"
    assert restored.status == RuntimeEventStatus.RUNNING
    assert restored.status_class == RuntimeStatusClass.ACTIVE
    assert restored.progress is not None
    assert restored.progress.percent == 25.0


def test_runtime_event_accepts_custom_status_and_error():
    event = RuntimeEvent(
        event_id="evt-1",
        runtime_id="runtime1",
        event_type="business.custom",
        timestamp=123.0,
        status="domain_waiting",
        status_class=RuntimeStatusClass.BLOCKED,
        error=RuntimeErrorInfo(
            type="ExampleError",
            message="example failure",
            retryable=True,
            context={"process_id": "process1"},
        ),
    )

    payload = event.model_dump(mode="json")

    assert payload["status"] == "domain_waiting"
    assert payload["status_class"] == "blocked"
    assert payload["error"]["retryable"] is True


def test_runtime_event_jsonl_store_appends_loads_and_waits(tmp_path):
    path = tmp_path / "events.runtime.jsonl"
    store = RuntimeEventJsonlStore(path)
    event = RuntimeEvent(
        event_id=1,
        runtime_id="runtime1",
        process_id="process1",
        event_type="process.started",
        timestamp=123.0,
        status=RuntimeEventStatus.RUNNING,
        status_class=RuntimeStatusClass.ACTIVE,
    )

    stored = store.append(event)
    restored = RuntimeEventJsonlStore.from_file(path)
    restored.append(
        RuntimeEvent(
            event_id=2,
            runtime_id="runtime1",
            process_id="process1",
            event_type="process.completed",
            timestamp=124.0,
            status=RuntimeEventStatus.SUCCEEDED,
            status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
        )
    )

    assert stored.event_id == 1
    assert [item.event_id for item in restored.list()] == [1, 2]
    assert [item.event_type for item in restored.wait_for_next(since=1, timeout=0.01)] == ["process.completed"]


def test_txn_event_payload_adapter_round_trips_legacy_shape():
    legacy = {
        "event_id": 3,
        "job_id": "job1",
        "timestamp": 125.0,
        "event_type": "job_state",
        "payload": {"status": "completed", "result": {"ok": True}},
    }

    event = txn_event_payload_to_runtime_event(legacy, runtime_id="runtime1")
    restored = runtime_event_to_txn_event_payload(event)

    assert event.runtime_id == "runtime1"
    assert event.process_id == "job1"
    assert event.event_type == "process.status.changed"
    assert event.status == "completed"
    assert event.status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert event.metadata["legacy_event_type"] == "job_state"
    assert restored == legacy


@dataclass
class ExampleEvent:
    event_id: int
    job_id: str
    timestamp: float
    event_type: str
    payload: dict[str, Any]


def _event_factory(
    event_id: int,
    job_id: str,
    timestamp: float,
    event_type: str,
    payload: dict[str, Any],
) -> ExampleEvent:
    return ExampleEvent(event_id, job_id, timestamp, event_type, payload)


def _event_loader(line: str) -> ExampleEvent:
    payload = json.loads(line)
    return ExampleEvent(**payload)


def test_event_buffer_persists_and_restores_jsonl(tmp_path):
    events_path = tmp_path / "events.jsonl"
    buffer = EventBuffer(
        "job1",
        events_path,
        event_factory=_event_factory,
        event_loader=_event_loader,
        event_id_getter=lambda event: event.event_id,
        event_json_dumper=lambda event: json.dumps(event.__dict__, ensure_ascii=False),
    )

    first = buffer.emit("job_state", {"status": "running"})
    second = buffer.emit("result", {"ok": True})
    restored = EventBuffer.from_file(
        "job1",
        events_path,
        event_factory=_event_factory,
        event_loader=_event_loader,
        event_id_getter=lambda event: event.event_id,
        event_json_dumper=lambda event: json.dumps(event.__dict__, ensure_ascii=False),
    )
    restored.emit("job_state", {"status": "completed"})

    assert first.event_id == 1
    assert second.event_id == 2
    assert [event.event_id for event in restored.list()] == [1, 2, 3]
    assert [event.event_type for event in restored.list(since=1)] == ["result", "job_state"]


def test_runtime_snapshot_loader_uses_monitor_snapshot_then_fallbacks(tmp_path):
    store = RuntimeStore(tmp_path / "runtime")
    store.write_runtime_info(
        runtime_info_payload(
            engine="ExampleEngine",
            job_type="example.job",
            job_id="job1",
            status="completed",
            runtime_dir=store.runtime_dir,
        )
    )
    store.write_status({"job_id": "job1", "status": "completed", "total": 2})
    store.write_snapshot({"job": {"job_id": "job1", "status": "completed", "total": 2}, "items": [1, 2]})

    loader = RuntimeSnapshotLoader(
        status_loader=json.loads,
        snapshot_loader=json.loads,
        monitor_loader=json.loads,
        monitor_from_snapshot=lambda snapshot: {"source": "snapshot", "total": len(snapshot["items"])},
        monitor_from_status=lambda status: {"source": "status", "total": status["total"]},
    )
    view = loader.load(store.runtime_dir)

    assert view is not None
    assert view.monitor == {"source": "snapshot", "total": 2}
    assert view.snapshot is not None
    assert view.runtime_info is not None
    assert view.runtime_info["engine"] == "ExampleEngine"

    store.write_monitor_snapshot({"source": "monitor", "total": 2})
    view = loader.load(store.runtime_dir)

    assert view is not None
    assert view.monitor == {"source": "monitor", "total": 2}
