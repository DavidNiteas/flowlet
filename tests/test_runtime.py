from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from flowlet.runtime import (
    EventBuffer,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeEventJsonlStore,
    RuntimeEventSidecarWriter,
    RuntimeEventStatus,
    RuntimeFrameworkReducer,
    RuntimeInfo,
    RuntimeProcessBase,
    RuntimeProcessCapabilities,
    RuntimeProcessContext,
    RuntimeProcessOperation,
    RuntimeProcessRunner,
    RuntimeProcessSpec,
    RuntimeProgress,
    RuntimeProjection,
    RuntimeReducer,
    RuntimeResourceRequest,
    RuntimeRetryPolicy,
    RuntimeSnapshotLoader,
    RuntimeStatusClass,
    RuntimeStore,
    RuntimeUnsupportedOperationError,
    list_runtime_artifacts,
    manager_record_to_runtime_event,
    runtime_event_payload,
    runtime_event_to_txn_event_payload,
    runtime_info_payload,
    runtime_process_spec_payload,
    runtime_projection_payload,
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
    assert restored.runtime_files.runtime_events == "runtime/events.runtime.jsonl"


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


def test_runtime_process_spec_payload_is_json_safe():
    payload = runtime_process_spec_payload(
        process_id="process1",
        process_type="example.process",
        display_name="Example process",
        parent_process_id="parent1",
        inputs={"domain": {"sample": "liver"}},
        metadata={"source": "test"},
        capabilities=RuntimeProcessCapabilities(can_cancel=True, can_cleanup=True),
        resource_request=RuntimeResourceRequest(cpu=2, memory_bytes=1024, labels={"queue": "local"}),
        retry_policy=RuntimeRetryPolicy(max_attempts=2, backoff_seconds=1.5),
    )

    restored = RuntimeProcessSpec.model_validate(payload)

    assert restored.resolved_process_id() == "process1"
    assert restored.capabilities.supports(RuntimeProcessOperation.CANCEL)
    assert not restored.capabilities.supports(RuntimeProcessOperation.PAUSE)
    assert payload["resource_request"]["labels"] == {"queue": "local"}
    assert payload["retry_policy"]["max_attempts"] == 2


def test_runtime_process_context_emits_standard_events(tmp_path):
    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    context = RuntimeProcessContext(
        runtime_id="runtime1",
        process_id="process1",
        parent_process_id="parent1",
        event_store=store,
        runtime_dir=tmp_path,
        metadata={"source": "test"},
    )

    started = context.emit_status(RuntimeEventStatus.RUNNING)
    progressed = context.emit_progress(1, total=2, description="half")
    artifact = context.emit_artifact(context.artifact_path("result.json"), payload={"kind": "result"})
    completed = context.emit_status(RuntimeEventStatus.SUCCEEDED)

    assert [event.event_id for event in store.list()] == [1, 2, 3, 4]
    assert started.event_type == "process.status.changed"
    assert progressed.event_type == "process.progressed"
    assert progressed.progress is not None
    assert progressed.progress.percent == 50.0
    assert artifact.payload["kind"] == "result"
    assert completed.status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert all(event.parent_process_id == "parent1" for event in store.list())


def test_runtime_process_context_emits_auxiliary_events(tmp_path):
    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    context = RuntimeProcessContext(runtime_id="runtime1", process_id="process1", event_store=store)

    log = context.emit_log("hello", level="warning")
    metric = context.emit_metric("memory", 10, unit="bytes")
    signal = context.emit_signal("ready", value=True, status=RuntimeEventStatus.RUNNING)
    checkpoint = context.checkpoint("stage-ready", payload={"stage": "prepare"})

    assert [event.event_type for event in store.list()] == [
        "log.emitted",
        "metric.sampled",
        "signal.changed",
        "process.checkpointed",
    ]
    assert log.message == "hello"
    assert log.payload["level"] == "warning"
    assert metric.payload == {"name": "memory", "value": 10, "unit": "bytes"}
    assert signal.payload == {"name": "ready", "value": True}
    assert signal.status_class == RuntimeStatusClass.ACTIVE
    assert checkpoint.payload == {"name": "stage-ready", "stage": "prepare"}


def test_runtime_process_runner_emits_lifecycle_events(tmp_path):
    class ExampleProcess(RuntimeProcessBase):
        def start(self, context: RuntimeProcessContext) -> dict[str, Any]:
            context.emit_progress(1, total=1)
            return {"ok": True}

    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    spec = RuntimeProcessSpec(process_id="process1", process_type="example.process", metadata={"source": "test"})
    context = RuntimeProcessContext(runtime_id="runtime1", process_id="process1", event_store=store)
    result = RuntimeProcessRunner(context).run(ExampleProcess(spec))

    assert result == {"ok": True}
    assert [event.event_type for event in store.list()] == [
        "process.status.changed",
        "process.progressed",
        "process.status.changed",
    ]
    assert store.list()[0].status_class == RuntimeStatusClass.ACTIVE
    assert store.list()[-1].status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert store.list()[-1].payload == {"result": {"ok": True}}


def test_runtime_process_runner_emits_failure_and_unsupported_events(tmp_path):
    class FailingProcess(RuntimeProcessBase):
        def start(self, context: RuntimeProcessContext) -> None:
            del context
            raise ValueError("bad process")

    class UnsupportedStartProcess(RuntimeProcessBase):
        pass

    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    context = RuntimeProcessContext(runtime_id="runtime1", process_id="process1", event_store=store)
    spec = RuntimeProcessSpec(process_id="process1", process_type="example.process")

    try:
        RuntimeProcessRunner(context).run(FailingProcess(spec))
    except ValueError:
        pass
    else:  # pragma: no cover
        raise AssertionError("failing process should raise")

    unsupported_store = RuntimeEventJsonlStore(tmp_path / "unsupported.runtime.jsonl")
    unsupported_context = RuntimeProcessContext(
        runtime_id="runtime1",
        process_id="process2",
        event_store=unsupported_store,
    )
    unsupported_spec = RuntimeProcessSpec(
        process_id="process2",
        process_type="example.unsupported",
        capabilities=RuntimeProcessCapabilities(can_start=False),
    )
    try:
        RuntimeProcessRunner(unsupported_context).run(UnsupportedStartProcess(unsupported_spec))
    except RuntimeUnsupportedOperationError:
        pass
    else:  # pragma: no cover
        raise AssertionError("unsupported start should raise")

    assert [event.event_type for event in store.list()] == ["process.status.changed", "process.failed"]
    assert store.list()[-1].error is not None
    assert store.list()[-1].error.type == "ValueError"
    assert [event.event_type for event in unsupported_store.list()] == ["process.start.unsupported"]


def test_runtime_framework_reducer_reconstructs_process_state(tmp_path):
    class ExampleProcess(RuntimeProcessBase):
        def start(self, context: RuntimeProcessContext) -> dict[str, Any]:
            context.emit_progress(1, total=2, description="half")
            context.emit_artifact(context.artifact_path("result.json"), payload={"kind": "result"})
            return {"ok": True}

    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    spec = RuntimeProcessSpec(process_id="process1", process_type="example.process")
    context = RuntimeProcessContext(
        runtime_id="runtime1",
        process_id="process1",
        event_store=store,
        runtime_dir=tmp_path,
    )
    RuntimeProcessRunner(context).run(ExampleProcess(spec))

    reducer: RuntimeReducer = RuntimeFrameworkReducer()
    projection = reducer.reduce(store.list())

    assert isinstance(projection, RuntimeProjection)
    assert projection.runtime_id == "runtime1"
    assert projection.status == RuntimeEventStatus.SUCCEEDED
    assert projection.status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert projection.terminal_success_count == 1
    assert projection.active_process_ids == []
    assert projection.processes["process1"].status == RuntimeEventStatus.SUCCEEDED
    assert projection.processes["process1"].result == {"ok": True}
    assert projection.progress_summary["process1"].percent == 50.0
    assert projection.artifact_index[0]["kind"] == "result"


def test_runtime_framework_reducer_summarizes_failure_and_writes_projection(tmp_path):
    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    context = RuntimeProcessContext(runtime_id="runtime1", process_id="process1", event_store=store)
    context.emit_status(RuntimeEventStatus.RUNNING)
    context.emit_error(RuntimeErrorInfo(type="ExampleError", message="bad"))

    projection_payload = runtime_projection_payload(store.list())
    projection = RuntimeProjection.model_validate(projection_payload)
    runtime_store = RuntimeStore(tmp_path / "runtime")
    runtime_store.write_projection(projection_payload)
    artifacts = list_runtime_artifacts(runtime_store.runtime_dir)

    assert projection.status == RuntimeEventStatus.FAILED
    assert projection.status_class == RuntimeStatusClass.TERMINAL_FAILURE
    assert projection.terminal_failure_count == 1
    assert projection.error_summary[0].type == "ExampleError"
    assert {artifact["path"] for artifact in artifacts} == {"runtime/projection.json"}


def test_runtime_process_base_reports_unsupported_operation(tmp_path):
    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    context = RuntimeProcessContext(runtime_id="runtime1", process_id="process1", event_store=store)
    process = RuntimeProcessBase(RuntimeProcessSpec(process_id="process1", process_type="example.process"))

    try:
        process.cancel(context)
    except RuntimeUnsupportedOperationError as exc:
        event = context.emit_error(exc.to_error_info(), event_type="process.cancel.unsupported")
    else:  # pragma: no cover
        raise AssertionError("cancel should be unsupported")

    assert event.event_type == "process.cancel.unsupported"
    assert event.error is not None
    assert event.error.type == "RuntimeUnsupportedOperationError"
    assert event.error.context == {"process_id": "process1", "operation": "cancel"}


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


def test_runtime_store_appends_standard_runtime_event_sidecar(tmp_path):
    store = RuntimeStore(tmp_path / "runtime")
    store.append_runtime_event(
        RuntimeEvent(
            event_id=1,
            runtime_id="runtime1",
            process_id="process1",
            event_type="process.started",
            timestamp=123.0,
        )
    )

    restored = store.runtime_event_store()
    restored.load()
    artifacts = list_runtime_artifacts(store.runtime_dir)

    assert [event.event_type for event in restored.list()] == ["process.started"]
    assert {artifact["path"] for artifact in artifacts} == {"runtime/events.runtime.jsonl"}


def test_runtime_event_sidecar_writer_disabled_does_not_write(tmp_path):
    writer = RuntimeEventSidecarWriter(tmp_path / "runtime", runtime_id="runtime1", enabled=False)
    event = RuntimeEvent(
        event_id=1,
        runtime_id="runtime1",
        process_id="process1",
        event_type="process.started",
        timestamp=123.0,
    )

    returned = writer.append_event(event)

    assert returned is event
    assert not (tmp_path / "runtime" / "runtime" / "events.runtime.jsonl").exists()


def test_runtime_event_sidecar_writer_mirrors_legacy_payload(tmp_path):
    writer = RuntimeEventSidecarWriter(tmp_path / "runtime", runtime_id="runtime1")

    writer.append_legacy_event(
        {
            "event_id": 2,
            "job_id": "job1",
            "timestamp": 124.0,
            "event_type": "progress",
            "payload": {"status": "running", "current": 1, "total": 2},
        },
        metadata={"source": "legacy-buffer"},
    )
    store = writer.store()
    store.load()

    [event] = store.list()
    assert event.runtime_id == "runtime1"
    assert event.process_id == "job1"
    assert event.event_type == "process.progressed"
    assert event.metadata["source"] == "legacy-buffer"
    assert event.metadata["legacy_event_type"] == "progress"


def test_runtime_event_sidecar_writer_mirrors_manager_record(tmp_path):
    writer = RuntimeEventSidecarWriter(tmp_path / "runtime", runtime_id="runtime1", process_id="process1")

    writer.append_manager_record(
        {
            "task_id": "task1",
            "description": "task 1",
            "current": 2,
            "total": 4,
            "status": "running",
        },
        record_type="progress",
        event_id=3,
    )
    store = writer.store()
    store.load()
    artifacts = list_runtime_artifacts(tmp_path / "runtime")

    [event] = store.list()
    assert event.process_id == "process1"
    assert event.event_type == "process.progressed"
    assert event.progress is not None
    assert event.progress.percent == 50.0
    assert {artifact["path"] for artifact in artifacts} == {"runtime/events.runtime.jsonl"}


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


def test_txn_event_payload_adapter_accepts_current_backend_event_shapes():
    samples = [
        {
            "event_id": 4,
            "job_id": "metams-job",
            "timestamp": 126.0,
            "event_type": "progress",
            "payload": {
                "task_id": "run:Liver-1:feature_find",
                "description": "Liver-1 feature_find",
                "current": 1,
                "total": 1,
                "status": "completed",
                "metadata": {"stage": "feature_find"},
            },
        },
        {
            "event_id": 5,
            "job_id": "masslib-job",
            "timestamp": 127.0,
            "event_type": "artifact",
            "payload": {
                "path": "annotation_run_manifest.json",
                "parent_id": "annotation_study",
                "status": "completed",
            },
        },
    ]

    events = [txn_event_payload_to_runtime_event(sample, runtime_id="runtime1") for sample in samples]

    assert [event.event_type for event in events] == ["process.progressed", "artifact.produced"]
    assert all(event.runtime_id == "runtime1" for event in events)
    assert all(event.process_id in {"metams-job", "masslib-job"} for event in events)
    assert events[0].status_class == RuntimeStatusClass.TERMINAL_SUCCESS


def test_manager_record_to_runtime_event_maps_progress_and_signal():
    progress = manager_record_to_runtime_event(
        {
            "task_id": "run:Liver-1:feature_find",
            "description": "Liver-1 feature_find",
            "current": 3,
            "total": 4,
            "status": "running",
            "metadata": {"stage": "feature_find"},
        },
        record_type="progress",
        event_id=10,
        runtime_id="runtime1",
        process_id="process1",
    )
    signal = manager_record_to_runtime_event(
        {
            "name": "run:Liver-1:stage:feature_find",
            "status": "failed",
            "error": "bad peak",
            "version": 2,
            "timestamp": 130.0,
        },
        record_type="signal",
        event_id=11,
        runtime_id="runtime1",
    )

    assert progress.event_type == "process.progressed"
    assert progress.subject_id == "run:Liver-1:feature_find"
    assert progress.progress is not None
    assert progress.progress.percent == 75.0
    assert signal.event_type == "signal.changed"
    assert signal.status_class == RuntimeStatusClass.TERMINAL_FAILURE
    assert signal.error is not None
    assert signal.error.message == "bad peak"


def test_manager_record_to_runtime_event_maps_log_stream_and_telemetry():
    log_event = manager_record_to_runtime_event(
        {"timestamp": 1.0, "task_id": "task1", "level": "INFO", "message": "started"},
        record_type="log",
        event_id=20,
        runtime_id="runtime1",
    )
    stream_event = manager_record_to_runtime_event(
        {"timestamp": 2.0, "stream": "stdout", "text": "hello", "source": "worker"},
        record_type="stream",
        event_id=21,
        runtime_id="runtime1",
    )
    telemetry_event = manager_record_to_runtime_event(
        {"timestamp": 3.0, "event_type": "metric", "name": "peak_count", "value": 42},
        record_type="telemetry",
        event_id=22,
        runtime_id="runtime1",
    )

    assert log_event.event_type == "log.emitted"
    assert log_event.message == "started"
    assert stream_event.event_type == "stream.chunk"
    assert stream_event.message == "hello"
    assert telemetry_event.event_type == "metric.sampled"
    assert telemetry_event.subject_id == "peak_count"


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
