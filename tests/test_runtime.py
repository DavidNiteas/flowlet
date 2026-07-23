from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from flowlet.fsm import StateMachineSpec, TransitionSpec
from flowlet.runtime import (
    CallableRuntimeProcess,
    EventBuffer,
    FSMRuntimeProcess,
    RuntimeAttemptStateError,
    RuntimeBackendExecutor,
    RuntimeBackendSessionStatus,
    RuntimeCheckpointMode,
    RuntimeCheckpointPolicy,
    RuntimeCheckpointRef,
    RuntimeCommandConflictError,
    RuntimeCommandStatus,
    RuntimeContinuationAssessment,
    RuntimeContinuationSelector,
    RuntimeDirectoryManager,
    RuntimeDurableStore,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeEventChannel,
    RuntimeEventJsonlStore,
    RuntimeEventSidecarWriter,
    RuntimeEventStatus,
    RuntimeEventType,
    RuntimeExecutionKind,
    RuntimeExecutionLedgerReducer,
    RuntimeExecutionStatus,
    RuntimeFrameworkReducer,
    RuntimeIdempotency,
    RuntimeIdentity,
    RuntimeIdentityMismatchError,
    RuntimeInfo,
    RuntimeLeaseConflictError,
    RuntimeLeaseLostError,
    RuntimeManagerBundle,
    RuntimeManagerEventBridge,
    RuntimeObservation,
    RuntimeProcessAttempt,
    RuntimeProcessAttemptReporter,
    RuntimeProcessBase,
    RuntimeProcessCapabilities,
    RuntimeProcessContext,
    RuntimeProcessGraph,
    RuntimeProcessOperation,
    RuntimeProcessRunner,
    RuntimeProcessSpec,
    RuntimeProcessState,
    RuntimeProgress,
    RuntimeProjection,
    RuntimeProjectionPolicy,
    RuntimeRecoveryAction,
    RuntimeRecoveryDecision,
    RuntimeRecoveryExecutionError,
    RuntimeRecoveryGraphError,
    RuntimeRecoveryPlan,
    RuntimeRecoveryPlanError,
    RuntimeRecoveryPlanner,
    RuntimeRecoveryStep,
    RuntimeReducer,
    RuntimeResourceRequest,
    RuntimeResourceUsage,
    RuntimeRetryPolicy,
    RuntimeSnapshotLoader,
    RuntimeStatusClass,
    RuntimeStore,
    RuntimeUnitState,
    RuntimeUnsupportedOperationError,
    build_runtime_process_graph,
    list_runtime_artifacts,
    load_runtime_observation,
    load_runtime_projection,
    manager_record_to_runtime_event,
    parse_sse_runtime_events,
    runtime_event_payload,
    runtime_event_to_txn_event_payload,
    runtime_info_payload,
    runtime_process_spec_payload,
    runtime_projection_payload,
    sse_encode_runtime_event,
    stream_runtime_event_jsonl,
    stream_runtime_event_store,
    txn_event_payload_to_runtime_event,
    wait_runtime_event_store,
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
    assert restored.runtime_files.durable_database == "runtime/runtime.db"
    assert restored.runtime_files.processes == "runtime/processes.json"


def test_runtime_process_context_observe_unit_does_not_mutate_process_state():
    store = RuntimeEventJsonlStore()
    context = RuntimeProcessContext(
        runtime_id="runtime-1",
        process_id="process-1",
        event_store=store,
    )

    context.emit_status(RuntimeEventStatus.RUNNING)
    with context.observe_unit("unit-1", unit_type="common.step") as unit:
        unit.progress(1, total=2)
    context.emit_status(RuntimeEventStatus.SUCCEEDED, payload={"result": {"ok": True}})

    projection = RuntimeFrameworkReducer().reduce(store.list())

    assert projection.processes["process-1"].status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert projection.processes["process-1"].result == {"ok": True}
    assert projection.units["unit-1"].status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert projection.units["unit-1"].unit_type == "common.step"
    assert projection.units["unit-1"].event_count == 3
    assert isinstance(projection.units["unit-1"], RuntimeUnitState)
    assert "unit-1" not in projection.processes


def test_runtime_projection_keeps_distinct_fsm_transition_units():
    store = RuntimeEventJsonlStore()
    context = RuntimeProcessContext(
        runtime_id="runtime-1",
        process_id="process-1",
        event_store=store,
    )

    context.emit_unit(
        RuntimeEventType.FSM_TRANSITION_COMPLETED,
        "fsm:advance:1",
        unit_type="fsm.transition",
        subject_type="fsm.transition",
        subject_id="advance",
        status=RuntimeEventStatus.SUCCEEDED,
        status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
    )
    context.emit_unit(
        RuntimeEventType.FSM_TRANSITION_COMPLETED,
        "fsm:advance:2",
        unit_type="fsm.transition",
        subject_type="fsm.transition",
        subject_id="advance",
        status=RuntimeEventStatus.SUCCEEDED,
        status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
    )

    projection = RuntimeFrameworkReducer().reduce(store.list())

    assert sorted(projection.units) == ["fsm:advance:1", "fsm:advance:2"]
    assert projection.processes == {}


def test_runtime_process_attempt_reporter_context_emits_units(tmp_path):
    identity = RuntimeIdentity(
        runtime_id="runtime-1",
        logical_task_id="task-1",
        generation=1,
        created_at=1.0,
    )
    store = RuntimeDurableStore.create(tmp_path / "runtime.db", identity)
    execution = store.begin_execution(
        execution_id="execution-1",
        kind=RuntimeExecutionKind.INITIAL,
        created_at=2.0,
        backend_session_id="session-1",
    )
    reporter = RuntimeProcessAttemptReporter(
        store,
        execution_id=execution.execution_id,
        backend_session_id="session-1",
    )
    reporter.declare(RuntimeProcessSpec(process_id="process-1", process_type="common.process"), timestamp=3.0)
    attempt = reporter.start("process-1", timestamp=4.0)

    context = reporter.context(attempt)
    with context.observe_unit("unit-1", unit_type="common.unit"):
        pass
    reporter.complete(attempt.attempt_id, result={"ok": True}, timestamp=5.0)

    projection = RuntimeFrameworkReducer().reduce(store.list())

    assert projection.attempts[attempt.attempt_id].status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert projection.units["unit-1"].process_id == "process-1"
    assert projection.units["unit-1"].status_class == RuntimeStatusClass.TERMINAL_SUCCESS


def test_runtime_event_channel_drains_standard_events():
    channel = RuntimeEventChannel.local()
    proxy = channel.proxy()
    context = proxy.context_for(runtime_id="runtime-1", process_id="process-1")
    context.emit_log("started")
    with context.observe_unit("unit-1"):
        pass

    target = RuntimeEventJsonlStore()
    drained = channel.drain_into(target)

    assert drained == 3
    projection = RuntimeFrameworkReducer().reduce(target.list())
    assert projection.units["unit-1"].status_class == RuntimeStatusClass.TERMINAL_SUCCESS


def test_callable_runtime_process_wrapper_runs():
    store = RuntimeEventJsonlStore()
    executor = RuntimeBackendExecutor(runtime_id="runtime-1", event_store=store)
    process = CallableRuntimeProcess(
        RuntimeProcessSpec(process_id="process-1", process_type="callable"),
        lambda value: {"value": value + 1},
        args=(1,),
    )
    executor.register(process)

    assert executor.run_process("process-1") == {"value": 2}
    projection = RuntimeFrameworkReducer().reduce(store.list())
    assert projection.processes["process-1"].status_class == RuntimeStatusClass.TERMINAL_SUCCESS


def test_fsm_runtime_process_wrapper_emits_transition_units():
    store = RuntimeEventJsonlStore()
    executor = RuntimeBackendExecutor(runtime_id="runtime-1", event_store=store)
    spec = StateMachineSpec.create(
        initial="new",
        terminal={"done"},
        transitions=[TransitionSpec("finish", "new", "done")],
    )
    process = FSMRuntimeProcess(
        RuntimeProcessSpec(process_id="process-1", process_type="fsm"),
        spec,
        backend="thread",
    )
    executor.register(process)

    result = executor.run_process("process-1")
    projection = RuntimeFrameworkReducer().reduce(store.list())

    assert result["state"] == "done"
    assert projection.processes["process-1"].status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert projection.units["fsm:finish:1"].subject_type == "fsm.transition"
    assert projection.units["fsm:finish:1"].status_class == RuntimeStatusClass.TERMINAL_SUCCESS


def test_durable_runtime_store_preserves_identity_and_continuation_ledger(tmp_path):
    identity = RuntimeIdentity(
        runtime_id="runtime-1",
        logical_task_id="annotation-1",
        generation=1,
        created_at=1.0,
    )
    store = RuntimeDurableStore.create(tmp_path / "runtime.db", identity)
    initial = store.begin_execution(
        execution_id="execution-1",
        kind=RuntimeExecutionKind.INITIAL,
        created_at=2.0,
        backend_session_id="session-1",
    )
    store.transition_execution("execution-1", RuntimeExecutionStatus.RUNNING, timestamp=3.0)
    store.transition_execution("execution-1", RuntimeExecutionStatus.INTERRUPTED, timestamp=4.0)
    continued = store.begin_execution(
        execution_id="execution-2",
        kind=RuntimeExecutionKind.CONTINUE,
        created_at=5.0,
        backend_session_id="session-2",
    )

    assert store.identity() == identity
    assert initial.ordinal == 1
    assert continued.ordinal == 2
    assert [event.event_id for event in store.list()] == [0, 1, 2, 3]
    assert {event.runtime_id for event in store.list()} == {"runtime-1"}
    ledger = store.ledger()
    assert ledger.execution_order == ["execution-1", "execution-2"]
    assert ledger.executions["execution-1"].status == RuntimeExecutionStatus.INTERRUPTED
    assert ledger.executions["execution-2"].kind == RuntimeExecutionKind.CONTINUE
    assert ledger.last_event_sequence == 3

    reopened = RuntimeDurableStore(tmp_path / "runtime.db", expected_runtime_id="runtime-1")
    assert reopened.ledger() == ledger
    with pytest.raises(RuntimeIdentityMismatchError):
        RuntimeDurableStore(tmp_path / "runtime.db", expected_runtime_id="another-runtime")
    with pytest.raises(RuntimeIdentityMismatchError):
        store.append(
            RuntimeEvent(
                event_id=-1,
                runtime_id="another-runtime",
                event_type="log.emitted",
                timestamp=6.0,
            )
        )
    with pytest.raises(ValueError, match="Invalid execution transition"):
        store.transition_execution("execution-1", RuntimeExecutionStatus.RUNNING, timestamp=7.0)


def test_durable_runtime_store_allocates_unique_sequences_across_writers(tmp_path):
    database_path = tmp_path / "runtime.db"
    RuntimeDurableStore.create(
        database_path,
        RuntimeIdentity(runtime_id="runtime-1", created_at=1.0),
    )

    def append_batch(worker: int) -> None:
        writer = RuntimeDurableStore(database_path, expected_runtime_id="runtime-1")
        for item in range(10):
            writer.append(
                RuntimeEvent(
                    event_id=-1,
                    runtime_id="runtime-1",
                    event_type="metric.sampled",
                    timestamp=float(worker * 10 + item),
                    payload={"worker": worker, "item": item},
                )
            )

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(append_batch, range(4)))

    store = RuntimeDurableStore(database_path, expected_runtime_id="runtime-1")
    events = store.list()
    assert [event.event_id for event in events] == list(range(40))
    assert [event.sequence for event in events] == list(range(40))


def test_durable_runtime_store_backend_session_lease_takeover(tmp_path):
    database_path = tmp_path / "runtime.db"
    store = RuntimeDurableStore.create(
        database_path,
        RuntimeIdentity(runtime_id="runtime-1", created_at=1.0),
    )
    first = store.acquire_backend_session(
        session_id="session-1", owner_id="backend-1", now=2.0, ttl=10.0
    )

    assert first.status == RuntimeBackendSessionStatus.ACTIVE
    assert store.active_backend_session(now=3.0) == first
    with pytest.raises(RuntimeLeaseConflictError):
        RuntimeDurableStore(database_path).acquire_backend_session(
            session_id="session-2", owner_id="backend-2", now=3.0, ttl=10.0
        )
    renewed = store.renew_backend_session(
        "session-1", owner_id="backend-1", now=4.0, ttl=10.0
    )
    assert renewed.expires_at == 14.0

    second = RuntimeDurableStore(database_path).acquire_backend_session(
        session_id="session-2", owner_id="backend-2", now=15.0, ttl=10.0
    )
    assert second.status == RuntimeBackendSessionStatus.ACTIVE
    assert [session.status for session in store.backend_sessions()] == [
        RuntimeBackendSessionStatus.EXPIRED,
        RuntimeBackendSessionStatus.ACTIVE,
    ]
    with pytest.raises(RuntimeLeaseLostError):
        store.renew_backend_session("session-1", owner_id="backend-1", now=16.0, ttl=10.0)

    released = store.release_backend_session(
        "session-2", owner_id="backend-2", now=17.0
    )
    assert released.status == RuntimeBackendSessionStatus.RELEASED
    assert store.active_backend_session(now=17.0) is None
    assert [event.event_type for event in store.list()] == [
        RuntimeEventType.BACKEND_SESSION_ACQUIRED,
        RuntimeEventType.BACKEND_SESSION_RENEWED,
        RuntimeEventType.BACKEND_SESSION_EXPIRED,
        RuntimeEventType.BACKEND_SESSION_ACQUIRED,
        RuntimeEventType.BACKEND_SESSION_RELEASED,
    ]


def test_durable_runtime_store_serializes_backend_session_acquisition(tmp_path):
    database_path = tmp_path / "runtime.db"
    RuntimeDurableStore.create(
        database_path,
        RuntimeIdentity(runtime_id="runtime-1", created_at=1.0),
    )

    def acquire(index: int) -> str:
        writer = RuntimeDurableStore(database_path)
        try:
            writer.acquire_backend_session(
                session_id=f"session-{index}",
                owner_id=f"backend-{index}",
                now=2.0,
                ttl=10.0,
            )
        except RuntimeLeaseConflictError:
            return "conflict"
        return "acquired"

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(acquire, range(4)))

    assert results.count("acquired") == 1
    assert results.count("conflict") == 3


def test_backend_session_takeover_reconciles_interrupted_work_once(tmp_path):
    store = RuntimeDurableStore.create(
        tmp_path / "runtime.db",
        RuntimeIdentity(runtime_id="runtime-1", created_at=1.0),
    )
    store.acquire_backend_session(
        session_id="session-1", owner_id="backend-1", now=2.0, ttl=5.0
    )
    store.begin_execution(
        execution_id="execution-1",
        kind=RuntimeExecutionKind.INITIAL,
        created_at=3.0,
        backend_session_id="session-1",
    )
    store.transition_execution("execution-1", RuntimeExecutionStatus.RUNNING, timestamp=4.0)
    for event_type, status, status_class in [
        (
            RuntimeEventType.PROCESS_ATTEMPT_CREATED,
            RuntimeEventStatus.PENDING,
            RuntimeStatusClass.NOT_STARTED,
        ),
        (
            RuntimeEventType.PROCESS_ATTEMPT_STARTED,
            RuntimeEventStatus.RUNNING,
            RuntimeStatusClass.ACTIVE,
        ),
    ]:
        store.append(
            RuntimeEvent(
                event_id=-1,
                runtime_id="runtime-1",
                execution_id="execution-1",
                process_id="process-1",
                attempt_id="attempt-1",
                event_type=event_type,
                timestamp=5.0,
                status=status,
                status_class=status_class,
                payload={"ordinal": 1, "backend_session_id": "session-1"},
            )
        )

    store.acquire_backend_session(
        session_id="session-2", owner_id="backend-2", now=8.0, ttl=5.0
    )
    before = store.last_event_sequence()
    reconciliation = store.reconcile_interrupted_work(
        "session-2", owner_id="backend-2", now=8.5
    )

    assert reconciliation.interrupted_execution_ids == ["execution-1"]
    assert reconciliation.interrupted_attempt_ids == ["attempt-1"]
    assert reconciliation.first_event_sequence == before + 1
    ledger = store.ledger()
    assert ledger.executions["execution-1"].status == RuntimeExecutionStatus.INTERRUPTED
    assert ledger.attempts["attempt-1"].status == RuntimeEventStatus.INTERRUPTED
    assert ledger.attempts["attempt-1"].error.type == "BackendSessionLost"
    assert store.rebuild_ledger() == ledger

    after = store.last_event_sequence()
    repeated = store.reconcile_interrupted_work("session-2", owner_id="backend-2", now=9.0)
    assert repeated.interrupted_execution_ids == []
    assert repeated.interrupted_attempt_ids == []
    assert store.last_event_sequence() == after


def test_durable_runtime_commands_are_idempotent_and_conflict_checked(tmp_path):
    database_path = tmp_path / "runtime.db"
    store = RuntimeDurableStore.create(
        database_path,
        RuntimeIdentity(runtime_id="runtime-1", created_at=1.0),
    )

    def reserve(_: int) -> bool:
        writer = RuntimeDurableStore(database_path)
        return writer.reserve_command(
            command_id="continue-command-1",
            command_type="runtime.continue",
            target_id="process-1",
            timestamp=2.0,
            payload={"reason": "resume interrupted work"},
        ).created

    with ThreadPoolExecutor(max_workers=4) as pool:
        created = list(pool.map(reserve, range(4)))
    assert created.count(True) == 1
    assert created.count(False) == 3
    assert len(store.commands()) == 1
    assert store.last_event_sequence() == 0

    with pytest.raises(RuntimeCommandConflictError, match="different request"):
        store.reserve_command(
            command_id="continue-command-1",
            command_type="runtime.rerun",
            timestamp=3.0,
        )

    completed = store.finish_command(
        "continue-command-1",
        status=RuntimeCommandStatus.SUCCEEDED,
        timestamp=4.0,
        result={"execution_id": "execution-2"},
    )
    repeated = store.finish_command(
        "continue-command-1",
        status=RuntimeCommandStatus.SUCCEEDED,
        timestamp=5.0,
        result={"execution_id": "execution-2"},
    )
    assert repeated == completed
    assert store.last_event_sequence() == 1
    assert store.rebuild_commands() == [completed]

    with sqlite3.connect(store.database_path) as connection:
        connection.execute("DELETE FROM runtime_commands")
    assert store.commands() == []
    assert store.rebuild_commands(replace=True) == [completed]
    assert RuntimeDurableStore(database_path).commands() == [completed]

    with pytest.raises(RuntimeCommandConflictError, match="different terminal result"):
        store.finish_command(
            "continue-command-1",
            status=RuntimeCommandStatus.FAILED,
            timestamp=6.0,
            error=RuntimeErrorInfo(type="LateFailure", message="conflicting replay"),
        )


def test_durable_runtime_store_materializes_attempts_and_projection_cursor(tmp_path):
    store = RuntimeDurableStore.create(
        tmp_path / "runtime.db",
        RuntimeIdentity(runtime_id="runtime-1", created_at=1.0),
    )
    store.begin_execution(
        execution_id="execution-1",
        kind=RuntimeExecutionKind.INITIAL,
        created_at=2.0,
    )
    store.write_process_specs(
        [RuntimeProcessSpec(process_id="process-1", process_type="example.process")]
    )
    transitions = [
        (RuntimeEventType.PROCESS_ATTEMPT_CREATED, RuntimeEventStatus.PENDING, RuntimeStatusClass.NOT_STARTED, 3.0),
        (RuntimeEventType.PROCESS_ATTEMPT_STARTED, RuntimeEventStatus.RUNNING, RuntimeStatusClass.ACTIVE, 4.0),
        (
            RuntimeEventType.PROCESS_ATTEMPT_COMPLETED,
            RuntimeEventStatus.SUCCEEDED,
            RuntimeStatusClass.TERMINAL_SUCCESS,
            5.0,
        ),
    ]
    for event_type, status, status_class, timestamp in transitions:
        store.append(
            RuntimeEvent(
                event_id=-1,
                runtime_id="runtime-1",
                execution_id="execution-1",
                process_id="process-1",
                attempt_id="attempt-1",
                event_type=event_type,
                timestamp=timestamp,
                status=status,
                status_class=status_class,
                payload={"ordinal": 1, "recovery_action": "retry"},
            )
        )

    ledger = store.ledger()
    attempt = ledger.attempts["attempt-1"]
    assert attempt.execution_id == "execution-1"
    assert attempt.operation == RuntimeProcessOperation.RETRY
    assert attempt.status == RuntimeEventStatus.SUCCEEDED
    assert attempt.first_event_sequence == 1
    assert attempt.last_event_sequence == 3
    assert ledger.executions["execution-1"].last_event_sequence == 3
    assert [spec.resolved_process_id() for spec in store.load_process_specs()] == ["process-1"]

    projection = RuntimeFrameworkReducer().reduce(store.list())
    store.write_projection(projection, through_sequence=store.last_event_sequence())
    store.append(
        RuntimeEvent(
            event_id=-1,
            runtime_id="runtime-1",
            event_type=RuntimeEventType.LOG_EMITTED,
            timestamp=6.0,
        )
    )

    persisted_projection, through_sequence = store.load_projection() or (None, None)
    assert persisted_projection == projection
    assert through_sequence == 3
    assert [event.event_id for event in store.events_after_projection()] == [4]
    assert store.ledger().projection_sequence == 3

    expected_ledger = store.ledger()
    assert store.rebuild_ledger() == expected_ledger

    with sqlite3.connect(store.database_path) as connection:
        connection.execute("DELETE FROM runtime_attempts")
        connection.execute("DELETE FROM runtime_executions")
    assert store.ledger().attempts == {}
    assert store.ledger().executions == {}

    assert store.rebuild_ledger(replace=True) == expected_ledger
    reopened = RuntimeDurableStore(store.database_path, expected_runtime_id="runtime-1")
    assert reopened.ledger() == expected_ledger


def test_execution_ledger_rebuild_rejects_invalid_execution_history():
    identity = RuntimeIdentity(runtime_id="runtime-1", created_at=1.0)
    reducer = RuntimeExecutionLedgerReducer()
    started = RuntimeEvent(
        event_id=0,
        sequence=0,
        runtime_id="runtime-1",
        execution_id="execution-1",
        event_type=RuntimeEventType.EXECUTION_STARTED,
        timestamp=2.0,
    )

    with pytest.raises(ValueError, match="precedes declaration"):
        reducer.reduce(identity, [started])

    created = RuntimeEvent(
        event_id=0,
        sequence=0,
        runtime_id="runtime-1",
        execution_id="execution-1",
        event_type=RuntimeEventType.EXECUTION_CREATED,
        timestamp=1.0,
        payload={"kind": "initial", "ordinal": 1},
    )
    duplicate = created.model_copy(update={"event_id": 1, "sequence": 1})
    with pytest.raises(ValueError, match="Duplicate execution.created"):
        reducer.reduce(identity, [created, duplicate])


def test_runtime_directory_manager_rerun_replaces_lineage_and_recovers_interrupted_reset(
    tmp_path, monkeypatch
):
    runtime_dir = tmp_path / "runtime"
    manager = RuntimeDirectoryManager(runtime_dir)
    original = RuntimeIdentity(
        runtime_id="runtime-1",
        logical_task_id="task-1",
        generation=1,
        created_at=1.0,
    )
    store = manager.create(original)
    store.append(
        RuntimeEvent(
            event_id=-1,
            runtime_id="runtime-1",
            event_type=RuntimeEventType.LOG_EMITTED,
            timestamp=2.0,
        )
    )
    (runtime_dir / "status.json").write_text("old runtime", encoding="utf-8")
    replacement = RuntimeIdentity(
        runtime_id="runtime-2",
        logical_task_id="task-1",
        generation=2,
        rerun_of_runtime_id="runtime-1",
        created_at=3.0,
    )
    cleaned: list[tuple[Path, RuntimeIdentity]] = []

    def cleanup(path: Path, identity: RuntimeIdentity) -> None:
        cleaned.append((path, identity))

    def fail_initialization(identity: RuntimeIdentity) -> None:
        del identity
        raise RuntimeError("synthetic reset interruption")

    monkeypatch.setattr(manager, "_initialize_new_store", fail_initialization)
    with pytest.raises(RuntimeError, match="synthetic reset interruption"):
        manager.rerun(
            replacement,
            expected_runtime_id="runtime-1",
            cleanup=cleanup,
        )

    assert manager.marker_path.exists()
    assert not (runtime_dir / "status.json").exists()
    recovered = RuntimeDirectoryManager(runtime_dir).recover_pending_reset()
    assert recovered is not None
    assert recovered.identity() == replacement
    assert recovered.last_event_sequence() == -1
    assert cleaned == [(runtime_dir, original)]
    assert not manager.marker_path.exists()
    assert not list(tmp_path.glob(".runtime.abandoned.*"))

    initial = recovered.begin_execution(
        execution_id="execution-1",
        kind=RuntimeExecutionKind.INITIAL,
        created_at=4.0,
    )
    assert initial.first_event_sequence == 0
    continued = RuntimeDirectoryManager(runtime_dir).continue_runtime(
        expected_runtime_id="runtime-2"
    )
    assert continued.identity() == replacement


def test_runtime_directory_manager_rejects_rerun_identity_reuse(tmp_path):
    manager = RuntimeDirectoryManager(tmp_path / "runtime")
    identity = RuntimeIdentity(runtime_id="runtime-1", generation=1, created_at=1.0)
    manager.create(identity)

    with pytest.raises(ValueError, match="new runtime_id"):
        manager.rerun(identity, expected_runtime_id="runtime-1")


def test_runtime_directory_manager_rejects_rerun_while_lease_is_live(tmp_path):
    manager = RuntimeDirectoryManager(tmp_path / "runtime")
    current = RuntimeIdentity(runtime_id="runtime-1", generation=1, created_at=1.0)
    store = manager.create(current)
    store.acquire_backend_session(
        session_id="session-1", owner_id="backend-1", now=2.0, ttl=10.0
    )
    replacement = RuntimeIdentity(
        runtime_id="runtime-2",
        generation=2,
        rerun_of_runtime_id="runtime-1",
        created_at=3.0,
    )

    with pytest.raises(RuntimeLeaseConflictError, match="Cannot rerun"):
        manager.rerun(replacement, expected_runtime_id="runtime-1", now=3.0)

    rerun = manager.rerun(replacement, expected_runtime_id="runtime-1", now=13.0)
    assert rerun.identity() == replacement


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
    store.write_process_specs([RuntimeProcessSpec(process_id="process1", process_type="example.process")])

    runtime_info = json.loads((store.runtime_dir / "runtime_info.json").read_text(encoding="utf-8"))
    artifacts = list_runtime_artifacts(store.runtime_dir)

    assert runtime_info["engine"] == "ExampleEngine"
    assert {item["path"] for item in artifacts} == {
        "runtime_info.json",
        "runtime/progress.json",
        "runtime/processes.json",
        "status.json",
    }
    assert store.load_process_specs()[0].resolved_process_id() == "process1"


def test_load_runtime_observation_reads_only_framework_artifacts(tmp_path):
    runtime_dir = tmp_path / "runtime"
    store = RuntimeStore(runtime_dir)
    store.write_process_specs([RuntimeProcessSpec(process_id="process1", process_type="example.process")])
    store.write_projection(
        RuntimeProjection(
            runtime_id="runtime1",
            units={
                "unit1": RuntimeUnitState(
                    unit_id="unit1",
                    process_id="process1",
                    status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
                )
            },
        ).model_dump(mode="json")
    )

    observation = load_runtime_observation(runtime_dir, runtime_id="business-id")

    assert isinstance(observation, RuntimeObservation)
    assert observation.runtime_id == "business-id"
    assert observation.projection is not None
    assert observation.process_specs[0].process_type == "example.process"
    assert observation.units["unit1"].process_id == "process1"
    assert observation.processes == {}
    assert observation.attempts == {}
    assert observation.is_available()


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


def test_runtime_event_type_vocabulary_is_recommended_but_event_types_remain_open():
    standard = RuntimeEvent(
        event_id=1,
        runtime_id="runtime1",
        event_type=RuntimeEventType.PROCESS_STATUS_CHANGED,
        timestamp=1.0,
    )
    custom = RuntimeEvent(
        event_id=2,
        runtime_id="runtime1",
        event_type="annotation.candidate.scored",
        timestamp=2.0,
    )

    assert standard.event_type == RuntimeEventType.PROCESS_STATUS_CHANGED
    assert custom.event_type == "annotation.candidate.scored"


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


def test_recoverable_process_contracts_are_json_safe_and_backward_compatible():
    legacy = RuntimeProcessSpec.model_validate({"process_id": "legacy", "process_type": "example.legacy"})
    spec = RuntimeProcessSpec(
        process_id="score",
        process_type="example.score",
        depends_on=["prepare"],
        execution_key="sample-1:score",
        input_fingerprint="sha256:input",
        implementation_version="2",
        idempotency=RuntimeIdempotency.IDEMPOTENT,
        checkpoint_policy=RuntimeCheckpointPolicy(
            mode=RuntimeCheckpointMode.OPTIONAL,
            format="example.v1",
            cursor_semantics="committed index",
        ),
        output_contract=["scores.parquet"],
    )
    checkpoint = RuntimeCheckpointRef(
        checkpoint_id="checkpoint-1",
        process_id="score",
        attempt_id="attempt-1",
        created_at=2.0,
        uri="runtime/checkpoints/checkpoint-1.json",
        cursor={"committed_index": 4},
        input_fingerprint=spec.input_fingerprint,
        implementation_version=spec.implementation_version,
    )
    decision = RuntimeRecoveryDecision(
        process_id="score",
        action=RuntimeRecoveryAction.RESUME,
        reason="validated committed cursor",
        source_attempt_id="attempt-1",
        checkpoint=checkpoint,
    )
    plan = RuntimeRecoveryPlan(
        plan_id="plan-1",
        source_runtime_id="runtime-old",
        target_runtime_id="runtime-new",
        created_at=3.0,
        steps=[RuntimeRecoveryStep(**decision.model_dump(mode="json"), depends_on=spec.depends_on)],
    )
    attempt = RuntimeProcessAttempt(
        attempt_id="attempt-2",
        process_id="score",
        runtime_id="runtime-new",
        ordinal=2,
        resumed_from_attempt_id="attempt-1",
        checkpoint_id=checkpoint.checkpoint_id,
    )
    event = RuntimeEvent(
        event_id=1,
        runtime_id="runtime-new",
        process_id="score",
        attempt_id=attempt.attempt_id,
        checkpoint_id=checkpoint.checkpoint_id,
        event_type=RuntimeEventType.PROCESS_ATTEMPT_CREATED,
        timestamp=4.0,
    )

    assert legacy.depends_on == []
    assert legacy.idempotency == RuntimeIdempotency.UNKNOWN
    assert spec.model_dump(mode="json")["checkpoint_policy"]["mode"] == "optional"
    assert plan.steps[0].checkpoint == checkpoint
    assert event.model_dump(mode="json")["attempt_id"] == "attempt-2"


def test_recovery_contracts_reject_ambiguous_or_unsafe_declarations():
    with pytest.raises(ValueError, match="depends_on entries must be unique"):
        RuntimeProcessSpec(process_id="score", process_type="example.score", depends_on=["prepare", "prepare"])

    with pytest.raises(ValueError, match="resume recovery decisions require a checkpoint"):
        RuntimeRecoveryDecision(
            process_id="score",
            action=RuntimeRecoveryAction.RESUME,
            reason="missing checkpoint",
        )

    with pytest.raises(ValueError, match="resume recovery steps require a checkpoint"):
        RuntimeRecoveryStep(
            process_id="score",
            action=RuntimeRecoveryAction.RESUME,
            reason="missing checkpoint",
        )

    duplicate_step = RuntimeRecoveryStep(
        process_id="score",
        action=RuntimeRecoveryAction.RESTART,
        reason="input changed",
    )
    with pytest.raises(ValueError, match="process_ids must be unique"):
        RuntimeRecoveryPlan(
            plan_id="plan-1",
            source_runtime_id="runtime-old",
            target_runtime_id="runtime-new",
            created_at=3.0,
            steps=[duplicate_step, duplicate_step],
        )


def test_runtime_process_graph_validates_dependencies_and_finds_downstream():
    graph = build_runtime_process_graph(
        [
            RuntimeProcessSpec(process_id="finalize", process_type="example.finalize", depends_on=["score"]),
            RuntimeProcessSpec(process_id="prepare", process_type="example.prepare"),
            RuntimeProcessSpec(process_id="report", process_type="example.report", depends_on=["score"]),
            RuntimeProcessSpec(process_id="score", process_type="example.score", depends_on=["prepare"]),
        ]
    )

    assert isinstance(graph, RuntimeProcessGraph)
    assert graph.topological_order == ["prepare", "score", "finalize", "report"]
    assert graph.dependencies["score"] == ["prepare"]
    assert graph.downstream({"prepare"}) == ["score", "finalize", "report"]
    assert graph.downstream({"score"}) == ["finalize", "report"]


@pytest.mark.parametrize(
    ("specs", "message"),
    [
        (
            [
                RuntimeProcessSpec(process_id="same", process_type="example.one"),
                RuntimeProcessSpec(process_id="same", process_type="example.two"),
            ],
            "Duplicate process_id",
        ),
        (
            [RuntimeProcessSpec(process_id="self", process_type="example.self", depends_on=["self"])],
            "cannot depend on itself",
        ),
        (
            [RuntimeProcessSpec(process_id="child", process_type="example.child", depends_on=["missing"])],
            "unknown dependencies",
        ),
        (
            [
                RuntimeProcessSpec(process_id="one", process_type="example.one", depends_on=["two"]),
                RuntimeProcessSpec(process_id="two", process_type="example.two", depends_on=["one"]),
            ],
            "contains a cycle",
        ),
    ],
)
def test_runtime_process_graph_rejects_invalid_declarations(specs, message):
    with pytest.raises(RuntimeRecoveryGraphError, match=message):
        build_runtime_process_graph(specs)


def test_runtime_projection_preserves_attempt_history_and_committed_checkpoints():
    checkpoint = RuntimeCheckpointRef(
        checkpoint_id="checkpoint-1",
        process_id="score",
        attempt_id="attempt-1",
        created_at=3.0,
        cursor={"committed_index": 4},
    )
    events = [
        RuntimeEvent(
            event_id=1,
            runtime_id="runtime1",
            process_id="score",
            attempt_id="attempt-1",
            event_type=RuntimeEventType.PROCESS_ATTEMPT_STARTED,
            timestamp=1.0,
            payload={"ordinal": 1},
        ),
        RuntimeEvent(
            event_id=2,
            runtime_id="runtime1",
            process_id="score",
            attempt_id="attempt-1",
            event_type=RuntimeEventType.PROCESS_ATTEMPT_FAILED,
            timestamp=2.0,
            error=RuntimeErrorInfo(type="TransientError", message="retry"),
        ),
        RuntimeEvent(
            event_id=3,
            runtime_id="runtime1",
            process_id="score",
            attempt_id="attempt-1",
            checkpoint_id=checkpoint.checkpoint_id,
            event_type=RuntimeEventType.CHECKPOINT_COMMITTED,
            timestamp=3.0,
            payload={"checkpoint": checkpoint.model_dump(mode="json")},
        ),
        RuntimeEvent(
            event_id=4,
            runtime_id="runtime1",
            process_id="score",
            attempt_id="attempt-2",
            checkpoint_id=checkpoint.checkpoint_id,
            event_type=RuntimeEventType.PROCESS_ATTEMPT_STARTED,
            timestamp=4.0,
            payload={"ordinal": 2, "resumed_from_attempt_id": "attempt-1"},
        ),
        RuntimeEvent(
            event_id=5,
            runtime_id="runtime1",
            process_id="score",
            attempt_id="attempt-2",
            event_type=RuntimeEventType.PROCESS_ATTEMPT_COMPLETED,
            timestamp=5.0,
        ),
    ]

    projection = RuntimeFrameworkReducer().reduce(events)

    assert projection.process_attempt_ids["score"] == ["attempt-1", "attempt-2"]
    assert projection.attempts["attempt-1"].status_class == RuntimeStatusClass.TERMINAL_FAILURE
    assert projection.attempts["attempt-2"].status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert projection.attempts["attempt-2"].resumed_from_attempt_id == "attempt-1"
    assert projection.processes["score"].current_attempt_id == "attempt-2"
    assert projection.processes["score"].status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert projection.status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert projection.checkpoints["checkpoint-1"] == checkpoint
    assert projection.latest_checkpoint_by_process == {"score": "checkpoint-1"}


def test_runtime_projection_ignores_observation_checkpoint_and_removes_invalidated_checkpoint():
    checkpoint = RuntimeCheckpointRef(
        checkpoint_id="checkpoint-1",
        process_id="score",
        attempt_id="attempt-1",
        created_at=1.0,
    )
    events = [
        RuntimeEvent(
            event_id=1,
            runtime_id="runtime1",
            process_id="score",
            event_type=RuntimeEventType.PROCESS_CHECKPOINTED,
            timestamp=1.0,
            payload={"name": "telemetry-only"},
        ),
        RuntimeEvent(
            event_id=2,
            runtime_id="runtime1",
            process_id="score",
            attempt_id="attempt-1",
            checkpoint_id="checkpoint-1",
            event_type=RuntimeEventType.CHECKPOINT_COMMITTED,
            timestamp=2.0,
            payload={"checkpoint": checkpoint.model_dump(mode="json")},
        ),
        RuntimeEvent(
            event_id=3,
            runtime_id="runtime1",
            process_id="score",
            attempt_id="attempt-1",
            checkpoint_id="checkpoint-1",
            event_type=RuntimeEventType.CHECKPOINT_INVALIDATED,
            timestamp=3.0,
        ),
    ]

    projection = RuntimeFrameworkReducer().reduce(events)

    assert projection.checkpoints == {}
    assert projection.latest_checkpoint_by_process == {}


def test_runtime_recovery_planner_blocks_stale_downstream_skip_and_persists_plan(tmp_path):
    specs = [
        RuntimeProcessSpec(process_id="prepare", process_type="example.prepare"),
        RuntimeProcessSpec(
            process_id="score",
            process_type="example.score",
            depends_on=["prepare"],
            idempotency=RuntimeIdempotency.IDEMPOTENT,
            capabilities=RuntimeProcessCapabilities(can_retry=True),
            retry_policy=RuntimeRetryPolicy(max_attempts=3),
        ),
        RuntimeProcessSpec(process_id="report", process_type="example.report", depends_on=["score"]),
    ]
    projection = RuntimeProjection(
        runtime_id="runtime-old",
        processes={
            "prepare": RuntimeProcessState(
                process_id="prepare",
                status=RuntimeEventStatus.SUCCEEDED,
                status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
            ),
            "score": RuntimeProcessState(
                process_id="score",
                status=RuntimeEventStatus.FAILED,
                status_class=RuntimeStatusClass.TERMINAL_FAILURE,
            ),
            "report": RuntimeProcessState(
                process_id="report",
                status=RuntimeEventStatus.SUCCEEDED,
                status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
            ),
        },
        process_attempt_ids={"score": ["attempt-1"]},
    )
    plan = RuntimeRecoveryPlanner().create_plan(
        specs=specs,
        source_projection=projection,
        decisions=[
            RuntimeRecoveryDecision(
                process_id="prepare",
                action=RuntimeRecoveryAction.SKIP,
                reason="validated output",
            ),
            RuntimeRecoveryDecision(
                process_id="score",
                action=RuntimeRecoveryAction.RETRY,
                reason="transient failure",
            ),
            RuntimeRecoveryDecision(
                process_id="report",
                action=RuntimeRecoveryAction.SKIP,
                reason="previous report exists",
            ),
        ],
        plan_id="plan-1",
        target_runtime_id="runtime-new",
        created_at=10.0,
    )
    plan_path = RuntimeStore(tmp_path).write_recovery_plan(plan)
    restored = RuntimeStore(tmp_path).load_recovery_plan("plan-1")

    assert [step.process_id for step in plan.steps] == ["prepare", "score", "report"]
    assert [step.action for step in plan.steps] == [
        RuntimeRecoveryAction.SKIP,
        RuntimeRecoveryAction.RETRY,
        RuntimeRecoveryAction.BLOCK,
    ]
    assert plan.steps[1].invalidates == ["report"]
    assert "upstream reexecution invalidates skip" in plan.steps[2].reason
    assert plan_path == tmp_path / "runtime" / "recovery_plans" / "plan-1.json"
    assert restored == plan


def test_runtime_recovery_planner_accepts_only_committed_compatible_checkpoint():
    checkpoint = RuntimeCheckpointRef(
        checkpoint_id="checkpoint-1",
        process_id="score",
        attempt_id="attempt-1",
        created_at=2.0,
        input_fingerprint="sha256:input",
        implementation_version="2",
    )
    spec = RuntimeProcessSpec(
        process_id="score",
        process_type="example.score",
        input_fingerprint="sha256:input",
        implementation_version="2",
        capabilities=RuntimeProcessCapabilities(can_resume=True),
        checkpoint_policy=RuntimeCheckpointPolicy(mode=RuntimeCheckpointMode.OPTIONAL),
    )
    projection = RuntimeProjection(
        runtime_id="runtime-old",
        checkpoints={checkpoint.checkpoint_id: checkpoint},
    )
    plan = RuntimeRecoveryPlanner().create_plan(
        specs=[spec],
        source_projection=projection,
        decisions=[
            RuntimeRecoveryDecision(
                process_id="score",
                action=RuntimeRecoveryAction.RESUME,
                reason="checkpoint validated",
                source_attempt_id="attempt-1",
                checkpoint=checkpoint,
            )
        ],
        plan_id="plan-1",
        target_runtime_id="runtime-new",
        created_at=3.0,
    )

    assert plan.steps[0].action == RuntimeRecoveryAction.RESUME
    assert plan.steps[0].checkpoint == checkpoint

    incompatible = checkpoint.model_copy(update={"input_fingerprint": "sha256:changed"})
    blocked = RuntimeRecoveryPlanner().create_plan(
        specs=[spec],
        source_projection=projection,
        decisions=[
            RuntimeRecoveryDecision(
                process_id="score",
                action=RuntimeRecoveryAction.RESUME,
                reason="stale checkpoint",
                checkpoint=incompatible,
            )
        ],
        plan_id="plan-2",
        target_runtime_id="runtime-new",
        created_at=4.0,
    )

    assert blocked.steps[0].action == RuntimeRecoveryAction.BLOCK
    assert blocked.steps[0].reason == "checkpoint is not committed in the source projection"


def test_runtime_recovery_planner_requires_complete_package_decisions():
    with pytest.raises(RuntimeRecoveryPlanError, match="coverage mismatch"):
        RuntimeRecoveryPlanner().create_plan(
            specs=[RuntimeProcessSpec(process_id="prepare", process_type="example.prepare")],
            source_projection=RuntimeProjection(runtime_id="runtime-old"),
            decisions=[],
            plan_id="plan-1",
            target_runtime_id="runtime-new",
            created_at=1.0,
        )


def test_runtime_continuation_selector_builds_minimum_affected_dag():
    specs = [
        RuntimeProcessSpec(process_id="prepare", process_type="example.prepare"),
        RuntimeProcessSpec(
            process_id="score",
            process_type="example.score",
            depends_on=["prepare"],
            idempotency=RuntimeIdempotency.IDEMPOTENT,
            capabilities=RuntimeProcessCapabilities(can_retry=True),
            retry_policy=RuntimeRetryPolicy(max_attempts=3),
        ),
        RuntimeProcessSpec(
            process_id="report",
            process_type="example.report",
            depends_on=["score"],
        ),
        RuntimeProcessSpec(process_id="unrelated", process_type="example.unrelated"),
    ]
    succeeded = RuntimeProcessState(
        process_id="placeholder",
        status=RuntimeEventStatus.SUCCEEDED,
        status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
    )
    projection = RuntimeProjection(
        runtime_id="runtime-1",
        processes={
            "prepare": succeeded.model_copy(update={"process_id": "prepare"}),
            "score": RuntimeProcessState(
                process_id="score",
                status=RuntimeEventStatus.INTERRUPTED,
                status_class=RuntimeStatusClass.TERMINAL_FAILURE,
            ),
            "report": succeeded.model_copy(update={"process_id": "report"}),
            "unrelated": succeeded.model_copy(update={"process_id": "unrelated"}),
        },
        process_attempt_ids={"score": ["score:attempt:1"]},
    )
    plan = RuntimeContinuationSelector().create_plan(
        specs=specs,
        source_projection=projection,
        assessments=[
            RuntimeContinuationAssessment(process_id="prepare", completed_output_valid=True),
            RuntimeContinuationAssessment(
                process_id="score",
                completed_output_valid=False,
                reason="interrupted backend attempt",
            ),
            RuntimeContinuationAssessment(process_id="report", completed_output_valid=True),
            RuntimeContinuationAssessment(process_id="unrelated", completed_output_valid=True),
        ],
        plan_id="continue-1",
        target_runtime_id="runtime-1",
        created_at=10.0,
    )

    assert [step.process_id for step in plan.steps] == [
        "prepare",
        "score",
        "report",
        "unrelated",
    ]
    assert [step.action for step in plan.steps] == [
        RuntimeRecoveryAction.SKIP,
        RuntimeRecoveryAction.RETRY,
        RuntimeRecoveryAction.RESTART,
        RuntimeRecoveryAction.SKIP,
    ]
    assert plan.steps[1].source_attempt_id == "score:attempt:1"
    assert plan.steps[1].invalidates == ["report"]


def test_runtime_continuation_selector_enforces_cleanup_gate():
    spec = RuntimeProcessSpec(
        process_id="publish",
        process_type="example.publish",
        idempotency=RuntimeIdempotency.REQUIRES_CLEANUP,
        capabilities=RuntimeProcessCapabilities(can_retry=True, can_cleanup=True),
        retry_policy=RuntimeRetryPolicy(max_attempts=3),
    )
    projection = RuntimeProjection(
        runtime_id="runtime-1",
        processes={
            "publish": RuntimeProcessState(
                process_id="publish",
                status=RuntimeEventStatus.FAILED,
                status_class=RuntimeStatusClass.TERMINAL_FAILURE,
            )
        },
        process_attempt_ids={"publish": ["publish:attempt:1"]},
    )

    def select(cleanup_completed: bool) -> RuntimeRecoveryAction:
        plan = RuntimeContinuationSelector().create_plan(
            specs=[spec],
            source_projection=projection,
            assessments=[
                RuntimeContinuationAssessment(
                    process_id="publish",
                    cleanup_completed=cleanup_completed,
                )
            ],
            plan_id=f"continue-{cleanup_completed}",
            target_runtime_id="runtime-1",
            created_at=2.0,
        )
        return plan.steps[0].action

    assert select(cleanup_completed=False) == RuntimeRecoveryAction.BLOCK
    assert select(cleanup_completed=True) == RuntimeRecoveryAction.RETRY

    with pytest.raises(RuntimeRecoveryPlanError, match="must preserve runtime_id"):
        RuntimeContinuationSelector().create_plan(
            specs=[spec],
            source_projection=projection,
            assessments=[RuntimeContinuationAssessment(process_id="publish")],
            plan_id="invalid-cross-runtime-continue",
            target_runtime_id="runtime-2",
            created_at=3.0,
        )


def test_runtime_backend_executor_persists_and_executes_recovery_plan(tmp_path):
    runtime_dir = tmp_path / "runtime"

    class RetryProcess(RuntimeProcessBase):
        def retry(self, context: RuntimeProcessContext) -> dict[str, bool]:
            assert (runtime_dir / "runtime" / "recovery_plans" / "plan-1.json").exists()
            assert context.attempt_id == "score:attempt:1"
            return {"retried": True}

    executor = RuntimeBackendExecutor(runtime_id="runtime-new", runtime_dir=runtime_dir)
    executor.register(
        RetryProcess(
            RuntimeProcessSpec(
                process_id="score",
                process_type="example.score",
                capabilities=RuntimeProcessCapabilities(can_retry=True),
            )
        )
    )
    plan = RuntimeRecoveryPlan(
        plan_id="plan-1",
        source_runtime_id="runtime-old",
        target_runtime_id="runtime-new",
        created_at=1.0,
        steps=[
            RuntimeRecoveryStep(
                process_id="prepare",
                action=RuntimeRecoveryAction.SKIP,
                reason="validated output",
                source_attempt_id="prepare:attempt:1",
            ),
            RuntimeRecoveryStep(
                process_id="score",
                action=RuntimeRecoveryAction.RETRY,
                reason="transient failure",
                depends_on=["prepare"],
                source_attempt_id="score:attempt:old",
            ),
        ],
    )

    results = executor.execute_recovery_plan(plan)
    projection = executor.projection()
    event_types = [event.event_type for event in executor.event_store.list()]

    assert results == {"prepare": None, "score": {"retried": True}}
    assert RuntimeEventType.RECOVERY_STARTED in event_types
    assert RuntimeEventType.PROCESS_ATTEMPT_CREATED in event_types
    assert RuntimeEventType.PROCESS_ATTEMPT_COMPLETED in event_types
    assert event_types[-1] == RuntimeEventType.RECOVERY_COMPLETED
    assert projection.processes["prepare"].status == RuntimeEventStatus.SKIPPED
    assert projection.attempts["score:attempt:1"].status_class == RuntimeStatusClass.TERMINAL_SUCCESS
    assert projection.attempts["score:attempt:1"].resumed_from_attempt_id == "score:attempt:old"


def test_runtime_backend_executor_records_failed_recovery_attempt(tmp_path):
    class FailingProcess(RuntimeProcessBase):
        def start(self, context: RuntimeProcessContext) -> None:
            raise RuntimeError(f"failed {context.attempt_id}")

    executor = RuntimeBackendExecutor(runtime_id="runtime-new", runtime_dir=tmp_path / "runtime")
    executor.register(FailingProcess(RuntimeProcessSpec(process_id="score", process_type="example.score")))
    plan = RuntimeRecoveryPlan(
        plan_id="plan-1",
        source_runtime_id="runtime-old",
        target_runtime_id="runtime-new",
        created_at=1.0,
        steps=[
            RuntimeRecoveryStep(
                process_id="score",
                action=RuntimeRecoveryAction.RESTART,
                reason="start over",
            )
        ],
    )

    with pytest.raises(RuntimeError, match="failed score:attempt:1"):
        executor.execute_recovery_plan(plan)

    projection = executor.projection()
    assert projection.attempts["score:attempt:1"].status_class == RuntimeStatusClass.TERMINAL_FAILURE
    assert executor.event_store.list()[-1].event_type == RuntimeEventType.RECOVERY_FAILED


def test_runtime_backend_executor_rejects_blocked_recovery_plan_before_dispatch(tmp_path):
    executor = RuntimeBackendExecutor(runtime_id="runtime-new", runtime_dir=tmp_path / "runtime")
    plan = RuntimeRecoveryPlan(
        plan_id="blocked",
        source_runtime_id="runtime-old",
        target_runtime_id="runtime-new",
        created_at=1.0,
        steps=[
            RuntimeRecoveryStep(
                process_id="score",
                action=RuntimeRecoveryAction.BLOCK,
                reason="requires review",
            )
        ],
    )

    with pytest.raises(RuntimeRecoveryExecutionError, match="contains blocked processes"):
        executor.execute_recovery_plan(plan)

    assert not (tmp_path / "runtime" / "runtime" / "recovery_plans" / "blocked.json").exists()


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
    resource_usage = context.emit_resource_usage(RuntimeResourceUsage(cpu_percent=25.0, memory_bytes=1024))
    artifact = context.emit_artifact(context.artifact_path("result.json"), payload={"kind": "result"})
    completed = context.emit_status(RuntimeEventStatus.SUCCEEDED)

    assert [event.event_id for event in store.list()] == [1, 2, 3, 4, 5]
    assert started.event_type == "process.status.changed"
    assert progressed.event_type == "process.progressed"
    assert progressed.progress is not None
    assert progressed.progress.percent == 50.0
    assert resource_usage.event_type == "resource.sampled"
    assert resource_usage.payload["resource_usage"]["memory_bytes"] == 1024
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


def test_runtime_process_context_commits_and_invalidates_recoverable_checkpoint(tmp_path):
    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    context = RuntimeProcessContext(
        runtime_id="runtime1",
        process_id="score",
        attempt_id="attempt-1",
        event_store=store,
    )
    checkpoint = RuntimeCheckpointRef(
        checkpoint_id="checkpoint-1",
        process_id="score",
        attempt_id="attempt-1",
        created_at=1.0,
        cursor={"committed_index": 4},
    )

    committed = context.commit_checkpoint(checkpoint)
    invalidated = context.invalidate_checkpoint(checkpoint.checkpoint_id, reason="input changed")

    assert committed.event_type == RuntimeEventType.CHECKPOINT_COMMITTED
    assert committed.attempt_id == "attempt-1"
    assert committed.checkpoint_id == "checkpoint-1"
    assert RuntimeCheckpointRef.model_validate(committed.payload["checkpoint"]) == checkpoint
    assert invalidated.event_type == RuntimeEventType.CHECKPOINT_INVALIDATED
    assert invalidated.payload == {"reason": "input changed"}

    with pytest.raises(ValueError, match="attempt_id must match"):
        context.commit_checkpoint(checkpoint.model_copy(update={"attempt_id": "attempt-other"}))


def test_runtime_manager_event_bridge_mirrors_manager_changes(tmp_path):
    runtime = RuntimeManagerBundle.create()
    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    bridge = RuntimeManagerEventBridge(
        runtime,
        store,
        runtime_id="runtime1",
        process_id="process1",
        metadata={"source": "manager-bridge"},
    )
    try:
        runtime.progress.register_task("task1", "Task", total=2)
        runtime.progress.update_progress("task1", current=1, status="running")
        runtime.signals.update("ready", status="running", value=True)
        runtime.logs.info("started", task_id="task1")
        runtime.streams.write_chunk("stdout", "hello", task_id="task1")
        runtime.telemetry.metric("items", 1, unit="count")

        events = bridge.sync()

        assert [event.event_id for event in events] == [1, 2, 3, 4, 5]
        assert [event.event_type for event in events] == [
            "process.progressed",
            "signal.changed",
            "log.emitted",
            "stream.chunk",
            "metric.sampled",
        ]
        assert all(event.metadata["source"] == "manager-bridge" for event in events)
        assert bridge.sync() == []

        runtime.logs.info("finished", task_id="task1")
        appended = bridge.sync()

        assert [event.event_id for event in appended] == [6]
        assert appended[0].message == "finished"
    finally:
        runtime.close()


def test_runtime_backend_executor_syncs_non_owned_manager_bridge(tmp_path):
    class ManagerProcess(RuntimeProcessBase):
        def start(self, context: RuntimeProcessContext) -> None:
            del context
            runtime.progress.register_task("task1", "Task", total=1)
            runtime.progress.update_progress("task1", current=1, status="completed")
            runtime.logs.info("process completed", task_id="task1")

    runtime = RuntimeManagerBundle.create()
    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    bridge = RuntimeManagerEventBridge(runtime, store, runtime_id="runtime1", process_id="manager-process")
    executor = RuntimeBackendExecutor(
        runtime_id="runtime1",
        runtime_dir=tmp_path / "runtime",
        event_store=store,
        manager_bridge=bridge,
    )
    executor.register(ManagerProcess(RuntimeProcessSpec(process_id="manager-process", process_type="example.manager")))
    try:
        executor.run_process("manager-process")

        assert [event.event_type for event in store.list()] == [
            "process.created",
            "process.attempt.created",
            "process.attempt.started",
            "process.status.changed",
            "process.status.changed",
            "process.attempt.completed",
            "process.progressed",
            "log.emitted",
        ]
        attempt = executor.projection().attempts["manager-process:attempt:1"]
        assert attempt.operation == RuntimeProcessOperation.START
        assert executor.projection().processes["manager-process"].status == RuntimeEventStatus.SUCCEEDED
    finally:
        runtime.close()


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
            context.emit_resource_usage(RuntimeResourceUsage(cpu_percent=20.0, memory_bytes=2048))
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
    assert projection.resource_usage_summary["process1"].memory_bytes == 2048
    assert projection.processes["process1"].resource_usage is not None
    assert projection.artifact_index[0]["kind"] == "result"


def test_runtime_framework_reducer_is_deterministic_for_unordered_input():
    events = [
        RuntimeEvent(
            event_id=3,
            runtime_id="runtime1",
            process_id="process1",
            event_type="artifact.produced",
            timestamp=3.0,
            payload={"path": "result.json", "kind": "result"},
        ),
        RuntimeEvent(
            event_id=1,
            runtime_id="runtime1",
            process_id="process1",
            event_type="process.created",
            timestamp=1.0,
            status=RuntimeEventStatus.PENDING,
            status_class=RuntimeStatusClass.NOT_STARTED,
            payload={"process_type": "example.process"},
        ),
        RuntimeEvent(
            event_id=2,
            runtime_id="runtime1",
            process_id="process1",
            event_type="process.status.changed",
            timestamp=2.0,
            status=RuntimeEventStatus.SUCCEEDED,
            status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
            payload={"result": {"ok": True}},
        ),
    ]

    reducer = RuntimeFrameworkReducer()
    ordered_payload = reducer.reduce(events).model_dump(mode="json")
    reversed_payload = reducer.reduce(list(reversed(events))).model_dump(mode="json")

    assert ordered_payload == reversed_payload


def test_runtime_framework_reducer_summarizes_failure_and_writes_projection(tmp_path):
    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    context = RuntimeProcessContext(runtime_id="runtime1", process_id="process1", event_store=store)
    context.emit_status(RuntimeEventStatus.RUNNING)
    context.emit_error(RuntimeErrorInfo(type="ExampleError", message="bad"))

    projection_payload = runtime_projection_payload(store.list())
    projection = RuntimeProjection.model_validate(projection_payload)
    runtime_store = RuntimeStore(tmp_path / "runtime")
    runtime_store.write_projection(projection_payload)
    restored_projection = runtime_store.load_projection()
    restored_from_path = load_runtime_projection(runtime_store.runtime_dir)
    artifacts = list_runtime_artifacts(runtime_store.runtime_dir)

    assert projection.status == RuntimeEventStatus.FAILED
    assert projection.status_class == RuntimeStatusClass.TERMINAL_FAILURE
    assert projection.terminal_failure_count == 1
    assert projection.error_summary[0].type == "ExampleError"
    assert restored_projection == projection
    assert restored_from_path == projection
    assert {artifact["path"] for artifact in artifacts} == {"runtime/projection.json"}


def test_runtime_framework_reducer_propagates_child_failure_by_policy(tmp_path):
    store = RuntimeEventJsonlStore(tmp_path / "events.runtime.jsonl")
    parent = RuntimeProcessContext(runtime_id="runtime1", process_id="parent", event_store=store)
    child = RuntimeProcessContext(
        runtime_id="runtime1",
        process_id="child",
        parent_process_id="parent",
        event_store=store,
    )
    parent.emit_status(RuntimeEventStatus.RUNNING)
    child.emit_status(RuntimeEventStatus.RUNNING)
    child.emit_error(RuntimeErrorInfo(type="ChildError", message="bad child"))

    default_projection = RuntimeFrameworkReducer().reduce(store.list())
    no_propagation = RuntimeFrameworkReducer(
        policy=RuntimeProjectionPolicy(propagate_child_failure=False)
    ).reduce(store.list())

    assert default_projection.processes["child"].status_class == RuntimeStatusClass.TERMINAL_FAILURE
    assert default_projection.processes["parent"].status_class == RuntimeStatusClass.TERMINAL_FAILURE
    assert default_projection.processes["parent"].error is not None
    assert default_projection.processes["parent"].error.type == "ChildError"
    assert default_projection.terminal_failure_count == 2
    assert no_propagation.processes["parent"].status_class == RuntimeStatusClass.ACTIVE
    assert no_propagation.terminal_failure_count == 1


def test_runtime_backend_executor_runs_processes_and_writes_projection(tmp_path):
    class ExampleProcess(RuntimeProcessBase):
        def start(self, context: RuntimeProcessContext) -> dict[str, Any]:
            context.emit_progress(1, total=1)
            return {"process_id": self.spec.resolved_process_id()}

    executor = RuntimeBackendExecutor(runtime_id="runtime1", runtime_dir=tmp_path / "runtime")
    executor.register(ExampleProcess(RuntimeProcessSpec(process_id="p1", process_type="example.process")))
    executor.register(
        ExampleProcess(
            RuntimeProcessSpec(process_id="p2", process_type="example.process", parent_process_id="p1")
        )
    )

    results = executor.run_all()
    projection = executor.projection()
    restored = RuntimeStore(tmp_path / "runtime").load_projection()
    specs = RuntimeStore(tmp_path / "runtime").load_process_specs()
    event_ids = [event.event_id for event in executor.event_store.list()]

    assert results["p1"] == {"process_id": "p1"}
    assert results["p2"] == {"process_id": "p2"}
    assert [event.event_type for event in executor.event_store.list()[:2]] == [
        "process.created",
        "process.created",
    ]
    assert event_ids == sorted(event_ids)
    assert len(set(event_ids)) == len(event_ids)
    assert projection.terminal_success_count == 2
    assert projection.processes["p2"].parent_process_id == "p1"
    assert restored == projection
    assert [spec.resolved_process_id() for spec in specs] == ["p1", "p2"]


def test_runtime_backend_executor_uses_durable_execution_and_attempt_identity(tmp_path):
    class ExampleProcess(RuntimeProcessBase):
        def start(self, context: RuntimeProcessContext) -> dict[str, Any]:
            return {"execution_id": context.execution_id}

    runtime_dir = tmp_path / "runtime"
    manager = RuntimeDirectoryManager(runtime_dir)
    store = manager.create(RuntimeIdentity(runtime_id="runtime-1", created_at=1.0))
    store.begin_execution(
        execution_id="execution-1",
        kind=RuntimeExecutionKind.INITIAL,
        created_at=2.0,
    )
    store.transition_execution("execution-1", RuntimeExecutionStatus.RUNNING, timestamp=3.0)
    spec = RuntimeProcessSpec(
        process_id="process-1",
        process_type="example.process",
        execution_key="stable-process",
        input_fingerprint="input-v1",
        implementation_version="implementation-v1",
    )
    executor = RuntimeBackendExecutor(
        runtime_id="runtime-1",
        execution_id="execution-1",
        runtime_dir=runtime_dir,
        event_store=store,
    )
    executor.register(ExampleProcess(spec))
    assert executor.run_process("process-1") == {"execution_id": "execution-1"}
    store.transition_execution("execution-1", RuntimeExecutionStatus.SUCCEEDED, timestamp=4.0)
    executor.write_projection()

    store.begin_execution(
        execution_id="execution-2",
        kind=RuntimeExecutionKind.CONTINUE,
        created_at=5.0,
    )
    store.transition_execution("execution-2", RuntimeExecutionStatus.RUNNING, timestamp=6.0)
    continued = RuntimeBackendExecutor(
        runtime_id="runtime-1",
        execution_id="execution-2",
        runtime_dir=runtime_dir,
        event_store=store,
    )
    created_count = sum(event.event_type == RuntimeEventType.PROCESS_CREATED for event in store.list())
    continued.register(ExampleProcess(spec))
    assert sum(event.event_type == RuntimeEventType.PROCESS_CREATED for event in store.list()) == created_count
    assert continued.run_process("process-1") == {"execution_id": "execution-2"}

    ledger = store.ledger()
    assert ledger.process_attempt_ids["process-1"] == [
        "process-1:attempt:1",
        "process-1:attempt:2",
    ]
    assert ledger.attempts["process-1:attempt:1"].execution_id == "execution-1"
    assert ledger.attempts["process-1:attempt:2"].execution_id == "execution-2"
    assert ledger.attempts["process-1:attempt:2"].execution_key == "stable-process"
    assert ledger.attempts["process-1:attempt:2"].input_fingerprint == "input-v1"
    assert store.load_projection() is not None


def test_process_attempt_reporter_records_external_execution_lifecycle(tmp_path):
    store = RuntimeDurableStore.create(
        tmp_path / "runtime.db",
        RuntimeIdentity(runtime_id="runtime-1", created_at=1.0),
    )
    store.begin_execution(
        execution_id="execution-1",
        kind=RuntimeExecutionKind.INITIAL,
        created_at=2.0,
    )
    reporter = RuntimeProcessAttemptReporter(
        store,
        execution_id="execution-1",
        backend_session_id="session-1",
    )
    spec = RuntimeProcessSpec(
        process_id="external-process",
        process_type="example.external",
        execution_key="stable-external-process",
    )

    assert reporter.declare(spec, timestamp=3.0)
    assert not reporter.declare(spec, timestamp=4.0)
    first = reporter.start("external-process", timestamp=5.0)
    completed = reporter.complete(first.attempt_id, result={"count": 2}, timestamp=6.0)
    skipped = reporter.skip("external-process", reason="output remains valid", timestamp=6.5)
    second = reporter.start(
        "external-process",
        operation=RuntimeProcessOperation.RETRY,
        resumed_from_attempt_id=first.attempt_id,
        timestamp=7.0,
    )
    failed = reporter.fail(second.attempt_id, RuntimeError("failed"), timestamp=8.0)

    assert first.attempt_id == "external-process:attempt:1"
    assert completed.status == RuntimeEventStatus.SUCCEEDED
    assert skipped.status == RuntimeEventStatus.SKIPPED
    assert second.attempt_id == "external-process:attempt:2"
    assert second.resumed_from_attempt_id == first.attempt_id
    assert failed.status == RuntimeEventStatus.FAILED
    assert failed.error is not None and failed.error.type == "RuntimeError"
    assert store.ledger().process_attempt_ids["external-process"] == [
        "external-process:attempt:1",
        "external-process:attempt:2",
    ]
    with pytest.raises(RuntimeAttemptStateError, match="already terminal"):
        reporter.complete(first.attempt_id, timestamp=9.0)


def test_durable_attempt_materialization_ignores_process_status_events(tmp_path):
    store = RuntimeDurableStore.create(
        tmp_path / "runtime.db",
        RuntimeIdentity(runtime_id="runtime-1", created_at=1.0),
    )
    store.begin_execution(
        execution_id="execution-1",
        kind=RuntimeExecutionKind.INITIAL,
        created_at=2.0,
    )
    reporter = RuntimeProcessAttemptReporter(store, execution_id="execution-1")
    reporter.declare(
        RuntimeProcessSpec(process_id="process-1", process_type="example.process"),
        timestamp=3.0,
    )
    attempt = reporter.start("process-1", timestamp=4.0)

    store.append(
        RuntimeEvent(
            event_id=-1,
            runtime_id="runtime-1",
            execution_id="execution-1",
            process_id="process-1",
            attempt_id=attempt.attempt_id,
            event_type=RuntimeEventType.PROCESS_STATUS_CHANGED,
            timestamp=5.0,
            status=RuntimeEventStatus.SUCCEEDED,
            status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
        )
    )

    assert store.ledger().attempts[attempt.attempt_id].status == RuntimeEventStatus.RUNNING
    assert reporter.complete(attempt.attempt_id, timestamp=6.0).status == (
        RuntimeEventStatus.SUCCEEDED
    )


def test_process_attempt_reporter_allocates_ordinals_across_store_instances(tmp_path):
    database_path = tmp_path / "runtime.db"
    store = RuntimeDurableStore.create(
        database_path,
        RuntimeIdentity(runtime_id="runtime-1", created_at=1.0),
    )
    store.begin_execution(
        execution_id="execution-1",
        kind=RuntimeExecutionKind.INITIAL,
        created_at=2.0,
    )
    store.declare_process(
        RuntimeProcessSpec(process_id="process-1", process_type="example.concurrent"),
        timestamp=3.0,
    )

    def start(index: int) -> str:
        reporter = RuntimeProcessAttemptReporter(
            RuntimeDurableStore(database_path),
            execution_id="execution-1",
        )
        return reporter.start("process-1", timestamp=4.0 + index).attempt_id

    with ThreadPoolExecutor(max_workers=4) as pool:
        attempt_ids = list(pool.map(start, range(4)))

    assert sorted(attempt_ids) == [f"process-1:attempt:{ordinal}" for ordinal in range(1, 5)]


def test_process_attempt_reporter_rejects_terminal_execution(tmp_path):
    store = RuntimeDurableStore.create(
        tmp_path / "runtime.db",
        RuntimeIdentity(runtime_id="runtime-1", created_at=1.0),
    )
    store.begin_execution(
        execution_id="execution-1",
        kind=RuntimeExecutionKind.INITIAL,
        created_at=2.0,
    )
    reporter = RuntimeProcessAttemptReporter(store, execution_id="execution-1")
    reporter.declare(
        RuntimeProcessSpec(process_id="process-1", process_type="example.process"),
        timestamp=3.0,
    )
    store.transition_execution(
        "execution-1", RuntimeExecutionStatus.RUNNING, timestamp=3.5
    )
    store.transition_execution(
        "execution-1", RuntimeExecutionStatus.SUCCEEDED, timestamp=4.0
    )

    with pytest.raises(RuntimeAttemptStateError, match="terminal execution"):
        reporter.start("process-1", timestamp=5.0)


def test_runtime_framework_reducer_prefers_durable_sequence_over_worker_clock():
    events = [
        RuntimeEvent(
            event_id=0,
            sequence=0,
            runtime_id="runtime-1",
            process_id="process-1",
            event_type=RuntimeEventType.PROCESS_STATUS_CHANGED,
            timestamp=20.0,
            status=RuntimeEventStatus.RUNNING,
            status_class=RuntimeStatusClass.ACTIVE,
        ),
        RuntimeEvent(
            event_id=1,
            sequence=1,
            runtime_id="runtime-1",
            process_id="process-1",
            event_type=RuntimeEventType.PROCESS_STATUS_CHANGED,
            timestamp=10.0,
            status=RuntimeEventStatus.SUCCEEDED,
            status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
        ),
    ]

    projection = RuntimeFrameworkReducer().reduce(events)

    assert projection.processes["process-1"].status == RuntimeEventStatus.SUCCEEDED


def test_runtime_framework_reducer_incrementally_rederives_parent_propagation():
    reducer = RuntimeFrameworkReducer()
    initial = [
        RuntimeEvent(
            event_id=0,
            sequence=0,
            runtime_id="runtime-1",
            process_id="parent",
            event_type=RuntimeEventType.PROCESS_STATUS_CHANGED,
            timestamp=1.0,
            status=RuntimeEventStatus.RUNNING,
            status_class=RuntimeStatusClass.ACTIVE,
        ),
        RuntimeEvent(
            event_id=1,
            sequence=1,
            runtime_id="runtime-1",
            process_id="child",
            parent_process_id="parent",
            event_type=RuntimeEventType.PROCESS_FAILED,
            timestamp=2.0,
            status=RuntimeEventStatus.FAILED,
            status_class=RuntimeStatusClass.TERMINAL_FAILURE,
        ),
    ]
    projection = reducer.reduce(initial)

    assert projection.processes["parent"].status == RuntimeEventStatus.FAILED
    assert projection.event_processes["parent"].status == RuntimeEventStatus.RUNNING

    continued = reducer.apply(
        projection,
        [
            RuntimeEvent(
                event_id=2,
                sequence=2,
                runtime_id="runtime-1",
                process_id="child",
                parent_process_id="parent",
                event_type=RuntimeEventType.PROCESS_COMPLETED,
                timestamp=3.0,
                status=RuntimeEventStatus.SUCCEEDED,
                status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
            )
        ],
    )

    assert continued.processes["child"].status == RuntimeEventStatus.SUCCEEDED
    assert continued.processes["parent"].status == RuntimeEventStatus.RUNNING
    assert continued.last_event_sequence == 2
    assert continued.event_count == 3


def test_runtime_framework_reducer_only_enforces_identity_for_incremental_state():
    reducer = RuntimeFrameworkReducer()
    first = RuntimeEvent(
        event_id=0,
        runtime_id="legacy-job-1",
        event_type=RuntimeEventType.LOG_EMITTED,
        timestamp=1.0,
    )
    second = RuntimeEvent(
        event_id=1,
        runtime_id="legacy-job-2",
        event_type=RuntimeEventType.LOG_EMITTED,
        timestamp=2.0,
    )

    legacy_projection = reducer.reduce([first, second])
    assert legacy_projection.runtime_id == "legacy-job-2"

    durable_projection = reducer.reduce([first])
    with pytest.raises(ValueError, match="runtime_id changed"):
        reducer.apply(durable_projection, [second])


def test_runtime_backend_executor_reports_failure_and_cancel(tmp_path):
    class FailingProcess(RuntimeProcessBase):
        def start(self, context: RuntimeProcessContext) -> None:
            del context
            raise ValueError("bad process")

    class CancelProcess(RuntimeProcessBase):
        def cancel(self, context: RuntimeProcessContext) -> None:
            assert context.is_cancel_requested()
            context.emit_log("cancel hook called")

    executor = RuntimeBackendExecutor(runtime_id="runtime1", runtime_dir=tmp_path / "runtime")
    executor.register(FailingProcess(RuntimeProcessSpec(process_id="fail", process_type="example.fail")))
    executor.register(RuntimeProcessBase(RuntimeProcessSpec(process_id="unsupported", process_type="example.base")))
    executor.register(
        CancelProcess(
            RuntimeProcessSpec(
                process_id="cancel",
                process_type="example.cancel",
                capabilities=RuntimeProcessCapabilities(can_cancel=True),
            )
        )
    )

    try:
        executor.run_process("fail")
    except ValueError:
        pass
    else:  # pragma: no cover
        raise AssertionError("failing process should raise")
    try:
        executor.cancel_process("unsupported")
    except RuntimeUnsupportedOperationError:
        pass
    else:  # pragma: no cover
        raise AssertionError("unsupported cancel should raise")
    executor.cancel_process("cancel")

    event_types = [event.event_type for event in executor.event_store.list()]
    projection = executor.projection()

    assert "process.failed" in event_types
    assert "process.cancel.unsupported" in event_types
    assert event_types[-2:] == ["log.emitted", "process.status.changed"]
    assert projection.processes["fail"].status_class == RuntimeStatusClass.TERMINAL_FAILURE
    assert projection.processes["unsupported"].status_class == RuntimeStatusClass.TERMINAL_FAILURE
    assert projection.processes["cancel"].status_class == RuntimeStatusClass.TERMINAL_CANCELLED


def test_runtime_backend_executor_dispatches_optional_hooks(tmp_path):
    class HookProcess(RuntimeProcessBase):
        def pause(self, context: RuntimeProcessContext) -> None:
            context.emit_log("pause hook")

        def resume(self, context: RuntimeProcessContext) -> None:
            context.emit_log("resume hook")

        def retry(self, context: RuntimeProcessContext) -> dict[str, Any]:
            context.emit_log("retry hook")
            return {"retry": True}

        def cleanup(self, context: RuntimeProcessContext) -> None:
            context.emit_log("cleanup hook")

    executor = RuntimeBackendExecutor(runtime_id="runtime1", runtime_dir=tmp_path / "runtime")
    executor.register(
        HookProcess(
            RuntimeProcessSpec(
                process_id="hooks",
                process_type="example.hooks",
                capabilities=RuntimeProcessCapabilities(
                    can_pause=True,
                    can_resume=True,
                    can_retry=True,
                    can_cleanup=True,
                ),
            )
        )
    )
    executor.register(RuntimeProcessBase(RuntimeProcessSpec(process_id="unsupported", process_type="example.base")))

    executor.pause_process("hooks")
    executor.resume_process("hooks")
    retry_result = executor.retry_process("hooks")
    executor.cleanup_process("hooks")
    try:
        executor.retry_process("unsupported")
    except RuntimeUnsupportedOperationError:
        pass
    else:  # pragma: no cover
        raise AssertionError("unsupported retry should raise")

    event_types = [event.event_type for event in executor.event_store.list()]
    statuses = [event.status for event in executor.event_store.list() if event.event_type == "process.status.changed"]

    assert retry_result == {"retry": True}
    assert event_types.count("log.emitted") == 4
    assert "process.retry.unsupported" in event_types
    assert statuses == [
        "paused",
        RuntimeEventStatus.RUNNING,
        RuntimeEventStatus.SUCCEEDED,
        RuntimeEventStatus.SUCCEEDED,
    ]


def test_runtime_backend_executor_samples_process_resources(tmp_path):
    class ResourceProcess(RuntimeProcessBase):
        def resources(self, context: RuntimeProcessContext) -> RuntimeResourceUsage:
            assert context.process_id == "resources"
            return RuntimeResourceUsage(cpu_percent=12.5, memory_bytes=4096, labels={"worker": "local"})

    executor = RuntimeBackendExecutor(runtime_id="runtime1", runtime_dir=tmp_path / "runtime")
    executor.register(ResourceProcess(RuntimeProcessSpec(process_id="resources", process_type="example.resources")))

    usage = executor.sample_process_resources("resources")
    projection = executor.projection()

    assert usage.memory_bytes == 4096
    assert executor.event_store.list()[-1].event_type == "resource.sampled"
    assert projection.resource_usage_summary["resources"].labels == {"worker": "local"}
    assert projection.processes["resources"].resource_usage == usage


def test_runtime_backend_executor_writes_projection_after_hook_failure(tmp_path):
    class FailingCancelProcess(RuntimeProcessBase):
        def cancel(self, context: RuntimeProcessContext) -> None:
            context.emit_log("before failure")
            raise ValueError("cancel failed")

    runtime_dir = tmp_path / "runtime"
    executor = RuntimeBackendExecutor(runtime_id="runtime1", runtime_dir=runtime_dir)
    executor.register(
        FailingCancelProcess(
            RuntimeProcessSpec(
                process_id="failing-cancel",
                process_type="example.failing-cancel",
                capabilities=RuntimeProcessCapabilities(can_cancel=True),
            )
        )
    )

    try:
        executor.cancel_process("failing-cancel")
    except ValueError as exc:
        assert str(exc) == "cancel failed"
    else:  # pragma: no cover
        raise AssertionError("failing cancel should raise")

    projection = RuntimeStore(runtime_dir).load_projection()

    assert projection is not None
    assert [event.event_type for event in executor.event_store.list()] == ["process.created", "log.emitted"]


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
    assert [item.event_id for item in restored.list(since="0")] == [1, 2]
    assert [item.event_type for item in restored.wait_for_next(since=1, timeout=0.01)] == ["process.completed"]


def test_runtime_event_store_supports_string_cursor_and_standard_streaming():
    store = RuntimeEventJsonlStore()
    started = store.append(
        RuntimeEvent(
            event_id="evt-1",
            runtime_id="runtime1",
            process_id="process1",
            event_type="process.status.changed",
            timestamp=1.0,
            status=RuntimeEventStatus.RUNNING,
            status_class=RuntimeStatusClass.ACTIVE,
        )
    )
    completed = store.append(
        RuntimeEvent(
            event_id="evt-2",
            runtime_id="runtime1",
            process_id="process1",
            event_type="process.status.changed",
            timestamp=2.0,
            status=RuntimeEventStatus.SUCCEEDED,
            status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
        )
    )
    trailing = store.append(
        RuntimeEvent(
            event_id="evt-3",
            runtime_id="runtime1",
            process_id="process1",
            event_type="log.emitted",
            timestamp=3.0,
            message="completed",
        )
    )

    streamed = list(
        stream_runtime_event_store(
            store,
            is_terminal=lambda event: event.event_id == completed.event_id,
        )
    )

    assert [event.event_id for event in store.list(since="evt-1")] == ["evt-2", "evt-3"]
    assert wait_runtime_event_store(store, since="evt-2", timeout=0.01) == [trailing]
    assert streamed == [started, completed, trailing]
    encoded = sse_encode_runtime_event(completed)
    assert "id: evt-2" in encoded
    assert "event: process.status.changed" in encoded
    assert json.loads(encoded.split("data: ", maxsplit=1)[1])['status'] == "succeeded"


def test_runtime_event_jsonl_stream_replays_trailing_events_and_parses_sse(tmp_path):
    path = tmp_path / "events.runtime.jsonl"
    store = RuntimeEventJsonlStore(path)
    started = store.append(
        RuntimeEvent(
            event_id="evt-1",
            runtime_id="runtime1",
            process_id="job1",
            event_type="process.status.changed",
            timestamp=1.0,
            status=RuntimeEventStatus.RUNNING,
            status_class=RuntimeStatusClass.ACTIVE,
        )
    )
    completed = store.append(
        RuntimeEvent(
            event_id="evt-2",
            runtime_id="runtime1",
            process_id="job1",
            event_type="process.status.changed",
            timestamp=2.0,
            status=RuntimeEventStatus.SUCCEEDED,
            status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
        )
    )
    trailing = store.append(
        RuntimeEvent(
            event_id="evt-3",
            runtime_id="runtime1",
            process_id="job1",
            event_type="log.emitted",
            timestamp=3.0,
            message="finished",
        )
    )

    streamed = list(
        stream_runtime_event_jsonl(
            path,
            is_terminal=lambda event: event.event_id == completed.event_id,
        )
    )
    frames = "".join(sse_encode_runtime_event(event) for event in streamed)

    assert streamed == [started, completed, trailing]
    assert list(parse_sse_runtime_events(iter(frames.splitlines()))) == streamed


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


def test_runtime_event_sidecar_writer_appends_process_spec_declaration(tmp_path):
    writer = RuntimeEventSidecarWriter(tmp_path / "runtime", runtime_id="runtime1")
    event = writer.append_process_spec(
        RuntimeProcessSpec(
            process_id="process1",
            process_type="example.process",
            display_name="Example process",
        ),
        event_id=0,
    )
    projection = writer.refresh_projection()

    assert event.event_type == "process.created"
    assert event.status == RuntimeEventStatus.PENDING
    assert event.payload["process_type"] == "example.process"
    assert projection.processes["process1"].status_class == RuntimeStatusClass.NOT_STARTED


def test_runtime_backend_executor_starts_empty_event_store_at_zero(tmp_path):
    class ExampleProcess(RuntimeProcessBase):
        pass

    executor = RuntimeBackendExecutor(runtime_id="runtime1", runtime_dir=tmp_path / "runtime")

    executor.register(ExampleProcess(RuntimeProcessSpec(process_id="process1", process_type="example")))

    [event] = executor.event_store.list()
    assert event.event_id == 0
    assert event.event_type == "process.created"


def test_runtime_backend_executor_continues_existing_event_cursor(tmp_path):
    class ExampleProcess(RuntimeProcessBase):
        pass

    runtime_dir = tmp_path / "runtime"
    first = RuntimeBackendExecutor(runtime_id="runtime1", runtime_dir=runtime_dir)
    first.register(ExampleProcess(RuntimeProcessSpec(process_id="process1", process_type="example")))

    resumed = RuntimeBackendExecutor(runtime_id="runtime1", runtime_dir=runtime_dir)
    resumed.register(ExampleProcess(RuntimeProcessSpec(process_id="process2", process_type="example")))

    assert [event.event_id for event in resumed.event_store.list()] == [0, 1]


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
    assert {artifact["path"] for artifact in artifacts} == {
        "runtime/events.runtime.jsonl",
        "runtime/projection.json",
    }


def test_runtime_event_sidecar_writer_persists_projection_for_stateful_events(tmp_path):
    runtime_dir = tmp_path / "runtime"
    writer = RuntimeEventSidecarWriter(runtime_dir, runtime_id="runtime1", process_id="process1")

    writer.append_event(
        RuntimeEvent(
            event_id=1,
            runtime_id="runtime1",
            process_id="process1",
            event_type="process.status.changed",
            timestamp=1.0,
            status=RuntimeEventStatus.RUNNING,
            status_class=RuntimeStatusClass.ACTIVE,
        )
    )
    writer.append_event(
        RuntimeEvent(
            event_id=2,
            runtime_id="runtime1",
            process_id="process1",
            event_type="log.emitted",
            timestamp=2.0,
            message="still running",
        )
    )

    projection = RuntimeStore(runtime_dir).load_projection()
    event_store = writer.store()
    event_store.load()

    assert projection is not None
    assert projection.event_count == 1
    assert projection.processes["process1"].status == RuntimeEventStatus.RUNNING
    assert event_store.list()[1].event_type == "log.emitted"


def test_runtime_event_sidecar_writer_uses_durable_store_as_canonical_source(tmp_path):
    runtime_dir = tmp_path / "runtime"
    durable = RuntimeDurableStore.create(
        runtime_dir / "runtime" / "runtime.db",
        RuntimeIdentity(runtime_id="runtime1", created_at=1.0),
    )
    durable.begin_execution(
        execution_id="execution1",
        kind=RuntimeExecutionKind.INITIAL,
        created_at=2.0,
    )
    writer = RuntimeEventSidecarWriter(
        runtime_dir,
        runtime_id="runtime1",
        process_id="process1",
        durable_store=durable,
        execution_id="execution1",
    )
    stored = writer.append_legacy_event(
        {
            "event_id": 0,
            "job_id": "process1",
            "timestamp": 3.0,
            "event_type": "job_state",
            "payload": {"status": "running"},
        }
    )

    compatibility = writer.store()
    compatibility.load()
    assert stored.event_id == 1
    assert stored.execution_id == "execution1"
    assert durable.list()[-1] == stored
    assert compatibility.list() == [stored]
    assert durable.load_projection() is not None

    durable.transition_execution(
        "execution1", RuntimeExecutionStatus.RUNNING, timestamp=4.0
    )
    assert [event.event_type for event in writer.export_durable_events()] == [
        RuntimeEventType.EXECUTION_CREATED,
        RuntimeEventType.EXECUTION_STARTED
    ]
    compatibility.load()
    assert [event.event_id for event in compatibility.list()] == [0, 1, 2]


def test_runtime_event_sidecar_export_fills_sparse_earlier_sequences(tmp_path):
    durable = RuntimeDurableStore.create(
        tmp_path / "runtime" / "runtime.db",
        RuntimeIdentity(runtime_id="runtime-1", created_at=1.0),
    )
    writer = RuntimeEventSidecarWriter(
        tmp_path,
        runtime_id="runtime-1",
        durable_store=durable,
    )
    first = durable.append(
        RuntimeEvent(
            event_id=-1,
            runtime_id="runtime-1",
            event_type=RuntimeEventType.LOG_EMITTED,
            timestamp=2.0,
            message="durable only",
        )
    )
    second = durable.append(
        RuntimeEvent(
            event_id=-1,
            runtime_id="runtime-1",
            event_type=RuntimeEventType.LOG_EMITTED,
            timestamp=3.0,
            message="already exported",
        )
    )
    RuntimeStore(tmp_path).append_runtime_event(second)

    exported = writer.export_durable_events()

    assert exported == [first]
    compatibility = RuntimeStore(tmp_path).runtime_event_store()
    compatibility.load()
    assert {event.sequence for event in compatibility.list()} == {
        first.sequence,
        second.sequence,
    }
    assert [event.sequence for event in compatibility.list()] == [0, 1]


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


def test_event_buffer_can_continue_persisted_cursor_without_loading_history(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(
        json.dumps(ExampleEvent(7, "old-job", 1.0, "job_state", {"status": "completed"}).__dict__)
        + "\n",
        encoding="utf-8",
    )
    buffer = EventBuffer(
        "new-job",
        events_path,
        event_factory=_event_factory,
        event_loader=_event_loader,
        event_id_getter=lambda event: event.event_id,
        event_json_dumper=lambda event: json.dumps(event.__dict__, ensure_ascii=False),
        initial_event_id=9,
    )

    event = buffer.emit("job_state", {"status": "queued"})

    assert event.event_id == 9
    assert [item.event_id for item in buffer.list()] == [9]


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
    store.write_process_specs([RuntimeProcessSpec(process_id="process1", process_type="example.process")])

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
    assert [spec.resolved_process_id() for spec in view.process_specs] == ["process1"]

    store.write_monitor_snapshot({"source": "monitor", "total": 2})
    view = loader.load(store.runtime_dir)

    assert view is not None
    assert view.monitor == {"source": "monitor", "total": 2}


def test_runtime_snapshot_loader_optionally_prefers_standard_projection(tmp_path):
    store = RuntimeStore(tmp_path / "runtime")
    store.write_monitor_snapshot({"source": "legacy-monitor", "total": 99})
    store.write_projection(
        RuntimeProjection(
            runtime_id="runtime1",
            status=RuntimeEventStatus.SUCCEEDED,
            status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
            terminal_success_count=2,
        ).model_dump(mode="json")
    )
    loader = RuntimeSnapshotLoader(
        status_loader=json.loads,
        snapshot_loader=json.loads,
        monitor_loader=json.loads,
        monitor_from_snapshot=lambda snapshot: snapshot,
        monitor_from_status=lambda status: status,
        monitor_from_projection=lambda projection: {
            "source": "projection",
            "status": projection.status,
            "completed": projection.terminal_success_count,
        },
    )

    view = loader.load(store.runtime_dir)

    assert view is not None
    assert view.monitor == {
        "source": "projection",
        "status": RuntimeEventStatus.SUCCEEDED,
        "completed": 2,
    }
    assert view.projection is not None
    assert view.projection.runtime_id == "runtime1"


def test_runtime_snapshot_loader_keeps_legacy_precedence_without_projection_mapper(tmp_path):
    store = RuntimeStore(tmp_path / "runtime")
    store.write_monitor_snapshot({"source": "legacy-monitor"})
    store.write_projection(RuntimeProjection(runtime_id="runtime1").model_dump(mode="json"))
    loader = RuntimeSnapshotLoader(
        status_loader=json.loads,
        snapshot_loader=json.loads,
        monitor_loader=json.loads,
        monitor_from_snapshot=lambda snapshot: snapshot,
        monitor_from_status=lambda status: status,
    )

    view = loader.load(store.runtime_dir)

    assert view is not None
    assert view.monitor == {"source": "legacy-monitor"}
    assert view.projection is not None
