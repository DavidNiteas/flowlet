"""SQLite-backed durable runtime event store and execution ledger."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from .command import (
    RuntimeCommandConflictError,
    RuntimeCommandRecord,
    RuntimeCommandReducer,
    RuntimeCommandReservation,
    RuntimeCommandStatus,
)
from .event_store import RuntimeEventCursor
from .identity import (
    RuntimeExecutionKind,
    RuntimeExecutionRecord,
    RuntimeExecutionStatus,
    RuntimeIdentity,
)
from .ledger import RuntimeExecutionLedger, RuntimeExecutionLedgerReducer
from .process import RuntimeProcessOperation, RuntimeProcessSpec
from .projection import RuntimeFrameworkReducer, RuntimeProjection
from .recovery import RuntimeProcessAttempt
from .schema import RuntimeErrorInfo, RuntimeEvent, RuntimeEventStatus, RuntimeEventType, RuntimeStatusClass
from .session import (
    RuntimeBackendSession,
    RuntimeBackendSessionStatus,
    RuntimeLeaseConflictError,
    RuntimeLeaseLostError,
    RuntimeSessionReconciliation,
)


class RuntimeIdentityMismatchError(ValueError):
    """Raised when a writer attempts to append into another runtime lineage."""


class RuntimeDurableStore:
    """Transactional local event journal with a rebuildable execution ledger.

    SQLite serializes appenders with ``BEGIN IMMEDIATE``. The store allocates
    canonical numeric event ids; caller-provided ids are only provisional.
    """

    def __init__(self, database_path: str | Path, *, expected_runtime_id: str | None = None) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._changed = threading.Condition()
        self._initialize_schema()
        identity = self.identity()
        if identity is None:
            raise ValueError(f"Durable runtime store is not initialized: {self.database_path}")
        if expected_runtime_id is not None and identity.runtime_id != expected_runtime_id:
            raise RuntimeIdentityMismatchError(
                f"Runtime store identity {identity.runtime_id!r} does not match {expected_runtime_id!r}"
            )

    @classmethod
    def create(cls, database_path: str | Path, identity: RuntimeIdentity) -> RuntimeDurableStore:
        """Create a new runtime lineage and reject an already initialized store."""
        path = Path(database_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        store = cls.__new__(cls)
        store.database_path = path
        store._changed = threading.Condition()
        store._initialize_schema()
        with store._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute("SELECT value FROM runtime_meta WHERE key = 'identity'").fetchone()
            if existing is not None:
                connection.rollback()
                raise FileExistsError(f"Durable runtime store is already initialized: {path}")
            connection.execute(
                "INSERT INTO runtime_meta(key, value) VALUES ('identity', ?)",
                (identity.model_dump_json(),),
            )
            connection.commit()
        return cls(path, expected_runtime_id=identity.runtime_id)

    def identity(self) -> RuntimeIdentity | None:
        """Load the immutable runtime lineage identity."""
        with self._connect() as connection:
            row = connection.execute("SELECT value FROM runtime_meta WHERE key = 'identity'").fetchone()
        return RuntimeIdentity.model_validate_json(row[0]) if row is not None else None

    def acquire_backend_session(
        self,
        *,
        session_id: str,
        owner_id: str,
        now: float,
        ttl: float,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeBackendSession:
        """Acquire exclusive ownership, expiring a stale owner atomically."""
        if ttl <= 0:
            raise ValueError("backend session ttl must be > 0")
        identity = self._require_identity()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            active = self._load_active_session_tx(connection)
            if active is not None and active.expires_at > now:
                connection.rollback()
                raise RuntimeLeaseConflictError(
                    f"Runtime is owned by live backend session {active.session_id!r}"
                )
            if active is not None:
                expired = active.model_copy(
                    update={"status": RuntimeBackendSessionStatus.EXPIRED, "released_at": now}
                )
                self._write_session_tx(connection, expired)
                self._append_session_event_tx(
                    connection, expired, RuntimeEventType.BACKEND_SESSION_EXPIRED, now
                )
            if self._load_session_tx(connection, session_id) is not None:
                connection.rollback()
                raise ValueError(f"Duplicate backend session_id: {session_id!r}")
            session = RuntimeBackendSession(
                session_id=session_id,
                runtime_id=identity.runtime_id,
                owner_id=owner_id,
                acquired_at=now,
                heartbeat_at=now,
                expires_at=now + ttl,
                metadata=metadata or {},
            )
            self._write_session_tx(connection, session)
            self._append_session_event_tx(
                connection, session, RuntimeEventType.BACKEND_SESSION_ACQUIRED, now
            )
            connection.commit()
        self._notify_changed()
        return session

    def renew_backend_session(
        self, session_id: str, *, owner_id: str, now: float, ttl: float
    ) -> RuntimeBackendSession:
        """Renew the current owner lease, rejecting expired or replaced sessions."""
        if ttl <= 0:
            raise ValueError("backend session ttl must be > 0")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = self._require_live_session_tx(connection, session_id, owner_id, now)
            session = session.model_copy(update={"heartbeat_at": now, "expires_at": now + ttl})
            self._write_session_tx(connection, session)
            self._append_session_event_tx(
                connection, session, RuntimeEventType.BACKEND_SESSION_RENEWED, now
            )
            connection.commit()
        self._notify_changed()
        return session

    def release_backend_session(
        self, session_id: str, *, owner_id: str, now: float
    ) -> RuntimeBackendSession:
        """Release the current lease and retain its durable session history."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = self._require_live_session_tx(connection, session_id, owner_id, now)
            session = session.model_copy(
                update={"status": RuntimeBackendSessionStatus.RELEASED, "released_at": now}
            )
            self._write_session_tx(connection, session)
            self._append_session_event_tx(
                connection, session, RuntimeEventType.BACKEND_SESSION_RELEASED, now
            )
            connection.commit()
        self._notify_changed()
        return session

    def backend_sessions(self) -> list[RuntimeBackendSession]:
        """Return backend attachment history in acquisition order."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT session_json FROM runtime_backend_sessions ORDER BY acquired_at, session_id"
            ).fetchall()
        return [RuntimeBackendSession.model_validate_json(row[0]) for row in rows]

    def active_backend_session(self, *, now: float) -> RuntimeBackendSession | None:
        """Return the current live owner without mutating an expired lease."""
        with self._connect() as connection:
            session = self._load_active_session_tx(connection)
        return session if session is not None and session.expires_at > now else None

    def reconcile_interrupted_work(
        self, session_id: str, *, owner_id: str, now: float
    ) -> RuntimeSessionReconciliation:
        """Append interruption facts for nonterminal work abandoned by old sessions."""
        error = RuntimeErrorInfo(
            type="BackendSessionLost",
            message="The backend session that owned this work is no longer live",
            retryable=True,
            context={"reconciled_by_session_id": session_id},
        )
        event_sequences: list[int] = []
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._require_live_session_tx(connection, session_id, owner_id, now)
            execution_rows = connection.execute(
                "SELECT record_json FROM runtime_executions ORDER BY ordinal"
            ).fetchall()
            executions = [
                RuntimeExecutionRecord.model_validate_json(row[0]) for row in execution_rows
            ]
            stale_executions = [
                execution
                for execution in executions
                if execution.status
                in {RuntimeExecutionStatus.PENDING, RuntimeExecutionStatus.RUNNING}
                and execution.backend_session_id != session_id
            ]
            stale_execution_ids = {execution.execution_id for execution in stale_executions}
            attempt_rows = connection.execute(
                "SELECT attempt_json FROM runtime_attempts ORDER BY process_id, ordinal"
            ).fetchall()
            attempts = [RuntimeProcessAttempt.model_validate_json(row[0]) for row in attempt_rows]
            stale_attempts = [
                attempt
                for attempt in attempts
                if attempt.status_class
                in {RuntimeStatusClass.NOT_STARTED, RuntimeStatusClass.ACTIVE}
                and (
                    attempt.execution_id in stale_execution_ids
                    or (
                        attempt.backend_session_id is not None
                        and attempt.backend_session_id != session_id
                    )
                )
            ]
            for attempt in stale_attempts:
                event = self._append_event_tx(
                    connection,
                    RuntimeEvent(
                        event_id=-1,
                        runtime_id=attempt.runtime_id,
                        execution_id=attempt.execution_id,
                        process_id=attempt.process_id,
                        attempt_id=attempt.attempt_id,
                        event_type=RuntimeEventType.PROCESS_ATTEMPT_INTERRUPTED,
                        timestamp=now,
                        status=RuntimeEventStatus.INTERRUPTED,
                        status_class=RuntimeStatusClass.TERMINAL_FAILURE,
                        error=error,
                        payload={
                            "ordinal": attempt.ordinal,
                            "backend_session_id": attempt.backend_session_id,
                            "reconciled_by_session_id": session_id,
                        },
                    ),
                )
                event_sequences.append(int(event.event_id))
            for execution in stale_executions:
                event = self._append_event_tx(
                    connection,
                    RuntimeEvent(
                        event_id=-1,
                        runtime_id=execution.runtime_id,
                        execution_id=execution.execution_id,
                        event_type=RuntimeEventType.EXECUTION_INTERRUPTED,
                        timestamp=now,
                        status=RuntimeExecutionStatus.INTERRUPTED,
                        status_class=RuntimeStatusClass.TERMINAL_FAILURE,
                        error=error,
                        payload={"reconciled_by_session_id": session_id},
                    ),
                )
                self._write_execution_tx(
                    connection,
                    execution.model_copy(
                        update={
                            "status": RuntimeExecutionStatus.INTERRUPTED,
                            "finished_at": now,
                            "last_event_sequence": int(event.event_id),
                            "error": error,
                        }
                    ),
                )
                event_sequences.append(int(event.event_id))
            connection.commit()
        if event_sequences:
            self._notify_changed()
        return RuntimeSessionReconciliation(
            session_id=session_id,
            interrupted_execution_ids=[item.execution_id for item in stale_executions],
            interrupted_attempt_ids=[item.attempt_id for item in stale_attempts],
            first_event_sequence=event_sequences[0] if event_sequences else None,
            last_event_sequence=event_sequences[-1] if event_sequences else None,
        )

    def append(self, event: RuntimeEvent) -> RuntimeEvent:
        """Atomically allocate a sequence, append an event, and update ledger indexes."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            stored = self._append_event_tx(connection, event)
            connection.commit()
        with self._changed:
            self._changed.notify_all()
        return stored

    def reserve_command(
        self,
        *,
        command_id: str,
        command_type: str,
        timestamp: float,
        target_id: str | None = None,
        execution_id: str | None = None,
        backend_session_id: str | None = None,
        payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeCommandReservation:
        """Accept one command id once and return an existing identical receipt."""
        identity = self._require_identity()
        request_payload = payload or {}
        fingerprint = _command_fingerprint(
            command_type=command_type,
            target_id=target_id,
            execution_id=execution_id,
            payload=request_payload,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = self._load_command_tx(connection, command_id)
            if existing is not None:
                connection.rollback()
                if existing.request_fingerprint != fingerprint:
                    raise RuntimeCommandConflictError(
                        f"command_id {command_id!r} was already used for a different request"
                    )
                return RuntimeCommandReservation(record=existing, created=False)
            event = self._append_event_tx(
                connection,
                RuntimeEvent(
                    event_id=-1,
                    runtime_id=identity.runtime_id,
                    execution_id=execution_id,
                    event_type=RuntimeEventType.COMMAND_ACCEPTED,
                    timestamp=timestamp,
                    subject_type="command",
                    subject_id=command_id,
                    status=RuntimeCommandStatus.ACCEPTED,
                    status_class=RuntimeStatusClass.NOT_STARTED,
                    payload={
                        "command_type": command_type,
                        "target_id": target_id,
                        "backend_session_id": backend_session_id,
                        "request_fingerprint": fingerprint,
                        "request": request_payload,
                    },
                    metadata=metadata or {},
                ),
            )
            sequence = int(event.event_id)
            record = RuntimeCommandRecord(
                command_id=command_id,
                runtime_id=identity.runtime_id,
                command_type=command_type,
                request_fingerprint=fingerprint,
                target_id=target_id,
                execution_id=execution_id,
                backend_session_id=backend_session_id,
                accepted_at=timestamp,
                first_event_sequence=sequence,
                last_event_sequence=sequence,
                payload=request_payload,
                metadata=metadata or {},
            )
            self._write_command_tx(connection, record)
            connection.commit()
        self._notify_changed()
        return RuntimeCommandReservation(record=record, created=True)

    def finish_command(
        self,
        command_id: str,
        *,
        status: RuntimeCommandStatus,
        timestamp: float,
        result: dict[str, Any] | None = None,
        error: RuntimeErrorInfo | None = None,
    ) -> RuntimeCommandRecord:
        """Append one terminal result, returning an identical prior result idempotently."""
        if status == RuntimeCommandStatus.ACCEPTED:
            raise ValueError("finish_command requires a terminal command status")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            record = self._load_command_tx(connection, command_id)
            if record is None:
                connection.rollback()
                raise KeyError(f"Unknown command_id: {command_id!r}")
            if record.status != RuntimeCommandStatus.ACCEPTED:
                connection.rollback()
                if record.status == status and record.result == result and record.error == error:
                    return record
                raise RuntimeCommandConflictError(
                    f"command_id {command_id!r} already has a different terminal result"
                )
            event_type = (
                RuntimeEventType.COMMAND_SUCCEEDED
                if status == RuntimeCommandStatus.SUCCEEDED
                else RuntimeEventType.COMMAND_FAILED
            )
            event = self._append_event_tx(
                connection,
                RuntimeEvent(
                    event_id=-1,
                    runtime_id=record.runtime_id,
                    execution_id=record.execution_id,
                    event_type=event_type,
                    timestamp=timestamp,
                    subject_type="command",
                    subject_id=record.command_id,
                    status=status,
                    status_class=(
                        RuntimeStatusClass.TERMINAL_SUCCESS
                        if status == RuntimeCommandStatus.SUCCEEDED
                        else RuntimeStatusClass.TERMINAL_FAILURE
                    ),
                    error=error,
                    payload={"result": result},
                ),
            )
            record = record.model_copy(
                update={
                    "status": status,
                    "finished_at": timestamp,
                    "last_event_sequence": int(event.event_id),
                    "result": result,
                    "error": error,
                }
            )
            self._write_command_tx(connection, record)
            connection.commit()
        self._notify_changed()
        return record

    def commands(self) -> list[RuntimeCommandRecord]:
        """Return durable command receipts in acceptance order."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT command_json FROM runtime_commands ORDER BY first_event_sequence"
            ).fetchall()
        return [RuntimeCommandRecord.model_validate_json(row[0]) for row in rows]

    def rebuild_commands(self, *, replace: bool = False) -> list[RuntimeCommandRecord]:
        """Rebuild command receipts from events and optionally replace their index."""
        rebuilt = RuntimeCommandReducer().reduce(self._require_identity(), self.list())
        if not replace:
            return rebuilt
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM runtime_commands")
            for record in rebuilt:
                self._write_command_tx(connection, record)
            connection.commit()
        return rebuilt

    def list(self, *, since: RuntimeEventCursor | None = None) -> list[RuntimeEvent]:
        """List canonical events after an optional sequence cursor."""
        numeric_since = _numeric_cursor(since)
        if since is not None and numeric_since is None:
            return []
        query = "SELECT event_json FROM runtime_events"
        parameters: tuple[Any, ...] = ()
        if numeric_since is not None:
            query += " WHERE sequence > ?"
            parameters = (numeric_since,)
        query += " ORDER BY sequence"
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [RuntimeEvent.model_validate_json(row[0]) for row in rows]

    def wait_for_next(
        self,
        *,
        since: RuntimeEventCursor | None = None,
        timeout: float | None = None,
    ) -> list[RuntimeEvent]:
        """Poll durable state while using an in-process condition for low latency."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            events = self.list(since=since)
            if events:
                return events
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return []
                wait_for = min(0.1, remaining)
            else:
                wait_for = 0.1
            with self._changed:
                self._changed.wait(wait_for)

    def load(self) -> None:
        """Satisfy RuntimeEventStore; SQLite reads are already durable and lazy."""

    def begin_execution(
        self,
        *,
        execution_id: str,
        kind: RuntimeExecutionKind,
        created_at: float,
        backend_session_id: str | None = None,
        recovery_plan_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeExecutionRecord:
        """Start an initial or continuation wave without changing runtime identity."""
        identity = self._require_identity()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute(
                "SELECT 1 FROM runtime_executions WHERE execution_id = ?", (execution_id,)
            ).fetchone():
                connection.rollback()
                raise ValueError(f"Duplicate execution_id: {execution_id!r}")
            ordinal = int(connection.execute("SELECT COUNT(*) FROM runtime_executions").fetchone()[0]) + 1
            if ordinal == 1 and kind != RuntimeExecutionKind.INITIAL:
                connection.rollback()
                raise ValueError("The first runtime execution must have kind='initial'")
            if ordinal > 1 and kind != RuntimeExecutionKind.CONTINUE:
                connection.rollback()
                raise ValueError("Existing runtime lineages can only begin continuation executions")
            record = RuntimeExecutionRecord(
                execution_id=execution_id,
                runtime_id=identity.runtime_id,
                kind=kind,
                ordinal=ordinal,
                backend_session_id=backend_session_id,
                recovery_plan_id=recovery_plan_id,
                created_at=created_at,
                metadata=metadata or {},
            )
            event = self._append_event_tx(
                connection,
                RuntimeEvent(
                    event_id=-1,
                    runtime_id=identity.runtime_id,
                    execution_id=execution_id,
                    event_type=RuntimeEventType.EXECUTION_CREATED,
                    timestamp=created_at,
                    status=RuntimeEventStatus.PENDING,
                    status_class=RuntimeStatusClass.NOT_STARTED,
                    payload={
                        "kind": kind,
                        "ordinal": ordinal,
                        "backend_session_id": backend_session_id,
                        "recovery_plan_id": recovery_plan_id,
                    },
                    metadata=metadata or {},
                ),
            )
            record.first_event_sequence = int(event.event_id)
            record.last_event_sequence = int(event.event_id)
            self._write_execution_tx(connection, record)
            connection.commit()
        with self._changed:
            self._changed.notify_all()
        return record

    def transition_execution(
        self,
        execution_id: str,
        status: RuntimeExecutionStatus,
        *,
        timestamp: float,
        error: RuntimeErrorInfo | None = None,
    ) -> RuntimeExecutionRecord:
        """Append one execution transition and update its ledger record atomically."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            record = self._load_execution_tx(connection, execution_id)
            if record is None:
                connection.rollback()
                raise KeyError(f"Unknown execution_id: {execution_id!r}")
            _validate_execution_transition(record.status, status)
            event_type, status_class = _execution_event(status)
            event = self._append_event_tx(
                connection,
                RuntimeEvent(
                    event_id=-1,
                    runtime_id=record.runtime_id,
                    execution_id=execution_id,
                    event_type=event_type,
                    timestamp=timestamp,
                    status=status.value,
                    status_class=status_class,
                    error=error,
                ),
            )
            updates: dict[str, Any] = {
                "status": status,
                "last_event_sequence": int(event.event_id),
                "error": error,
            }
            if status == RuntimeExecutionStatus.RUNNING:
                updates["started_at"] = timestamp
            elif status in {
                RuntimeExecutionStatus.SUCCEEDED,
                RuntimeExecutionStatus.FAILED,
                RuntimeExecutionStatus.CANCELLED,
                RuntimeExecutionStatus.INTERRUPTED,
            }:
                updates["finished_at"] = timestamp
            record = record.model_copy(update=updates)
            self._write_execution_tx(connection, record)
            connection.commit()
        with self._changed:
            self._changed.notify_all()
        return record

    def write_process_specs(self, specs: list[RuntimeProcessSpec]) -> None:
        """Upsert stable logical process declarations without rewriting events."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for spec in specs:
                connection.execute(
                    """
                    INSERT INTO runtime_process_specs(process_id, spec_json)
                    VALUES (?, ?)
                    ON CONFLICT(process_id) DO UPDATE SET spec_json = excluded.spec_json
                    """,
                    (spec.resolved_process_id(), spec.model_dump_json()),
                )
            connection.commit()

    def load_process_specs(self) -> list[RuntimeProcessSpec]:
        """Load process declarations in stable process-id order."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT spec_json FROM runtime_process_specs ORDER BY process_id"
            ).fetchall()
        return [RuntimeProcessSpec.model_validate_json(row[0]) for row in rows]

    def write_projection(self, projection: RuntimeProjection, *, through_sequence: int) -> None:
        """Persist an incrementally replaceable projection cursor."""
        if projection.runtime_id != self._require_identity().runtime_id:
            raise RuntimeIdentityMismatchError("Projection runtime_id does not match durable store identity")
        last_sequence = self.last_event_sequence()
        if through_sequence > last_sequence:
            raise ValueError("Projection cursor cannot exceed the durable event sequence")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO runtime_projection(singleton, through_sequence, projection_json)
                VALUES (1, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    through_sequence = excluded.through_sequence,
                    projection_json = excluded.projection_json
                """,
                (through_sequence, projection.model_dump_json()),
            )
            connection.commit()

    def load_projection(self) -> tuple[RuntimeProjection, int] | None:
        """Load the materialized projection and its applied event cursor."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT projection_json, through_sequence FROM runtime_projection WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return None
        return RuntimeProjection.model_validate_json(row[0]), int(row[1])

    def events_after_projection(self) -> list[RuntimeEvent]:
        """Return only events not represented by the persisted projection."""
        persisted = self.load_projection()
        return self.list(since=persisted[1] if persisted is not None else None)

    def refresh_projection(
        self,
        reducer: RuntimeFrameworkReducer | None = None,
    ) -> RuntimeProjection:
        """Incrementally apply durable events, rebuilding old projection formats when needed."""
        reducer = reducer or RuntimeFrameworkReducer()
        persisted = self.load_projection()
        if persisted is None:
            projection = reducer.reduce(self.list())
        else:
            projection, through_sequence = persisted
            pending = self.list(since=through_sequence)
            if not pending:
                return projection
            try:
                projection = reducer.apply(projection, pending)
            except ValueError as exc:
                if "predates incremental process state" not in str(exc):
                    raise
                projection = reducer.reduce(self.list())
        self.write_projection(projection, through_sequence=self.last_event_sequence())
        return projection

    def ledger(self) -> RuntimeExecutionLedger:
        """Load the current execution and process-attempt materialization."""
        identity = self._require_identity()
        with self._connect() as connection:
            execution_rows = connection.execute(
                "SELECT record_json FROM runtime_executions ORDER BY ordinal"
            ).fetchall()
            attempt_rows = connection.execute(
                "SELECT attempt_json FROM runtime_attempts ORDER BY process_id, ordinal"
            ).fetchall()
            projection_row = connection.execute(
                "SELECT through_sequence FROM runtime_projection WHERE singleton = 1"
            ).fetchone()
        executions = [RuntimeExecutionRecord.model_validate_json(row[0]) for row in execution_rows]
        attempts = [RuntimeProcessAttempt.model_validate_json(row[0]) for row in attempt_rows]
        process_attempt_ids: dict[str, list[str]] = {}
        for attempt in attempts:
            process_attempt_ids.setdefault(attempt.process_id, []).append(attempt.attempt_id)
        return RuntimeExecutionLedger(
            identity=identity,
            executions={record.execution_id: record for record in executions},
            execution_order=[record.execution_id for record in executions],
            attempts={attempt.attempt_id: attempt for attempt in attempts},
            process_attempt_ids=process_attempt_ids,
            last_event_sequence=self.last_event_sequence(),
            projection_sequence=int(projection_row[0]) if projection_row is not None else -1,
        )

    def rebuild_ledger(self, *, replace: bool = False) -> RuntimeExecutionLedger:
        """Rebuild ledger indexes from events and optionally replace materialized rows."""
        current_projection = self.load_projection()
        projection_sequence = current_projection[1] if current_projection is not None else -1
        rebuilt = RuntimeExecutionLedgerReducer().reduce(
            self._require_identity(),
            self.list(),
            projection_sequence=projection_sequence,
        )
        if not replace:
            return rebuilt
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM runtime_executions")
            connection.execute("DELETE FROM runtime_attempts")
            for execution_id in rebuilt.execution_order:
                self._write_execution_tx(connection, rebuilt.executions[execution_id])
            for attempt in rebuilt.attempts.values():
                self._write_attempt_tx(connection, attempt)
            connection.commit()
        return rebuilt

    def last_event_sequence(self) -> int:
        """Return the latest allocated sequence, or -1 for an empty runtime."""
        with self._connect() as connection:
            return int(connection.execute("SELECT COALESCE(MAX(sequence), -1) FROM runtime_events").fetchone()[0])

    def _require_identity(self) -> RuntimeIdentity:
        identity = self.identity()
        if identity is None:  # pragma: no cover - constructor prevents this
            raise ValueError("Durable runtime store has no identity")
        return identity

    def _append_event_tx(self, connection: sqlite3.Connection, event: RuntimeEvent) -> RuntimeEvent:
        identity_row = connection.execute("SELECT value FROM runtime_meta WHERE key = 'identity'").fetchone()
        if identity_row is None:
            raise ValueError("Durable runtime store has no identity")
        runtime_id = RuntimeIdentity.model_validate_json(identity_row[0]).runtime_id
        if event.runtime_id != runtime_id:
            raise RuntimeIdentityMismatchError(
                f"Event runtime_id {event.runtime_id!r} does not match durable lineage {runtime_id!r}"
            )
        sequence = int(
            connection.execute("SELECT COALESCE(MAX(sequence), -1) + 1 FROM runtime_events").fetchone()[0]
        )
        stored = event.model_copy(update={"event_id": sequence, "sequence": sequence})
        connection.execute(
            """
            INSERT INTO runtime_events(
                sequence, runtime_id, execution_id, process_id, attempt_id,
                event_type, timestamp, event_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sequence,
                stored.runtime_id,
                stored.execution_id,
                stored.process_id,
                stored.attempt_id,
                stored.event_type,
                stored.timestamp,
                stored.model_dump_json(),
            ),
        )
        self._apply_attempt_event_tx(connection, stored)
        if stored.execution_id is not None:
            execution = self._load_execution_tx(connection, stored.execution_id)
            if execution is not None:
                self._write_execution_tx(
                    connection,
                    execution.model_copy(update={"last_event_sequence": sequence}),
                )
        return stored

    def _apply_attempt_event_tx(self, connection: sqlite3.Connection, event: RuntimeEvent) -> None:
        if event.attempt_id is None or event.process_id is None:
            return
        attempt = self._load_attempt_tx(connection, event.attempt_id)
        if attempt is None:
            ordinal = int(event.payload.get("ordinal") or 1)
            attempt = RuntimeProcessAttempt(
                attempt_id=event.attempt_id,
                process_id=event.process_id,
                runtime_id=event.runtime_id,
                execution_id=event.execution_id,
                ordinal=ordinal,
                operation=_attempt_operation(event.payload),
                execution_key=event.payload.get("execution_key"),
                input_fingerprint=event.payload.get("input_fingerprint"),
                implementation_version=event.payload.get("implementation_version"),
                resumed_from_attempt_id=event.payload.get("resumed_from_attempt_id"),
                checkpoint_id=event.checkpoint_id,
                backend_session_id=event.payload.get("backend_session_id"),
                first_event_sequence=int(event.event_id),
            )
        updates: dict[str, Any] = {"last_event_sequence": int(event.event_id)}
        if event.status is not None:
            updates["status"] = event.status
        if event.status_class is not None:
            updates["status_class"] = event.status_class
        if event.error is not None:
            updates["error"] = event.error
        if event.event_type == RuntimeEventType.PROCESS_ATTEMPT_STARTED:
            updates["started_at"] = event.timestamp
        if event.event_type in {
            RuntimeEventType.PROCESS_ATTEMPT_COMPLETED,
            RuntimeEventType.PROCESS_ATTEMPT_FAILED,
            RuntimeEventType.PROCESS_ATTEMPT_CANCELLED,
            RuntimeEventType.PROCESS_ATTEMPT_INTERRUPTED,
        }:
            updates["finished_at"] = event.timestamp
        self._write_attempt_tx(connection, attempt.model_copy(update=updates))

    @staticmethod
    def _load_attempt_tx(connection: sqlite3.Connection, attempt_id: str) -> RuntimeProcessAttempt | None:
        row = connection.execute(
            "SELECT attempt_json FROM runtime_attempts WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        return RuntimeProcessAttempt.model_validate_json(row[0]) if row is not None else None

    @staticmethod
    def _write_attempt_tx(connection: sqlite3.Connection, attempt: RuntimeProcessAttempt) -> None:
        connection.execute(
            """
            INSERT INTO runtime_attempts(attempt_id, process_id, execution_id, ordinal, attempt_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(attempt_id) DO UPDATE SET
                execution_id = excluded.execution_id,
                ordinal = excluded.ordinal,
                attempt_json = excluded.attempt_json
            """,
            (
                attempt.attempt_id,
                attempt.process_id,
                attempt.execution_id,
                attempt.ordinal,
                attempt.model_dump_json(),
            ),
        )

    @staticmethod
    def _load_execution_tx(
        connection: sqlite3.Connection, execution_id: str
    ) -> RuntimeExecutionRecord | None:
        row = connection.execute(
            "SELECT record_json FROM runtime_executions WHERE execution_id = ?", (execution_id,)
        ).fetchone()
        return RuntimeExecutionRecord.model_validate_json(row[0]) if row is not None else None

    @staticmethod
    def _write_execution_tx(connection: sqlite3.Connection, record: RuntimeExecutionRecord) -> None:
        connection.execute(
            """
            INSERT INTO runtime_executions(
                execution_id, ordinal, status, first_event_sequence, last_event_sequence, record_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(execution_id) DO UPDATE SET
                status = excluded.status,
                first_event_sequence = excluded.first_event_sequence,
                last_event_sequence = excluded.last_event_sequence,
                record_json = excluded.record_json
            """,
            (
                record.execution_id,
                record.ordinal,
                record.status,
                record.first_event_sequence,
                record.last_event_sequence,
                record.model_dump_json(),
            ),
        )

    @staticmethod
    def _load_session_tx(
        connection: sqlite3.Connection, session_id: str
    ) -> RuntimeBackendSession | None:
        row = connection.execute(
            "SELECT session_json FROM runtime_backend_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        return RuntimeBackendSession.model_validate_json(row[0]) if row is not None else None

    @staticmethod
    def _load_active_session_tx(connection: sqlite3.Connection) -> RuntimeBackendSession | None:
        row = connection.execute(
            "SELECT session_json FROM runtime_backend_sessions WHERE status = 'active'"
        ).fetchone()
        return RuntimeBackendSession.model_validate_json(row[0]) if row is not None else None

    @staticmethod
    def _write_session_tx(connection: sqlite3.Connection, session: RuntimeBackendSession) -> None:
        connection.execute(
            """
            INSERT INTO runtime_backend_sessions(session_id, status, acquired_at, session_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                status = excluded.status,
                session_json = excluded.session_json
            """,
            (session.session_id, session.status, session.acquired_at, session.model_dump_json()),
        )

    def _append_session_event_tx(
        self,
        connection: sqlite3.Connection,
        session: RuntimeBackendSession,
        event_type: RuntimeEventType,
        timestamp: float,
    ) -> RuntimeEvent:
        return self._append_event_tx(
            connection,
            RuntimeEvent(
                event_id=-1,
                runtime_id=session.runtime_id,
                event_type=event_type,
                timestamp=timestamp,
                subject_type="backend_session",
                subject_id=session.session_id,
                status=session.status,
                status_class=(
                    RuntimeStatusClass.ACTIVE
                    if session.status == RuntimeBackendSessionStatus.ACTIVE
                    else RuntimeStatusClass.TERMINAL_SUCCESS
                ),
                payload={
                    "owner_id": session.owner_id,
                    "heartbeat_at": session.heartbeat_at,
                    "expires_at": session.expires_at,
                },
                metadata=session.metadata,
            ),
        )

    def _require_live_session_tx(
        self,
        connection: sqlite3.Connection,
        session_id: str,
        owner_id: str,
        now: float,
    ) -> RuntimeBackendSession:
        session = self._load_active_session_tx(connection)
        if (
            session is None
            or session.session_id != session_id
            or session.owner_id != owner_id
            or session.expires_at <= now
        ):
            connection.rollback()
            raise RuntimeLeaseLostError(f"Backend session lease is not live: {session_id!r}")
        return session

    def _notify_changed(self) -> None:
        with self._changed:
            self._changed.notify_all()

    @staticmethod
    def _load_command_tx(
        connection: sqlite3.Connection, command_id: str
    ) -> RuntimeCommandRecord | None:
        row = connection.execute(
            "SELECT command_json FROM runtime_commands WHERE command_id = ?", (command_id,)
        ).fetchone()
        return RuntimeCommandRecord.model_validate_json(row[0]) if row is not None else None

    @staticmethod
    def _write_command_tx(connection: sqlite3.Connection, record: RuntimeCommandRecord) -> None:
        connection.execute(
            """
            INSERT INTO runtime_commands(command_id, status, first_event_sequence, command_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(command_id) DO UPDATE SET
                status = excluded.status,
                command_json = excluded.command_json
            """,
            (
                record.command_id,
                record.status,
                record.first_event_sequence,
                record.model_dump_json(),
            ),
        )

    def _initialize_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runtime_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runtime_events (
                    sequence INTEGER PRIMARY KEY,
                    runtime_id TEXT NOT NULL,
                    execution_id TEXT,
                    process_id TEXT,
                    attempt_id TEXT,
                    event_type TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    event_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS runtime_events_process_idx
                    ON runtime_events(process_id, sequence);
                CREATE INDEX IF NOT EXISTS runtime_events_execution_idx
                    ON runtime_events(execution_id, sequence);
                CREATE TABLE IF NOT EXISTS runtime_process_specs (
                    process_id TEXT PRIMARY KEY,
                    spec_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runtime_executions (
                    execution_id TEXT PRIMARY KEY,
                    ordinal INTEGER NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    first_event_sequence INTEGER,
                    last_event_sequence INTEGER,
                    record_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runtime_attempts (
                    attempt_id TEXT PRIMARY KEY,
                    process_id TEXT NOT NULL,
                    execution_id TEXT,
                    ordinal INTEGER NOT NULL,
                    attempt_json TEXT NOT NULL,
                    UNIQUE(process_id, ordinal)
                );
                CREATE TABLE IF NOT EXISTS runtime_projection (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    through_sequence INTEGER NOT NULL,
                    projection_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runtime_backend_sessions (
                    session_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    acquired_at REAL NOT NULL,
                    session_json TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS runtime_backend_sessions_active_idx
                    ON runtime_backend_sessions(status) WHERE status = 'active';
                CREATE TABLE IF NOT EXISTS runtime_commands (
                    command_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    first_event_sequence INTEGER NOT NULL,
                    command_json TEXT NOT NULL
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30.0)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection


def _numeric_cursor(cursor: RuntimeEventCursor | None) -> int | None:
    if cursor is None or isinstance(cursor, int):
        return cursor
    try:
        return int(cursor)
    except ValueError:
        return None


def _command_fingerprint(
    *,
    command_type: str,
    target_id: str | None,
    execution_id: str | None,
    payload: dict[str, Any],
) -> str:
    canonical = json.dumps(
        {
            "command_type": command_type,
            "target_id": target_id,
            "execution_id": execution_id,
            "payload": payload,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _execution_event(
    status: RuntimeExecutionStatus,
) -> tuple[RuntimeEventType, RuntimeStatusClass]:
    mapping = {
        RuntimeExecutionStatus.PENDING: (
            RuntimeEventType.EXECUTION_CREATED,
            RuntimeStatusClass.NOT_STARTED,
        ),
        RuntimeExecutionStatus.RUNNING: (
            RuntimeEventType.EXECUTION_STARTED,
            RuntimeStatusClass.ACTIVE,
        ),
        RuntimeExecutionStatus.SUCCEEDED: (
            RuntimeEventType.EXECUTION_COMPLETED,
            RuntimeStatusClass.TERMINAL_SUCCESS,
        ),
        RuntimeExecutionStatus.FAILED: (
            RuntimeEventType.EXECUTION_FAILED,
            RuntimeStatusClass.TERMINAL_FAILURE,
        ),
        RuntimeExecutionStatus.CANCELLED: (
            RuntimeEventType.EXECUTION_CANCELLED,
            RuntimeStatusClass.TERMINAL_CANCELLED,
        ),
        RuntimeExecutionStatus.INTERRUPTED: (
            RuntimeEventType.EXECUTION_INTERRUPTED,
            RuntimeStatusClass.TERMINAL_FAILURE,
        ),
    }
    return mapping[status]


def _attempt_operation(payload: dict[str, Any]) -> RuntimeProcessOperation | None:
    mapping = {
        "start": RuntimeProcessOperation.START,
        "resume": RuntimeProcessOperation.RESUME,
        "retry": RuntimeProcessOperation.RETRY,
        "restart": RuntimeProcessOperation.START,
    }
    value = payload.get("operation", payload.get("recovery_action"))
    return mapping.get(str(value))


def _validate_execution_transition(
    current: RuntimeExecutionStatus,
    target: RuntimeExecutionStatus,
) -> None:
    allowed = {
        RuntimeExecutionStatus.PENDING: {
            RuntimeExecutionStatus.RUNNING,
            RuntimeExecutionStatus.CANCELLED,
            RuntimeExecutionStatus.INTERRUPTED,
        },
        RuntimeExecutionStatus.RUNNING: {
            RuntimeExecutionStatus.SUCCEEDED,
            RuntimeExecutionStatus.FAILED,
            RuntimeExecutionStatus.CANCELLED,
            RuntimeExecutionStatus.INTERRUPTED,
        },
    }
    if target not in allowed.get(current, set()):
        raise ValueError(f"Invalid execution transition: {current.value!r} -> {target.value!r}")
