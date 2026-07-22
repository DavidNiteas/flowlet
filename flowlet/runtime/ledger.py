"""Materialized process execution ledger contracts."""

from __future__ import annotations

from pydantic import BaseModel, Field

from .identity import RuntimeExecutionRecord, RuntimeIdentity
from .projection import RuntimeFrameworkReducer
from .recovery import RuntimeProcessAttempt
from .schema import RuntimeEvent, RuntimeEventType


class RuntimeExecutionLedger(BaseModel):
    """Rebuildable execution index materialized from the runtime event log."""

    schema_version: int = 1
    identity: RuntimeIdentity
    executions: dict[str, RuntimeExecutionRecord] = Field(default_factory=dict)
    execution_order: list[str] = Field(default_factory=list)
    attempts: dict[str, RuntimeProcessAttempt] = Field(default_factory=dict)
    process_attempt_ids: dict[str, list[str]] = Field(default_factory=dict)
    last_event_sequence: int = -1
    projection_sequence: int = -1


class RuntimeExecutionLedgerReducer:
    """Rebuild execution and attempt indexes exclusively from durable events."""

    def reduce(
        self,
        identity: RuntimeIdentity,
        events: list[RuntimeEvent],
        *,
        projection_sequence: int = -1,
    ) -> RuntimeExecutionLedger:
        ordered = sorted(events, key=_event_sequence)
        executions: dict[str, RuntimeExecutionRecord] = {}
        execution_order: list[str] = []
        for event in ordered:
            if event.runtime_id != identity.runtime_id:
                raise ValueError("Ledger event runtime_id does not match runtime identity")
            if event.execution_id is None:
                continue
            sequence = _event_sequence(event)
            record = executions.get(event.execution_id)
            if event.event_type == RuntimeEventType.EXECUTION_CREATED:
                if record is not None:
                    raise ValueError(f"Duplicate execution.created event: {event.execution_id!r}")
                record = RuntimeExecutionRecord(
                    execution_id=event.execution_id,
                    runtime_id=identity.runtime_id,
                    kind=event.payload["kind"],
                    ordinal=int(event.payload["ordinal"]),
                    backend_session_id=event.payload.get("backend_session_id"),
                    recovery_plan_id=event.payload.get("recovery_plan_id"),
                    created_at=event.timestamp,
                    first_event_sequence=sequence,
                    last_event_sequence=sequence,
                    metadata=event.metadata,
                )
                executions[event.execution_id] = record
                execution_order.append(event.execution_id)
                continue
            if record is None:
                raise ValueError(f"Execution event precedes declaration: {event.execution_id!r}")
            updates = {"last_event_sequence": sequence}
            transition = _EXECUTION_EVENT_STATUS.get(event.event_type)
            if transition is not None:
                updates["status"] = transition
                updates["error"] = event.error
                if event.event_type == RuntimeEventType.EXECUTION_STARTED:
                    updates["started_at"] = event.timestamp
                else:
                    updates["finished_at"] = event.timestamp
            executions[event.execution_id] = record.model_copy(update=updates)

        projection = RuntimeFrameworkReducer().reduce(ordered)
        return RuntimeExecutionLedger(
            identity=identity,
            executions=executions,
            execution_order=execution_order,
            attempts=projection.attempts,
            process_attempt_ids=projection.process_attempt_ids,
            last_event_sequence=_event_sequence(ordered[-1]) if ordered else -1,
            projection_sequence=projection_sequence,
        )


_EXECUTION_EVENT_STATUS = {
    RuntimeEventType.EXECUTION_STARTED: "running",
    RuntimeEventType.EXECUTION_COMPLETED: "succeeded",
    RuntimeEventType.EXECUTION_FAILED: "failed",
    RuntimeEventType.EXECUTION_CANCELLED: "cancelled",
    RuntimeEventType.EXECUTION_INTERRUPTED: "interrupted",
}


def _event_sequence(event: RuntimeEvent) -> int:
    if event.sequence is None:
        raise ValueError("Execution ledger requires canonical event sequences")
    return event.sequence
