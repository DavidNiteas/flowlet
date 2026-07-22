"""Durable idempotent command contracts for runtime control operations."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator

from .identity import RuntimeIdentity
from .schema import RuntimeErrorInfo, RuntimeEvent, RuntimeEventType


class RuntimeCommandStatus(StrEnum):
    """Durable command receipt state."""

    ACCEPTED = "accepted"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RuntimeCommandRecord(BaseModel):
    """One idempotency-keyed request and its durable result."""

    schema_version: int = 1
    command_id: str
    runtime_id: str
    command_type: str
    request_fingerprint: str
    target_id: str | None = None
    execution_id: str | None = None
    backend_session_id: str | None = None
    status: RuntimeCommandStatus = RuntimeCommandStatus.ACCEPTED
    accepted_at: float
    finished_at: float | None = None
    first_event_sequence: int
    last_event_sequence: int
    payload: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | None = None
    error: RuntimeErrorInfo | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("command_id", "runtime_id", "command_type", "request_fingerprint")
    @classmethod
    def _validate_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("command identity fields must not be empty")
        return value


class RuntimeCommandReservation(BaseModel):
    """Result of reserving an idempotent command."""

    record: RuntimeCommandRecord
    created: bool


class RuntimeCommandConflictError(ValueError):
    """Raised when one command id is reused for a different request."""


class RuntimeCommandReducer:
    """Rebuild command receipts exclusively from canonical command events."""

    def reduce(
        self, identity: RuntimeIdentity, events: list[RuntimeEvent]
    ) -> list[RuntimeCommandRecord]:
        records: dict[str, RuntimeCommandRecord] = {}
        order: list[str] = []
        command_events = {
            RuntimeEventType.COMMAND_ACCEPTED,
            RuntimeEventType.COMMAND_SUCCEEDED,
            RuntimeEventType.COMMAND_FAILED,
        }
        for event in sorted(events, key=_event_sequence):
            if event.event_type not in command_events:
                continue
            if event.runtime_id != identity.runtime_id:
                raise ValueError("Command event runtime_id does not match runtime identity")
            command_id = event.subject_id
            if event.subject_type != "command" or command_id is None:
                raise ValueError("Command events require a command subject identity")
            sequence = _event_sequence(event)
            record = records.get(command_id)
            if event.event_type == RuntimeEventType.COMMAND_ACCEPTED:
                if record is not None:
                    raise ValueError(f"Duplicate command.accepted event: {command_id!r}")
                record = RuntimeCommandRecord(
                    command_id=command_id,
                    runtime_id=identity.runtime_id,
                    command_type=str(event.payload["command_type"]),
                    request_fingerprint=str(event.payload["request_fingerprint"]),
                    target_id=event.payload.get("target_id"),
                    execution_id=event.execution_id,
                    backend_session_id=event.payload.get("backend_session_id"),
                    accepted_at=event.timestamp,
                    first_event_sequence=sequence,
                    last_event_sequence=sequence,
                    payload=event.payload.get("request", {}),
                    metadata=event.metadata,
                )
                records[command_id] = record
                order.append(command_id)
                continue
            if record is None:
                raise ValueError(f"Command result precedes acceptance: {command_id!r}")
            status = (
                RuntimeCommandStatus.SUCCEEDED
                if event.event_type == RuntimeEventType.COMMAND_SUCCEEDED
                else RuntimeCommandStatus.FAILED
            )
            records[command_id] = record.model_copy(
                update={
                    "status": status,
                    "finished_at": event.timestamp,
                    "last_event_sequence": sequence,
                    "result": event.payload.get("result"),
                    "error": event.error,
                }
            )
        return [records[command_id] for command_id in order]


def _event_sequence(event: RuntimeEvent) -> int:
    if event.sequence is None:
        raise ValueError("Command reducer requires canonical event sequences")
    return event.sequence
