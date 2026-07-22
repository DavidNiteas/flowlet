"""Backend-session lease contracts for one durable runtime lineage."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class RuntimeBackendSessionStatus(StrEnum):
    """Lifecycle state of one backend attachment to a runtime."""

    ACTIVE = "active"
    RELEASED = "released"
    EXPIRED = "expired"


class RuntimeBackendSession(BaseModel):
    """Exclusive, renewable ownership lease for runtime coordination."""

    schema_version: int = 1
    session_id: str
    runtime_id: str
    owner_id: str
    status: RuntimeBackendSessionStatus = RuntimeBackendSessionStatus.ACTIVE
    acquired_at: float
    heartbeat_at: float
    expires_at: float
    released_at: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("session_id", "runtime_id", "owner_id")
    @classmethod
    def _validate_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("backend session identity fields must not be empty")
        return value


class RuntimeSessionReconciliation(BaseModel):
    """Immutable summary of stale work reconciled by a live backend session."""

    session_id: str
    interrupted_execution_ids: list[str] = Field(default_factory=list)
    interrupted_attempt_ids: list[str] = Field(default_factory=list)
    first_event_sequence: int | None = None
    last_event_sequence: int | None = None


class RuntimeLeaseConflictError(RuntimeError):
    """Raised when another live backend session owns the runtime."""


class RuntimeLeaseLostError(RuntimeError):
    """Raised when a backend tries to use a lease it no longer owns."""
