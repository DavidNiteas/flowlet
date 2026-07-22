"""Stable identity contracts for durable runtime execution lineages."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator

from .schema import RuntimeErrorInfo


class RuntimeExecutionKind(StrEnum):
    """How one execution wave entered an existing runtime lineage."""

    INITIAL = "initial"
    CONTINUE = "continue"


class RuntimeExecutionStatus(StrEnum):
    """Durable lifecycle status for one runtime execution wave."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


class RuntimeIdentity(BaseModel):
    """Identity of one immutable runtime lineage.

    A continue operation keeps this identity. A rerun creates another identity,
    even when the new runtime reuses the same filesystem location.
    """

    schema_version: int = 1
    runtime_id: str
    generation: int = 1
    logical_task_id: str | None = None
    created_at: float
    rerun_of_runtime_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("runtime_id")
    @classmethod
    def _validate_runtime_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("runtime_id must not be empty")
        return value

    @field_validator("generation")
    @classmethod
    def _validate_generation(cls, value: int) -> int:
        if value < 1:
            raise ValueError("runtime generation must be >= 1")
        return value


class RuntimeExecutionRecord(BaseModel):
    """One initial or continuation execution wave inside a runtime."""

    schema_version: int = 1
    execution_id: str
    runtime_id: str
    kind: RuntimeExecutionKind
    ordinal: int
    status: RuntimeExecutionStatus = RuntimeExecutionStatus.PENDING
    backend_session_id: str | None = None
    recovery_plan_id: str | None = None
    created_at: float
    started_at: float | None = None
    finished_at: float | None = None
    first_event_sequence: int | None = None
    last_event_sequence: int | None = None
    error: RuntimeErrorInfo | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("execution_id", "runtime_id")
    @classmethod
    def _validate_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("execution identity fields must not be empty")
        return value

    @field_validator("ordinal")
    @classmethod
    def _validate_ordinal(cls, value: int) -> int:
        if value < 1:
            raise ValueError("execution ordinal must be >= 1")
        return value
