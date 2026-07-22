"""Business-neutral recovery declarations and plan contracts."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from .schema import RuntimeErrorInfo, RuntimeEventStatus, RuntimeStatusClass


class RuntimeRecoveryAction(StrEnum):
    """Framework recovery actions selected for a logical process."""

    SKIP = "skip"
    RESUME = "resume"
    RETRY = "retry"
    RESTART = "restart"
    BLOCK = "block"


class RuntimeCheckpointRef(BaseModel):
    """Portable reference to a business-owned recoverable checkpoint."""

    checkpoint_id: str
    process_id: str
    attempt_id: str
    created_at: float
    uri: str | None = None
    cursor: dict[str, Any] | None = None
    input_fingerprint: str | None = None
    implementation_version: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("checkpoint_id", "process_id", "attempt_id")
    @classmethod
    def _validate_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("checkpoint identity fields must not be empty")
        return value


class RuntimeProcessAttempt(BaseModel):
    """Persistable state of one execution attempt for a logical process."""

    attempt_id: str
    process_id: str
    runtime_id: str
    ordinal: int
    status: RuntimeEventStatus | str = RuntimeEventStatus.PENDING
    status_class: RuntimeStatusClass = RuntimeStatusClass.NOT_STARTED
    started_at: float | None = None
    finished_at: float | None = None
    resumed_from_attempt_id: str | None = None
    checkpoint_id: str | None = None
    error: RuntimeErrorInfo | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("ordinal")
    @classmethod
    def _validate_ordinal(cls, value: int) -> int:
        if value < 1:
            raise ValueError("attempt ordinal must be >= 1")
        return value


class RuntimeRecoveryDecision(BaseModel):
    """One package-supplied recovery decision consumed by a planner."""

    process_id: str
    action: RuntimeRecoveryAction
    reason: str
    source_attempt_id: str | None = None
    checkpoint: RuntimeCheckpointRef | None = None
    invalidates: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_checkpoint_action(self) -> RuntimeRecoveryDecision:
        if self.action == RuntimeRecoveryAction.RESUME and self.checkpoint is None:
            raise ValueError("resume recovery decisions require a checkpoint")
        if self.checkpoint is not None and self.checkpoint.process_id != self.process_id:
            raise ValueError("checkpoint process_id must match the recovery process_id")
        return self


class RuntimeRecoveryStep(BaseModel):
    """Ordered, reviewable action in a persisted local recovery plan."""

    process_id: str
    action: RuntimeRecoveryAction
    reason: str
    depends_on: list[str] = Field(default_factory=list)
    source_attempt_id: str | None = None
    checkpoint: RuntimeCheckpointRef | None = None
    invalidates: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_checkpoint_action(self) -> RuntimeRecoveryStep:
        if self.action == RuntimeRecoveryAction.RESUME and self.checkpoint is None:
            raise ValueError("resume recovery steps require a checkpoint")
        if self.checkpoint is not None and self.checkpoint.process_id != self.process_id:
            raise ValueError("checkpoint process_id must match the recovery process_id")
        return self


class RuntimeRecoveryPlan(BaseModel):
    """Persisted recovery plan produced before any business action runs."""

    schema_version: int = 1
    plan_id: str
    source_runtime_id: str
    target_runtime_id: str
    created_at: float
    steps: list[RuntimeRecoveryStep]
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_steps(self) -> RuntimeRecoveryPlan:
        process_ids = [step.process_id for step in self.steps]
        if len(process_ids) != len(set(process_ids)):
            raise ValueError("recovery plan process_ids must be unique")
        return self
