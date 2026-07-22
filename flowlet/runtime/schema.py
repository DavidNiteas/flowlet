"""Standard runtime event schemas."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class RuntimeEventStatus(StrEnum):
    """Recommended framework event status vocabulary."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"
    BLOCKED = "blocked"
    UNKNOWN = "unknown"


class RuntimeStatusClass(StrEnum):
    """Framework-level status classes for custom status values."""

    NOT_STARTED = "not_started"
    ACTIVE = "active"
    TERMINAL_SUCCESS = "terminal_success"
    TERMINAL_FAILURE = "terminal_failure"
    TERMINAL_CANCELLED = "terminal_cancelled"
    BLOCKED = "blocked"
    UNKNOWN = "unknown"


class RuntimeProgress(BaseModel):
    """Optional standard progress payload for runtime events."""

    current: float | int = 0
    total: float | int | None = None
    unit: str | None = None
    percent: float | None = None
    description: str | None = None

    @field_validator("percent")
    @classmethod
    def _validate_percent(cls, value: float | None) -> float | None:
        if value is None:
            return None
        return max(0.0, min(100.0, float(value)))

    def model_post_init(self, __context: Any) -> None:
        if self.percent is None and self.total not in (None, 0):
            self.percent = max(0.0, min(100.0, (float(self.current) / float(self.total)) * 100.0))


class RuntimeErrorInfo(BaseModel):
    """Standard error payload carried by runtime events."""

    type: str
    message: str
    traceback: str | None = None
    retryable: bool | None = None
    cause: dict[str, Any] | None = None
    context: dict[str, Any] = Field(default_factory=dict)


class RuntimeEvent(BaseModel):
    """Business-neutral runtime event envelope."""

    schema_version: int = 1
    event_id: int | str
    runtime_id: str
    event_type: str
    timestamp: float
    process_id: str | None = None
    parent_process_id: str | None = None
    sequence: int | None = None
    subject_type: str | None = None
    subject_id: str | None = None
    parent_subject_id: str | None = None
    status: RuntimeEventStatus | str | None = None
    status_class: RuntimeStatusClass | None = None
    progress: RuntimeProgress | None = None
    message: str | None = None
    error: RuntimeErrorInfo | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    causation_id: int | str | None = None
    correlation_id: int | str | None = None

    @field_validator("event_type")
    @classmethod
    def _validate_event_type(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("event_type must not be empty")
        return value

    @field_validator("runtime_id")
    @classmethod
    def _validate_runtime_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("runtime_id must not be empty")
        return value


def runtime_event_payload(**kwargs: Any) -> dict[str, Any]:
    """Build a JSON-safe standard runtime event payload."""
    return RuntimeEvent(**kwargs).model_dump(mode="json")
