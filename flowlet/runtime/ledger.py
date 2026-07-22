"""Materialized process execution ledger contracts."""

from __future__ import annotations

from pydantic import BaseModel, Field

from .identity import RuntimeExecutionRecord, RuntimeIdentity
from .recovery import RuntimeProcessAttempt


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
