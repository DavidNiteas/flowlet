"""Opt-in standard RuntimeEvent sidecar writer."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .adapters import manager_record_to_runtime_event, txn_event_payload_to_runtime_event
from .durable_store import RuntimeDurableStore
from .event_store import RuntimeEventJsonlStore
from .process import RuntimeProcessSpec
from .projection import RuntimeFrameworkReducer, RuntimeProjection, RuntimeProjectionPolicy
from .schema import RuntimeEvent, RuntimeEventStatus, RuntimeEventType, RuntimeStatusClass
from .store import RuntimeStore


class RuntimeEventSidecarWriter:
    """Mirror legacy runtime payloads into the standard RuntimeEvent sidecar."""

    def __init__(
        self,
        runtime_dir: str | Path,
        *,
        runtime_id: str | None = None,
        process_id: str | None = None,
        enabled: bool = True,
        write_projection: bool = True,
        projection_policy: RuntimeProjectionPolicy | None = None,
        durable_store: RuntimeDurableStore | None = None,
        execution_id: str | None = None,
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.runtime_id = runtime_id
        self.process_id = process_id
        self.enabled = enabled
        self.write_projection = write_projection
        self.projection_policy = projection_policy or RuntimeProjectionPolicy()
        self.durable_store = durable_store
        self.execution_id = execution_id
        self._store = RuntimeStore(self.runtime_dir)

    def append_event(self, event: RuntimeEvent) -> RuntimeEvent:
        """Append one already-normalized RuntimeEvent."""
        if not self.enabled:
            return event
        if self.durable_store is not None:
            if event.execution_id is None and self.execution_id is not None:
                event = event.model_copy(update={"execution_id": self.execution_id})
            stored = self.durable_store.append(event)
            self._store.append_runtime_event(stored)
        else:
            stored = self._store.append_runtime_event(event)
        if self.write_projection and _affects_projection(stored):
            self.refresh_projection()
        return stored

    def append(self, event: RuntimeEvent) -> RuntimeEvent:
        """Implement RuntimeEventStore while retaining compatibility export."""
        return self.append_event(event)

    def list(self, *, since: int | str | None = None) -> list[RuntimeEvent]:
        """Read canonical events when configured, otherwise read the JSONL store."""
        if self.durable_store is not None:
            return self.durable_store.list(since=since)
        event_store = self.store()
        event_store.load()
        return event_store.list(since=since)

    def wait_for_next(
        self, *, since: int | str | None = None, timeout: float | None = None
    ) -> list[RuntimeEvent]:
        """Wait against the canonical event source."""
        if self.durable_store is not None:
            return self.durable_store.wait_for_next(since=since, timeout=timeout)
        event_store = self.store()
        event_store.load()
        return event_store.wait_for_next(since=since, timeout=timeout)

    def load(self) -> None:
        """Satisfy RuntimeEventStore; durable stores are already lazy."""
        if self.durable_store is None:
            self.store().load()

    def append_process_spec(self, spec: RuntimeProcessSpec, *, event_id: int | str) -> RuntimeEvent:
        """Append the standard declaration event for a persisted process spec."""
        event = RuntimeEvent(
            event_id=event_id,
            runtime_id=self.runtime_id or str(self.runtime_dir),
            process_id=spec.resolved_process_id(),
            parent_process_id=spec.parent_process_id,
            event_type=RuntimeEventType.PROCESS_CREATED,
            timestamp=time.time(),
            subject_type="process",
            subject_id=spec.resolved_process_id(),
            status=RuntimeEventStatus.PENDING,
            status_class=RuntimeStatusClass.NOT_STARTED,
            payload={
                "process_type": spec.process_type,
                "display_name": spec.display_name,
            },
            metadata=spec.metadata,
        )
        return self.append_event(event)

    def append_legacy_event(self, payload: dict[str, Any], metadata: dict[str, Any] | None = None) -> RuntimeEvent:
        """Mirror a TxnEvent-like payload to the sidecar stream."""
        event = txn_event_payload_to_runtime_event(
            payload,
            runtime_id=self.runtime_id,
            metadata=metadata,
        )
        if self.process_id is not None and event.process_id is None:
            event = event.model_copy(update={"process_id": self.process_id})
        return self.append_event(event)

    def append_manager_record(
        self,
        record: Any,
        *,
        record_type: str,
        event_id: int | str,
        process_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeEvent:
        """Mirror a Flowlet manager record to the sidecar stream."""
        event = manager_record_to_runtime_event(
            record,
            record_type=record_type,
            event_id=event_id,
            runtime_id=self.runtime_id or str(self.runtime_dir),
            process_id=process_id or self.process_id,
            metadata=metadata,
        )
        return self.append_event(event)

    def store(self) -> RuntimeEventJsonlStore:
        """Return the standard RuntimeEvent JSONL store."""
        return self._store.runtime_event_store()

    def next_event_id(self) -> int:
        """Return the next numeric id after events already persisted in this runtime."""
        if self.durable_store is not None:
            return self.durable_store.last_event_sequence() + 1
        event_store = self.store()
        event_store.load()
        next_id = 0
        for event in event_store.list():
            numeric_id = _numeric_event_id(event)
            if numeric_id is not None:
                next_id = max(next_id, numeric_id + 1)
        return next_id

    def refresh_projection(self) -> RuntimeProjection:
        """Rebuild and persist the framework projection from sidecar events."""
        if self.durable_store is not None:
            projection = self.durable_store.refresh_projection(
                RuntimeFrameworkReducer(policy=self.projection_policy)
            )
            self._store.write_projection(projection.model_dump(mode="json"))
            return projection
        event_store = self.store()
        event_store.load()
        projection = RuntimeFrameworkReducer(policy=self.projection_policy).reduce(event_store.list())
        self._store.write_projection(projection.model_dump(mode="json"))
        return projection

    def export_durable_events(self, *, since: int | None = None) -> list[RuntimeEvent]:
        """Incrementally export canonical durable events missing from compatibility JSONL."""
        if self.durable_store is None:
            return []
        compatibility = self.store()
        compatibility.load()
        existing_sequences = {
            event.sequence for event in compatibility.list() if event.sequence is not None
        }
        exported: list[RuntimeEvent] = []
        for event in self.durable_store.list(since=since):
            if event.sequence in existing_sequences:
                continue
            exported.append(event)
        if exported:
            compatibility.replace(self.durable_store.list())
        return exported


def _affects_projection(event: RuntimeEvent) -> bool:
    return bool(
        event.status is not None
        or event.status_class is not None
        or event.progress is not None
        or event.error is not None
        or event.event_type
        in {
            RuntimeEventType.ARTIFACT_PRODUCED,
            RuntimeEventType.ARTIFACT_UPDATED,
            RuntimeEventType.ARTIFACT_REMOVED,
        }
    )


def _numeric_event_id(event: RuntimeEvent) -> int | None:
    try:
        return int(event.event_id)
    except (TypeError, ValueError):
        return None
