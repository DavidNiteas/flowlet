"""Opt-in standard RuntimeEvent sidecar writer."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .adapters import manager_record_to_runtime_event, txn_event_payload_to_runtime_event
from .event_store import RuntimeEventJsonlStore
from .projection import RuntimeFrameworkReducer, RuntimeProjection, RuntimeProjectionPolicy
from .schema import RuntimeEvent, RuntimeEventType
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
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.runtime_id = runtime_id
        self.process_id = process_id
        self.enabled = enabled
        self.write_projection = write_projection
        self.projection_policy = projection_policy or RuntimeProjectionPolicy()
        self._store = RuntimeStore(self.runtime_dir)

    def append_event(self, event: RuntimeEvent) -> RuntimeEvent:
        """Append one already-normalized RuntimeEvent."""
        if not self.enabled:
            return event
        stored = self._store.append_runtime_event(event)
        if self.write_projection and _affects_projection(stored):
            self.refresh_projection()
        return stored

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

    def refresh_projection(self) -> RuntimeProjection:
        """Rebuild and persist the framework projection from sidecar events."""
        event_store = self.store()
        event_store.load()
        projection = RuntimeFrameworkReducer(policy=self.projection_policy).reduce(event_store.list())
        self._store.write_projection(projection.model_dump(mode="json"))
        return projection


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
