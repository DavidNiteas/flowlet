"""Opt-in standard RuntimeEvent sidecar writer."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .adapters import manager_record_to_runtime_event, txn_event_payload_to_runtime_event
from .event_store import RuntimeEventJsonlStore
from .schema import RuntimeEvent
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
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.runtime_id = runtime_id
        self.process_id = process_id
        self.enabled = enabled
        self._store = RuntimeStore(self.runtime_dir)

    def append_event(self, event: RuntimeEvent) -> RuntimeEvent:
        """Append one already-normalized RuntimeEvent."""
        if not self.enabled:
            return event
        return self._store.append_runtime_event(event)

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

