"""Bridge Flowlet manager snapshots into the standard runtime event store."""

from __future__ import annotations

from typing import Any

from .adapters import manager_record_to_runtime_event
from .event_store import RuntimeEventStore
from .schema import RuntimeEvent


class RuntimeManagerEventBridge:
    """Incrementally mirror a manager bundle into a ``RuntimeEventStore``.

    The bridge owns only observation conversion and cursors. It does not start,
    schedule, or otherwise control the managers or their enclosing process.
    """

    def __init__(
        self,
        runtime: Any,
        event_store: RuntimeEventStore,
        *,
        runtime_id: str,
        process_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.runtime = runtime
        self.event_store = event_store
        self.runtime_id = runtime_id
        self.process_id = process_id
        self.metadata = metadata or {}
        self._offsets: dict[str, Any] = {
            "progress": {},
            "signals": {},
            "logs": 0,
            "streams": 0,
            "telemetry": 0,
        }

    def sync(self) -> list[RuntimeEvent]:
        """Append manager changes since the previous call and return them."""
        events: list[RuntimeEvent] = []
        progress = self.runtime.progress.get_all_progress()
        for task_id, record in progress.items():
            payload = _json_safe_record(record)
            if self._offsets["progress"].get(task_id) != payload:
                events.append(self._append(record, "progress"))
                self._offsets["progress"][task_id] = payload

        signals = self.runtime.signals.snapshot()
        for name, record in signals.items():
            payload = _json_safe_record(record)
            version = payload.get("version")
            if self._offsets["signals"].get(name) != version:
                events.append(self._append(record, "signal"))
                self._offsets["signals"][name] = version

        events.extend(self._append_new("logs", self.runtime.logs.get_records(), "log"))
        events.extend(self._append_new("streams", self.runtime.streams.get_chunks(), "stream"))
        events.extend(self._append_new("telemetry", self.runtime.telemetry.get_events(), "telemetry"))
        return events

    def _append_new(self, offset_name: str, records: list[Any], record_type: str) -> list[RuntimeEvent]:
        offset = self._offsets[offset_name]
        events = [self._append(record, record_type) for record in records[offset:]]
        self._offsets[offset_name] = len(records)
        return events

    def _append(self, record: Any, record_type: str) -> RuntimeEvent:
        event = manager_record_to_runtime_event(
            record,
            record_type=record_type,
            event_id=_next_event_id(self.event_store.list()),
            runtime_id=self.runtime_id,
            process_id=self.process_id,
            metadata=self.metadata,
        )
        return self.event_store.append(event)


def _next_event_id(events: list[RuntimeEvent]) -> int:
    next_id = 1
    for event in events:
        event_id = _numeric_event_id(event.event_id)
        if event_id is not None:
            next_id = max(next_id, event_id + 1)
    return next_id


def _numeric_event_id(event_id: int | str) -> int | None:
    try:
        return int(event_id)
    except (TypeError, ValueError):
        return None


def _json_safe_record(record: Any) -> dict[str, Any]:
    if hasattr(record, "to_dict"):
        payload = record.to_dict()
    elif hasattr(record, "model_dump"):
        payload = record.model_dump(mode="json")
    elif isinstance(record, dict):
        payload = record
    else:
        payload = {"value": repr(record)}
    return payload if isinstance(payload, dict) else {"value": payload}
