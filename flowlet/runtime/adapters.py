"""Compatibility adapters for legacy runtime event payloads."""

from __future__ import annotations

from typing import Any

from .schema import RuntimeEvent, RuntimeEventStatus, RuntimeStatusClass

LEGACY_TXN_EVENT_TYPE_MAP = {
    "job_state": "process.status.changed",
    "progress": "process.progressed",
    "log": "log.emitted",
    "stream": "stream.chunk",
    "telemetry": "metric.sampled",
    "signal": "signal.changed",
    "artifact": "artifact.produced",
    "error": "error.raised",
    "result": "process.completed",
}


def txn_event_payload_to_runtime_event(
    payload: dict[str, Any],
    *,
    runtime_id: str | None = None,
    event_type_map: dict[str, str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> RuntimeEvent:
    """Convert a TxnEvent-like payload to a standard RuntimeEvent.

    The adapter is intentionally business-package agnostic. It expects only the
    legacy shape used by current runtime backends: event_id, job_id, timestamp,
    event_type, and payload.
    """
    legacy_payload = payload.get("payload")
    if not isinstance(legacy_payload, dict):
        legacy_payload = {}
    legacy_event_type = str(payload.get("event_type") or "event")
    mapped_type = (event_type_map or LEGACY_TXN_EVENT_TYPE_MAP).get(legacy_event_type, legacy_event_type)
    process_id = _optional_str(payload.get("job_id"))
    event_metadata = {
        "legacy_event_type": legacy_event_type,
        **(metadata or {}),
    }
    status = _status_from_legacy_payload(legacy_payload)
    return RuntimeEvent(
        event_id=payload.get("event_id") or 0,
        runtime_id=runtime_id or process_id or "runtime",
        process_id=process_id,
        event_type=mapped_type,
        timestamp=float(payload.get("timestamp") or 0.0),
        status=status,
        status_class=_status_class(status),
        payload=legacy_payload,
        metadata=event_metadata,
    )


def runtime_event_to_txn_event_payload(event: RuntimeEvent) -> dict[str, Any]:
    """Convert a RuntimeEvent to a TxnEvent-like compatibility payload."""
    legacy_event_type = event.metadata.get("legacy_event_type")
    if not isinstance(legacy_event_type, str):
        legacy_event_type = _legacy_event_type_from_runtime_event(event.event_type)
    return {
        "event_id": event.event_id,
        "job_id": event.process_id or event.runtime_id,
        "timestamp": event.timestamp,
        "event_type": legacy_event_type,
        "payload": event.payload,
    }


def _status_from_legacy_payload(payload: dict[str, Any]) -> str | RuntimeEventStatus | None:
    status = payload.get("status")
    return str(status) if status is not None else None


def _status_class(status: str | RuntimeEventStatus | None) -> RuntimeStatusClass | None:
    if status is None:
        return None
    if status in {"created", "queued", "pending"}:
        return RuntimeStatusClass.NOT_STARTED
    if status == "running":
        return RuntimeStatusClass.ACTIVE
    if status in {"completed", "succeeded", RuntimeEventStatus.SUCCEEDED}:
        return RuntimeStatusClass.TERMINAL_SUCCESS
    if status in {"failed", RuntimeEventStatus.FAILED}:
        return RuntimeStatusClass.TERMINAL_FAILURE
    if status in {"cancelled", RuntimeEventStatus.CANCELLED}:
        return RuntimeStatusClass.TERMINAL_CANCELLED
    if status in {"blocked", RuntimeEventStatus.BLOCKED}:
        return RuntimeStatusClass.BLOCKED
    return RuntimeStatusClass.UNKNOWN


def _legacy_event_type_from_runtime_event(event_type: str) -> str:
    for legacy_type, mapped_type in LEGACY_TXN_EVENT_TYPE_MAP.items():
        if mapped_type == event_type:
            return legacy_type
    return event_type


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
