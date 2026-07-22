"""Compatibility adapters for legacy runtime event payloads."""

from __future__ import annotations

from typing import Any

from .schema import RuntimeErrorInfo, RuntimeEvent, RuntimeEventStatus, RuntimeProgress, RuntimeStatusClass

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


def manager_record_to_runtime_event(
    record: Any,
    *,
    record_type: str,
    event_id: int | str,
    runtime_id: str,
    process_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> RuntimeEvent:
    """Convert a Flowlet manager record to a standard RuntimeEvent.

    Supported record types are ``progress``, ``signal``, ``log``, ``stream``,
    and ``telemetry``. The input may be a dict, Pydantic-style model, or
    BaseConfig-style object exposing ``to_dict``.
    """
    payload = _json_safe_record(record)
    event_metadata = {"manager_record_type": record_type, **(metadata or {})}
    if record_type == "progress":
        return _progress_record_to_runtime_event(payload, event_id, runtime_id, process_id, event_metadata)
    if record_type == "signal":
        return _signal_record_to_runtime_event(payload, event_id, runtime_id, process_id, event_metadata)
    if record_type == "log":
        return _payload_event(
            payload,
            event_id=event_id,
            runtime_id=runtime_id,
            process_id=process_id,
            event_type="log.emitted",
            subject_type="log",
            subject_id=_optional_str(payload.get("task_id")),
            metadata=event_metadata,
            message=_optional_str(payload.get("message")),
            timestamp=float(payload.get("timestamp") or 0.0),
        )
    if record_type == "stream":
        return _payload_event(
            payload,
            event_id=event_id,
            runtime_id=runtime_id,
            process_id=process_id,
            event_type="stream.chunk",
            subject_type="stream",
            subject_id=_optional_str(payload.get("task_id") or payload.get("source") or payload.get("stream")),
            metadata=event_metadata,
            message=_optional_str(payload.get("text")),
            timestamp=float(payload.get("timestamp") or 0.0),
        )
    if record_type == "telemetry":
        return _payload_event(
            payload,
            event_id=event_id,
            runtime_id=runtime_id,
            process_id=process_id,
            event_type="metric.sampled",
            subject_type="telemetry",
            subject_id=_optional_str(payload.get("name") or payload.get("event_type")),
            metadata=event_metadata,
            timestamp=float(payload.get("timestamp") or 0.0),
        )
    raise ValueError(f"Unsupported manager record type: {record_type}")


def _status_from_legacy_payload(payload: dict[str, Any]) -> str | RuntimeEventStatus | None:
    status = payload.get("status")
    return str(status) if status is not None else None


def _progress_record_to_runtime_event(
    payload: dict[str, Any],
    event_id: int | str,
    runtime_id: str,
    process_id: str | None,
    metadata: dict[str, Any],
) -> RuntimeEvent:
    status = _status_from_legacy_payload(payload)
    current = payload.get("current") or 0
    total = payload.get("total")
    progress = RuntimeProgress(
        current=current,
        total=total,
        description=_optional_str(payload.get("description")),
    )
    return _payload_event(
        payload,
        event_id=event_id,
        runtime_id=runtime_id,
        process_id=process_id,
        event_type="process.progressed",
        subject_type="progress",
        subject_id=_optional_str(payload.get("task_id")),
        status=status,
        status_class=_status_class(status),
        progress=progress,
        metadata=metadata,
    )


def _signal_record_to_runtime_event(
    payload: dict[str, Any],
    event_id: int | str,
    runtime_id: str,
    process_id: str | None,
    metadata: dict[str, Any],
) -> RuntimeEvent:
    status = _status_from_legacy_payload(payload)
    error = None
    if payload.get("error") is not None:
        error = RuntimeErrorInfo(type="SignalError", message=str(payload["error"]))
    return _payload_event(
        payload,
        event_id=event_id,
        runtime_id=runtime_id,
        process_id=process_id,
        event_type="signal.changed",
        subject_type="signal",
        subject_id=_optional_str(payload.get("name")),
        status=status,
        status_class=_status_class(status),
        error=error,
        metadata=metadata,
        timestamp=float(payload.get("timestamp") or 0.0),
    )


def _payload_event(
    payload: dict[str, Any],
    *,
    event_id: int | str,
    runtime_id: str,
    process_id: str | None,
    event_type: str,
    subject_type: str,
    subject_id: str | None,
    metadata: dict[str, Any],
    timestamp: float | None = None,
    status: str | RuntimeEventStatus | None = None,
    status_class: RuntimeStatusClass | None = None,
    progress: RuntimeProgress | None = None,
    message: str | None = None,
    error: RuntimeErrorInfo | None = None,
) -> RuntimeEvent:
    event_timestamp = timestamp
    if event_timestamp is None:
        event_timestamp = float(payload.get("updated_at") or payload.get("timestamp") or 0.0)
    return RuntimeEvent(
        event_id=event_id,
        runtime_id=runtime_id,
        process_id=process_id,
        event_type=event_type,
        timestamp=event_timestamp,
        subject_type=subject_type,
        subject_id=subject_id,
        status=status,
        status_class=status_class,
        progress=progress,
        message=message,
        error=error,
        payload=payload,
        metadata=metadata,
    )


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


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
