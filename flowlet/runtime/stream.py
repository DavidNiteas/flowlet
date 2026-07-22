"""Standard RuntimeEventStore waiting, streaming, and SSE helpers."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from pathlib import Path

from .event_store import RuntimeEventCursor, RuntimeEventJsonlStore, RuntimeEventStore
from .schema import RuntimeEvent


def wait_runtime_event_store(
    event_store: RuntimeEventStore,
    *,
    since: RuntimeEventCursor | None = None,
    timeout: float | None = None,
) -> list[RuntimeEvent]:
    """Wait for standard events from a live event store."""
    return event_store.wait_for_next(since=since, timeout=timeout)


def stream_runtime_event_store(
    event_store: RuntimeEventStore,
    *,
    since: RuntimeEventCursor | None = None,
    heartbeat_interval: float = 1.0,
    idle_timeout: float | None = None,
    is_terminal: Callable[[RuntimeEvent], bool] | None = None,
) -> Iterator[RuntimeEvent | None]:
    """Yield standard events and heartbeat markers from a live event store.

    A terminal predicate is intentionally opt-in. Flowlet cannot infer whether
    a terminal process event ends an enclosing business runtime.
    """
    last_id = since
    last_emit = time.time()
    idle_started = time.time()
    while True:
        batch = wait_runtime_event_store(event_store, since=last_id, timeout=heartbeat_interval)
        if batch:
            idle_started = time.time()
            for event in batch:
                last_id = event.event_id
                last_emit = time.time()
                yield event
                if is_terminal is not None and is_terminal(event):
                    for trailing in event_store.list(since=last_id):
                        last_id = trailing.event_id
                        yield trailing
                    return
            continue
        now = time.time()
        if idle_timeout is not None and now - idle_started >= idle_timeout:
            return
        if now - last_emit >= heartbeat_interval:
            last_emit = now
            yield None


def sse_encode_runtime_event(event: RuntimeEvent) -> str:
    """Encode a standard runtime event as one Server-Sent Events frame."""
    data = json.dumps(event.model_dump(mode="json"), ensure_ascii=False)
    return f"id: {event.event_id}\nevent: {event.event_type}\ndata: {data}\n\n"


def stream_runtime_event_jsonl(
    events_path: str | Path,
    *,
    since: RuntimeEventCursor | None = None,
    heartbeat_interval: float = 1.0,
    idle_timeout: float | None = None,
    is_terminal: Callable[[RuntimeEvent], bool] | None = None,
) -> Iterator[RuntimeEvent | None]:
    """Poll an append-only runtime event JSONL file as a standard event stream."""
    if heartbeat_interval <= 0:
        raise ValueError("heartbeat_interval must be > 0")
    last_id = since
    last_emit = time.time()
    idle_started = time.time()
    path = Path(events_path)
    while True:
        store = RuntimeEventJsonlStore.from_file(path)
        batch = store.list(since=last_id)
        if batch:
            idle_started = time.time()
            for event in batch:
                last_id = event.event_id
                last_emit = time.time()
                yield event
                if is_terminal is not None and is_terminal(event):
                    for trailing in store.list(since=last_id):
                        last_id = trailing.event_id
                        yield trailing
                    return
            continue
        now = time.time()
        if idle_timeout is not None and now - idle_started >= idle_timeout:
            return
        if now - last_emit >= heartbeat_interval:
            last_emit = now
            yield None
        time.sleep(min(heartbeat_interval, 0.1))


def parse_sse_runtime_events(lines: Iterator[str]) -> Iterator[RuntimeEvent]:
    """Parse standard RuntimeEvent SSE frames emitted by this module."""
    fields: dict[str, list[str]] = {}
    for raw_line in lines:
        line = raw_line.rstrip("\r\n")
        if not line:
            event = _runtime_event_from_sse_fields(fields)
            fields = {}
            if event is not None:
                yield event
            continue
        if line.startswith(":"):
            continue
        name, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        fields.setdefault(name, []).append(value)
    event = _runtime_event_from_sse_fields(fields)
    if event is not None:
        yield event


def _runtime_event_from_sse_fields(fields: dict[str, list[str]]) -> RuntimeEvent | None:
    data = fields.get("data")
    if not data:
        return None
    return RuntimeEvent.model_validate_json("\n".join(data))
