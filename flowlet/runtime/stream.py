"""Standard RuntimeEventStore waiting, streaming, and SSE helpers."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator

from .event_store import RuntimeEventCursor, RuntimeEventStore
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
