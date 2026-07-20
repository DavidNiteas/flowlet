"""Thread-safe runtime event buffering utilities."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Generic, TypeVar

EventT = TypeVar("EventT")


class EventBuffer(Generic[EventT]):
    """Thread-safe in-memory event buffer with optional JSONL persistence."""

    def __init__(
        self,
        job_id: str,
        events_path: str | Path | None = None,
        *,
        event_factory: Callable[[int, str, float, str, dict[str, Any]], EventT],
        event_loader: Callable[[str], EventT],
        event_id_getter: Callable[[EventT], int],
        event_json_dumper: Callable[[EventT], str],
    ) -> None:
        self.job_id = job_id
        self.events_path = Path(events_path) if events_path is not None else None
        self._event_factory = event_factory
        self._event_loader = event_loader
        self._event_id_getter = event_id_getter
        self._event_json_dumper = event_json_dumper
        if self.events_path is not None:
            self.events_path.parent.mkdir(parents=True, exist_ok=True)
        self._events: list[EventT] = []
        self._next_id = 1
        self._lock = threading.RLock()
        self._changed = threading.Condition(self._lock)

    @classmethod
    def from_file(
        cls,
        job_id: str,
        events_path: str | Path,
        *,
        event_factory: Callable[[int, str, float, str, dict[str, Any]], EventT],
        event_loader: Callable[[str], EventT],
        event_id_getter: Callable[[EventT], int],
        event_json_dumper: Callable[[EventT], str],
    ) -> EventBuffer[EventT]:
        """Create an event buffer initialized from an existing JSONL file."""
        buffer = cls(
            job_id,
            events_path,
            event_factory=event_factory,
            event_loader=event_loader,
            event_id_getter=event_id_getter,
            event_json_dumper=event_json_dumper,
        )
        if buffer.events_path is None or not buffer.events_path.exists():
            return buffer
        buffer.load_from_file()
        return buffer

    def load_from_file(self) -> None:
        """Load persisted events from this buffer's JSONL path."""
        if self.events_path is None or not self.events_path.exists():
            return
        with self._changed:
            for line in self.events_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                event = self._event_loader(line)
                self._events.append(event)
                self._next_id = max(self._next_id, self._event_id_getter(event) + 1)

    def emit(self, event_type: str, payload: dict[str, Any] | None = None) -> EventT:
        """Append one event and notify waiters."""
        with self._changed:
            event = self._event_factory(self._next_id, self.job_id, time.time(), event_type, payload or {})
            self._next_id += 1
            self._events.append(event)
            if self.events_path is not None:
                with self.events_path.open("a", encoding="utf-8") as fh:
                    fh.write(self._event_json_dumper(event) + "\n")
            self._changed.notify_all()
            return event

    def list(self, *, since: int | None = None) -> list[EventT]:
        """Return buffered events, optionally filtering by event id."""
        with self._lock:
            if since is None:
                return list(self._events)
            return [event for event in self._events if self._event_id_getter(event) > since]

    def wait_for_next(self, *, since: int | None = None, timeout: float | None = None) -> list[EventT]:
        """Wait until at least one matching event is available or timeout expires."""
        with self._changed:
            self._changed.wait_for(lambda: bool(self.list(since=since)), timeout=timeout)
            return self.list(since=since)


def sse_encode_event(event: Any, *, event_id: int, event_type: str, payload: dict[str, Any]) -> str:
    """Encode one runtime event as a Server-Sent Events frame."""
    del event
    data = json.dumps(payload, ensure_ascii=False)
    return f"id: {event_id}\nevent: {event_type}\ndata: {data}\n\n"
