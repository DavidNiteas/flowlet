"""RuntimeEvent store interfaces and JSONL implementation."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Protocol

from .schema import RuntimeEvent

RuntimeEventCursor = int | str


class RuntimeEventStore(Protocol):
    """Minimal store protocol for standard runtime events."""

    def append(self, event: RuntimeEvent) -> RuntimeEvent:
        """Append one event and return the stored event."""
        ...

    def list(self, *, since: RuntimeEventCursor | None = None) -> list[RuntimeEvent]:
        """List events after a numeric or known string event cursor."""
        ...

    def wait_for_next(
        self,
        *,
        since: RuntimeEventCursor | None = None,
        timeout: float | None = None,
    ) -> list[RuntimeEvent]:
        """Wait for matching events or return an empty list when timeout expires."""
        ...

    def load(self) -> None:
        """Load persisted events into memory."""
        ...


class RuntimeEventJsonlStore:
    """Thread-safe in-memory RuntimeEvent store with optional JSONL persistence."""

    def __init__(self, events_path: str | Path | None = None) -> None:
        self.events_path = Path(events_path) if events_path is not None else None
        if self.events_path is not None:
            self.events_path.parent.mkdir(parents=True, exist_ok=True)
        self._events: list[RuntimeEvent] = []
        self._lock = threading.RLock()
        self._changed = threading.Condition(self._lock)

    @classmethod
    def from_file(cls, events_path: str | Path) -> RuntimeEventJsonlStore:
        """Create a store initialized from an existing JSONL file."""
        store = cls(events_path)
        store.load()
        return store

    def append(self, event: RuntimeEvent) -> RuntimeEvent:
        """Append one event and notify waiters."""
        with self._changed:
            self._events.append(event)
            if self.events_path is not None:
                with self.events_path.open("a", encoding="utf-8") as fh:
                    fh.write(event.model_dump_json() + "\n")
            self._changed.notify_all()
            return event

    def list(self, *, since: RuntimeEventCursor | None = None) -> list[RuntimeEvent]:
        """Return stored events, optionally filtering numeric event ids."""
        with self._lock:
            if since is None:
                return list(self._events)
            if isinstance(since, str):
                for index in range(len(self._events) - 1, -1, -1):
                    if str(self._events[index].event_id) == since:
                        return list(self._events[index + 1 :])
                try:
                    numeric_since = int(since)
                except ValueError:
                    return []
                return [event for event in self._events if _numeric_event_id(event) > numeric_since]
            return [event for event in self._events if _numeric_event_id(event) > since]

    def wait_for_next(
        self,
        *,
        since: RuntimeEventCursor | None = None,
        timeout: float | None = None,
    ) -> list[RuntimeEvent]:
        """Wait until matching events are available or timeout expires."""
        with self._changed:
            self._changed.wait_for(lambda: bool(self.list(since=since)), timeout=timeout)
            return self.list(since=since)

    def load(self) -> None:
        """Load events from JSONL, replacing the in-memory event list."""
        if self.events_path is None or not self.events_path.exists():
            return
        with self._changed:
            events: list[RuntimeEvent] = []
            for line in self.events_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                events.append(RuntimeEvent.model_validate_json(line))
            self._events = events
            self._changed.notify_all()

    def replace(self, events: list[RuntimeEvent]) -> None:
        """Replace a compatibility export with one canonical ordered snapshot."""
        with self._changed:
            self._events = list(events)
            if self.events_path is not None:
                temporary = self.events_path.with_name(f".{self.events_path.name}.tmp")
                with temporary.open("w", encoding="utf-8") as fh:
                    for event in self._events:
                        fh.write(event.model_dump_json() + "\n")
                temporary.replace(self.events_path)
            self._changed.notify_all()


def _numeric_event_id(event: RuntimeEvent) -> int:
    if isinstance(event.event_id, int):
        return event.event_id
    try:
        return int(event.event_id)
    except (TypeError, ValueError):
        return -1
