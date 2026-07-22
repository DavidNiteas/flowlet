"""RuntimeEvent store interfaces and JSONL implementation."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Protocol

from .schema import RuntimeEvent


class RuntimeEventStore(Protocol):
    """Minimal store protocol for standard runtime events."""

    def append(self, event: RuntimeEvent) -> RuntimeEvent:
        """Append one event and return the stored event."""
        ...

    def list(self, *, since: int | None = None) -> list[RuntimeEvent]:
        """List events, optionally filtering numeric event ids greater than ``since``."""
        ...

    def wait_for_next(self, *, since: int | None = None, timeout: float | None = None) -> list[RuntimeEvent]:
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

    def list(self, *, since: int | None = None) -> list[RuntimeEvent]:
        """Return stored events, optionally filtering numeric event ids."""
        with self._lock:
            if since is None:
                return list(self._events)
            return [event for event in self._events if _numeric_event_id(event) > since]

    def wait_for_next(self, *, since: int | None = None, timeout: float | None = None) -> list[RuntimeEvent]:
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


def _numeric_event_id(event: RuntimeEvent) -> int:
    if isinstance(event.event_id, int):
        return event.event_id
    try:
        return int(event.event_id)
    except (TypeError, ValueError):
        return -1
