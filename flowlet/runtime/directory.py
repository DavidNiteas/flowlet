"""Guarded create, continue, and rerun operations for runtime directories."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

from .durable_store import RuntimeDurableStore
from .identity import RuntimeIdentity
from .info import RuntimeFileLayout
from .session import RuntimeLeaseConflictError

RuntimeCleanup = Callable[[Path, RuntimeIdentity], None]


class RuntimeDirectoryManager:
    """Own the durable state directory while retaining an external reset lock."""

    def __init__(self, runtime_dir: str | Path, *, layout: RuntimeFileLayout | None = None) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.layout = layout or RuntimeFileLayout()
        self.control_dir = self.runtime_dir / ".control"
        self.lock_path = self.control_dir / "reset_lock.sqlite"
        self.marker_path = self.control_dir / "reset.json"

    @property
    def database_path(self) -> Path:
        """Return the canonical durable database location."""
        return self.runtime_dir / self.layout.durable_database

    def create(self, identity: RuntimeIdentity) -> RuntimeDurableStore:
        """Initialize a clean runtime directory and its immutable identity."""
        with self._exclusive_lock():
            self._recover_pending_reset_locked()
            remaining = [path for path in self.runtime_dir.iterdir() if path != self.control_dir]
            if remaining:
                raise FileExistsError(f"Runtime directory is not empty: {self.runtime_dir}")
            return RuntimeDurableStore.create(self.database_path, identity)

    def continue_runtime(self, *, expected_runtime_id: str) -> RuntimeDurableStore:
        """Open the same lineage after completing any interrupted reset transaction."""
        with self._exclusive_lock():
            self._recover_pending_reset_locked()
            return RuntimeDurableStore(
                self.database_path,
                expected_runtime_id=expected_runtime_id,
            )

    def rerun(
        self,
        new_identity: RuntimeIdentity,
        *,
        expected_runtime_id: str,
        cleanup: RuntimeCleanup | None = None,
        now: float | None = None,
    ) -> RuntimeDurableStore:
        """Abandon an old lineage and initialize a clean generation in place."""
        with self._exclusive_lock():
            self._recover_pending_reset_locked()
            current_store = RuntimeDurableStore(
                self.database_path,
                expected_runtime_id=expected_runtime_id,
            )
            live_session = current_store.active_backend_session(
                now=time.time() if now is None else now
            )
            if live_session is not None:
                raise RuntimeLeaseConflictError(
                    f"Cannot rerun while backend session {live_session.session_id!r} owns the runtime"
                )
            current = current_store.identity()
            if current is None:  # pragma: no cover - store constructor guards this
                raise ValueError("Current runtime identity is unavailable")
            _validate_rerun_identity(current, new_identity)
            if cleanup is not None:
                cleanup(self.runtime_dir, current)
            abandoned = self.runtime_dir.parent / f".{self.runtime_dir.name}.abandoned.{uuid4().hex}"
            self._write_marker(
                {
                    "schema_version": 1,
                    "phase": "prepared",
                    "abandoned_path": str(abandoned),
                    "new_identity": new_identity.model_dump(mode="json"),
                }
            )
            self._complete_reset_locked()
            return RuntimeDurableStore(
                self.database_path,
                expected_runtime_id=new_identity.runtime_id,
            )

    def recover_pending_reset(self) -> RuntimeDurableStore | None:
        """Finish a durable rerun marker left by process termination."""
        with self._exclusive_lock():
            identity = self._recover_pending_reset_locked()
            if identity is None:
                return None
            return RuntimeDurableStore(self.database_path, expected_runtime_id=identity.runtime_id)

    def _recover_pending_reset_locked(self) -> RuntimeIdentity | None:
        if not self.marker_path.exists():
            return None
        payload = json.loads(self.marker_path.read_text(encoding="utf-8"))
        identity = RuntimeIdentity.model_validate(payload["new_identity"])
        self._complete_reset_locked()
        return identity

    def _complete_reset_locked(self) -> None:
        payload = json.loads(self.marker_path.read_text(encoding="utf-8"))
        identity = RuntimeIdentity.model_validate(payload["new_identity"])
        abandoned = Path(payload["abandoned_path"])
        if self.database_path.exists():
            try:
                RuntimeDurableStore(self.database_path, expected_runtime_id=identity.runtime_id)
            except ValueError:
                pass
            else:
                if abandoned.exists():
                    shutil.rmtree(abandoned)
                self.marker_path.unlink()
                _fsync_directory(self.control_dir)
                return
        abandoned.mkdir(parents=True, exist_ok=True)
        for path in list(self.runtime_dir.iterdir()):
            if path == self.control_dir:
                continue
            target = abandoned / path.name
            if target.exists():
                raise FileExistsError(f"Reset staging target already exists: {target}")
            os.replace(path, target)
        _fsync_directory(self.runtime_dir)
        self._write_marker({**payload, "phase": "moved"})
        self._initialize_new_store(identity)
        self._write_marker({**payload, "phase": "initialized"})
        shutil.rmtree(abandoned)
        self.marker_path.unlink()
        _fsync_directory(self.control_dir)

    def _initialize_new_store(self, identity: RuntimeIdentity) -> None:
        if self.database_path.exists():
            RuntimeDurableStore(self.database_path, expected_runtime_id=identity.runtime_id)
            return
        RuntimeDurableStore.create(self.database_path, identity)

    def _write_marker(self, payload: dict[str, Any]) -> None:
        self.control_dir.mkdir(parents=True, exist_ok=True)
        temporary = self.marker_path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.marker_path)
        _fsync_directory(self.control_dir)

    @contextmanager
    def _exclusive_lock(self) -> Iterator[None]:
        self.control_dir.mkdir(parents=True, exist_ok=True)
        with _sqlite_exclusive_lock(self.lock_path):
            yield


@contextmanager
def _sqlite_exclusive_lock(path: Path) -> Iterator[None]:
    connection = sqlite3.connect(path, isolation_level=None, timeout=0.0)
    try:
        _acquire_sqlite_exclusive_lock(connection)
        try:
            yield
        finally:
            connection.rollback()
    finally:
        connection.close()


def _acquire_sqlite_exclusive_lock(connection: sqlite3.Connection) -> None:
    while not _try_acquire_sqlite_exclusive_lock(connection):
        time.sleep(0.05)


def _try_acquire_sqlite_exclusive_lock(connection: sqlite3.Connection) -> bool:
    try:
        connection.execute("BEGIN EXCLUSIVE")
    except sqlite3.OperationalError as exc:
        if not _is_sqlite_lock_error(exc):
            raise
        return False
    return True


def _is_sqlite_lock_error(exc: sqlite3.OperationalError) -> bool:
    error_code = getattr(exc, "sqlite_errorcode", None)
    if error_code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
        return True
    message = str(exc).lower()
    return "locked" in message or "busy" in message


def _validate_rerun_identity(current: RuntimeIdentity, new: RuntimeIdentity) -> None:
    if new.runtime_id == current.runtime_id:
        raise ValueError("Rerun must create a new runtime_id")
    if new.generation != current.generation + 1:
        raise ValueError("Rerun generation must increment by exactly one")
    if new.rerun_of_runtime_id != current.runtime_id:
        raise ValueError("Rerun identity must reference the abandoned runtime_id")
    if (
        current.logical_task_id is not None
        and new.logical_task_id is not None
        and new.logical_task_id != current.logical_task_id
    ):
        raise ValueError("Rerun cannot change logical_task_id")


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
