"""Process-attempt reporting for execution owned by an external scheduler."""

from __future__ import annotations

import time
from typing import Any

from .durable_store import RuntimeDurableStore
from .process import RuntimeProcessOperation, RuntimeProcessSpec
from .recovery import RuntimeProcessAttempt
from .schema import (
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeEventStatus,
    RuntimeEventType,
    RuntimeStatusClass,
)


class RuntimeProcessAttemptReporter:
    """Record externally executed process lifecycles in one durable runtime."""

    def __init__(
        self,
        store: RuntimeDurableStore,
        *,
        execution_id: str,
        backend_session_id: str | None = None,
    ) -> None:
        self.store = store
        self.execution_id = execution_id
        self.backend_session_id = backend_session_id

    def declare(self, spec: RuntimeProcessSpec, *, timestamp: float | None = None) -> bool:
        """Declare a stable logical process once within the runtime lineage."""
        return self.store.declare_process(
            spec, timestamp=time.time() if timestamp is None else timestamp
        )

    def start(
        self,
        process_id: str,
        *,
        operation: RuntimeProcessOperation = RuntimeProcessOperation.START,
        resumed_from_attempt_id: str | None = None,
        checkpoint_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        timestamp: float | None = None,
    ) -> RuntimeProcessAttempt:
        """Allocate and start the next attempt for a declared process."""
        return self.store.begin_process_attempt(
            process_id,
            execution_id=self.execution_id,
            timestamp=time.time() if timestamp is None else timestamp,
            operation=operation,
            backend_session_id=self.backend_session_id,
            resumed_from_attempt_id=resumed_from_attempt_id,
            checkpoint_id=checkpoint_id,
            metadata=metadata,
        )

    def skip(
        self,
        process_id: str,
        *,
        reason: str,
        metadata: dict[str, Any] | None = None,
        timestamp: float | None = None,
    ) -> RuntimeEvent:
        """Report a continuation skip without creating a new process attempt."""
        specs = {
            spec.resolved_process_id(): spec for spec in self.store.load_process_specs()
        }
        spec = specs.get(process_id)
        if spec is None:
            raise KeyError(f"Unknown process_id: {process_id!r}")
        identity = self.store.identity()
        if identity is None:  # pragma: no cover - durable stores require identity
            raise ValueError("Durable runtime store has no identity")
        return self.store.append(
            RuntimeEvent(
                event_id=-1,
                runtime_id=identity.runtime_id,
                execution_id=self.execution_id,
                process_id=process_id,
                parent_process_id=spec.parent_process_id,
                event_type=RuntimeEventType.PROCESS_STATUS_CHANGED,
                timestamp=time.time() if timestamp is None else timestamp,
                status=RuntimeEventStatus.SKIPPED,
                status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
                message=reason,
                payload={"reason": reason, "backend_session_id": self.backend_session_id},
                metadata=metadata or {},
            )
        )

    def complete(
        self,
        attempt_id: str,
        *,
        result: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        timestamp: float | None = None,
    ) -> RuntimeProcessAttempt:
        """Record successful completion of one active attempt."""
        return self.store.finish_process_attempt(
            attempt_id,
            event_type=RuntimeEventType.PROCESS_ATTEMPT_COMPLETED,
            timestamp=time.time() if timestamp is None else timestamp,
            result=result,
            metadata=metadata,
        )

    def fail(
        self,
        attempt_id: str,
        error: RuntimeErrorInfo | Exception,
        *,
        metadata: dict[str, Any] | None = None,
        timestamp: float | None = None,
    ) -> RuntimeProcessAttempt:
        """Record failed completion of one active attempt."""
        return self.store.finish_process_attempt(
            attempt_id,
            event_type=RuntimeEventType.PROCESS_ATTEMPT_FAILED,
            timestamp=time.time() if timestamp is None else timestamp,
            error=_error_info(error),
            metadata=metadata,
        )

    def cancel(
        self,
        attempt_id: str,
        error: RuntimeErrorInfo | Exception | None = None,
        *,
        metadata: dict[str, Any] | None = None,
        timestamp: float | None = None,
    ) -> RuntimeProcessAttempt:
        """Record cancellation of one active attempt."""
        return self.store.finish_process_attempt(
            attempt_id,
            event_type=RuntimeEventType.PROCESS_ATTEMPT_CANCELLED,
            timestamp=time.time() if timestamp is None else timestamp,
            error=_error_info(error) if error is not None else None,
            metadata=metadata,
        )

    def interrupt(
        self,
        attempt_id: str,
        error: RuntimeErrorInfo | Exception | None = None,
        *,
        metadata: dict[str, Any] | None = None,
        timestamp: float | None = None,
    ) -> RuntimeProcessAttempt:
        """Record interruption of one active attempt."""
        return self.store.finish_process_attempt(
            attempt_id,
            event_type=RuntimeEventType.PROCESS_ATTEMPT_INTERRUPTED,
            timestamp=time.time() if timestamp is None else timestamp,
            error=_error_info(error) if error is not None else None,
            metadata=metadata,
        )


def _error_info(error: RuntimeErrorInfo | Exception) -> RuntimeErrorInfo:
    if isinstance(error, RuntimeErrorInfo):
        return error
    return RuntimeErrorInfo(type=type(error).__name__, message=str(error))
