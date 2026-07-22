"""Minimal framework runtime executor for registered processes."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .durable_store import RuntimeDurableStore
from .event_store import RuntimeEventJsonlStore, RuntimeEventStore
from .manager_bridge import RuntimeManagerEventBridge
from .process import (
    RuntimeProcess,
    RuntimeProcessContext,
    RuntimeProcessOperation,
    RuntimeProcessRunner,
    RuntimeProcessSpec,
    RuntimeResourceUsage,
    RuntimeUnsupportedOperationError,
)
from .projection import RuntimeFrameworkReducer, RuntimeProjection, RuntimeProjectionPolicy
from .recovery import (
    RuntimeRecoveryAction,
    RuntimeRecoveryExecutionError,
    RuntimeRecoveryPlan,
    RuntimeRecoveryStep,
)
from .reporter import RuntimeProcessAttemptReporter
from .schema import RuntimeErrorInfo, RuntimeEvent, RuntimeEventStatus, RuntimeEventType, RuntimeStatusClass
from .store import RuntimeStore


class RuntimeBackendExecutor:
    """Business-neutral executor that runs registered runtime processes."""

    def __init__(
        self,
        *,
        runtime_id: str,
        execution_id: str | None = None,
        backend_session_id: str | None = None,
        runtime_dir: str | Path | None = None,
        event_store: RuntimeEventStore | None = None,
        manager_bridge: RuntimeManagerEventBridge | None = None,
        projection_policy: RuntimeProjectionPolicy | None = None,
    ) -> None:
        self.runtime_id = runtime_id
        self.execution_id = execution_id
        self.backend_session_id = backend_session_id
        self.runtime_dir = Path(runtime_dir) if runtime_dir is not None else None
        self.event_store = event_store or RuntimeEventJsonlStore(
            RuntimeStore(self.runtime_dir).path(RuntimeStore(self.runtime_dir).layout.runtime_events)
            if self.runtime_dir is not None
            else None
        )
        self.event_store.load()
        if manager_bridge is not None and manager_bridge.event_store is not self.event_store:
            raise ValueError("manager_bridge must write to the executor event_store")
        self.manager_bridge = manager_bridge
        self.projection_policy = projection_policy or RuntimeProjectionPolicy()
        self._processes: dict[str, RuntimeProcess] = {}

    def register(self, process: RuntimeProcess) -> RuntimeProcess:
        """Register a process implementation."""
        process_id = process.spec.resolved_process_id()
        durable_store = self._durable_event_store()
        if durable_store is not None:
            durable_store.declare_process(process.spec, timestamp=time.time())
            self._processes[process_id] = process
            self._write_process_specs()
            self.write_projection()
            return process
        existing_specs = {spec.resolved_process_id(): spec for spec in self._load_process_specs()}
        existing = existing_specs.get(process_id)
        if existing is not None and existing != process.spec:
            raise ValueError(f"Process spec changed within runtime lineage: {process_id!r}")
        self._processes[process_id] = process
        self._write_process_specs()
        if existing is None:
            self._context_for(process).emit_process_created(
                process.spec.process_type,
                display_name=process.spec.display_name,
                metadata=process.spec.metadata,
            )
        self.write_projection()
        return process

    def run_process(self, process_id: str) -> Any:
        """Run one registered process and persist the current projection."""
        process = self._get_process(process_id)
        durable_store = self._durable_event_store()
        if durable_store is not None and self.execution_id is not None:
            reporter = RuntimeProcessAttemptReporter(
                durable_store,
                execution_id=self.execution_id,
                backend_session_id=self.backend_session_id,
            )
            attempt = reporter.start(process_id)
            context = self._context_for(process, attempt_id=attempt.attempt_id)
            self.sync_manager_events()
            try:
                result = RuntimeProcessRunner(context).run(process)
            except Exception as exc:
                reporter.fail(attempt.attempt_id, exc)
                raise
            else:
                reporter.complete(
                    attempt.attempt_id,
                    result=result if isinstance(result, dict) else {"value": result},
                )
                return result
            finally:
                self.sync_manager_events()
                self.write_projection()
        ordinal = len(self.projection().process_attempt_ids.get(process_id, [])) + 1
        attempt_id = f"{process_id}:attempt:{ordinal}"
        context = self._context_for(process, attempt_id=attempt_id)
        attempt_payload = {
            "ordinal": ordinal,
            "operation": RuntimeProcessOperation.START,
            "execution_key": process.spec.execution_key,
            "input_fingerprint": process.spec.input_fingerprint,
            "implementation_version": process.spec.implementation_version,
            "backend_session_id": self.backend_session_id,
        }
        self.sync_manager_events()
        try:
            self._emit_attempt_event(
                context,
                RuntimeEventType.PROCESS_ATTEMPT_CREATED,
                status=RuntimeEventStatus.PENDING,
                status_class=RuntimeStatusClass.NOT_STARTED,
                payload=attempt_payload,
            )
            self._emit_attempt_event(
                context,
                RuntimeEventType.PROCESS_ATTEMPT_STARTED,
                status=RuntimeEventStatus.RUNNING,
                status_class=RuntimeStatusClass.ACTIVE,
                payload=attempt_payload,
            )
            result = RuntimeProcessRunner(context).run(process)
        except Exception as exc:
            self._emit_attempt_event(
                context,
                RuntimeEventType.PROCESS_ATTEMPT_FAILED,
                status=RuntimeEventStatus.FAILED,
                status_class=RuntimeStatusClass.TERMINAL_FAILURE,
                error=RuntimeErrorInfo(type=type(exc).__name__, message=str(exc)),
                payload=attempt_payload,
            )
            raise
        else:
            self._emit_attempt_event(
                context,
                RuntimeEventType.PROCESS_ATTEMPT_COMPLETED,
                status=RuntimeEventStatus.SUCCEEDED,
                status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
                payload={
                    **attempt_payload,
                    "result": result if isinstance(result, dict) else {"value": result},
                },
            )
            return result
        finally:
            self.sync_manager_events()
            self.write_projection()

    def run_all(self) -> dict[str, Any]:
        """Run all registered processes in registration order."""
        results: dict[str, Any] = {}
        for process_id in list(self._processes):
            results[process_id] = self.run_process(process_id)
        return results

    def execute_recovery_plan(self, plan: RuntimeRecoveryPlan) -> dict[str, Any]:
        """Persist and execute an unblocked recovery plan in declared order."""
        if self.runtime_dir is None:
            raise RuntimeRecoveryExecutionError("runtime_dir is required to persist a recovery plan")
        if plan.target_runtime_id != self.runtime_id:
            raise RuntimeRecoveryExecutionError("recovery plan target_runtime_id does not match executor runtime_id")
        blocked = [step.process_id for step in plan.steps if step.action == RuntimeRecoveryAction.BLOCK]
        if blocked:
            raise RuntimeRecoveryExecutionError(f"recovery plan contains blocked processes: {blocked!r}")
        required_process_ids = {
            step.process_id for step in plan.steps if step.action != RuntimeRecoveryAction.SKIP
        }
        missing = required_process_ids.difference(self._processes)
        if missing:
            raise RuntimeRecoveryExecutionError(f"recovery processes are not registered: {sorted(missing)!r}")
        completed_steps: set[str] = set()
        for step in plan.steps:
            unavailable_dependencies = set(step.depends_on).difference(completed_steps)
            if unavailable_dependencies:
                raise RuntimeRecoveryExecutionError(
                    f"recovery step {step.process_id!r} precedes dependencies: "
                    f"{sorted(unavailable_dependencies)!r}"
                )
            completed_steps.add(step.process_id)

        RuntimeStore(self.runtime_dir).write_recovery_plan(plan)
        self._emit_recovery_event(
            RuntimeEventType.RECOVERY_STARTED,
            status=RuntimeEventStatus.RUNNING,
            status_class=RuntimeStatusClass.ACTIVE,
            payload={"plan_id": plan.plan_id, "source_runtime_id": plan.source_runtime_id},
        )
        results: dict[str, Any] = {}
        try:
            for step in plan.steps:
                if step.action == RuntimeRecoveryAction.SKIP:
                    self._emit_skip(step)
                    results[step.process_id] = None
                    continue
                results[step.process_id] = self._execute_recovery_step(step)
        except Exception as exc:
            self._emit_recovery_event(
                RuntimeEventType.RECOVERY_FAILED,
                status=RuntimeEventStatus.FAILED,
                status_class=RuntimeStatusClass.TERMINAL_FAILURE,
                error=RuntimeErrorInfo(type=type(exc).__name__, message=str(exc)),
                payload={"plan_id": plan.plan_id},
            )
            self.write_projection()
            raise
        self._emit_recovery_event(
            RuntimeEventType.RECOVERY_COMPLETED,
            status=RuntimeEventStatus.SUCCEEDED,
            status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
            payload={"plan_id": plan.plan_id},
        )
        self.write_projection()
        return results

    def cancel_process(self, process_id: str) -> None:
        """Dispatch a cancel hook when supported."""
        self._dispatch_hook(
            process_id,
            RuntimeProcessOperation.CANCEL,
            terminal_status=RuntimeEventStatus.CANCELLED,
            terminal_message="process cancelled",
            request_cancel=True,
        )

    def pause_process(self, process_id: str) -> None:
        """Dispatch a pause hook when supported."""
        self._dispatch_hook(
            process_id,
            RuntimeProcessOperation.PAUSE,
            terminal_status="paused",
            terminal_message="process paused",
        )

    def resume_process(self, process_id: str) -> None:
        """Dispatch a resume hook when supported."""
        self._dispatch_hook(
            process_id,
            RuntimeProcessOperation.RESUME,
            terminal_status=RuntimeEventStatus.RUNNING,
            terminal_message="process resumed",
        )

    def retry_process(self, process_id: str) -> Any:
        """Dispatch a retry hook when supported."""
        return self._dispatch_hook(
            process_id,
            RuntimeProcessOperation.RETRY,
            terminal_status=RuntimeEventStatus.SUCCEEDED,
            terminal_message="process retried",
        )

    def cleanup_process(self, process_id: str) -> None:
        """Dispatch a cleanup hook when supported."""
        self._dispatch_hook(
            process_id,
            RuntimeProcessOperation.CLEANUP,
            terminal_status=RuntimeEventStatus.SUCCEEDED,
            terminal_message="process cleaned up",
        )

    def sample_process_resources(self, process_id: str) -> RuntimeResourceUsage:
        """Collect a process resource observation and append a standard event."""
        process = self._get_process(process_id)
        context = self._context_for(process)
        self.sync_manager_events()
        try:
            usage = process.resources(context)
            if not isinstance(usage, RuntimeResourceUsage):
                usage = RuntimeResourceUsage.model_validate(usage)
            context.emit_resource_usage(usage)
            return usage
        finally:
            self.sync_manager_events()
            self.write_projection()

    def projection(self) -> RuntimeProjection:
        """Return the current framework projection."""
        durable_store = self._durable_event_store()
        if durable_store is not None:
            return durable_store.refresh_projection(
                RuntimeFrameworkReducer(policy=self.projection_policy)
            )
        return RuntimeFrameworkReducer(policy=self.projection_policy).reduce(self.event_store.list())

    def sync_manager_events(self) -> list[Any]:
        """Synchronize the optional manager bridge without owning its lifecycle."""
        if self.manager_bridge is None:
            return []
        return self.manager_bridge.sync()

    def write_projection(self) -> RuntimeProjection:
        """Persist and return the current framework projection."""
        projection = self.projection()
        if self.runtime_dir is not None:
            RuntimeStore(self.runtime_dir).write_projection(projection.model_dump(mode="json"))
        return projection

    def _write_process_specs(self) -> None:
        specs = {spec.resolved_process_id(): spec for spec in self._load_process_specs()}
        specs.update({process.spec.resolved_process_id(): process.spec for process in self._processes.values()})
        values = list(specs.values())
        durable_store = self._durable_event_store()
        if durable_store is not None:
            durable_store.write_process_specs(values)
        if self.runtime_dir is not None:
            RuntimeStore(self.runtime_dir).write_process_specs(values)

    def _load_process_specs(self) -> list[RuntimeProcessSpec]:
        durable_store = self._durable_event_store()
        if durable_store is not None:
            return durable_store.load_process_specs()
        if self.runtime_dir is not None:
            return RuntimeStore(self.runtime_dir).load_process_specs()
        return []

    def _durable_event_store(self) -> RuntimeDurableStore | None:
        if isinstance(self.event_store, RuntimeDurableStore):
            return self.event_store
        value = getattr(self.event_store, "durable_store", None)
        return value if isinstance(value, RuntimeDurableStore) else None

    def _context_for(
        self,
        process: RuntimeProcess,
        *,
        attempt_id: str | None = None,
        checkpoint_id: str | None = None,
    ) -> RuntimeProcessContext:
        process_id = process.spec.resolved_process_id()
        return RuntimeProcessContext(
            runtime_id=self.runtime_id,
            process_id=process_id,
            parent_process_id=process.spec.parent_process_id,
            execution_id=self.execution_id,
            attempt_id=attempt_id,
            checkpoint_id=checkpoint_id,
            event_store=self.event_store,
            runtime_dir=self.runtime_dir,
            metadata=process.spec.metadata,
            event_id_start=_next_event_id(self.event_store.list()),
        )

    def _execute_recovery_step(self, step: RuntimeRecoveryStep) -> Any:
        process = self._get_process(step.process_id)
        existing_attempts = self.projection().process_attempt_ids.get(step.process_id, [])
        ordinal = len(existing_attempts) + 1
        attempt_id = f"{step.process_id}:attempt:{ordinal}"
        checkpoint_id = step.checkpoint.checkpoint_id if step.checkpoint is not None else None
        context = self._context_for(process, attempt_id=attempt_id, checkpoint_id=checkpoint_id)
        attempt_payload: dict[str, Any] = {
            "ordinal": ordinal,
            "recovery_action": step.action,
            "execution_key": process.spec.execution_key,
            "input_fingerprint": process.spec.input_fingerprint,
            "implementation_version": process.spec.implementation_version,
            "backend_session_id": self.backend_session_id,
        }
        if step.source_attempt_id is not None:
            attempt_payload["resumed_from_attempt_id"] = step.source_attempt_id
        self._emit_attempt_event(
            context,
            RuntimeEventType.PROCESS_ATTEMPT_CREATED,
            status=RuntimeEventStatus.PENDING,
            status_class=RuntimeStatusClass.NOT_STARTED,
            payload=attempt_payload,
        )
        self._emit_attempt_event(
            context,
            RuntimeEventType.PROCESS_ATTEMPT_STARTED,
            status=RuntimeEventStatus.RUNNING,
            status_class=RuntimeStatusClass.ACTIVE,
            payload=attempt_payload,
        )
        try:
            if step.action == RuntimeRecoveryAction.RESUME:
                result = process.resume(context)
            elif step.action == RuntimeRecoveryAction.RETRY:
                result = process.retry(context)
            elif step.action == RuntimeRecoveryAction.RESTART:
                result = process.start(context)
            else:  # pragma: no cover - guarded by plan validation
                raise RuntimeRecoveryExecutionError(f"unsupported recovery action: {step.action}")
        except Exception as exc:
            self._emit_attempt_event(
                context,
                RuntimeEventType.PROCESS_ATTEMPT_FAILED,
                status=RuntimeEventStatus.FAILED,
                status_class=RuntimeStatusClass.TERMINAL_FAILURE,
                error=RuntimeErrorInfo(type=type(exc).__name__, message=str(exc)),
                payload=attempt_payload,
            )
            raise
        self._emit_attempt_event(
            context,
            RuntimeEventType.PROCESS_ATTEMPT_COMPLETED,
            status=RuntimeEventStatus.SUCCEEDED,
            status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
            payload={
                **attempt_payload,
                "result": result if isinstance(result, dict) else {"value": result},
            },
        )
        return result

    def _emit_attempt_event(
        self,
        context: RuntimeProcessContext,
        event_type: RuntimeEventType,
        *,
        status: RuntimeEventStatus,
        status_class: RuntimeStatusClass,
        payload: dict[str, Any],
        error: RuntimeErrorInfo | None = None,
    ) -> RuntimeEvent:
        return context.emit(
            RuntimeEvent(
                event_id=_next_event_id(self.event_store.list()),
                runtime_id=self.runtime_id,
                process_id=context.process_id,
                parent_process_id=context.parent_process_id,
                execution_id=context.execution_id,
                attempt_id=context.attempt_id,
                checkpoint_id=context.checkpoint_id,
                event_type=event_type,
                timestamp=time.time(),
                status=status,
                status_class=status_class,
                error=error,
                payload=payload,
            )
        )

    def _emit_skip(self, step: RuntimeRecoveryStep) -> RuntimeEvent:
        return self.event_store.append(
            RuntimeEvent(
                event_id=_next_event_id(self.event_store.list()),
                runtime_id=self.runtime_id,
                execution_id=self.execution_id,
                process_id=step.process_id,
                event_type=RuntimeEventType.PROCESS_STATUS_CHANGED,
                timestamp=time.time(),
                status=RuntimeEventStatus.SKIPPED,
                status_class=RuntimeStatusClass.TERMINAL_SUCCESS,
                payload={
                    "recovery_action": step.action,
                    "source_attempt_id": step.source_attempt_id,
                    "reason": step.reason,
                },
            )
        )

    def _emit_recovery_event(
        self,
        event_type: RuntimeEventType,
        *,
        status: RuntimeEventStatus,
        status_class: RuntimeStatusClass,
        payload: dict[str, Any],
        error: RuntimeErrorInfo | None = None,
    ) -> RuntimeEvent:
        return self.event_store.append(
            RuntimeEvent(
                event_id=_next_event_id(self.event_store.list()),
                runtime_id=self.runtime_id,
                execution_id=self.execution_id,
                event_type=event_type,
                timestamp=time.time(),
                status=status,
                status_class=status_class,
                error=error,
                payload=payload,
            )
        )

    def _get_process(self, process_id: str) -> RuntimeProcess:
        try:
            return self._processes[process_id]
        except KeyError as exc:
            raise KeyError(f"Runtime process {process_id!r} is not registered.") from exc

    def _dispatch_hook(
        self,
        process_id: str,
        operation: RuntimeProcessOperation,
        *,
        terminal_status: RuntimeEventStatus | str,
        terminal_message: str,
        request_cancel: bool = False,
    ) -> Any:
        process = self._get_process(process_id)
        context = self._context_for(process)
        self.sync_manager_events()
        if not process.spec.capabilities.supports(operation):
            error = RuntimeUnsupportedOperationError(process_id, operation)
            context.emit_error(error.to_error_info(), event_type=f"process.{operation.value}.unsupported")
            self.write_projection()
            raise error
        if request_cancel:
            context.request_cancel()
        try:
            result = getattr(process, operation.value)(context)
        except RuntimeUnsupportedOperationError as exc:
            context.emit_error(exc.to_error_info(), event_type=f"process.{exc.operation.value}.unsupported")
            raise
        else:
            context.emit_status(
                terminal_status,
                message=terminal_message,
                payload={"result": result if isinstance(result, dict) else {"value": result}},
            )
            return result
        finally:
            self.sync_manager_events()
            self.write_projection()


def _next_event_id(events: list[Any]) -> int:
    next_id = 0
    for event in events:
        numeric_id = _numeric_event_id(event)
        if numeric_id is not None:
            next_id = max(next_id, numeric_id + 1)
    return next_id


def _numeric_event_id(event: Any) -> int | None:
    try:
        return int(event.event_id)
    except (AttributeError, TypeError, ValueError):
        return None
