"""Generic process adapters for existing callable and FSM units."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..fsm import FSMEdgeNode, FSMRuntime, FSMStatus, StateMachineSpec
from .process import RuntimeProcessBase, RuntimeProcessContext, RuntimeProcessSpec
from .schema import RuntimeErrorInfo, RuntimeEventStatus


class CallableRuntimeProcess(RuntimeProcessBase):
    """Wrap a callable as one restartable runtime process."""

    def __init__(
        self,
        spec: RuntimeProcessSpec,
        target: Callable[..., Any],
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        pass_context: bool = False,
    ) -> None:
        super().__init__(spec)
        self.target = target
        self.args = args
        self.kwargs = kwargs or {}
        self.pass_context = pass_context

    def start(self, context: RuntimeProcessContext) -> Any:
        if self.pass_context:
            return self.target(*self.args, runtime_context=context, **self.kwargs)
        return self.target(*self.args, **self.kwargs)

    def retry(self, context: RuntimeProcessContext) -> Any:
        return self.start(context)


class FSMRuntimeProcess(RuntimeProcessBase):
    """Wrap an FSMEdgeNode or StateMachineSpec as one runtime process."""

    def __init__(
        self,
        spec: RuntimeProcessSpec,
        fsm: FSMEdgeNode | StateMachineSpec,
        *,
        backend: str = "thread",
        initializer: Callable[[], dict[str, Any]] | None = None,
        events: list[str | None] | tuple[str | None, ...] | None = None,
        max_steps: int = 100,
    ) -> None:
        super().__init__(spec)
        self.fsm = fsm
        self.backend = backend
        self.initializer = initializer
        self.events = events
        self.max_steps = max_steps

    def start(self, context: RuntimeProcessContext) -> dict[str, Any]:
        if isinstance(self.fsm, FSMEdgeNode):
            runtime = self.fsm.run_until_terminal(self.events, max_steps=self.max_steps)
            _emit_fsm_history(context, runtime)
            if runtime.status == FSMStatus.FAILED:
                raise RuntimeError(runtime.last_error or "FSM process failed")
            return _runtime_payload(runtime)
        with FSMEdgeNode(
            self.fsm,
            backend=self.backend,  # type: ignore[arg-type]
            initializer=self.initializer,
        ) as fsm:
            runtime = fsm.run_until_terminal(self.events, max_steps=self.max_steps)
            _emit_fsm_history(context, runtime)
            if runtime.status == FSMStatus.FAILED:
                raise RuntimeError(runtime.last_error or "FSM process failed")
            return _runtime_payload(runtime)

    def retry(self, context: RuntimeProcessContext) -> dict[str, Any]:
        return self.start(context)


def _emit_fsm_history(context: RuntimeProcessContext, runtime: FSMRuntime) -> None:
    for index, record in enumerate(runtime.history, start=1):
        event_type = (
            "fsm.transition.failed" if record.error is not None else "fsm.transition.completed"
        )
        context.emit_unit(
            event_type,
            f"fsm:{record.transition}:{index}",
            unit_type="fsm.transition",
            subject_type="fsm.transition",
            subject_id=record.transition,
            status=RuntimeEventStatus.FAILED if record.error is not None else RuntimeEventStatus.SUCCEEDED,
            error=RuntimeErrorInfo(type="FSMTransitionError", message=record.error) if record.error else None,
            payload={
                "transition": record.transition,
                "source": record.source,
                "target": record.target,
                "event": record.event,
                "elapsed": record.elapsed,
            },
        )


def _runtime_payload(runtime: FSMRuntime) -> dict[str, Any]:
    return {
        "state": runtime.state,
        "status": runtime.status,
        "last_error": runtime.last_error,
        "transition_count": len(runtime.history),
    }
