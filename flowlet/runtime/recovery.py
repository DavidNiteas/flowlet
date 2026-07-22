"""Business-neutral recovery declarations and plan contracts."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field, field_validator, model_validator

from .process import RuntimeCheckpointMode, RuntimeIdempotency, RuntimeProcessOperation, RuntimeProcessSpec
from .schema import RuntimeErrorInfo, RuntimeEventStatus, RuntimeStatusClass

if TYPE_CHECKING:
    from .projection import RuntimeProjection


class RuntimeRecoveryAction(StrEnum):
    """Framework recovery actions selected for a logical process."""

    SKIP = "skip"
    RESUME = "resume"
    RETRY = "retry"
    RESTART = "restart"
    BLOCK = "block"


class RuntimeRecoveryGraphError(ValueError):
    """Raised when process declarations do not form a valid dependency DAG."""


class RuntimeRecoveryPlanError(ValueError):
    """Raised when supplied recovery decisions cannot form a complete plan."""


class RuntimeRecoveryExecutionError(RuntimeError):
    """Raised before dispatch when a recovery plan is not executable."""


class RuntimeProcessGraph(BaseModel):
    """Validated dependency graph with deterministic topological ordering."""

    process_ids: list[str]
    topological_order: list[str]
    dependencies: dict[str, list[str]]
    dependents: dict[str, list[str]]

    def downstream(self, process_ids: list[str] | set[str]) -> list[str]:
        """Return all transitive dependents in topological order."""
        unknown = set(process_ids).difference(self.process_ids)
        if unknown:
            raise KeyError(f"Unknown process ids: {sorted(unknown)!r}")
        found: set[str] = set()
        pending = list(process_ids)
        while pending:
            process_id = pending.pop()
            for dependent in self.dependents[process_id]:
                if dependent not in found:
                    found.add(dependent)
                    pending.append(dependent)
        return [process_id for process_id in self.topological_order if process_id in found]


def build_runtime_process_graph(specs: list[RuntimeProcessSpec]) -> RuntimeProcessGraph:
    """Validate declarations and return their deterministic dependency DAG."""
    specs_by_id: dict[str, RuntimeProcessSpec] = {}
    for spec in specs:
        process_id = spec.resolved_process_id()
        if process_id in specs_by_id:
            raise RuntimeRecoveryGraphError(f"Duplicate process_id: {process_id!r}")
        specs_by_id[process_id] = spec

    process_ids = sorted(specs_by_id)
    dependencies: dict[str, list[str]] = {}
    dependents = {process_id: [] for process_id in process_ids}
    for process_id in process_ids:
        declared = sorted(specs_by_id[process_id].depends_on)
        if process_id in declared:
            raise RuntimeRecoveryGraphError(f"Process {process_id!r} cannot depend on itself")
        missing = set(declared).difference(specs_by_id)
        if missing:
            raise RuntimeRecoveryGraphError(
                f"Process {process_id!r} has unknown dependencies: {sorted(missing)!r}"
            )
        dependencies[process_id] = declared
        for dependency in declared:
            dependents[dependency].append(process_id)
    for values in dependents.values():
        values.sort()

    remaining_dependency_count = {
        process_id: len(dependencies[process_id]) for process_id in process_ids
    }
    ready = sorted(
        process_id for process_id, count in remaining_dependency_count.items() if count == 0
    )
    topological_order: list[str] = []
    while ready:
        process_id = ready.pop(0)
        topological_order.append(process_id)
        for dependent in dependents[process_id]:
            remaining_dependency_count[dependent] -= 1
            if remaining_dependency_count[dependent] == 0:
                ready.append(dependent)
                ready.sort()
    if len(topological_order) != len(process_ids):
        cycle_process_ids = sorted(set(process_ids).difference(topological_order))
        raise RuntimeRecoveryGraphError(
            f"Process dependency graph contains a cycle involving: {cycle_process_ids!r}"
        )

    return RuntimeProcessGraph(
        process_ids=process_ids,
        topological_order=topological_order,
        dependencies=dependencies,
        dependents=dependents,
    )


class RuntimeCheckpointRef(BaseModel):
    """Portable reference to a business-owned recoverable checkpoint."""

    checkpoint_id: str
    process_id: str
    attempt_id: str
    created_at: float
    uri: str | None = None
    cursor: dict[str, Any] | None = None
    input_fingerprint: str | None = None
    implementation_version: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("checkpoint_id", "process_id", "attempt_id")
    @classmethod
    def _validate_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("checkpoint identity fields must not be empty")
        return value


class RuntimeProcessAttempt(BaseModel):
    """Persistable state of one execution attempt for a logical process."""

    attempt_id: str
    process_id: str
    runtime_id: str
    execution_id: str | None = None
    ordinal: int
    operation: RuntimeProcessOperation | None = None
    execution_key: str | None = None
    input_fingerprint: str | None = None
    implementation_version: str | None = None
    status: RuntimeEventStatus | str = RuntimeEventStatus.PENDING
    status_class: RuntimeStatusClass = RuntimeStatusClass.NOT_STARTED
    started_at: float | None = None
    finished_at: float | None = None
    resumed_from_attempt_id: str | None = None
    checkpoint_id: str | None = None
    backend_session_id: str | None = None
    first_event_sequence: int | None = None
    last_event_sequence: int | None = None
    error: RuntimeErrorInfo | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("ordinal")
    @classmethod
    def _validate_ordinal(cls, value: int) -> int:
        if value < 1:
            raise ValueError("attempt ordinal must be >= 1")
        return value


class RuntimeRecoveryDecision(BaseModel):
    """One package-supplied recovery decision consumed by a planner."""

    process_id: str
    action: RuntimeRecoveryAction
    reason: str
    source_attempt_id: str | None = None
    checkpoint: RuntimeCheckpointRef | None = None
    invalidates: list[str] = Field(default_factory=list)
    preserve_on_upstream_reexecution: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_checkpoint_action(self) -> RuntimeRecoveryDecision:
        if self.action == RuntimeRecoveryAction.RESUME and self.checkpoint is None:
            raise ValueError("resume recovery decisions require a checkpoint")
        if self.checkpoint is not None and self.checkpoint.process_id != self.process_id:
            raise ValueError("checkpoint process_id must match the recovery process_id")
        return self


class RuntimeContinuationAssessment(BaseModel):
    """Package evidence consumed by framework DAG continuation selection."""

    process_id: str
    completed_output_valid: bool = False
    checkpoint_id: str | None = None
    preferred_action: RuntimeRecoveryAction | None = None
    force_reexecute: bool = False
    cleanup_completed: bool = False
    preserve_on_upstream_reexecution: bool = False
    invalidates: list[str] = Field(default_factory=list)
    reason: str = "runtime state assessment"
    metadata: dict[str, Any] = Field(default_factory=dict)


class RuntimeRecoveryStep(BaseModel):
    """Ordered, reviewable action in a persisted local recovery plan."""

    process_id: str
    action: RuntimeRecoveryAction
    reason: str
    depends_on: list[str] = Field(default_factory=list)
    source_attempt_id: str | None = None
    checkpoint: RuntimeCheckpointRef | None = None
    invalidates: list[str] = Field(default_factory=list)
    preserve_on_upstream_reexecution: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_checkpoint_action(self) -> RuntimeRecoveryStep:
        if self.action == RuntimeRecoveryAction.RESUME and self.checkpoint is None:
            raise ValueError("resume recovery steps require a checkpoint")
        if self.checkpoint is not None and self.checkpoint.process_id != self.process_id:
            raise ValueError("checkpoint process_id must match the recovery process_id")
        return self


class RuntimeRecoveryPlan(BaseModel):
    """Persisted recovery plan produced before any business action runs."""

    schema_version: int = 1
    plan_id: str
    source_runtime_id: str
    target_runtime_id: str
    created_at: float
    steps: list[RuntimeRecoveryStep]
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_steps(self) -> RuntimeRecoveryPlan:
        process_ids = [step.process_id for step in self.steps]
        if len(process_ids) != len(set(process_ids)):
            raise ValueError("recovery plan process_ids must be unique")
        return self


class RuntimeRecoveryPlanner:
    """Build deterministic plans from framework state and package decisions."""

    def create_plan(
        self,
        *,
        specs: list[RuntimeProcessSpec],
        source_projection: RuntimeProjection,
        decisions: list[RuntimeRecoveryDecision],
        plan_id: str,
        target_runtime_id: str,
        created_at: float,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeRecoveryPlan:
        """Validate package decisions and return a reviewable recovery plan."""
        graph = build_runtime_process_graph(specs)
        specs_by_id = {spec.resolved_process_id(): spec for spec in specs}
        decisions_by_id = {decision.process_id: decision for decision in decisions}
        if len(decisions_by_id) != len(decisions):
            raise RuntimeRecoveryPlanError("Recovery decisions must contain unique process_ids")
        missing = set(graph.process_ids).difference(decisions_by_id)
        unknown = set(decisions_by_id).difference(graph.process_ids)
        if missing or unknown:
            raise RuntimeRecoveryPlanError(
                f"Recovery decision coverage mismatch: missing={sorted(missing)!r}, unknown={sorted(unknown)!r}"
            )

        steps_by_id: dict[str, RuntimeRecoveryStep] = {}
        reexecuted: set[str] = set()
        for process_id in graph.topological_order:
            decision = decisions_by_id[process_id]
            action, reason = self._eligible_action(
                spec=specs_by_id[process_id],
                projection=source_projection,
                decision=decision,
            )
            blocking_dependencies = [
                dependency
                for dependency in graph.dependencies[process_id]
                if steps_by_id[dependency].action == RuntimeRecoveryAction.BLOCK
            ]
            changed_dependencies = [
                dependency for dependency in graph.dependencies[process_id] if dependency in reexecuted
            ]
            if blocking_dependencies:
                action = RuntimeRecoveryAction.BLOCK
                reason = f"blocked by dependencies: {blocking_dependencies!r}"
            elif (
                action == RuntimeRecoveryAction.SKIP
                and changed_dependencies
                and not decision.preserve_on_upstream_reexecution
            ):
                action = RuntimeRecoveryAction.BLOCK
                reason = f"upstream reexecution invalidates skip: {changed_dependencies!r}"

            invalidates = set(decision.invalidates)
            if action in {
                RuntimeRecoveryAction.RESUME,
                RuntimeRecoveryAction.RETRY,
                RuntimeRecoveryAction.RESTART,
            }:
                reexecuted.add(process_id)
                invalidates.update(graph.downstream({process_id}))
            steps_by_id[process_id] = RuntimeRecoveryStep(
                process_id=process_id,
                action=action,
                reason=reason,
                depends_on=graph.dependencies[process_id],
                source_attempt_id=decision.source_attempt_id,
                checkpoint=decision.checkpoint,
                invalidates=sorted(invalidates),
                preserve_on_upstream_reexecution=decision.preserve_on_upstream_reexecution,
                metadata=decision.metadata,
            )

        return RuntimeRecoveryPlan(
            plan_id=plan_id,
            source_runtime_id=source_projection.runtime_id,
            target_runtime_id=target_runtime_id,
            created_at=created_at,
            steps=[steps_by_id[process_id] for process_id in graph.topological_order],
            metadata=metadata or {},
        )

    @staticmethod
    def _eligible_action(
        *,
        spec: RuntimeProcessSpec,
        projection: RuntimeProjection,
        decision: RuntimeRecoveryDecision,
    ) -> tuple[RuntimeRecoveryAction, str]:
        process_id = spec.resolved_process_id()
        state = projection.processes.get(process_id)
        if decision.action == RuntimeRecoveryAction.SKIP:
            if state is None or state.status_class != RuntimeStatusClass.TERMINAL_SUCCESS:
                return RuntimeRecoveryAction.BLOCK, "skip requires a terminal-success source process"
        elif decision.action == RuntimeRecoveryAction.RESUME:
            if not spec.capabilities.supports(RuntimeProcessOperation.RESUME):
                return RuntimeRecoveryAction.BLOCK, "process does not declare resume capability"
            if spec.checkpoint_policy.mode == RuntimeCheckpointMode.NONE:
                return RuntimeRecoveryAction.BLOCK, "process does not declare recoverable checkpoints"
            checkpoint = decision.checkpoint
            if checkpoint is None or projection.checkpoints.get(checkpoint.checkpoint_id) != checkpoint:
                return RuntimeRecoveryAction.BLOCK, "checkpoint is not committed in the source projection"
            if spec.input_fingerprint is not None and checkpoint.input_fingerprint != spec.input_fingerprint:
                return RuntimeRecoveryAction.BLOCK, "checkpoint input fingerprint does not match"
            if (
                spec.implementation_version is not None
                and checkpoint.implementation_version != spec.implementation_version
            ):
                return RuntimeRecoveryAction.BLOCK, "checkpoint implementation version does not match"
        elif decision.action == RuntimeRecoveryAction.RETRY:
            if not spec.capabilities.supports(RuntimeProcessOperation.RETRY):
                return RuntimeRecoveryAction.BLOCK, "process does not declare retry capability"
            if spec.idempotency == RuntimeIdempotency.NON_IDEMPOTENT:
                return RuntimeRecoveryAction.BLOCK, "non-idempotent process cannot be retried automatically"
            if (
                spec.idempotency == RuntimeIdempotency.REQUIRES_CLEANUP
                and not decision.metadata.get("cleanup_completed", False)
            ):
                return RuntimeRecoveryAction.BLOCK, "process requires confirmed cleanup before retry"
            attempts = projection.process_attempt_ids.get(process_id, [])
            max_attempts = spec.retry_policy.max_attempts if spec.retry_policy is not None else 1
            if len(attempts) >= max_attempts:
                return RuntimeRecoveryAction.BLOCK, "process retry policy is exhausted"
        elif decision.action == RuntimeRecoveryAction.RESTART:
            if not spec.capabilities.supports(RuntimeProcessOperation.START):
                return RuntimeRecoveryAction.BLOCK, "process does not declare start capability"
            if spec.idempotency == RuntimeIdempotency.NON_IDEMPOTENT:
                return RuntimeRecoveryAction.BLOCK, "non-idempotent process cannot restart automatically"
            if (
                spec.idempotency == RuntimeIdempotency.REQUIRES_CLEANUP
                and not decision.metadata.get("cleanup_completed", False)
            ):
                return RuntimeRecoveryAction.BLOCK, "process requires confirmed cleanup before restart"
        return decision.action, decision.reason


class RuntimeContinuationSelector:
    """Select the minimum executable DAG subgraph from state and package evidence."""

    def create_plan(
        self,
        *,
        specs: list[RuntimeProcessSpec],
        source_projection: RuntimeProjection,
        assessments: list[RuntimeContinuationAssessment],
        plan_id: str,
        target_runtime_id: str,
        created_at: float,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeRecoveryPlan:
        """Build a full plan whose skip steps represent nodes outside local work."""
        if target_runtime_id != source_projection.runtime_id:
            raise RuntimeRecoveryPlanError(
                "Continuation must preserve runtime_id; use rerun for a new lineage"
            )
        graph = build_runtime_process_graph(specs)
        specs_by_id = {spec.resolved_process_id(): spec for spec in specs}
        assessments_by_id = {item.process_id: item for item in assessments}
        if len(assessments_by_id) != len(assessments):
            raise RuntimeRecoveryPlanError("Continuation assessments must have unique process_ids")
        missing = set(graph.process_ids).difference(assessments_by_id)
        unknown = set(assessments_by_id).difference(graph.process_ids)
        if missing or unknown:
            raise RuntimeRecoveryPlanError(
                "Continuation assessment coverage mismatch: "
                f"missing={sorted(missing)!r}, unknown={sorted(unknown)!r}"
            )

        affected: set[str] = set()
        for process_id in graph.topological_order:
            assessment = assessments_by_id[process_id]
            state = source_projection.processes.get(process_id)
            if (
                assessment.force_reexecute
                or state is None
                or state.status_class != RuntimeStatusClass.TERMINAL_SUCCESS
                or not assessment.completed_output_valid
            ):
                affected.add(process_id)
            unknown_invalidations = set(assessment.invalidates).difference(graph.process_ids)
            if unknown_invalidations:
                raise RuntimeRecoveryPlanError(
                    f"Assessment for {process_id!r} invalidates unknown processes: "
                    f"{sorted(unknown_invalidations)!r}"
                )
            affected.update(assessment.invalidates)

        for process_id in graph.topological_order:
            assessment = assessments_by_id[process_id]
            if assessment.preserve_on_upstream_reexecution:
                continue
            if any(dependency in affected for dependency in graph.dependencies[process_id]):
                affected.add(process_id)

        decisions = [
            self._decision(
                process_id=process_id,
                spec=specs_by_id[process_id],
                projection=source_projection,
                assessment=assessments_by_id[process_id],
                affected=process_id in affected,
            )
            for process_id in graph.topological_order
        ]
        return RuntimeRecoveryPlanner().create_plan(
            specs=specs,
            source_projection=source_projection,
            decisions=decisions,
            plan_id=plan_id,
            target_runtime_id=target_runtime_id,
            created_at=created_at,
            metadata=metadata,
        )

    @staticmethod
    def _decision(
        *,
        process_id: str,
        spec: RuntimeProcessSpec,
        projection: RuntimeProjection,
        assessment: RuntimeContinuationAssessment,
        affected: bool,
    ) -> RuntimeRecoveryDecision:
        metadata = {**assessment.metadata, "cleanup_completed": assessment.cleanup_completed}
        if not affected:
            return RuntimeRecoveryDecision(
                process_id=process_id,
                action=RuntimeRecoveryAction.SKIP,
                reason="completed process and outputs remain valid",
                preserve_on_upstream_reexecution=assessment.preserve_on_upstream_reexecution,
                metadata=metadata,
            )

        attempt_ids = projection.process_attempt_ids.get(process_id, [])
        source_attempt_id = attempt_ids[-1] if attempt_ids else None
        checkpoint = (
            projection.checkpoints.get(assessment.checkpoint_id)
            if assessment.checkpoint_id is not None
            else None
        )
        action = assessment.preferred_action
        if action is None and checkpoint is not None and spec.capabilities.supports(
            RuntimeProcessOperation.RESUME
        ):
            action = RuntimeRecoveryAction.RESUME
        if action is None and source_attempt_id is not None and spec.capabilities.supports(
            RuntimeProcessOperation.RETRY
        ):
            action = RuntimeRecoveryAction.RETRY
        if action is None:
            action = RuntimeRecoveryAction.RESTART
        return RuntimeRecoveryDecision(
            process_id=process_id,
            action=action,
            reason=assessment.reason,
            source_attempt_id=source_attempt_id,
            checkpoint=checkpoint,
            invalidates=assessment.invalidates,
            preserve_on_upstream_reexecution=assessment.preserve_on_upstream_reexecution,
            metadata=metadata,
        )
