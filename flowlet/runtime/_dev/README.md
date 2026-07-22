# Flowlet Runtime Backend Redesign

This document set defines the next-generation Flowlet runtime backend model.

The goal is to make Flowlet a business-neutral runtime framework for observable execution. Flowlet should provide standard events, standard processes, event storage, runtime projections, and process orchestration interfaces. It should not define domain job semantics such as OpenMS analysis, annotation search, study layout, workspace policies, or business monitor summaries.

## Documents

- [01_runtime_model.md](01_runtime_model.md): Core model, boundaries, and terminology.
- [02_event_and_process_contracts.md](02_event_and_process_contracts.md): Standard event and standard process contracts.
- [03_compatibility_with_existing_backends.md](03_compatibility_with_existing_backends.md): Native compatibility plan for MetaMSTools and MassLib4Search.
- [04_development_roadmap.md](04_development_roadmap.md): Phased implementation roadmap and acceptance criteria.
- [05_boundary_audit.md](05_boundary_audit.md): Current Flowlet runtime boundary audit.
- [06_handoff_log.md](06_handoff_log.md): Implementation handoff log.
- [07_real_workspace_regression.md](07_real_workspace_regression.md): Real liver workspace regression procedure.
- [08_business_projection_adapters.md](08_business_projection_adapters.md): Package-owned projection adapter contract for CLI/TUI readers.
- [09_legacy_migration_plan.md](09_legacy_migration_plan.md): Gated plan for retiring duplicated legacy event writes without losing historical compatibility.
- [10_legacy_consumer_census.md](10_legacy_consumer_census.md): Repository-owned legacy event readers and writers for Phase 9 Wave 1.
- [11_live_presentation_contract.md](11_live_presentation_contract.md): Read-only live GUI/TUI standard-event transport and presentation contract.
- [12_endpoint_version_policy.md](12_endpoint_version_policy.md): Approval-ready policy and consumer-census template for legacy event endpoints.
- [13_runtime_redesign_completion_audit.md](13_runtime_redesign_completion_audit.md): Requirement-to-evidence audit and explicit remaining Phase 9 gates.
- [14_recoverable_process_runtime.md](14_recoverable_process_runtime.md): Process dependency, attempt, checkpoint, and local recovery-plan design.

## Design Position

Flowlet runtime is an observable execution framework:

```text
RuntimeBackend
  = event reporting system
  + process orchestration executor
  + runtime projection engine
  + standard IO/layout interface
```

The two primary runtime layers are:

- Standard event: the minimum observable unit.
- Standard process: the minimum runtime operation unit.

An event records that something observable happened. A process represents an independently addressable execution unit that may be started, cancelled, paused, resumed, retried, cleaned up, scheduled, and observed, depending on capabilities declared by the concrete backend and process implementation.

FSM, workflow, executable unit, and business functions can all run inside a process. They emit standard events into the same runtime event stream.

## Boundary Rules

Flowlet owns:

- Runtime event envelope and event store interfaces.
- Runtime process interfaces and capability model.
- Runtime context used by processes to report events, state, errors, artifacts, and metrics.
- Runtime manager bundle, event buffer, snapshot loader, runtime layout, and projection interfaces.
- Business-neutral process orchestration hooks.

Flowlet does not own:

- Business `JobSpec`, `JobRecord`, `JobSnapshot`, or `JobMonitorSnapshot` schemas.
- OpenMS stages, annotation job types, study/workspace semantics, or resume policies.
- Product API route tables for MetaMSTools or MassLib4Search.
- Business-specific monitor summary logic.

## Compatibility Requirement

The redesign must be natively compatible with the current MetaMSTools and MassLib4Search runtime backends.

Compatibility means:

- Current `runtime_info.json`, `events.jsonl`, `status.json`, `snapshot.json`, `monitor_snapshot.json`, and `runtime/*` files remain readable.
- Existing `runtime-snapshot print --format json` behavior remains valid during migration.
- Existing backend APIs and CLI/TUI consumers can be migrated incrementally.
- Business-specific schemas can wrap or adapt Flowlet runtime events and processes without losing current fields.
