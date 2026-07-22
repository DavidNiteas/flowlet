# Development Roadmap

## Phase 0: Boundary Audit

Status: completed. See [05_boundary_audit.md](05_boundary_audit.md).

### Goal

Confirm Flowlet runtime owns only framework-level concepts.

### Work

- Review current `flowlet.runtime` APIs.
- Mark APIs as:
  - framework-stable
  - compatibility helper
  - candidate for redesign
  - should move back to business packages
- Specifically review:
  - `TERMINAL_JOB_STATUSES`
  - `FlowletJobBackendBase`
  - backend lifecycle helpers
  - snapshot loader
  - event buffer

### Acceptance

- A boundary audit document is added under `_dev`.
- No API is removed yet.
- A deprecation/migration list is written for risky APIs.

## Phase 1: RuntimeEvent Schema

Status: completed.

### Goal

Introduce a standard event envelope without replacing existing business events.

### Work

- Add `RuntimeEvent`.
- Add `RuntimeEventStatus`.
- Add `RuntimeStatusClass`.
- Add `RuntimeErrorInfo`.
- Add `RuntimeProgress`.
- Add JSON-safe validation tests.
- Add event type constants or enum-like string namespace.

### Acceptance

- `RuntimeEvent.model_validate(...)` accepts framework events.
- Custom business `event_type`, `status`, `payload`, and `metadata` are accepted.
- Error and progress payloads serialize to JSON.
- Ruff and Flowlet tests pass.

### Current Implementation Notes

- `RuntimeEvent`, `RuntimeEventStatus`, `RuntimeStatusClass`, `RuntimeErrorInfo`, `RuntimeProgress`, and `runtime_event_payload` are introduced in `flowlet.runtime.schema`.
- `RuntimeEventType` provides the recommended framework type vocabulary for
  runtime/process/unit/FSM/artifact/observability events. It is intentionally
  not a validator: `RuntimeEvent.event_type` remains an open string for custom
  business event types.
- The schema is intentionally business-neutral and accepts custom `event_type`, custom `status`, JSON-safe `payload`, and JSON-safe `metadata`.
- Event storage, legacy `TxnEvent` adapters, and manager-to-event conversion are deferred to Phase 2.

## Phase 2: RuntimeEvent Store and Adapters

Status: completed.

### Goal

Make standard events storable and compatible with current `TxnEvent`.

### Work

- Add `RuntimeEventStore` interface.
- Add JSONL implementation.
- Add append/list/wait/load APIs.
- Add adapter:
  - `TxnEvent`-like dict to `RuntimeEvent`
  - `RuntimeEvent` to legacy dict
- Decide sidecar vs existing `events.jsonl` strategy.

### Acceptance

- JSONL store can append, restore, and continue event ids.
- Existing `EventBuffer` can be backed by or adapted to `RuntimeEventStore`.
- MetaMSTools/MassLib4Search tests still pass if compatibility adapter is enabled.

### Current Implementation Notes

- `RuntimeEventStore` protocol is introduced in `flowlet.runtime.event_store`.
- `RuntimeEventJsonlStore` provides append/list/wait/load behavior with optional JSONL persistence.
- `txn_event_payload_to_runtime_event(...)` converts current TxnEvent-like dictionaries to `RuntimeEvent` without importing business packages.
- `runtime_event_to_txn_event_payload(...)` converts a `RuntimeEvent` back to the legacy dictionary shape for compatibility.
- The legacy event adapter preserves `legacy_event_type` in `RuntimeEvent.metadata`.
- `manager_record_to_runtime_event(...)` converts current Flowlet progress, signal, log, stream, and telemetry manager records to standard events.
- Tests include current-style MetaMSTools and MassLib4Search legacy event payload shapes.
- Business backend integration is not implemented yet.
- The standard event sidecar path is `runtime/events.runtime.jsonl`.
- `RuntimeFileLayout.runtime_events` exposes this path.
- `RuntimeStore.runtime_event_store()` and `RuntimeStore.append_runtime_event(...)` write to this sidecar.
- `RuntimeEventSidecarWriter` provides an opt-in bridge for:
  - already-normalized `RuntimeEvent` objects
  - legacy `TxnEvent`-like dictionaries
  - Flowlet manager records (`progress`, `signal`, `log`, `stream`, `telemetry`)
- Stateful sidecar events refresh `runtime/projection.json`; log and stream
  events remain append-only without forcing a full projection rebuild.
- MetaMSTools and MassLib4Search backend regressions verify that a persisted
  runtime contains a projection with the completed job process state.
- The sidecar writer is framework-only and does not import MetaMSTools or MassLib4Search.

### Business Backend Integration Notes

- MetaMSTools txn `EventBuffer` now mirrors emitted legacy events to `RuntimeEventSidecarWriter` when persistence is enabled.
- MassLib4Search txn `EventBuffer` now mirrors emitted legacy events to `RuntimeEventSidecarWriter` when persistence is enabled.
- Both integrations keep legacy `events.jsonl` unchanged.

### Remaining Phase 2 Work

- Keep legacy `events.jsonl` unchanged until readers migrate.
- Add fixtures from actual runtime files when fixture ownership is decided.
- Use [07_real_workspace_regression.md](07_real_workspace_regression.md) as the real liver sample validation procedure.
- Validate strict sidecar output on the real liver sample workspace after a fresh run.

## Phase 3: RuntimeProcess Contracts

Status: completed.

### Goal

Introduce process as the minimum runtime operation unit.

### Work

- Add `RuntimeProcessSpec`.
- Add `RuntimeProcessCapabilities`.
- Add `RuntimeResourceRequest`.
- Add `RuntimeProcessState`.
- Add `RuntimeProcessContext`.
- Add protocol or base class for process implementations.
- Add unsupported-operation error contract.

### Acceptance

- A simple process can emit start/progress/completed events.
- A process can declare unsupported cancel/pause/retry operations.
- A process context can report errors and artifacts.
- Unit tests cover success, failure, and unsupported operation.

### Current Implementation Notes

- `RuntimeProcessSpec`, `RuntimeProcessCapabilities`, `RuntimeResourceRequest`, `RuntimeResourceUsage`, `RuntimeRetryPolicy`, and `RuntimeProcessState` are introduced in `flowlet.runtime.process`.
- `RuntimeProcessContext` can emit standard status, progress, artifact, and error events into a `RuntimeEventStore`.
- `RuntimeProcessContext` also emits standard log, metric, signal, checkpoint, and resource-usage events.
- `RuntimeProcess` is a protocol for concrete implementations.
- `RuntimeProcessBase` provides default unsupported-operation hook behavior.
- `RuntimeUnsupportedOperationError` converts to the standard `RuntimeErrorInfo` contract.
- `RuntimeProcessRunner` wraps process execution with standard start/completed/failed/unsupported events.
- Resource observation belongs to the Phase 3 process contract and Phase 4
  projection: `resources()` and `emit_resource_usage()` produce
  `resource.sampled`, while the projection records the latest sample per
  process. It does not define resource scheduling.

## Phase 4: Runtime Projection Reducers

Status: in progress.

### Goal

Derive runtime state from standard events.

### Work

- Add `RuntimeReducer` interface.
- Add framework state reducer:
  - runtime status
  - process tree
  - active processes
  - terminal counts
  - error summary
  - artifact index
  - progress summary
- Add projection writer using existing `RuntimeStore`.

### Acceptance

- Given an event stream, reducer reconstructs process state.
- Failed child process propagates framework-level failure state according to explicit rules.
- Projection output is deterministic.
- Existing snapshot loader remains compatible.

### Current Implementation Notes

- `RuntimeProjection` is introduced as the framework-level state projection.
- `RuntimeReducer` is introduced as the reducer protocol.
- `RuntimeFrameworkReducer` reduces standard `RuntimeEvent` streams into:
  - runtime status
  - process map
  - active process ids
  - terminal counts
  - error summary
  - artifact index
  - progress summary
- `RuntimeFileLayout.projection` defines `runtime/projection.json`.
- `RuntimeStore.write_projection(...)` writes a projection payload without touching business `snapshot.json` or `monitor_snapshot.json`.
- `RuntimeProjectionPolicy` makes parent/child terminal-state propagation explicit.
- `RuntimeStore.load_projection()` and `load_runtime_projection(...)` provide basic projection loading.

### Remaining Phase 4 Work

- Add business reducers in MetaMSTools and MassLib4Search only after the framework projection stabilizes.
- Expand projection compatibility tests before CLI/TUI readers consume `runtime/projection.json`.

## Phase 5: Runtime Backend Executor Prototype

Status: in progress.

### Goal

Provide a framework runtime backend capable of executing registered processes and storing events.

### Work

- Add runtime backend prototype.
- Register process specs.
- Start processes.
- Dispatch optional cancel/pause/resume/retry/cleanup hooks.
- Emit standard events around hook execution.
- Persist events and projections.
- Integrate current runtime manager bundle.

### Acceptance

- A test backend runs multiple processes.
- Process failure emits standard error event.
- Cancel request works for a process that supports cancel.
- Unsupported cancel returns standard unsupported-operation state.
- Runtime files are written using current layout.

### Current Implementation Notes

- `RuntimeBackendExecutor` is introduced as a minimal framework executor.
- It registers `RuntimeProcess` implementations by resolved process id.
- Registration persists their serializable declarations to
  `runtime/processes.json`; this manifest intentionally excludes implementation
  objects and does not imply process restartability.
- Registration also emits `process.created` before any operation hook runs,
  making the declared lifecycle boundary visible in the standard event stream.
- It runs processes through `RuntimeProcessRunner`.
- It allocates monotonically increasing event ids across processes sharing one event store.
- It dispatches cancel hooks and emits unsupported-operation events when cancel is not supported.
- It dispatches pause, resume, retry, and cleanup hooks with standard unsupported-operation events.
- A `paused` status remains in the framework `active` status class.
- It writes `runtime/projection.json` through `RuntimeStore`.
- `RuntimeManagerEventBridge` incrementally mirrors a `RuntimeManagerBundle` into a `RuntimeEventStore` without controlling it.
- The executor optionally receives a bridge that writes to its own event store, synchronizes it at operation boundaries, and never closes the bridge or bundle.
- `sample_process_resources(...)` invokes the optional process observation hook,
  writes `resource.sampled`, and persists the resulting projection without
  allocating or scheduling resources.

### Remaining Phase 5 Work

- Add scheduler/resource integration only after process resource semantics are stable.
- Keep queueing/threading/business lifecycle policy in business packages or explicit adapters.

## Phase 6: MetaMSTools Compatibility Adapter

### Goal

Adapt MetaMSTools runtime backend to emit standard events without changing business output.

### Work

- Wrap current OpenMS analysis job as one or more runtime processes.
- Emit `RuntimeEvent` sidecar stream.
- Keep current `TxnEvent` stream.
- Add reducer or adapter that can rebuild current monitor snapshot.
- Preserve `.metams/runtime` layout.

### Acceptance

- Existing MetaMSTools tests pass.
- Real liver sample `.metams/runtime` snapshot still works.
- New standard event stream exists and validates.
- Business monitor output remains unchanged.

### Current Implementation Notes

- MetaMSTools keeps its current txn/OpenMS execution path and legacy
  `TxnEvent` stream, while writing the standard sidecar and projection.
- Each persisted job now declares the root
  `metams.openms.analysis` `RuntimeProcessSpec` in `runtime/processes.json`.
- Run/stage hierarchy remains business-owned in `JobSnapshot` and is not
  fabricated from the root declaration.

## Phase 7: MassLib4Search Compatibility Adapter

### Goal

Adapt MassLib4Search runtime backend to emit standard events without changing annotation/search outputs.

### Work

- Wrap annotation search and library build as runtime processes.
- Represent annotation study/run/FSM steps as process or subject hierarchy.
- Keep resume policy business-owned.
- Emit `RuntimeEvent` sidecar stream.
- Preserve `.annotation/<annotation_id>/runtime` layout.

### Acceptance

- Existing MassLib4Search tests pass.
- Existing annotation workflow test passes.
- Real liver sample `.annotation/spec_spec_unispec_pos/runtime` snapshot still works.
- New standard event stream exists and validates.

### Current Implementation Notes

- MassLib4Search keeps annotation/search execution and resume semantics in its
  existing backend while writing the standard sidecar and projection.
- Each persisted job writes one root `RuntimeProcessSpec` whose process type is
  the existing job type, such as `annotation.search` or `search_lib.build`.
- Annotation studies, runs, and FSM stages remain business subjects until they
  need independently addressable process operations.

## Phase 8: Reader Migration

### Goal

Move CLI/TUI/runtime-snapshot readers toward event-derived projections.

### Work

- Add event projection readers.
- Keep existing snapshot readers as fallback.
- Add compatibility mode for old runtime directories.
- Update CLI/TUI to prefer projection when available.

### Current Implementation Notes

- `RuntimeSnapshotView` carries an optional standard `RuntimeProjection`.
- `RuntimeSnapshotView` also carries optional persisted `RuntimeProcessSpec`
  declarations from `runtime/processes.json`; historical directories receive an
  empty list.
- `RuntimeSnapshotLoader` supports an optional business-owned
  `monitor_from_projection` adapter.
- A supplied adapter makes the standard projection the monitor source;
  unmodified readers retain monitor/snapshot/status precedence.
- MetaMSTools and MassLib4Search private CLI adapters now use a projection to
  normalize the business job lifecycle while preserving their existing run,
  stage, and runtime-task monitor data.
- A projection is used only with a valid business `JobSnapshot` or `JobRecord`.
- `RuntimeEventStore` now supports standard numeric and string cursors, waiting,
  streaming, and standard SSE encoding without relying on legacy `job_state`.
- MetaMSTools TUI exposes a read-only `Process Specs` page, while
  MassLib4Search adds the same framework declaration and projection summary to
  its existing `Spec` page. These pages do not alter business monitor counts or
  offer lifecycle controls.

### Acceptance

- Old runtime directories remain readable.
- New runtime directories can be inspected from standard events.
- CLI JSON output still contains `runtime_info`, `monitor`, and `snapshot`.
- A CLI with both a projection and a business record prefers the projected job
  lifecycle without inferring business task hierarchy.
- Terminal projections normalize business aggregate counters from the existing
  planned-run count while preserving legacy run/stage/task detail.
- Standard stream callers provide their own terminal predicate and can drain
  trailing events after it matches.
- `stream_runtime_event_jsonl` provides durable sidecar polling, and standard
  SSE encoder/parser helpers preserve the complete `RuntimeEvent` envelope.
- Both business FastAPI adapters provide additive `/runtime-events` endpoints;
  legacy `/events` remains unchanged.
- Both business HTTP clients expose `runtime_events()` and parse complete
  standard SSE frames into `RuntimeEvent` values. Current TUI/GUI monitors
  deliberately remain on their business event/projection paths until they have
  a defined business adapter for standard event presentation.
- Both business CLI wrappers preserve the optional process-spec list from the
  Flowlet view without using it to infer run/stage/FSM grouping or resume work.

## Phase 9: Legacy Cleanup

### Goal

Deprecate legacy event paths only after compatibility is proven.

### Work

- Mark legacy `TxnEvent` storage as compatibility API.
- Keep readers for historical runtime directories.
- Remove duplicated business event buffering only when both packages have migrated.

### Acceptance

- Public migration notes exist.
- No real sample regression.
- Downstream business packages no longer duplicate framework event store logic.

## Standard Regression Commands

Run through Pixi only:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/tests/test_runtime.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py -q
pixi run -e dev-all-gpu pytest MetaMSTools/tests/txn/backend/test_txn_backend.py MetaMSTools/tests/cli/test_cli.py -q
pixi run -e dev-all-gpu pytest MassLib4Search/tests/txn/test_backend.py MassLib4Search/tests/cli/test_cli.py MassLib4Search/tests/txn/search/test_annotation_workflow.py -q
```

Real sample checks:

```bash
pixi run -e dev-all-gpu meta-ms-tools runtime-snapshot print data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.metams/runtime --format json
pixi run -e dev-all-gpu masslib4search runtime-snapshot print data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.annotation/spec_spec_unispec_pos/runtime --format json
```
