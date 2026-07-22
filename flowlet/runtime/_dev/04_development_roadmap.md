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

Status: in progress.

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
- The schema is intentionally business-neutral and accepts custom `event_type`, custom `status`, JSON-safe `payload`, and JSON-safe `metadata`.
- Event storage, legacy `TxnEvent` adapters, and manager-to-event conversion are deferred to Phase 2.

## Phase 2: RuntimeEvent Store and Adapters

Status: in progress.

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

Status: in progress.

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

- `RuntimeProcessSpec`, `RuntimeProcessCapabilities`, `RuntimeResourceRequest`, `RuntimeRetryPolicy`, and `RuntimeProcessState` are introduced in `flowlet.runtime.process`.
- `RuntimeProcessContext` can emit standard status, progress, artifact, and error events into a `RuntimeEventStore`.
- `RuntimeProcessContext` also emits standard log, metric, signal, and checkpoint events.
- `RuntimeProcess` is a protocol for concrete implementations.
- `RuntimeProcessBase` provides default unsupported-operation hook behavior.
- `RuntimeUnsupportedOperationError` converts to the standard `RuntimeErrorInfo` contract.
- `RuntimeProcessRunner` wraps process execution with standard start/completed/failed/unsupported events.

### Remaining Phase 3 Work

- Decide whether resource usage belongs in Phase 3 or Phase 4 reducers.
- Decide whether pause/resume/retry/cancel runner dispatch belongs in Phase 3 or Phase 5 executor prototype.
- Keep business process mappings in MetaMSTools and MassLib4Search adapters, not in Flowlet.

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

### Remaining Phase 4 Work

- Add parent/child failure propagation rules that are explicit and configurable.
- Add projection loading helpers if CLI/TUI readers start consuming `runtime/projection.json`.
- Add business reducers in MetaMSTools and MassLib4Search only after the framework projection stabilizes.

## Phase 5: Runtime Backend Executor Prototype

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

## Phase 8: Reader Migration

### Goal

Move CLI/TUI/runtime-snapshot readers toward event-derived projections.

### Work

- Add event projection readers.
- Keep existing snapshot readers as fallback.
- Add compatibility mode for old runtime directories.
- Update CLI/TUI to prefer projection when available.

### Acceptance

- Old runtime directories remain readable.
- New runtime directories can be inspected from standard events.
- CLI JSON output still contains `runtime_info`, `monitor`, and `snapshot`.

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
