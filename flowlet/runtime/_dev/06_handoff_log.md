# Handoff Log

This file records implementation progress for the runtime backend redesign.

## 2026-07-22

### Current-Layout Regression Audit

The real liver workspace was reclassified into two evidence levels:

- Its persisted MetaMSTools and MassLib4Search directories are valid
  reader-compatibility fixtures: legacy events, standard sidecars, and
  projections all remain readable.
- They are not current-writer fixtures. Both predate `runtime/processes.json`
  and the reserved root `process.created` event at sidecar event id `0`.

`validate_runtime_sidecar.py` now has `--require-current-layout`. In addition
to sidecar validation it requires a non-empty process manifest, root
declaration consistency, and a projection containing that root. Projection
event count is intentionally bounded rather than exactly equal to JSONL count:
append-only log and stream events do not trigger a projection rewrite.

The required follow-up is a fresh, isolated execution against the real liver
inputs. Do not overwrite `.metams/runtime`, an existing annotation id, or study
results. Record the commands and artifact counts in
[07_real_workspace_regression.md](07_real_workspace_regression.md).

### MetaMSTools Fresh Regression

Completed a fresh isolated three-file liver analysis using the synchronous
backend. The run completed successfully in 90.515 seconds and is recorded in
[07_real_workspace_regression.md](07_real_workspace_regression.md).

It is the first real persisted runtime confirmed to satisfy the complete
current writer contract: `runtime/processes.json`, root `process.created` at
sidecar id `0`, valid projection, legacy compatibility stream, and a completed
projection-aware package snapshot reader.

MassLib4Search must still receive the same fresh-run evidence using a new
workspace annotation id.

### MassLib4Search Fresh Regression

Completed the workspace-mode real liver annotation using the new
`runtime_regression_current` id. The run completed in 17.043 seconds, retained
the existing annotation result, and produced a 53 MB new annotation result.

Its runtime is the second real persisted directory confirmed to satisfy the
complete current writer contract. Both fresh package runtimes now pass strict
sidecar/current-layout validation and their respective projection-aware
snapshot CLI readers.

The MassLib4Search process logged a non-fatal Transformers warning about
instantiating a `unimol` model as `clip`. Record it as a separate model
configuration follow-up; it does not affect runtime protocol validation.

### Final Regression Verification

After both real runs, the following checks passed:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/tests/test_runtime.py \
  MetaMSTools/MetaMSTools/txn/backend MetaMSTools/MetaMSTools/cli \
  MetaMSTools/tests/txn/backend/test_txn_backend.py MetaMSTools/tests/cli/test_cli.py \
  MassLib4Search/python/MassLib4Search/txn/backend MassLib4Search/python/MassLib4Search/cli \
  MassLib4Search/tests/txn/test_backend.py MassLib4Search/tests/cli/test_cli.py \
  MassLib4Search/tests/txn/search/test_annotation_workflow.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py \
  MetaMSTools/tests/txn/backend/test_txn_backend.py \
  MassLib4Search/tests/txn/test_backend.py -q
pixi run -e dev-all-gpu pytest MetaMSTools/tests/cli/test_cli.py \
  MassLib4Search/tests/cli/test_cli.py \
  MassLib4Search/tests/txn/search/test_annotation_workflow.py -q
```

Ruff passed. The framework and backend suite reported 73 passed; the CLI and
annotation workflow suite also passed. Phase 9 remains intentionally not
started: legacy streams remain required compatibility data until a separately
approved removal plan exists.

### Phase 9 Planning

Added [09_legacy_migration_plan.md](09_legacy_migration_plan.md). It freezes
the current dual-write state, inventories duplicated compatibility layers, and
defines the required census, business-owned presentation adapters, API version
policy, standard-only validation, and major-version approval gates. No legacy
writer or reader was changed.

### Phase 9 Wave 1 Repository Census

Added [10_legacy_consumer_census.md](10_legacy_consumer_census.md). All direct
in-repository legacy readers and writers now have a named package owner and
migration target. External consumers and endpoint-version approval remain
explicit product-owner decisions; no compatibility path was changed.

### Phase 9 Wave 2 Reader Baseline

Added a MetaMSTools CLI regression for a runtime containing only standard
projection files and `status.json`. Both business packages now prove that their
package-owned adapter can construct terminal root lifecycle monitor aggregates
without legacy events or snapshots. This is not a live UI migration and does
not reconstruct business run/stage/FSM hierarchy from Flowlet data.

### Live Presentation Contract

Added [11_live_presentation_contract.md](11_live_presentation_contract.md).
It distinguishes framework observation from business monitor rendering, fixes
the complete RuntimeEvent transport requirements, and proposes an additive
MetaMSTools GUI route as the first live migration. Legacy SSE and WebSocket
routes remain unchanged until endpoint policy approval.

### MetaMSTools GUI Standard SSE Proxy

Implemented `GET /api/v1/tasks/{task_id}/runtime-events` as an additive GUI
proxy. It relays complete standard RuntimeEvent SSE frames from the backend,
supports string cursors, and closes on a post-start backend transport failure
instead of emitting a fabricated business event. GUI/client regression tests
passed (30 tests); legacy GUI SSE and WebSocket contracts were not changed.

### Standard Stream Availability

Both package backend `/runtime-events` endpoints now return `409 Conflict` for
a known job without `runtime/events.runtime.jsonl`, preventing an hour-long
empty poll for a historical or incomplete runtime. MetaMSTools GUI preflights
the artifact listing and returns the same error before opening its SSE proxy.
The legacy `/events` endpoints and historical snapshot readers remain
unchanged.

### Runtime Observation API

Both backend APIs now expose a framework-only `runtime-observation` JSON
resource containing the persisted projection and process declarations. Their
HTTP clients expose `runtime_observation()`, and MetaMSTools GUI proxies the
same resource. This gives live UI work a stable read-only observation snapshot
without exposing business monitor inference or controls.

### Completed

- Added initial design document set under `flowlet/flowlet/runtime/_dev`.
- Completed Phase 0 boundary audit:
  - See [05_boundary_audit.md](05_boundary_audit.md).
  - Current migration-support APIs are classified.
  - APIs that should not be expanded are explicitly listed.
- Started Phase 1 RuntimeEvent schema implementation:
  - `flowlet.runtime.schema.RuntimeEvent`
  - `RuntimeEventStatus`
  - `RuntimeStatusClass`
  - `RuntimeProgress`
  - `RuntimeErrorInfo`
  - `runtime_event_payload`
- Exported the new schema objects from:
  - `flowlet.runtime`
  - top-level `flowlet`
- Added tests for:
  - JSON-safe runtime event payloads.
  - Custom business status with framework `status_class`.
  - Standard error payload serialization.

### Validation

Commands run:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py --fix
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py -q
```

Result:

```text
All checks passed.
6 passed.
```

### Continued Progress

Phase 2 RuntimeEvent store and adapters were started.

Added:

- `flowlet.runtime.event_store.RuntimeEventStore`
- `RuntimeEventJsonlStore`
- `flowlet.runtime.adapters.LEGACY_TXN_EVENT_TYPE_MAP`
- `txn_event_payload_to_runtime_event(...)`
- `runtime_event_to_txn_event_payload(...)`
- `manager_record_to_runtime_event(...)`
- `RuntimeFileLayout.runtime_events = "runtime/events.runtime.jsonl"`
- `RuntimeStore.runtime_event_store()`
- `RuntimeStore.append_runtime_event(...)`

Added tests for:

- JSONL append/load/list/wait behavior.
- TxnEvent-like dictionary to RuntimeEvent conversion.
- RuntimeEvent back to legacy dictionary conversion.
- Current-style MetaMSTools and MassLib4Search legacy event payload shapes.
- Flowlet manager record conversion for progress, signal, log, stream, and telemetry.
- Standard RuntimeEvent sidecar writing through `RuntimeStore`.
- Opt-in sidecar mirroring through `RuntimeEventSidecarWriter`.

Validation commands:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py --fix
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py -q
```

Result:

```text
All checks passed.
12 passed.
```

### Continued Progress: Sidecar Writer

Added:

- `flowlet.runtime.sidecar.RuntimeEventSidecarWriter`

The writer can append:

- already-normalized `RuntimeEvent` objects
- legacy `TxnEvent`-like dictionary payloads
- Flowlet manager records for progress, signal, log, stream, and telemetry

This keeps the current legacy `events.jsonl` stream unchanged while allowing
business packages to opt into the standard sidecar stream at
`runtime/events.runtime.jsonl`.

Added tests for:

- disabled sidecar writer no-op behavior
- legacy payload mirroring
- manager record mirroring
- artifact listing for the standard sidecar file

Validation commands:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py --fix
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py -q
```

Result:

```text
All checks passed.
15 passed.
```

### Open Work

Phase 1 is not fully complete until the schema is reviewed against real MetaMSTools and MassLib4Search event payloads.

Recommended next steps:

1. Run strict sidecar validation on the real liver workspace after a fresh MetaMSTools and MassLib4Search execution.
2. Add fixtures from actual runtime files once fixture ownership is decided.
3. Keep CLI/TUI readers on existing projections until event-derived projections are implemented.
4. Continue Phase 3 `RuntimeProcess` contracts after sidecar validation lands.

### Continued Progress: Business Sidecar Integration

Added initial business-package integration:

- MetaMSTools txn `EventBuffer` accepts an optional `RuntimeEventSidecarWriter`.
- MassLib4Search txn `EventBuffer` accepts an optional `RuntimeEventSidecarWriter`.
- Both backends create the writer for persisted runtime directories.
- In-memory jobs keep sidecar writing disabled.
- Restored jobs get a sidecar writer for future emitted events, but loading legacy `events.jsonl` does not replay old events into the sidecar.

Compatibility behavior:

- Existing `events.jsonl` is still written by the original `EventBuffer`.
- Standard sidecar events are written to `runtime/events.runtime.jsonl`.
- Existing event readers remain on the legacy stream.

Added regression assertions in business backend tests:

- MetaMSTools persisted inline runtime emits parseable `RuntimeEvent` sidecar events.
- MassLib4Search persisted inline runtime emits parseable `RuntimeEvent` sidecar events.

Validation commands:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime/_dev MetaMSTools/MetaMSTools/txn/backend/backend.py MetaMSTools/MetaMSTools/txn/backend/events.py MetaMSTools/tests/txn/backend/test_txn_backend.py MassLib4Search/python/MassLib4Search/txn/backend/backend.py MassLib4Search/python/MassLib4Search/txn/backend/events.py MassLib4Search/tests/txn/test_backend.py --fix
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime/_dev MetaMSTools/MetaMSTools/txn/backend/backend.py MetaMSTools/MetaMSTools/txn/backend/events.py MetaMSTools/tests/txn/backend/test_txn_backend.py MassLib4Search/python/MassLib4Search/txn/backend/backend.py MassLib4Search/python/MassLib4Search/txn/backend/events.py MassLib4Search/tests/txn/test_backend.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py -q
pixi run -e dev-all-gpu pytest MetaMSTools/tests/txn/backend/test_txn_backend.py -q
pixi run -e dev-all-gpu pytest MassLib4Search/tests/txn/test_backend.py -q
```

Result:

```text
All checks passed.
flowlet/tests/test_runtime.py: 15 passed.
MetaMSTools/tests/txn/backend/test_txn_backend.py: 21 passed.
MassLib4Search/tests/txn/test_backend.py: 13 passed.
```

### Continued Progress: Real Workspace Validator

Added:

- [07_real_workspace_regression.md](07_real_workspace_regression.md)
- `validate_runtime_sidecar.py`

The validator is framework-level only. It checks:

- legacy `events.jsonl` exists and can be adapted to `RuntimeEvent`
- optional strict standard sidecar validation for `runtime/events.runtime.jsonl`

Current real workspace observation:

```text
.metams/runtime: 292 legacy events, no sidecar events.
.annotation/spec_spec_unispec_pos/runtime: 185 legacy events, no sidecar events.
```

This is expected for historical runs made before sidecar writing was added.
Strict validation should be run only after a fresh execution with the current
code.

### Continued Progress: RuntimeProcess Contracts

Started Phase 3 implementation.

Added:

- `flowlet.runtime.process.RuntimeProcessSpec`
- `RuntimeProcessCapabilities`
- `RuntimeResourceRequest`
- `RuntimeRetryPolicy`
- `RuntimeProcessState`
- `RuntimeProcessOperation`
- `RuntimeProcessContext`
- `RuntimeProcess`
- `RuntimeProcessBase`
- `RuntimeProcessRunner`
- `RuntimeUnsupportedOperationError`
- `runtime_process_spec_payload(...)`

Scope:

- These APIs are business-neutral framework contracts.
- They do not define OpenMS, annotation search, study layout, resume policy, or business monitor semantics.
- `RuntimeProcessContext` writes standard `RuntimeEvent` objects to a `RuntimeEventStore`.
- `RuntimeProcessBase` gives concrete implementations a default unsupported-operation behavior.
- `RuntimeProcessRunner` wraps one process start call with standard started/completed/failed/unsupported events.
- Context helpers now cover status, progress, artifact, error, log, metric, signal, and checkpoint events.

The Phase 3 contract is now complete. Resource usage is a process observation,
not a scheduler concern: `RuntimeResourceUsage`, `resources()`,
`emit_resource_usage()`, and executor sampling write `resource.sampled`; the
framework projection retains the latest sample per process. Scheduling,
allocation, queues, and business process mappings remain outside Flowlet.

Validation commands:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py --fix
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py -q
```

### Continued Progress: Resource Observation Contract

- `RuntimeResourceUsage` separates current process observations from static
  `RuntimeResourceRequest` intent.
- Process code can push a sample through its context or expose a `resources()`
  hook for `RuntimeBackendExecutor.sample_process_resources()`.
- The projection stores the latest sample per process in a framework-only
  `resource_usage_summary`; no scheduling policy was added.

Validation:

- `flowlet/tests/test_runtime.py`: 37 passed.

### Continued Progress: Process Manifest Snapshot Loading

- `RuntimeSnapshotLoader` now exposes optional `process_specs` from
  `runtime/processes.json`; a missing or unreadable manifest yields an empty
  list, preserving historical runtime compatibility.
- MetaMSTools and MassLib4Search CLI wrapper views forward this field without
  changing monitor calculations or deriving business hierarchy from it.
- The manifest remains a declaration-only artifact, not an execution resume
  mechanism.

Validation:

- Flowlet runtime plus both business CLI suites: 78 passed.

### Continued Progress: Process Specification Manifest

- `RuntimeFileLayout.processes` defines `runtime/processes.json`.
- `RuntimeBackendExecutor.register()` writes the current ordered
  `RuntimeProcessSpec` declarations, and `RuntimeStore.load_process_specs()`
  restores them for observers.
- The manifest is deliberately declarative. It does not serialize business
  implementations or claim that a process can be resumed after restart.

Validation:

- `flowlet/tests/test_runtime.py`: 37 passed.

Result:

```text
All checks passed.
21 passed.
```

### Continued Progress: Runtime Projection Reducers

Started Phase 4 implementation.

Added:

- `flowlet.runtime.projection.RuntimeProjection`
- `RuntimeReducer`
- `RuntimeFrameworkReducer`
- `RuntimeProjectionPolicy`
- `runtime_projection_payload(...)`
- `load_runtime_projection(...)`
- `RuntimeFileLayout.projection = "runtime/projection.json"`
- `RuntimeStore.write_projection(...)`
- `RuntimeStore.load_projection()`

Scope:

- The reducer consumes only standard `RuntimeEvent` objects.
- The projection is framework-level state only.
- It does not replace business `snapshot.json` or `monitor_snapshot.json`.
- Business monitor summaries remain in MetaMSTools and MassLib4Search.

The current framework reducer derives:

- runtime status
- process states
- active process ids
- terminal success/failure/cancelled counts
- error summary
- artifact index
- progress summary

Remaining Phase 4 work:

- Keep business reducers out of Flowlet.
- Expand projection compatibility tests before CLI/TUI readers consume `runtime/projection.json`.

Validation commands:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py --fix
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py -q
```

Result:

```text
All checks passed.
24 passed.
```

### Continued Progress: Runtime Backend Executor Prototype

Started Phase 5 implementation.

Added:

- `flowlet.runtime.executor.RuntimeBackendExecutor`

Scope:

- The executor is a minimal framework prototype.
- It runs registered `RuntimeProcess` implementations through `RuntimeProcessRunner`.
- It persists standard events to a `RuntimeEventStore`.
- It writes framework projection output to `runtime/projection.json`.
- It dispatches cancel hooks and emits unsupported-operation events when cancel is not supported.
- It dispatches pause, resume, retry, and cleanup hooks with the same supported/unsupported event pattern.
- A `paused` status projects as an active process; it is not a terminal outcome.
- `RuntimeManagerEventBridge` treats `RuntimeManagerBundle` as a standard event source while leaving manager lifecycle outside the executor.
- The executor accepts an optional bridge only when it shares its event store, synchronizes it at operation boundaries, and does not close it.
- It does not define queueing, threading, retry policy execution, business job lifecycle, or business monitor semantics.

Current tests cover:

- running multiple processes through one executor
- monotonically increasing event ids across registered processes
- persisted framework projection
- process failure event emission
- supported cancel hook dispatch
- unsupported cancel event emission
- supported pause/resume/retry/cleanup hook dispatch
- unsupported retry event emission
- manager bundle bridge conversion, deduplication, and incremental append
- executor synchronization of a non-owned manager bridge
- projection persistence when a lifecycle hook raises an ordinary exception

Remaining Phase 5 work:

- Keep business lifecycle adapters outside Flowlet.

Validation commands:

```bash
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py --fix
pixi run -e dev-all-gpu ruff check flowlet/flowlet/runtime flowlet/flowlet/__init__.py flowlet/tests/test_runtime.py
pixi run -e dev-all-gpu pytest flowlet/tests/test_runtime.py -q
```

Result:

```text
All checks passed.
32 passed.
```

### Continued Progress: Projection-Aware Snapshot Reads

Added a framework-only reader migration seam.

- `RuntimeSnapshotView` now exposes an optional `RuntimeProjection`.
- `RuntimeSnapshotLoader` loads it when `runtime/projection.json` exists.
- A business package can pass `monitor_from_projection` to opt into
  projection-first monitor construction.
- Without that adapter, legacy monitor/snapshot/status precedence is unchanged.
- Flowlet does not infer business monitor fields from the framework projection.

Validation:

- `flowlet/tests/test_runtime.py`: 32 passed.
- The real liver `.metams/runtime` CLI snapshot remains readable and reports
  3 completed runs.
- The real liver `.annotation/spec_spec_unispec_pos/runtime` CLI snapshot
  remains readable and reports 3 completed runs.

### Continued Progress: Business Projection Reader Adapters

- MetaMSTools and MassLib4Search CLI readers retain the Flowlet projection in
  their private runtime view.
- When a valid business `JobSnapshot` or `JobRecord` is also available, each
  package applies the projection's aggregate lifecycle to that record before
  invoking its existing monitor reducer.
- `succeeded` maps to each package's existing `completed` status vocabulary.
- Run/stage/FSM/runtime-task hierarchy remains sourced from the business
  snapshot, never inferred from arbitrary framework process ids.
- CLI regression tests cover a legacy `running` monitor overridden by a
  standard succeeded projection.

Validation:

- MetaMSTools and MassLib4Search CLI tests: 41 passed.

### Continued Progress: Terminal Projection Aggregate Repair

- A real MetaMSTools projection exposed an inconsistent legacy snapshot summary:
  the projected job was completed while its aggregate monitor still said 0/3.
- Both business adapters now normalize terminal aggregate counters from their
  existing `JobRecord.planned_run_count`, while retaining snapshot run, stage,
  FSM, and runtime-task details.
- The real `.metams/runtime` reader now reports completed 3/3 with projection
  enabled.

### Continued Progress: Sidecar Projection Persistence

- `RuntimeEventSidecarWriter` now rebuilds and writes
  `runtime/projection.json` after a stateful standard event.
- Projection refresh applies to status, progress, errors, and artifact events;
  logs and streams remain append-only to avoid needless full reductions.
- Sidecar refresh reloads the persisted JSONL stream before reducing, so it
  cannot write an empty projection from a fresh in-memory store.
- Both business backend regression suites verify that the persisted projection
  contains the completed job process after an inline run.

Validation:

- Flowlet runtime plus MetaMSTools and MassLib4Search backend tests:
  67 passed.

### Real Workspace End-to-End Regression

Fresh runs were completed in the target liver workspace using the persisted
MetaMSTools config and the MassLib4Search annotation config.

- MetaMSTools: 386 legacy events, 94 standard sidecar events, succeeded
  projection, and projection-aware monitor completed 3/3.
- MassLib4Search: 370 legacy events, 185 standard sidecar events, succeeded
  projection, and projection-aware monitor completed 3/3.
- Annotation results exist under
  `annotations/spec_spec_unispec_pos/search_annotation_results_lib`.
- `validate_runtime_sidecar.py --require-sidecar` passes for both target
  runtime directories.

### Continued Progress: Standard Event Store Streaming

- `RuntimeEventStore` now accepts numeric and string event cursors.
- `wait_runtime_event_store`, `stream_runtime_event_store`, and
  `sse_encode_runtime_event` provide the standard-store read path.
- Standard streaming does not treat every terminal process as a terminal
  runtime. The caller supplies an explicit terminal predicate and receives any
  trailing events before the iterator ends.
- Legacy `wait_runtime_events` and `stream_runtime_events` remain compatibility
  helpers for current business `EventBuffer` APIs.

Validation:

- `flowlet/tests/test_runtime.py`: 34 passed.

### Continued Progress: Standard Sidecar HTTP API

- Flowlet now provides JSONL sidecar polling and full standard SSE parsing in
  addition to the in-memory standard event-store stream.
- MetaMSTools and MassLib4Search expose additive
  `GET /api/jobs/{job_id}/runtime-events` endpoints. They replay
  `runtime/events.runtime.jsonl` and leave legacy `/events` untouched.
- The adapter owns root-job termination: a terminal status class only ends the
  stream when the event process id equals the requested job id. Flowlet drains
  events already in the sidecar after that event.
- Existing business `completed` event status remains a permitted custom value;
  `terminal_success` provides its framework-neutral terminal classification.

Validation:

- `flowlet/tests/test_runtime.py`: 35 passed.
- MetaMSTools and MassLib4Search backend suites: 34 passed.

### Continued Progress: Standard HTTP Client Read Path

- Both `TxnBackendHttpClient` implementations now expose
  `runtime_events(job_id, since=...)`, returning Flowlet `RuntimeEvent` values
  from the additive standard SSE endpoint.
- The client API preserves numeric and string cursor ids and uses Flowlet's
  full-frame parser rather than rebuilding state from SSE event names.
- TUI and GUI remain on their legacy business event and projection paths. This
  is intentional: their monitor models need package-owned run/stage/FSM
  grouping that Flowlet cannot infer.

Validation:

- MetaMSTools and MassLib4Search HTTP client tests: 10 passed.

### Continued Progress: Standard Event Type Vocabulary

- `RuntimeEventType` now defines the recommended Flowlet vocabulary for
  runtime, process, unit, FSM, artifact, log, metric, signal, stream, and
  error events.
- `RuntimeEvent.event_type` remains an open string. The enum prevents drift in
  framework code without blocking package-owned domain events.
- Projection and sidecar artifact handling use the standard constants, making
  the vocabulary a real framework dependency rather than documentation only.

Validation:

- `flowlet/tests/test_runtime.py`: 36 passed.

### Continued Progress: Resource Observation Contract

- `RuntimeResourceUsage` distinguishes sampled process usage from static
  `RuntimeResourceRequest` intent.
- Process code can call `emit_resource_usage()` or expose `resources()` for
  executor sampling. The resulting `resource.sampled` event updates the latest
  per-process framework projection value.
- No scheduling, allocation, queue, node-selection, or business resource
  policy was added to Flowlet.

Validation:

- `flowlet/tests/test_runtime.py`: 37 passed.

### Current Real Workspace Revalidation

- The strict sidecar validator passes against the user-specified liver
  workspace: MetaMSTools has 386 legacy / 94 standard events and MassLib4Search
  has 370 legacy / 185 standard events.
- Both projection-aware runtime-snapshot CLIs succeed and report 3 completed
  units with no remaining work.

### Continued Progress: Business Root Process Declarations

- MetaMSTools now writes one root `metams.openms.analysis` process spec for
  every persisted txn job.
- MassLib4Search now writes one root process spec using its existing job type,
  including `annotation.search` and `search_lib.build`.
- These declarations use the same job id as the standard sidecar root process.
  They describe the business boundary without moving OpenMS, annotation runs,
  FSM stages, workspace layout, or resume policy into Flowlet.

Validation:

- MetaMSTools and MassLib4Search backend suites: 34 passed.
- MetaMSTools and MassLib4Search CLI suites: 41 passed.

### Continued Progress: Observable Process Registration

- `RuntimeBackendExecutor.register()` now emits `process.created` with the
  declared process type and pending/not-started lifecycle state.
- `RuntimeEventSidecarWriter.append_process_spec()` gives compatibility
  backends the same framework event. MetaMSTools and MassLib4Search reserve id
  `0` for their persisted root declaration, preserving legacy-mirrored ids.
- This makes a root process observable before execution without changing the
  legacy `TxnEvent` stream, business status vocabulary, or output layout.

Validation:

- Flowlet runtime plus both business backend suites: 72 passed.

### Continued Progress: Read-only TUI Framework Inspection

- MetaMSTools runtime TUI now has a `Process Specs` page showing the standard
  projection summary and persisted process declarations.
- MassLib4Search adds those same framework items to its existing `Spec` page.
- The UI does not use declarations to calculate business monitor state or
  execute lifecycle controls; OpenMS/annotation run-stage interpretation
  remains package-owned.

Validation:

- MetaMSTools and MassLib4Search CLI suites: 42 passed.

### Phase 4/5 Completion Evidence

- `RuntimeFrameworkReducer` has explicit deterministic-order coverage: the
  same standard events reduced in forward or reverse input order produce an
  identical projection.
- Framework reducer coverage includes process state, progress, artifacts,
  resources, child failure propagation, and persisted projection loading.
- `RuntimeBackendExecutor` coverage includes registration/declaration events,
  multi-process execution, hook dispatch, unsupported operations, failures,
  manager bridge synchronization, resource sampling, manifests, and runtime
  projection persistence.
- Scheduling and resource allocation are intentionally not executor work; the
  existing request/usage contracts are the handoff point for a future separate
  scheduler.

Validation:

- `flowlet/tests/test_runtime.py`: 39 passed.

### Boundary Reminder

Do not add domain fields to `RuntimeEvent`.

Keep these in business packages:

- OpenMS stage names.
- Annotation search job types.
- Study/workspace paths as interpreted domain concepts.
- Business monitor summaries.
- Resume policy.
