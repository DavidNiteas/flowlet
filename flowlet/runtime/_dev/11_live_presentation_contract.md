# Live Runtime Presentation Contract

## Status

Design baseline for Phase 9 Wave 2. No existing GUI, TUI, WebSocket, or legacy
SSE endpoint changes behavior merely by adopting this document.

## Purpose

Define how a business package may present Flowlet `RuntimeEvent` values live
without treating them as a replacement for its business monitor model.

The contract applies to read-only observability surfaces only:

- GUI framework-observability panels.
- TUI framework-observability pages.
- Additive SSE/WebSocket streams intended for diagnostic clients.

It does not define task control, job cancellation, retry, resume, scheduling,
run/stage/FSM hierarchy, workspace navigation, or result interpretation.

## Runtime Observation Snapshot

Both business backend APIs expose the additive JSON endpoint:

```text
GET /api/jobs/{job_id}/runtime-observation
```

The response is deliberately framework-only:

```json
{
  "runtime_id": "<business job id>",
  "projection": { "...": "RuntimeProjection or null" },
  "process_specs": [{ "...": "RuntimeProcessSpec" }]
}
```

It returns `409 Conflict` when neither a projection nor a process declaration
is available. It does not expose or derive business run/stage/FSM detail. Both
HTTP clients expose `runtime_observation()`, and MetaMSTools GUI proxies it at:

```text
GET /api/v1/tasks/{task_id}/runtime-observation
```

Flowlet owns the JSON-safe framework shape as `RuntimeObservation` and
`load_runtime_observation()`. Business backends provide the `runtime_id` from
their known job record and translate an unavailable observation into their HTTP
status; they do not duplicate projection/manifest loading logic.

## Current Baseline

| Surface | MetaMSTools | MassLib4Search | Standard-event state |
| --- | --- | --- | --- |
| Backend SSE | `/api/jobs/{id}/runtime-events` | Same | Additive and available |
| Backend snapshot | `/api/jobs/{id}/runtime-observation` | Same | Framework-only additive JSON |
| HTTP client | `runtime_events()` | Same | Additive and available |
| Runtime snapshot CLI/TUI | Projection + process-spec read-only page | Projection + process-spec read-only section | Available from persisted files |
| Interactive job TUI | Business snapshots/monitor | Business snapshots/monitor | No live standard feed |
| Browser GUI | Additive standard SSE/observation proxy plus legacy SSE and snapshot-polling WebSocket | No equivalent package GUI | Meta framework observation available; legacy monitor unchanged |

## Standard Frame Contract

A standard live frame is the complete `RuntimeEvent` envelope serialized by
Flowlet's `sse_encode_runtime_event`. No package may drop `event_id`,
`runtime_id`, `process_id`, `status_class`, `payload`, `metadata`, or `error`
to create an incompatible abbreviated standard stream.

Required transport behavior:

1. The stream supports `since` with a numeric or string event id.
2. Durable replay reads `runtime/events.runtime.jsonl` before live polling.
3. A package supplies the root-terminal predicate using its job id and the
   framework `status_class`; Flowlet does not identify business completion.
4. Heartbeats may be transport comments only and are not `RuntimeEvent` values.
5. An unavailable sidecar returns a package transport error; it must not
   fabricate a standard event from a business snapshot.

Both backend suites verify durable replay from `since=0`: the root declaration
at id `0` is not repeated, later events are returned, and the terminal root
event remains visible.

## Presentation Model

Each package must create a presentation adapter with two outputs.

### Framework Observation

This output may display only framework facts:

- Root runtime/process status and `status_class`.
- Process ids, types, declarations, capabilities, retry metadata, and resource
  request/usage as declared.
- Standard progress, errors, artifacts, logs, metrics, signals, checkpoints,
  and event cursors.
- Projection aggregate counts and active process ids.

It may group information by `process_id` and `parent_process_id`, but labels
the grouping as framework process structure.

### Business Monitor

This output remains package-owned. It uses `JobRecord`, `JobSnapshot`,
`JobMonitorSnapshot`, and package business adapters for:

- MetaMSTools run IDs and OpenMS stages.
- MassLib4Search annotation id, study/result paths, run/FSM stages, and resume
  semantics.
- Lifecycle controls and their authorization.

A standard event may cause a refresh of the business monitor, but cannot be
used by itself to construct business hierarchy or enable a control.

## Merge Rules

1. **Identity:** business record supplies job identity, paths, job type, and
   planned unit count. Framework `runtime_id` is observational confirmation.
2. **Root lifecycle:** when projection and a valid business record are present,
   the package adapter may normalize the root lifecycle from projection.
3. **Detail:** existing business run/stage/task detail wins over a generic
   process tree. Missing business detail remains missing; do not synthesize it.
4. **Error display:** show framework errors as diagnostic entries. The business
   error field remains authoritative for a business failure summary.
5. **Cursor:** persist the last displayed standard event id per client session,
   not in the business runtime state.
6. **Ordering:** treat event ids as transport cursors. UI ordering uses the
   event timestamp only for display and never for state reduction.

## MetaMSTools Browser Migration

Implemented additive route:

```text
GET /api/v1/tasks/{task_id}/runtime-events?since=<cursor>
```

It proxies the backend `runtime_events()` client and uses standard SSE frames.
It is separate from:

```text
GET /api/v1/tasks/{task_id}/events
WS  /api/v1/tasks/{task_id}/events
```

The route proxies the backend `runtime_events()` client and preserves complete
RuntimeEvent SSE frames, including string or numeric `since` cursors. Backend
transport failures after a stream has started close the standard stream rather
than fabricating a non-standard runtime event.

Both business backend routes return `409 Conflict` for a known job whose
standard sidecar is unavailable. The Meta GUI proxy preflights its artifact
listing and returns the same `409` before SSE headers are sent.

The existing browser WebSocket remains a business snapshot stream. A future
standard WebSocket, if needed, must use a new route and transmit the complete
`RuntimeEvent` JSON envelope, not the current `_make_event` shape.

Acceptance:

- The route replay test verifies an unchanged RuntimeEvent envelope and string
  cursor handling.
- A missing runtime sidecar returns a transport error without changing the
  legacy route.
- GUI framework observation is read-only; cancel continues through the current
  business API.

## TUI Migration

For either package, the first live standard-event TUI view is a separate
framework page. It consumes standard events or a refreshed standard projection
and shows process declarations and diagnostics. Its refresh cycle may also
refresh the current business monitor, but the business monitor keeps its
existing source and layout.

Do not change `run_interactive_monitor` to consume a mixed event type. It is a
business-monitor workflow and needs a package-owned redesign if it migrates.

Acceptance:

- Non-interactive `runtime-snapshot tui --once` renders the framework view from
  projection/process manifest.
- Live framework page handles reconnect/replay cursor without changing business
  monitor counts.
- Existing interactive monitor tests continue to use business snapshots.

## Endpoint Version Policy Proposal

This is a proposal requiring package/product owner approval:

| Endpoint | Proposed status | Earliest removal condition |
| --- | --- | --- |
| `/api/jobs/{id}/events` | Supported legacy endpoint | Major-version review after consumer census |
| `/api/jobs/{id}/runtime-events` | Supported standard endpoint | None planned |
| Meta GUI `/tasks/{id}/events` | Supported legacy proxy | New GUI route and external-client migration complete |
| Meta GUI `/tasks/{id}/runtime-events` | Additive standard proxy | None planned |
| Meta GUI task WebSocket | Supported business snapshot transport | Separate standard WebSocket decision |

No endpoint is deprecated by this proposal until an owner publishes a version,
notice period, and migration guide. The approval-ready record is
`12_endpoint_version_policy.md`.

## Implementation Order

1. Completed: add and test the Meta GUI additive runtime-event proxy.
2. Add a package-local framework-observation presenter for one live surface.
3. Add reconnect/replay and sidecar-unavailable tests.
4. Obtain endpoint policy approval and external-consumer census.
5. Only then consider optional standard-only UI modes.

## Non-Goals

- No Flowlet imports from business packages.
- No automatic lifecycle controls from standard process capabilities.
- No replacement of existing legacy SSE/WebSocket routes.
- No process hierarchy inference for OpenMS or annotation execution.
