# Legacy Event Migration Plan

## Status

Planned. This document defines the gates for reducing legacy event duplication.
It does not authorize removal of `events.jsonl`, `TxnEvent`, or the existing
business monitor contracts.

## Decision

`RuntimeEvent` is the canonical framework observability envelope for new
persisted runtimes. It is not yet the canonical business monitor model.

The following remain compatibility contracts until their consumers have an
approved migration:

- `events.jsonl` and the package-local `TxnEvent` schemas.
- `GET /api/jobs/{job_id}/events` and its package HTTP client methods.
- Business `JobSnapshot`, `JobMonitorSnapshot`, TUI monitor pages, and GUI
  monitor projections.
- Flowlet `EventBuffer`, `mirror_runtime_events`, and manager-file exporters.

The standard sidecar remains:

```text
runtime/events.runtime.jsonl
```

It is exposed by the additive `/runtime-events` endpoints and the package
`runtime_events()` client methods.

## Current Duplication Inventory

### Flowlet

| Component | Current role | Migration state |
| --- | --- | --- |
| `RuntimeEventJsonlStore` | Standard durable event storage | Canonical framework store |
| `RuntimeEventSidecarWriter` | Converts legacy/manager records to standard events | Required bridge |
| `EventBuffer` | Generic legacy-buffer base | Compatibility API |
| `mirror_runtime_events` | Mirrors manager records into legacy buffers | Compatibility API |
| `RuntimeSnapshotLoader` | Combines projection with legacy business records | Compatibility reader |

### MetaMSTools and MassLib4Search

Each package currently owns the same compatibility layers:

| Component | Current role | Required before removal |
| --- | --- | --- |
| `txn/backend/schemas.py:TxnEvent` | Legacy typed event payload | Consumer migration or adapter package |
| `txn/backend/events.py:EventBuffer` | Writes `events.jsonl`, then mirrors sidecar | Standard-event native business adapter |
| `txn/backend/backend.py:mirror_runtime_events` | Exports manager records into legacy stream | Standard monitor presentation adapter |
| `txn/backend/app.py:/events` | Legacy SSE endpoint | Explicit API versioning/deprecation policy |
| CLI/TUI monitor loaders | Read business snapshot and legacy monitor state | Read-only standard-event presentation adapter |

The packages already expose `/runtime-events`, preserve root process
declarations, and retain the existing domain monitor hierarchy. That is the
correct dual-write state; it is not a signal to delete legacy files.

## Non-Negotiable Invariants

1. Flowlet must never infer OpenMS stage, annotation FSM, run identity,
   workspace path, result location, or resume policy from `RuntimeEvent`.
2. A runtime directory must remain readable when it contains only legacy files,
   only standard framework files, or both.
3. Standard and legacy readers must not silently report contradictory root
   lifecycle states. The business adapter owns conflict resolution.
4. Removing a legacy writer requires a durable standard replacement for every
   consumer, not merely a passing sidecar validator.
5. Historical directories remain immutable migration fixtures.

## Migration Waves

### Wave 0: Freeze Legacy Surface

Do not add new business semantics to `TxnEvent`, `EventBuffer`,
`mirror_runtime_events`, or `events.jsonl`. New generic observability fields
belong in `RuntimeEvent` and its payload/metadata contract.

Acceptance:

- New framework behavior has a standard-event representation.
- A boundary review confirms no new Flowlet import from either business package.

### Wave 1: Consumer Census and Version Policy

For each package, enumerate all direct readers of `events.jsonl`, `TxnEvent`,
`/events`, and `EventBuffer`. Record external API clients separately from
in-repository callers. Publish a version/deprecation policy for `/events`.

Acceptance:

- Every reader has an owner and a migration target.
- No external consumer is assumed absent merely because repository search is
  empty.
- Product owners approve the legacy endpoint version policy.

Repository-owned consumers are recorded in
`10_legacy_consumer_census.md`. The outstanding work is the external consumer
census and product approval; it is not safe to infer either from source search.

### Wave 2: Package-Owned Standard Presentation Adapter

Implement one adapter per business package that consumes `RuntimeProjection`,
`RuntimeEvent`, and a valid business record to produce the package's existing
monitor view. It may normalize root lifecycle aggregates, but must preserve
business run/stage/FSM detail from business sources.

Acceptance:

- CLI, TUI, GUI, and HTTP monitor views can operate with a standard-event-only
  runtime plus a valid business record.
- Tests cover terminal success, failure, cancellation, and a historical
  legacy-only runtime.
- No Flowlet code imports MetaMSTools or MassLib4Search.

### Wave 3: Standard Writer as the Primary Internal Feed

Route new internal observability consumers through the sidecar store or a
standard stream. Retain a legacy adapter writer only for declared old
consumers. Do not reverse-convert arbitrary standard events into domain events
unless the package owns the mapping.

Acceptance:

- Each retained legacy write has a named consumer and removal date/version.
- Sidecar replay, live SSE, process manifest, and projection are tested for
  persisted success, failure, and cancellation cases.
- The real liver regression passes for both packages with the same workspace
  ownership rules documented in `07_real_workspace_regression.md`.

### Wave 4: Deprecation and Optional Legacy Writer Disablement

Add an explicit, versioned compatibility switch that disables legacy event
*writing* only for new runtimes. Never use it to rewrite or delete historical
runtime directories. Keep legacy reading enabled.

Acceptance:

- Default remains dual-write until a major-version policy says otherwise.
- A standard-only fresh runtime works in all supported package UIs/APIs.
- A legacy-only historical runtime still works in all supported readers.
- Rollback is configuration-only and does not alter persisted event files.

### Wave 5: Major-Version Removal Review

Only after the approved deprecation interval, remove the optional legacy writer
from new runtimes. Keep a read-only importer/adapter for historical
`events.jsonl` as long as retained studies require it.

Acceptance:

- Consumer census has no unsupported readers.
- Standard-only real liver regression succeeds for both packages.
- Release notes and migration guide are published.
- Business and framework maintainers jointly approve removal.

## Explicit Non-Goals

- Do not replace business `JobSnapshot` or `JobMonitorSnapshot` with Flowlet
  models.
- Do not make the Flowlet executor schedule OpenMS or annotation work.
- Do not delete `.metams`, `.annotation`, study artifacts, or annotation
  results as part of event migration.
- Do not claim numerical annotation-model correctness from runtime protocol
  success. The observed `unimol`/`clip` warning is a model configuration issue.

## Handoff Checklist

Before starting Wave 1, a new engineer should:

1. Read `05_boundary_audit.md`, this document, and
   `08_business_projection_adapters.md`.
2. Re-run the strict validator and both snapshot readers on the fresh liver
   runtime directories documented in `07_real_workspace_regression.md`.
3. Open a package-owned issue for the consumer census and API version policy.
4. Keep Flowlet changes framework-only; place business presentation adapters in
   MetaMSTools and MassLib4Search.
