# Durable Runtime Foundation

## Status

Phase 11 in progress. Stable identity contracts, the first transactional
event/ledger store, recoverable runtime-directory reset, incremental
projections, rebuildable ledgers, backend-session leases, and interrupted-work
reconciliation, durable commands, and generic DAG continuation selection are
implemented. Execution-wave orchestration and business backend migration
remain open.

## Objective

Make Flowlet the runtime foundation used by MetaMSTools and MassLib4Search txn,
instead of a standard-event sidecar attached to two independent job backends.
The foundation must preserve one execution lineage across backend restarts,
append observations without rewriting history, and reconstruct the minimum DAG
subgraph required to continue unfinished work.

Flowlet owns runtime identity, durable event order, execution and attempt
records, generic dependency readiness, projection cursors, leases, and control
contracts. Packages continue to own process implementations, business output
validation, cleanup/compensation, FSM semantics, and business presentation.

## Current-State Audit

| Area | Current state | Required destination |
| --- | --- | --- |
| Runtime identity | Both txn backends use each new `job_id` as `runtime_id` | One stable runtime lineage across continue operations |
| Shared annotation runtime | MassLib4Search appends process declarations but overwrites `job_spec.json`, `status.json`, and `runtime_info.json` | Append execution ledger; single-value exports become current-state compatibility views |
| Backend restart | Both packages convert every nonterminal job to `BackendRestarted/failed` | Reconcile lost session leases, mark attempts interrupted, and continue the affected DAG |
| Event storage | Package legacy buffers plus standard JSONL sidecar | Transactional canonical event journal with centrally allocated sequence |
| Projection | Stateful sidecar events reload and reduce the complete JSONL | Incremental reducer from durable projection cursor, with full rebuild fallback |
| Process execution | Business backends own job threads and package schedulers | Flowlet owns runtime/execution/process lifecycle; packages supply process adapters |
| Recovery | Meta recovery uses the reference executor; Mass persists a plan then uses its own scheduler | Durable plan execution ledger shared by both packages |

The existing compatibility APIs remain available during migration. Their
schemas are not expanded with new business meaning.

## Identity Model

```text
RuntimeIdentity
  runtime_id             immutable lineage id
  logical_task_id        optional stable business-facing identity
  generation             increases for rerun lineages
  rerun_of_runtime_id    optional audit link, never an event cursor link

RuntimeExecutionRecord
  execution_id           one initial or continue wave
  runtime_id             inherited by continue
  kind                   initial | continue
  ordinal                monotonic within the runtime
  backend_session_id     attachment/lease owner, not business identity

RuntimeProcessSpec
  process_id             stable logical DAG node
  execution_key          cross-runtime logical equivalence

RuntimeProcessAttempt
  attempt_id             immutable physical invocation
  execution_id           wave that dispatched it
  process_id             inherited logical node
```

A backend restart changes only `backend_session_id`. A continue operation keeps
`runtime_id`, `logical_task_id`, and all `process_id` values, creates a new
`execution_id`, and creates new attempts only for selected nodes.

## Rerun And Continue

### Continue

`continue_runtime` is an append-only operation on the same runtime lineage.

1. Acquire a runtime lease and open the durable journal with the expected
   `runtime_id`.
2. Reconcile nonterminal attempts whose backend session lease is lost.
3. Incrementally apply events after the projection cursor.
4. Validate package-owned outputs and checkpoints.
5. Build the affected DAG subgraph from pending, failed, interrupted, stale,
   or invalid-output nodes plus invalidated downstream nodes.
6. Treat valid completed ancestors and unrelated nodes as satisfied without
   dispatching them.
7. Append one `execution.created` event and new attempt events. Never rewrite
   prior failure or completion records.

### Rerun

`rerun_runtime` abandons the prior runtime lineage. It is not a large continue.

1. Acquire an exclusive reset lock outside the directory being replaced.
2. Reject reset while a valid runtime lease exists.
3. Invoke package cleanup/compensation for business outputs.
4. Atomically move the old runtime aside, initialize a clean directory with a
   new `runtime_id` and incremented generation, then remove the abandoned
   runtime after successful initialization.
5. Start a complete DAG as execution ordinal 1 with event sequence 0.

Flowlet cleans only framework runtime state. A package decides whether study or
annotation artifacts may be deleted, replaced, versioned, or retained.

## Durable Journal

`RuntimeDurableStore` is the first canonical-store candidate. It uses SQLite
WAL and `BEGIN IMMEDIATE` to serialize local appenders. In one transaction it:

- Verifies the immutable runtime identity.
- Allocates a canonical integer event sequence.
- Stores the complete `RuntimeEvent` envelope.
- Updates execution and process-attempt ledger rows.

The store also keeps process declarations and an incremental projection
cursor. `refresh_projection()` applies only events after `through_sequence`.
Direct event-derived process state is retained separately from propagated
parent presentation state, so a continued child success removes an earlier
derived parent failure. Old projection formats trigger a full rebuild.

The SQLite store assumes a local runtime backend is the writer authority.
Distributed workers report events to that authority; they do not concurrently
write a database over an arbitrary network filesystem. Transport and writer
leases are Phase 11 work, not an invitation for package workers to open the
database directly.

`runtime/events.runtime.jsonl` remains the compatibility stream during
migration. It will become an export/read adapter only after all package readers
use the durable API.

## Execution Ledger

The ledger is a rebuildable materialized index, not a second source of truth.
It contains:

- Runtime identity and ordered execution waves.
- Immutable process attempts grouped by process.
- Attempt operation, fingerprints, implementation version, source attempt,
  checkpoint, session owner, event bounds, status, and error.
- Last durable event sequence and projection sequence.

Appending an attempt event and changing its ledger row occur in one database
transaction. Future reconciliation must be expressible as new events and must
be able to rebuild the same ledger from an empty materialization.

`RuntimeExecutionLedgerReducer` now rebuilds executions and attempts only from
canonical events. `rebuild_ledger(replace=True)` transactionally replaces
deleted or damaged materialized rows. Missing and duplicate
`execution.created` facts are rejected instead of inferred.

## Backend Session Lease And Reconciliation

`RuntimeBackendSession` is an exclusive runtime coordination lease. Its
identity is an attachment, never the runtime or logical task identity. Acquire,
renew, release, and expiration append standard events in the same transaction
that changes the operational lease row. SQLite serializes concurrent
acquisition, and an unexpired owner prevents takeover.

After an expired lease is replaced, `reconcile_interrupted_work()` appends
`process.attempt.interrupted` and `execution.interrupted` for nonterminal work
owned by prior sessions. It does not turn backend loss into a business failure,
select retry/resume, or validate artifacts. Reconciliation is idempotent: once
the stale records are terminal, another call appends nothing.

## Durable Commands

`RuntimeCommandRecord` gives control requests a stable idempotency key. The
first reservation appends `command.accepted`; replaying an identical request
returns the same receipt, while reusing the id for a different request fails.
Terminal results append `command.succeeded` or `command.failed` and are also
idempotent. `RuntimeCommandReducer` and `rebuild_commands(replace=True)` keep
the command table rebuildable from canonical events.

This is durable request deduplication, not a claim that arbitrary external
side effects are exactly-once. A command left accepted after a crash is
resolved through process idempotency, checkpoint, cleanup, and reconciliation
contracts.

## DAG Continuation Selection

`RuntimeContinuationSelector` combines the process DAG, framework projection,
and package-supplied `RuntimeContinuationAssessment` evidence. Packages report
whether completed outputs remain valid, which committed checkpoint is usable,
and whether required cleanup ran. Flowlet computes the affected downstream
closure, skips valid ancestors and unrelated nodes, and selects
resume/retry/restart under declared capabilities and retry limits.

A continuation selector rejects a changed target `runtime_id`.
`REQUIRES_CLEANUP` retry/restart actions remain blocked until the package
explicitly confirms cleanup. Flowlet does not inspect or delete business
artifacts itself.

## Implemented Baseline

- `RuntimeIdentity`, `RuntimeExecutionKind`, `RuntimeExecutionStatus`, and
  `RuntimeExecutionRecord`.
- Optional `execution_id` on `RuntimeEvent` and `RuntimeProcessAttempt`.
- Extended attempt provenance and event-sequence fields.
- `RuntimeExecutionLedger`.
- `RuntimeDurableStore` identity verification, transactional event allocation,
  concurrent local writers, execution transitions, process specs, attempt
  materialization, and projection cursor storage.
- `RuntimeDirectoryManager` guarded create/continue/rerun operations. Reset
  keeps its lock and durable marker under `.control`, stages old contents, and
  completes an interrupted initialization on the next open.
- The reference executor propagates `execution_id`, records attempts for
  ordinary process starts as well as recovery, reuses unchanged process specs
  on continue, and writes durable process/projection materializations.
- Standard execution lifecycle event vocabulary.
- Incremental projection application with deterministic full-rebuild fallback.
- Event-only ledger rebuild and transactional materialization replacement.
- Exclusive backend-session leases with atomic stale-owner takeover.
- Append-only, idempotent interrupted execution/attempt reconciliation.
- Idempotency-keyed, event-rebuildable durable command receipts.
- Minimum affected-DAG continuation selection with an explicit cleanup gate.

Current tests prove four store instances append 40 unique events with canonical
sequences `0..39`, a reopened store preserves identity and ledger state,
continue creates execution ordinal 2 under the same runtime, identity mismatch
is rejected, and projection reads can begin after the persisted cursor. Lease
tests prove one winner among four concurrent acquirers and deterministic stale
session reconciliation. Flowlet runtime tests pass at 72 tests.

## Remaining Phases

1. Refactor the reference executor around execution waves and the durable
   store.
2. Finish the started MetaMSTools migration by connecting OpenMS run-level
   continuation to the durable root execution lineage.
3. Migrate MassLib4Search annotation runtime, run processes, and study process.
4. Switch package standard readers to the durable store, retain declared
   legacy adapters, and run crash-injection plus real-workspace acceptance.

MetaMSTools migration is now underway: new persisted jobs use the durable
store as canonical source, while `events.runtime.jsonl` remains an export for
legacy SSE readers. Root execution/attempt/session lifecycle and expired lease
reconciliation are native. Its backend can also create a continuation execution
under the same runtime and use package artifact assessments plus the Flowlet
selector/executor to skip valid OpenMS runs and execute missing runs. Full
study finalization and real-workspace acceptance remain open.

MassLib4Search root migration is also underway. A shared annotation backend
runtime now has one durable `runtime_id`; each initial/resume job becomes an
execution wave and retains its own root process/attempt identity. Recovery
plans now preserve source/target runtime identity. Annotation run and study
execution are still package-owned without native Flowlet attempt dispatch, so
that process-level migration remains open.

## Acceptance

- Continue never changes `runtime_id` or completed attempt history.
- Rerun never reuses a prior `runtime_id`, cursor, or checkpoint namespace.
- Concurrent appenders cannot allocate duplicate or out-of-order sequences.
- A crash between event and ledger writes cannot expose one without the other.
- Ledger and projection can be deleted and rebuilt identically from events.
- Backend restart produces interrupted/reconciled events instead of mutating a
  nonterminal job directly to failed.
- DAG continuation dispatches only the affected subgraph.
- Both txn packages execute through Flowlet runtime lifecycle APIs while their
  business FSMs and artifact policies remain package-owned.
