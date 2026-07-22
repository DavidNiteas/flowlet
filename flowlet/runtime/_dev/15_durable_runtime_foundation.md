# Durable Runtime Foundation

## Status

Phase 11 in progress. Stable identity contracts, the first transactional
event/ledger store, recoverable runtime-directory reset, and native executor
attempt recording are implemented. Interrupted-attempt reconciliation,
incremental reducer integration, and business backend migration remain open.

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

The baseline also stores process declarations and an incremental projection
cursor. A projection can load only events after `through_sequence`; it remains
fully rebuildable from events.

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

Current tests prove four store instances append 40 unique events with canonical
sequences `0..39`, a reopened store preserves identity and ledger state,
continue creates execution ordinal 2 under the same runtime, identity mismatch
is rejected, and projection reads can begin after the persisted cursor.

## Remaining Phases

1. Add backend-session leases around the implemented directory reset lock.
2. Add incremental projection application and deterministic ledger rebuild
   verification.
3. Add backend-session leases, interrupted-attempt reconciliation, and command
   idempotency.
4. Add generic DAG continuation selection and enforce cleanup requirements.
5. Refactor the reference executor around execution waves and the durable
   store.
6. Migrate MetaMSTools txn root and OpenMS run processes.
7. Migrate MassLib4Search annotation runtime, run processes, and study process.
8. Switch package standard readers to the durable store, retain declared
   legacy adapters, and run crash-injection plus real-workspace acceptance.

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
