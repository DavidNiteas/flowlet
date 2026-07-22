# Recoverable Process Runtime

## Status

Phase 10 design baseline and implementation handoff. The first schema increment
is implemented; planner, reducer, executor, and package adapters remain staged
work described below.

## Objective

Turn a standard process into the minimum recoverable execution unit. Given
process declarations, dependency edges, historical events, attempts,
checkpoints, and artifacts, Flowlet can produce a reviewable local recovery
plan that skips valid completed work and selects retry, restart, resume, or
blocked actions for the rest.

Flowlet does not decide whether an OpenMS run shard or annotation result is
scientifically reusable. A package validator supplies that decision and
reconstructs the executable business object.

## Boundary

Flowlet owns:

- Process dependency and execution-identity fields.
- Attempt and checkpoint reference envelopes.
- Recovery action, decision, step, and plan schemas.
- Dependency-graph validation and deterministic framework eligibility rules.
- Append-only recovery events, projections, plan persistence, and execution
  dispatch interfaces.

Business packages own:

- Stable input fingerprints and implementation-version values.
- Validation of checkpoint payloads and output artifacts.
- Construction of FSMs, Ray tasks, OpenMS actions, or search executors.
- Cleanup, compensation, transaction, and idempotency behavior.
- The final decision that a domain result can be skipped or resumed.

Telemetry checkpoints remain observations. They become recoverable only when a
package creates a `RuntimeCheckpointRef` and validates its referenced state.

## Identity Model

- `process_id` identifies the logical process inside a runtime graph.
- `execution_key` identifies equivalent logical work across runtimes.
- `attempt_id` identifies one immutable execution attempt.
- `runtime_id` identifies the event and projection namespace.
- `input_fingerprint` and `implementation_version` determine whether previous
  output is a candidate for reuse.

Retry never rewrites a failed attempt. It creates another attempt for the same
logical process. Cross-runtime recovery points back to a source runtime and
attempt while writing new events into the target runtime.

## Dependency Model

`parent_process_id` is an observation hierarchy edge. `depends_on` is an
execution dependency edge. They are independent. The planner must reject
unknown dependencies, self-dependencies, and cycles before producing a plan.

When an upstream process restarts, dependent downstream results are invalid by
default. A package validator may prove them reusable using fingerprints and
artifact contracts; Flowlet does not infer that proof.

## Recovery Actions

| Action | Meaning |
| --- | --- |
| `skip` | Reuse a validated terminal-success attempt and its outputs. |
| `resume` | Continue from a validated recoverable checkpoint. |
| `retry` | Create another attempt under the declared retry policy. |
| `restart` | Discard prior execution position and start a new attempt. |
| `block` | Do not execute because dependencies or business validation failed. |

A `resume` decision must include a checkpoint. `skip` requires package proof
that fingerprints and declared outputs remain valid. Non-idempotent work cannot
be retried automatically unless the package provides cleanup or compensation.

## Streaming Position

`RuntimeCheckpointRef.cursor` is an opaque JSON object. A package defines
offset, watermark, committed-output, and delivery semantics. Flowlet stores and
transports the cursor but does not compare domain positions or promise
exactly-once execution.

## Compatibility Audit

MetaMSTools already persists completed run shards under `.metams/streaming` and
validates source/config/reference fingerprints before reuse. This is the first
candidate package validator. Existing run status and lightweight artifact files
remain authoritative business data; the standard checkpoint references them.

MassLib4Search already creates a new annotation job with `resume` policy,
loads completed run results, validates a resume-safety manifest, and schedules
only pending run items. This is the second candidate validator. Flowlet should
describe those run decisions as a plan without replacing annotation output
policy or manifest semantics.

Neither package currently persists standard process attempts or recovery
plans. Existing job-level root process declarations remain valid because all
new fields are optional or have backward-compatible defaults.

## Development Sequence

1. Completed: schema baseline for dependencies, execution identity,
   idempotency, checkpoint policy, attempts, checkpoint references, decisions,
   and plans.
2. Completed: graph validation, deterministic topological ordering, and
   transitive downstream invalidation closure.
3. Completed: framework projection retains immutable attempt history and only
   indexes explicitly committed, structurally valid recoverable checkpoints.
4. Completed: planner combines complete package-supplied decisions with hard
   framework eligibility checks and persists `runtime/recovery_plans/*.json`
   before execution.
5. Completed: local recovery executor persists an unblocked plan, emits
   recovery and immutable attempt events, dispatches registered package hooks,
   and never reconstructs business implementations from manifests.
   `RuntimeProcessContext.commit_checkpoint()` and `invalidate_checkpoint()`
   are the only framework helpers that add/remove recoverable checkpoint
   references; the legacy `checkpoint()` helper remains telemetry-only.
6. Add MetaMSTools and MassLib4Search adapters and package-level tests.
7. Run isolated real-liver skip, failure/retry, and checkpoint-resume cases.

## Acceptance

- Old process manifests and sidecars still validate unchanged.
- Cyclic or missing dependencies cannot produce an executable plan.
- Failed attempts remain in history after retry succeeds.
- Resume requires a package-validated checkpoint.
- Upstream restart blocks or invalidates downstream work by default.
- Recovery plans are persisted before execution and can be reviewed without
  importing either business package.
- Both package adapters preserve their current workspace and output behavior.
- Fresh isolated liver regressions prove skip, retry, and available-checkpoint
  continuation with numerical/business outputs still readable.
