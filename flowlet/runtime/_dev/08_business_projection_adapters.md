# Business Projection Adapter Design

## Purpose

`RuntimeProjection` is deliberately framework-only. It records runtime and
process state, but it does not contain the business `JobRecord` required by
the existing MetaMSTools and MassLib4Search `JobMonitorSnapshot` models.

The reader migration must therefore combine two inputs:

```text
RuntimeProjection                 framework status, process counts, progress
JobSnapshot or JobRecord          business identity, paths, result, options
                 \               /
                  business adapter
                        |
                JobMonitorSnapshot
```

Flowlet owns the first input and the projection-aware reader seam. Each
business package owns the adapter and its monitor schema.

## Reader Contract

For a fresh runtime that has `runtime/projection.json`:

1. Load the standard projection.
2. Load legacy `snapshot.json` when present, otherwise `status.json`, to
   recover the business `JobRecord`.
3. The business CLI/TUI adapter may build its monitor from the projection plus
   that record.
4. If no valid business record is available, preserve the existing legacy
   fallback rather than fabricating a record from framework fields.

This keeps the current CLI JSON contract intact: `runtime_info`, `monitor`,
and `snapshot` remain present. The projection can be exposed as an additional
field only when the relevant CLI format is intentionally extended.

`RuntimeSnapshotView.process_specs` provides an additional optional framework
observation input. It is loaded from `runtime/processes.json` when present and
is empty for historical directories. Business readers may display declared
capabilities or resource requests, but must not treat the manifest as a
serialized implementation, a resume checkpoint, or a source of run/stage/FSM
identity.

The same restriction applies to live UI: standard HTTP events may be consumed
as an additional observability feed, but existing TUI/GUI monitor rendering
continues to use business snapshots and legacy business events. A UI migration
requires a package-owned presentation adapter; it must not infer runs, stages,
or annotation state from framework process ids.

## MetaMSTools Adapter

Inputs:

- `RuntimeProjection`
- `JobSnapshot.job`, or `JobRecord` from `status.json`
- optional existing `JobSnapshot.runs` and `runtime_tasks`

Required mapping:

- `projection.terminal_success_count` -> completed process count only when
  the chosen process hierarchy represents runnable units.
- `projection.terminal_failure_count` -> failed count.
- `projection.terminal_cancelled_count` -> cancelled count.
- `projection.active_process_ids` -> running process count.
- `projection.progress_summary` -> optional `RuntimeTaskSnapshot` updates.

Do not infer `RunSnapshot` identifiers or OpenMS stage names from arbitrary
process ids. Preserve `JobSnapshot.runs` when it exists; run/stage grouping is
MetaMSTools-owned.

## MassLib4Search Adapter

Inputs:

- `RuntimeProjection`
- `JobSnapshot.job`, or `JobRecord` from `status.json`
- optional existing `JobSnapshot.runtime_tasks`

Required mapping:

- Use the projection only for aggregate lifecycle and active-process state.
- Keep annotation id, study path, result directories, resume policy, and FSM
  stage interpretation in MassLib4Search.
- Preserve business `runtime_tasks` when a legacy snapshot exists. Mapping an
  arbitrary process id to an annotation run, artifact, or FSM stage is a
  MassLib4Search decision.

## Implementation Sequence

1. Completed: each CLI's private `_load_runtime_snapshot_view` retains
   `view.projection` from Flowlet.
2. Completed: each package has a private adapter that receives a projection
   plus a known `JobRecord`/`JobSnapshot`.
3. Completed: the adapter is used only when both inputs are valid; otherwise
   the current monitor/snapshot/status result remains in place.
4. Completed: CLI tests construct a temporary runtime with a legacy record and
   `runtime/projection.json`, and verify projected lifecycle precedence.
5. Ongoing regression: re-run the real liver snapshot commands. Historical
   directories currently lack projections and must continue to use fallback
   behavior.

## Acceptance

- Flowlet has no imports from MetaMSTools or MassLib4Search.
- A projection-first CLI result still validates against the package's existing
  `JobMonitorSnapshot` model.
- Legacy runtime directories remain readable without a projection.
- The standard projection is never treated as a source of business paths,
  result payloads, workspace semantics, or resume state.
