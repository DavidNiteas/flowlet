# Compatibility with MetaMSTools and MassLib4Search

## Compatibility Goal

The new runtime model must be introduced without breaking the existing MetaMSTools and MassLib4Search runtime backends.

Both systems currently expose:

- Runtime directory layout.
- `runtime_info.json`.
- `status.json`.
- `job_spec.json`.
- `events.jsonl`.
- `snapshot.json`.
- `monitor_snapshot.json`.
- `runtime/progress.json`.
- `runtime/signals.json`.
- `runtime/logs.jsonl`.
- `runtime/telemetry.jsonl`.
- CLI/TUI `runtime-snapshot` readers.
- Backend lifecycle APIs.

The migration must preserve these interfaces until downstream consumers have moved to the new event/process model.

## Current Common Mechanisms

The following mechanisms have already been moved into Flowlet:

- `RuntimeFileLayout`
- `RuntimeInfo`
- `RuntimeStore`
- `list_runtime_artifacts`
- `EventBuffer`
- `RuntimeSnapshotLoader`
- `RuntimeManagerBundle`
- backend lifecycle support helpers

These are compatibility foundations, not the final model.

## Current Business-Owned Semantics

MetaMSTools still owns:

- OpenMS config and presets.
- OpenMS analysis execution.
- `JobSpec`, `JobRecord`, `JobSnapshot`, `JobMonitorSnapshot`.
- Run/stage monitor summary logic.
- `.metams` workspace semantics.
- CLI/TUI business rendering.

MassLib4Search still owns:

- Annotation/search config and presets.
- Search library build execution.
- Annotation resume policy.
- `JobSpec`, `JobRecord`, `JobSnapshot`, `JobMonitorSnapshot`.
- Annotation monitor summary logic.
- `.annotation` workspace semantics.
- CLI/TUI business rendering.

These should remain outside Flowlet.

## Compatibility Mapping

Current `TxnEvent` can be adapted to `RuntimeEvent`.

Current fields:

```text
event_id
job_id
timestamp
event_type
payload
```

Mapping:

```text
RuntimeEvent.event_id       <- TxnEvent.event_id
RuntimeEvent.runtime_id     <- runtime_info.job_id or runtime directory id
RuntimeEvent.process_id     <- TxnEvent.job_id
RuntimeEvent.timestamp      <- TxnEvent.timestamp
RuntimeEvent.event_type     <- mapped event_type
RuntimeEvent.payload        <- TxnEvent.payload
RuntimeEvent.metadata       <- compatibility metadata
RuntimeEvent.schema_version <- 1
```

Current event types can map as:

```text
job_state  -> process.status.changed
progress   -> process.progressed or unit.progressed
log        -> log.emitted
stream     -> stream.chunk
telemetry  -> metric.sampled
signal     -> signal.changed
artifact   -> artifact.produced
error      -> error.raised
result     -> process.completed
```

This mapping should be implemented as an adapter, not by changing business code all at once.

## Runtime Manager Event Mapping

Current Flowlet managers produce progress, signal, log, stream, and telemetry records.

The new model should define these as standard runtime events:

```text
ProgressManager update -> unit.progressed or process.progressed
SignalPool update      -> signal.changed
LogManager record      -> log.emitted
StreamManager chunk    -> stream.chunk
TelemetryManager event -> metric.sampled
```

Existing projection files should continue to be emitted.

## Process Mapping: MetaMSTools

Possible process hierarchy:

```text
process: metams.openms.analysis
  process: run:Liver-1
    process or subject: load
    process or subject: preprocess
    process or subject: feature_find
    process or subject: interpret
    process or subject: align
    process or subject: convert
    process or subject: export_artifacts
```

OpenMS stages may begin as subjects within a process. Later they can become child processes if cancellation/retry/resource declaration is needed.

Compatibility rule:

- Keep existing `JobSnapshot.runs` and `JobMonitorSnapshot.runs`.
- Add runtime event generation behind the existing backend.
- Business monitor reducers can consume standard events but still output current schema.

## Process Mapping: MassLib4Search

Possible process hierarchy:

```text
process: annotation.search
  process: annotation_study
    process: run:Liver-1
      process or subject: prepare_runtime
      process or subject: pre_filter
      process or subject: pair_candidates
      process or subject: primary_score
      process or subject: final_score
      process or subject: synthesize_ms2
      process or subject: synthesize_feature
      process or subject: assemble_result
      process or subject: persist_run_result

process: search_lib.build
```

Resume remains business-owned. Flowlet should only expose process retry/restart hooks when MassLib4Search explicitly implements them.

Compatibility rule:

- Keep annotation-specific result paths and workspace semantics in MassLib4Search.
- Represent business steps as process metadata, not Flowlet primitives.
- Allow MassLib4Search to provide business reducers for annotation monitor summaries.

## Compatibility Phases

### Phase A: Dual Event Emission

Existing `TxnEvent` stays unchanged.

Flowlet adds `RuntimeEvent` and adapters:

- `TxnEvent -> RuntimeEvent`
- `RuntimeEvent -> TxnEvent` when needed for existing API compatibility.

### Phase B: RuntimeEvent Store as Canonical Sidecar

Keep current `events.jsonl` format until readers are migrated.

Add one of:

- `runtime/events.runtime.jsonl`, or
- versioned event envelope in existing `events.jsonl` with backward-compatible loader.

Decision must be made before implementation. The safer route is a sidecar file first.

### Phase C: Projection Reducers

Introduce reducers that produce:

- Current status projection.
- Current progress/signal/log/telemetry projection.
- Current monitor snapshot compatibility projection.

### Phase D: Business Backend Migration

MetaMSTools and MassLib4Search start using:

- `RuntimeProcessContext` for event reporting.
- `RuntimeProcessSpec` for business operation registration.
- Business adapters for current `JobSpec` and `JobRecord`.

### Phase E: Cleanup Legacy Event APIs

Only after CLI/TUI/API readers support RuntimeEvent:

- Deprecate direct `TxnEvent` as canonical storage.
- Keep compatibility readers for old runtime directories.

## Compatibility Acceptance Requirements

Every phase must pass:

- Existing MetaMSTools backend tests.
- Existing MassLib4Search backend tests.
- Existing CLI runtime snapshot tests.
- Real sample runtime snapshot checks:

```text
data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.metams/runtime
data/large_files/ms_exp_datas/900_human_metabolites_db/workspace/900_human_metabolites_liver/.annotation/spec_spec_unispec_pos/runtime
```

Expected:

- `runtime-snapshot print --format json` still includes `runtime_info`, `monitor`, and `snapshot`.
- Existing annotation outputs remain under the study workspace.
- Existing `.metams` and `.annotation` runtime layouts remain readable.

