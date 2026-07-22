# Boundary Audit

Status: Phase 0 completed.

This audit classifies the current `flowlet.runtime` APIs against the redesigned runtime model. The purpose is to keep Flowlet focused on framework-level runtime concepts and prevent accidental ownership of business semantics from MetaMSTools or MassLib4Search.

## Classification Legend

- Framework-stable: belongs in Flowlet as a generic runtime framework API.
- Compatibility helper: acceptable during migration, but should not become the central model.
- Candidate for redesign: useful today, but should be reshaped around standard events and processes.
- Business-owned: should remain outside Flowlet.

## Current API Classification

### Runtime Layout and IO

`RuntimeFileLayout`

- Classification: framework-stable.
- Reason: standard runtime file layout is a framework IO contract.
- Notes: future process-oriented files should be added without breaking current layout.

`RuntimeInfo`

- Classification: framework-stable.
- Reason: runtime/execution metadata is business-neutral.
- Notes: should eventually include `runtime_id` and optional process root identifiers.

`runtime_info_payload`

- Classification: compatibility helper.
- Reason: helps current business backends write `runtime_info.json`.
- Direction: keep, but avoid adding business fields. Future API should prefer constructing `RuntimeInfo` directly.

`RuntimeStore`

- Classification: framework-stable.
- Reason: standard writes for runtime files are framework-level.
- Notes: can later add event-store and projection-store helpers.

`list_runtime_artifacts`

- Classification: framework-stable.
- Reason: artifact enumeration under a runtime directory is business-neutral.

### Event Buffer and Snapshot Loader

`EventBuffer`

- Classification: compatibility helper.
- Reason: it is generic and reusable, but its current interface is modeled after legacy buffered events.
- Direction: keep during migration. The canonical model should become `RuntimeEventStore`.

`sse_encode_event`

- Classification: framework-stable.
- Reason: SSE frame encoding is protocol-level and does not define business routes.
- Notes: should eventually accept `RuntimeEvent` directly.

`RuntimeSnapshotLoader`

- Classification: compatibility helper.
- Reason: it loads current snapshot/status/monitor files with business validators.
- Direction: keep as compatibility reader. New projections should be event-derived.

`RuntimeSnapshotView`

- Classification: compatibility helper.
- Reason: useful wrapper for current readers; not the canonical runtime state model.

### Runtime Managers

`RuntimeManagerBundle`

- Classification: framework-stable.
- Reason: bundle of Flowlet managers is a framework runtime concept.
- Notes: should become one possible event source for `RuntimeEvent`.

`export_runtime_manager_files`

- Classification: compatibility helper.
- Reason: writes current projection files from Flowlet managers.
- Direction: keep until projections are event-derived. Do not add business semantics.

`mirror_runtime_events`

- Classification: candidate for redesign.
- Reason: currently mirrors manager records into legacy event buffers.
- Direction: replace or supplement with manager-to-`RuntimeEvent` conversion.

`model_dump_json_safe`

- Classification: framework-stable.
- Reason: JSON-safe dumping is generic runtime infrastructure.

### Backend Lifecycle Support

`TERMINAL_JOB_STATUSES`

- Classification: candidate for redesign.
- Reason: status categories are framework-level, but the name `JOB` is too business-shaped.
- Direction: replace with process/runtime status classes. Keep only as compatibility constant until migrated.

`NON_TERMINAL_JOB_STATUSES`

- Classification: candidate for redesign.
- Reason: same as above.
- Direction: replace with `RuntimeStatusClass` and process state helpers.

`FlowletJobBackendBase`

- Classification: compatibility helper.
- Reason: generic state container is useful, but `JobBackend` naming is too close to business backend vocabulary.
- Direction: avoid expanding it. Future implementation should introduce `RuntimeBackend` and `RuntimeProcess` concepts.

`new_mirror_offsets`

- Classification: compatibility helper.
- Reason: tied to current manager mirroring implementation.
- Direction: replace with event-store offsets or projection cursor state.

`wait_runtime_events`

- Classification: compatibility helper.
- Reason: wait loops are framework-level.
- Direction: keep for legacy buffers. `wait_runtime_event_store` is now the
  standard-store counterpart.

`stream_runtime_events`

- Classification: compatibility helper.
- Reason: event streaming is framework-level.
- Direction: keep legacy `job_state` handling in the compatibility helper.
  `stream_runtime_event_store` is the standard counterpart and requires an
  explicit business terminal predicate rather than guessing from a process event.

`stream_runtime_event_jsonl`, `sse_encode_runtime_event`, and `parse_sse_runtime_events`

- Classification: stable framework runtime transport API.
- Reason: they only move the standard `RuntimeEvent` envelope across durable
  JSONL and SSE boundaries; they do not interpret business event payloads.
- Direction: business HTTP clients may adopt them alongside legacy event APIs.

`write_runtime_status`

- Classification: compatibility helper.
- Reason: writes current status projection.
- Direction: future status should be event-derived projection.

## Boundary Decision

No current API needs to be moved back immediately. However, several APIs must not be expanded:

- `FlowletJobBackendBase`
- `TERMINAL_JOB_STATUSES`
- `NON_TERMINAL_JOB_STATUSES`
- `mirror_runtime_events`
- `new_mirror_offsets`
- `write_runtime_status`

These APIs are migration support. The next stable layer should be:

- `RuntimeEvent`
- `RuntimeEventStore`
- `RuntimeProcess`
- `RuntimeProcessContext`
- `RuntimeProjection`
- `RuntimeBackend`

## Immediate Next Work

Phase 1 starts by adding standard event schemas:

- `RuntimeEvent`
- `RuntimeEventStatus`
- `RuntimeStatusClass`
- `RuntimeErrorInfo`
- `RuntimeProgress`

These types should be framework-stable and must not mention MetaMSTools, MassLib4Search, OpenMS, annotation, study, workspace, or job-specific business fields.
