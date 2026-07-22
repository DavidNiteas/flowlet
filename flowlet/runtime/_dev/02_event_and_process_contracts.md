# Event and Process Contracts

## Standard Event Contract

Flowlet should define a `RuntimeEvent` envelope. The envelope is framework-owned. Business payloads live inside `payload` and `metadata`.

Proposed fields:

```text
event_id: int | str
runtime_id: str
process_id: str | None
parent_process_id: str | None
event_type: str
timestamp: float
sequence: int | None
subject_type: str | None
subject_id: str | None
parent_subject_id: str | None
status: RuntimeEventStatus | str | None
status_class: RuntimeStatusClass | None
progress: RuntimeProgress | None
message: str | None
error: RuntimeErrorInfo | None
payload: dict
metadata: dict
causation_id: str | int | None
correlation_id: str | int | None
schema_version: int
```

The event envelope must be JSON-safe and append-only.

## Store Cursor and Streaming Contract

`RuntimeEventStore` supports numeric and string event cursors.

- Numeric cursors select events with a greater numeric event id.
- A known string cursor resumes after its last matching event in append order.
- An unknown string cursor returns no events; callers must reconnect from a
  known cursor or use an empty cursor for a full replay.

`stream_runtime_event_store(...)` yields `RuntimeEvent` objects and `None`
heartbeat markers. It has no default terminal condition: a terminal child
process is not necessarily the terminal state of a business runtime. A caller
that owns that policy supplies an explicit `is_terminal(event)` predicate.

`sse_encode_runtime_event(...)` emits the full standard event envelope as the
SSE `data` payload, using its event id and event type as frame metadata.

## Event Type Vocabulary

Flowlet should provide a recommended event type vocabulary, but allow extension strings.

`RuntimeEventType` implements this vocabulary as a `StrEnum`. It is a source
of constants for framework code and consumers, not a closed validation enum:
the envelope continues to accept business event types such as
`annotation.candidate.scored`.

Recommended framework event types:

```text
runtime.started
runtime.completed
runtime.failed
runtime.cancelled

process.created
process.started
process.progressed
process.completed
process.failed
process.cancelled
process.paused
process.resumed
process.retrying
process.cleaned_up

unit.started
unit.progressed
unit.completed
unit.failed
unit.cancelled

fsm.transition.started
fsm.transition.completed
fsm.transition.failed

artifact.produced
artifact.updated
artifact.removed

log.emitted
metric.sampled
signal.changed
stream.chunk
error.raised
```

Business packages can define additional event types, but they should still use the standard envelope.

## Status Model

Events may report status. Flowlet should provide a small framework status vocabulary:

```text
pending
running
succeeded
failed
cancelled
skipped
blocked
unknown
```

Business-specific statuses are allowed when paired with a framework-level `status_class`.

Suggested status classes:

```text
not_started
active
terminal_success
terminal_failure
terminal_cancelled
blocked
unknown
```

This lets Flowlet projections know whether something is active or terminal without understanding domain status names.

## Error Contract

Every event may carry an error.

Proposed `RuntimeErrorInfo` fields:

```text
type: str
message: str
traceback: str | None
retryable: bool | None
cause: dict | None
context: dict
```

Error propagation should support:

- Direct event error reporting.
- Process-level failure reduction from child events.
- Causation chains through `causation_id`.
- Correlation chains through `correlation_id`.

## Progress Contract

Progress is optional. When present, it should use a standard shape:

```text
current: float | int
total: float | int | None
unit: str | None
percent: float | None
description: str | None
```

Flowlet can compute percent if `current` and `total` are available.

## Subject and Process Relationship

`process_id` identifies the runtime operation unit.

`subject_id` identifies the observed thing inside that process. For many process-level events, `subject_id == process_id` is acceptable.

Examples:

```text
process_id=annotate-study
subject_id=run:Liver-1:fsm:primary_score
event_type=fsm.transition.completed
```

`parent_process_id` expresses runtime operation hierarchy.

`parent_subject_id` expresses observation tree hierarchy.

`causation_id` expresses direct cause.

`correlation_id` expresses a larger execution chain.

## Standard Process Contract

Flowlet should define a process interface. The interface is framework-owned, but behavior is implemented by business or execution backends.

Proposed process-facing types:

```text
RuntimeProcessSpec
RuntimeProcessCapabilities
RuntimeProcessContext
RuntimeProcessHandle
RuntimeProcessController
```

## RuntimeProcessSpec

The process spec should be business-neutral.

Proposed fields:

```text
process_id: str | None
process_type: str
display_name: str | None
parent_process_id: str | None
inputs: dict
metadata: dict
capabilities: RuntimeProcessCapabilities
resource_request: RuntimeResourceRequest | None
retry_policy: RuntimeRetryPolicy | None
```

Business job specs can wrap or generate process specs.

## RuntimeProcessCapabilities

Capabilities describe operations the runtime may invoke.

Proposed fields:

```text
can_start: bool
can_cancel: bool
can_pause: bool
can_resume: bool
can_retry: bool
can_cleanup: bool
can_checkpoint: bool
```

Capabilities are declarative. A capability being true means the process implementation supplies the matching hook.

## RuntimeProcess Hooks

Flowlet provides the standard hook names. Concrete implementations decide behavior.

```text
start(context) -> result
cancel(context) -> None
pause(context) -> None
resume(context) -> None
retry(context) -> result
cleanup(context) -> None
status(context) -> RuntimeProcessState
resources(context) -> RuntimeResourceUsage
```

Unsupported operations should return a standard unsupported-operation error or status.

## Resource Observation

`RuntimeResourceRequest` declares the static capacity a process asks for.
`RuntimeResourceUsage` reports one observed sample and may include CPU/GPU
percentages, memory/disk/network byte counters, labels, and free-form metadata.
Neither model performs allocation, quota enforcement, node selection, or
scheduling.

A process may provide `resources(context) -> RuntimeResourceUsage`. The
executor's `sample_process_resources(process_id)` invokes that hook and writes
a `resource.sampled` event. `RuntimeProcessContext.emit_resource_usage(...)`
is available when a process reports its own sample. The framework projection
keeps the latest value per process in both the process state and
`resource_usage_summary`.

## RuntimeProcessContext

The context is how a process talks to the runtime.

It should provide:

```text
emit(event)
emit_status(...)
emit_progress(...)
emit_error(...)
emit_artifact(...)
emit_log(...)
emit_metric(...)
emit_signal(...)
is_cancel_requested()
raise_if_cancelled()
checkpoint(...)
artifact_path(...)
```

The context does not know domain semantics. Business packages pass domain metadata through `payload` and `metadata`.

## Projections and Reducers

Flowlet should define reducer/projection interfaces:

```text
RuntimeReducer
RuntimeProjection
RuntimeState
ProcessState
```

Reducers consume events and produce framework-level state:

- Current runtime status.
- Process tree.
- Active processes.
- Completed/failed/cancelled counts.
- Error summary.
- Artifact index.
- Progress summary.
- Timeline.

Business packages can provide additional reducers for business monitor summaries.

## Standard Event Streaming

The durable standard stream is `runtime/events.runtime.jsonl`. Flowlet exposes
three framework-level helpers around that file:

- `stream_runtime_event_jsonl(...)` polls the append-only sidecar and yields
  `RuntimeEvent` objects plus `None` heartbeat markers.
- `sse_encode_runtime_event(...)` writes a complete SSE frame whose `id` is the
  standard event id, whose event name is `event_type`, and whose data is the
  full schema-versioned event JSON.
- `parse_sse_runtime_events(...)` restores full standard event frames for an
  HTTP client without reconstructing data from event names.

An event stream must receive an explicit `is_terminal(event)` predicate. A
terminal child process does not necessarily terminate the enclosing runtime.
After a match, the helper drains events already appended to the same sidecar
snapshot, then ends the iterator. Missing sidecars are valid for a newly
created live runtime and produce heartbeats until an event appears or the
caller-selected idle timeout expires.
